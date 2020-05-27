use std::sync::Arc;
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};
use std::thread;
use std::time::Duration;

use log::{debug, info, warn, error};
use chrono::prelude::Utc;

use backends::KVStore;
use db::{Db, DbMetadata};
use schema::{Schema, ValueType};
use {Tx, TxReport, Entity, Record, Value, TxItem, Result, Fact};

pub struct Transactor {
    next_id: i64,
    current_db: Db,
    store: Arc<dyn KVStore>,
    latest_tx: i64,
    last_indexed_tx: i64,

    /// Interactions with a running transactor happen over an event
    /// channel.
    recv: Receiver<Event>,
    send: Sender<Event>,

    /// While asynchronously rebuilding the durable indices, it's
    /// necessary to keep track of transactions which will need to be
    /// added to the rebuilt indices' in-memory trees before swapping
    /// over.
    catchup_txs: Option<Vec<TxRaw>>,
    throttled: bool,
}

/// Represents any input that might need to be given to a
/// running transactor. Usually this would be a transaction to
/// process, but any other interrupts that require linearization
/// (e.g. swapping over to a new index) would be delivered through the
/// same channel.
enum Event {
    Tx(Tx, Sender<TxReport>),
    RebuiltIndex(Db),
    Stop,
}

/// TxHandle is a wrapper over Transactor that provides a thread-safe
/// interface for submitting transactions and receiving their results,
/// abstracting away the implementation of the thread-safety.
#[derive(Clone)]
pub struct TxHandle {
    chan: Sender<Event>,
}

impl TxHandle {
    pub fn new(transactor: &Transactor) -> TxHandle {
        let chan = transactor.send.clone();

        TxHandle { chan }
    }

    pub fn transact(&self, tx: Tx) -> Result<TxReport> {
        let (report_send, report_recv) = mpsc::channel();
        self.chan.send(Event::Tx(tx, report_send))?;
        match report_recv.recv() {
            Ok(report) => Ok(report),
            Err(msg) => Err(msg.into()),
        }
    }

    pub fn close(&self) -> Result<()>{
        Ok(self.chan.send(Event::Stop)?)
    }
}

#[derive(Clone, Debug)]
pub struct TxRaw {
    pub id: i64,
    pub records: Vec<Record>,
}

impl Transactor {
    /// Creates a transactor by retrieving the database metadata from
    /// the store (if it exists already) or creating the metadata for
    /// a new database (if no metadata is present in the store).
    pub fn new(store: Arc<dyn KVStore>) -> Result<Transactor> {
        let (send, recv) = mpsc::channel();

        match store.get_metadata() {
            Ok(metadata) => {
                let mut next_id = metadata.next_id;
                let last_id = metadata.last_indexed_tx;
                let mut latest_tx = last_id;
                let mut db = Db::new(metadata, store.clone());
                let novelty = store.get_txs(last_id)?;
                for tx in novelty {
                    for record in tx.records {
                        let Entity(e) = record.entity;
                        if e > next_id {
                            next_id = e + 1;
                        }
                        db = db.add_record(record)?;
                    }

                    latest_tx = tx.id;
                }

                Ok(Transactor {
                    next_id,
                    store: store.clone(),
                    latest_tx: latest_tx,
                    last_indexed_tx: last_id,
                    current_db: db,
                    send,
                    recv,
                    catchup_txs: None,
                    throttled: false,
                })
            }
            // FIXME: this should happen if metadata is None, not on error
            Err(_) => {
                let (current_db, next_id) = create_db(store.clone())?;
                let mut tx = Transactor {
                    next_id,
                    store: store,
                    latest_tx: 0,
                    last_indexed_tx: -1,
                    current_db,
                    send,
                    recv,
                    catchup_txs: None,
                    throttled: false,
                };

                save_metadata(&tx.current_db, tx.next_id, tx.last_indexed_tx)?;

                // We need to persist the bootstrapping data because
                // it's not in the transaction log.
                // FIXME: unwind this from the channel communication code
                // which isn't needed here
                tx.rebuild_indices();
                match tx.recv.recv().unwrap() {
                    Event::RebuiltIndex(new_db) => {
                        tx.switch_to_rebuilt_indexes(new_db)?;
                    },
                    // no one can send messages on this channel before
                    // we return the transactor, so the only message
                    // that can arrive is the one notifying that the
                    // rebuild is complete
                    _ => unreachable!()
                }
                Ok(tx)
            }
        }
    }

    /// Builds a new set of durable indices by combining the existing
    /// durable indices and the in-memory indices.
    fn rebuild_indices(&mut self) -> () {
        info!("Rebuilding indices...");
        let checkpoint = self.current_db.clone();
        let send = self.send.clone();
        self.catchup_txs = Some(Vec::new());

        thread::spawn(move || {
            let Db {
                eav,
                ave,
                aev,
                vae,
                ..
            } = checkpoint;

            let new_ave_handle = thread::spawn(move || ave.rebuild());
            let new_aev_handle = thread::spawn(move || aev.rebuild());
            let new_vae_handle = thread::spawn(move || vae.rebuild());
            let new_eav = eav.rebuild();
            let new_ave = new_ave_handle.join().unwrap();
            let new_aev = new_aev_handle.join().unwrap();
            let new_vae = new_vae_handle.join().unwrap();

            send.send(Event::RebuiltIndex(Db {
                eav: new_eav,
                ave: new_ave,
                aev: new_aev,
                vae: new_vae,
                schema: checkpoint.schema.clone(),
                store: checkpoint.store.clone(),
            }))
        });
    }

    fn switch_to_rebuilt_indexes(&mut self, new_db: Db) -> Result<()> {
        // First, replay the catchup transactions into the new DB.
        // (This function should never be called when catchup_txs is
        // None.)
        //
        // FIXME: this part should still happen asynchronously,
        // because it might take a while (Really what would be better
        // is to maintain an extra in-memory tree of the new facts as
        // they are added and then just swap that in, but that would
        // require some big changes to the index api exposing its
        // externals. Worthwhile?)
        info!("Replaying {} transactions on rebuilt indices...", self.catchup_txs.as_ref().map_or(0, |v| v.len()));
        let mut final_db = new_db;
        let catchup_txs = std::mem::replace(&mut self.catchup_txs, None);
        for tx in catchup_txs.unwrap() {
            for rec in tx.records {
                final_db = final_db.add_record(rec)?;
            }
        }

        info!("Switching over to rebuilt indices.");
        save_metadata(&final_db, self.next_id, self.latest_tx)?;
        self.current_db = final_db;

        // If the mem index filled up during the rebuild, we need to
        // immediately kick off another.
        if self.throttled {
            self.rebuild_indices();
            info!("Unthrottling.");
            self.throttled = false;
        }

        Ok(())
    }

    fn process_tx(&mut self, tx: Tx) -> Result<Vec<Entity>> {
        debug!("processing tx {:?}", tx);
        let mut new_entities = vec![];
        let tx_id = self.get_id();
        let tx_entity = Entity(tx_id);
        let mut raw_tx = TxRaw {
            id: tx_id,
            records: vec![],
        };

        // This is a macro and not a helper function or closure
        // because it's inconvenient to mutably borrow raw_tx and then
        // drop it in time.
        macro_rules! add {
            ( $db:expr, $e:expr, $a: expr, $v:expr, $tx:expr ) => {
                {
                    let (nextdb, record) = $db.add(Fact::new($e, $a, $v), $tx)?;
                    raw_tx.records.push(record);
                    nextdb
                }
            }
        }

        let tx_timestamp = Value::Timestamp(Utc::now());
        let mut db_after = add!(&self.current_db, tx_entity, "db:txTimestamp".to_string(), tx_timestamp, tx_entity);
        for item in tx.items {
            match item {
                TxItem::Addition(f) => {
                    db_after = add!(&db_after, f.entity, f.attribute, f.value, tx_entity);
                }
                TxItem::NewEntity(ht) => {
                    let entity = Entity(self.get_id());
                    for (k, v) in ht {
                        db_after = add!(&db_after, entity, k, v, tx_entity);
                    }
                    new_entities.push(entity);
                }
                TxItem::Retraction(f) => {
                    let (nextdb, record) = db_after.retract(Fact::new(f.entity, f.attribute, f.value), tx_entity)?;
                    db_after = nextdb;
                    raw_tx.records.push(record);
                }
            }
        }

        // FIXME: Race condition. If adding the tx completes but
        // saving the metadata does not, the tx log will be polluted.
        self.store.add_tx(&raw_tx)?;
        self.latest_tx = raw_tx.id;
        if let Some(txs) = self.catchup_txs.as_mut() {
            txs.push(raw_tx.clone());
        }

        save_metadata(&db_after, self.next_id, self.last_indexed_tx)?;
        self.current_db = db_after;

        if self.current_db.mem_index_size() > 100_000 {
            match self.catchup_txs {
                Some(_) => {
                    if !self.throttled && self.current_db.mem_index_size() > 1_000_000 {
                        warn!(
                            "Mem limit high water mark surpassed during reindexing -- throttling transactions."
                        );
                        self.throttled = true;
                    }
                }
                None => self.rebuild_indices(),
            }
        }

        if self.throttled {
            debug!("throttled - sleeping");
            thread::sleep(Duration::from_millis(1000));
        }

        Ok(new_entities)
    }

    fn get_id(&mut self) -> i64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Runs the transactor, listening on an MPSC channel for
    /// transactions and other events.
    pub fn run(&mut self) -> Result<()> {
        loop {
            match self.recv.recv().unwrap() {
                Event::Tx(tx, cb_chan) => {
                    // TODO: check for more txs & batch them.
                    // Ignoring the result because it's not important
                    // for correctness whether or not the client
                    // receives the response.
                    let _ = match self.process_tx(tx) {
                        Ok(new_entities) => cb_chan.send(TxReport::Success { new_entities }),
                        Err(e) => cb_chan.send(TxReport::Failure(format!("{:?}", e)))
                    };
                }
                Event::RebuiltIndex(new_db) => {
                    self.switch_to_rebuilt_indexes(new_db)?;
                },
                Event::Stop => break
            }
        }

        Ok(())
    }
}

/// Saves the db metadata (index root nodes, entity ID state) to
/// storage, when implemented by the storage backend (i.e. when
/// not using in-memory storage).
fn save_metadata(db: &Db, next_id: i64, last_indexed_tx: i64) -> Result<()> {
    let metadata = DbMetadata {
        next_id,
        last_indexed_tx,
        schema: db.schema.clone(),
        eav: db.eav.durable_root(),
        aev: db.aev.durable_root(),
        ave: db.ave.durable_root(),
        vae: db.vae.durable_root(),
    };

    db.store.set_metadata(&metadata)?;
    Ok(())
}

fn create_db(store: Arc<dyn KVStore>) -> Result<(Db, i64)> {
    use {EAVT, AVET, VAET, AEVT};
    use durable_tree;

    let eav_root = durable_tree::DurableTree::create(store.clone(), EAVT)?.root;
    let ave_root = durable_tree::DurableTree::create(store.clone(), AVET)?.root;
    let aev_root = durable_tree::DurableTree::create(store.clone(), AEVT)?.root;
    let vae_root = durable_tree::DurableTree::create(store.clone(), VAET)?.root;

    let mut next_id = 0;
    let mut get_next_id = || {
        let result = next_id;
        next_id += 1;
        return result;
    };

    let metadata = DbMetadata {
        next_id: 0,
        last_indexed_tx: 0,
        schema: Schema::empty(),
        eav: eav_root,
        ave: ave_root,
        aev: aev_root,
        vae: vae_root,
    };

    let idents = &[
        "db:ident",
        "db:txTimestamp",
        "db:valueType",
        "db:indexed",
        "db:type:ident",
        "db:type:string",
        "db:type:timestamp",
        "db:type:ref",
        "db:type:boolean",
    ];

    let value_types = &[
        ("db:ident", "db:type:ident"),
        ("db:valueType", "db:type:ident"),
        ("db:txTimestamp", "db:type:timestamp"),
        ("db:indexed", "db:type:boolean"),
    ];

    let initial_tx_entity = Entity(get_next_id());
    let ident_entities = idents.iter().map(|i| (i, Entity(get_next_id()))).collect::<Vec<_>>();

    let mut db = Db::new(metadata, store);

    for (name, entity) in ident_entities.iter() {
        db.schema = db.schema.add_ident(*entity, name.to_string());
    }

    let entity_for_ident = |name| {
        let (i, e) = ident_entities.iter().find(|(i, e)| *i == name).unwrap();
        *e
    };

    for (name, valueType) in value_types {
        db.schema = db.schema.add_ident(entity_for_ident(name), valueType.to_string());
    }

    // Add entity for initial transaction
    let timestamp_ident_entity = entity_for_ident(&"db:txTimestamp");
    let mut facts = vec![(initial_tx_entity, timestamp_ident_entity, Value::Timestamp(Utc::now()))];

    // Add all the idents
    for name in idents {
        facts.push((entity_for_ident(name), entity_for_ident(&"db:ident"), Value::Ident((*name).into())))
    }

    // Add all the value types
    for (name, value_type) in value_types {
        facts.push((entity_for_ident(name), entity_for_ident(&"db:valueType"), Value::Ident((*value_type).into())));
    }

    db = facts.into_iter().fold(db, move |db, (e, a, v)| {
        db.add_record(Record::addition(e, a, v, initial_tx_entity)).unwrap()
    });

    Ok((db, get_next_id()))
}
