use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};
use std::thread;
use std::time::Duration;

use chrono::prelude::Utc;

use backends::KVStore;
use db::{Db, DbContents, ValueType};
use {Tx, TxReport, Entity, Record, Value, TxItem, Result, IdentMap, Fact};

pub struct Transactor {
    next_id: i64,
    current_db: Db,
    store: Arc<KVStore>,
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
    pub fn new(store: Arc<KVStore>) -> Result<Transactor> {
        let (send, recv) = mpsc::channel();

        match store.get_contents() {
            Ok(contents) => {
                let mut next_id = contents.next_id;
                let last_id = contents.last_indexed_tx;
                let mut latest_tx = last_id;
                let mut db = Db::new(contents, store.clone());
                let novelty = store.get_txs(last_id)?;
                for tx in novelty {
                    for record in tx.records {
                        let Entity(e) = record.entity;
                        if e > next_id {
                            next_id = e + 1;
                        }
                        db = db.add(record)?;
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
            // FIXME: this should happen if contents is None, not on error
            Err(_) => {
                let mut tx = Transactor {
                    next_id: 8,
                    store: store.clone(),
                    latest_tx: 0,
                    last_indexed_tx: -1,
                    current_db: create_db(store)?,
                    send,
                    recv,
                    catchup_txs: None,
                    throttled: false,
                };

                save_contents(&tx.current_db, tx.next_id, tx.last_indexed_tx)?;
                // FIXME: Is this necessary?
                tx.rebuild_indices()?;
                Ok(tx)
            }
        }
    }

    /// Builds a new set of durable indices by combining the existing
    /// durable indices and the in-memory indices.
    pub fn rebuild_indices(&mut self) -> Result<()> {
        println!("Rebuilding indices...");
        let checkpoint = self.current_db.clone();
        let send = self.send.clone();
        self.catchup_txs = Some(Vec::new());

        thread::spawn(move || {
            let Db {
                ref eav,
                ref ave,
                ref aev,
                ref vae,
                ..
            } = checkpoint;

            let new_eav = eav.rebuild();
            let new_ave = ave.rebuild();
            let new_aev = aev.rebuild();
            let new_vae = vae.rebuild();

            send.send(Event::RebuiltIndex(Db {
                eav: new_eav,
                ave: new_ave,
                aev: new_aev,
                vae: new_vae,
                idents: checkpoint.idents.clone(),
                schema: checkpoint.schema.clone(),
                store: checkpoint.store.clone(),
            }))
        });

        Ok(())
    }

    fn switch_to_rebuilt_indexes(&mut self, new_db: Db) -> Result<()> {
        // First, replay the catchup transactions into the new DB.
        // (This function should never be called when catchup_txs is
        // None.)
        // FIXME: this part should still happen asynchronously, because it might take a while
        println!("Replaying {} transactions on rebuilt indices...", self.catchup_txs.as_ref().map_or(0, |v| v.len()));
        let mut final_db = new_db;
        let catchup_txs = std::mem::replace(&mut self.catchup_txs, None);
        for tx in catchup_txs.unwrap() {
            for rec in tx.records {
                final_db = final_db.add(rec)?;
            }
        }

        println!("Switching over to rebuilt indices.");
        save_contents(&final_db, self.next_id, self.latest_tx)?;
        self.current_db = final_db;

        // If the mem index filled up during the rebuild, we need to
        // immediately kick off another.
        if self.throttled {
            self.rebuild_indices()?;
            println!("Unthrottling.");
            self.throttled = false;
        }

        Ok(())
    }

    // FIXME: make this not public (fix local transactor in conn.rs)
    pub fn process_tx(&mut self, tx: Tx) -> Result<TxReport> {
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
            ( $db:expr, $rec:expr ) => {
                {
                    let rec: Record = $rec.clone();
                    raw_tx.records.push(rec.clone());
                    $db.add(rec)
                }
            }
        }

        let attr = self.current_db.idents.get_entity("db:txInstant").unwrap();
        let mut db_after =
            add!(
                &self.current_db,
                Record::addition(tx_entity, attr, Value::Timestamp(Utc::now()), tx_entity)
            )?;
        for item in tx.items {
            match item {
                TxItem::Addition(f) => {
                    let attr =
                        match check_schema_and_get_attr(&f, &db_after.idents, &db_after.schema) {
                            Ok(attr) => attr,
                            Err(e) => return Ok(TxReport::Failure(e.to_string())),
                        };
                    db_after = add!(
                        &db_after,
                        Record::addition(f.entity, attr, f.value, tx_entity)
                    )?;
                }
                TxItem::NewEntity(ht) => {
                    let entity = Entity(self.get_id());
                    for (k, v) in ht {
                        let attr = match check_schema_and_get_attr(
                            &Fact::new(entity, k, v.clone()),
                            &db_after.idents,
                            &db_after.schema,
                        ) {
                            Ok(attr) => attr,
                            Err(e) => return Ok(TxReport::Failure(e)),
                        };

                        db_after = add!(&db_after, Record::addition(entity, attr, v, tx_entity))?;
                    }
                    new_entities.push(entity);
                }
                TxItem::Retraction(f) => {
                    let attr =
                        match check_schema_and_get_attr(&f, &db_after.idents, &db_after.schema) {
                            Ok(attr) => attr,
                            Err(e) => return Ok(TxReport::Failure(e)),
                        };
                    db_after = add!(
                        &db_after,
                        Record::retraction(f.entity, attr, f.value, tx_entity)
                    )?;
                }
            }
        }

        // FIXME: Race condition. If adding the tx completes but
        // saving the contents does not, the tx log will be polluted.
        self.store.add_tx(&raw_tx)?;
        self.latest_tx = raw_tx.id;
        if let Some(txs) = self.catchup_txs.as_mut() {
            txs.push(raw_tx.clone());
        }

        save_contents(&db_after, self.next_id, self.last_indexed_tx)?;
        self.current_db = db_after;

        if self.current_db.mem_index_size() > 100_000 {
            match self.catchup_txs {
                Some(_) => {
                    if !self.throttled && self.current_db.mem_index_size() > 1_000_000 {
                        println!(
                            "Mem limit high water mark surpassed during reindexing -- throttling transactions."
                        );
                        self.throttled = true;
                    }
                }
                None => self.rebuild_indices()?,
            }
        }

        if self.throttled {
            println!("throttled - sleeping");
            thread::sleep(Duration::from_millis(1000));
        }

        Ok(TxReport::Success { new_entities })
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
                    let result = self.process_tx(tx)?;
                    // We don't actually care whether the client gets
                    // the report or not.
                    let _ = cb_chan.send(result);
                }
                Event::RebuiltIndex(new_db) => {
                    self.switch_to_rebuilt_indexes(new_db)?;
                }
            }
        }
    }
}

/// Saves the db metadata (index root nodes, entity ID state) to
/// storage, when implemented by the storage backend (i.e. when
/// not using in-memory storage).
fn save_contents(db: &Db, next_id: i64, last_indexed_tx: i64) -> Result<()> {
    let contents = DbContents {
        next_id,
        last_indexed_tx,
        idents: db.idents.clone(),
        schema: db.schema.clone(),
        eav: db.eav.durable_root(),
        aev: db.aev.durable_root(),
        ave: db.ave.durable_root(),
        vae: db.vae.durable_root(),
    };

    db.store.set_contents(&contents)?;
    Ok(())
}

/// Checks the fact's attribute and value against the schema,
/// returning the corresponding attribute entity if successful and an
/// error otherwise.
// FIXME: horribly complected, why do these at the same time?
fn check_schema_and_get_attr(
    fact: &Fact,
    idents: &IdentMap,
    schema: &HashMap<Entity, ValueType>,
) -> ::std::result::Result<Entity, String> {
    let attr = match idents.get_entity(&fact.attribute) {
        Some(a) => a,
        None => {
            return Err(
                format!(
                    "invalid attribute: ident '{}' does not exist",
                    &fact.attribute
                ).into(),
            )
        }
    };

    let actual_type = match fact.value {
        Value::String(_) => ValueType::String,
        Value::Entity(_) => ValueType::Entity,
        Value::Timestamp(_) => ValueType::Timestamp,
        Value::Ident(_) => ValueType::Ident,
    };

    match schema.get(&attr) {
        Some(schema_type) => {
            if *schema_type == actual_type {
                return Ok(attr);
            } else {
                return Err(
                    format!(
                        "type mismatch: expected {:?}, got {:?}",
                        schema_type,
                        actual_type
                    ).into(),
                );
            }
        }
        None => {
            return Err(
                format!(
                    "invalid attribute: `{}` does not specify value type",
                    &fact.attribute
                ).into(),
            );
        }
    }
}

fn create_db(store: Arc<KVStore>) -> Result<Db> {
    use {EAVT, AVET, VAET, AEVT};
    use durable_tree;

    let eav_root = durable_tree::DurableTree::create(store.clone(), EAVT)?.root;
    let ave_root = durable_tree::DurableTree::create(store.clone(), AVET)?.root;
    let aev_root = durable_tree::DurableTree::create(store.clone(), AEVT)?.root;
    let vae_root = durable_tree::DurableTree::create(store.clone(), VAET)?.root;

    let contents = DbContents {
        next_id: 0,
        last_indexed_tx: 0,
        idents: IdentMap::default(),
        schema: HashMap::default(),
        eav: eav_root,
        ave: ave_root,
        aev: aev_root,
        vae: vae_root,
    };

    let mut db = Db::new(contents, store);
    db.idents = db.idents.add("db:ident".to_string(), Entity(1));
    db.idents = db.idents.add("db:valueType".to_string(), Entity(3));

    db.schema.insert(Entity(1), ValueType::Ident);
    db.schema.insert(Entity(3), ValueType::Ident);

    // Bootstrap some attributes we need to run transactions,
    // because they need to reference one another.

    // Initial transaction entity
    db = db.add(
        Record::addition(
            Entity(0),
            Entity(2),
            Value::Timestamp(Utc::now()),
            Entity(0),
        ),
    )?;

    // Entity for the db:ident attribute
    db = db.add(
        Record::addition(
            Entity(1),
            Entity(1),
            Value::Ident("db:ident".into()),
            Entity(0),
        ),
    )?;

    // Entity for the db:txInstant attribute
    db = db.add(
        Record::addition(
            Entity(2),
            Entity(1),
            Value::Ident("db:txInstant".into()),
            Entity(0),
        ),
    )?;

    // Entity for the db:valueType attribute
    db = db.add(
        Record::addition(
            Entity(3),
            Entity(1),
            Value::Ident("db:valueType".into()),
            Entity(0),
        ),
    )?;

    // Value type for the db:ident attribute
    // FIXME: this should be a reference, not an ident.
    db = db.add(
        Record::addition(
            Entity(1),
            Entity(3),
            Value::Ident("db:type:ident".into()),
            Entity(0),
        ),
    )?;

    // Value type for the db:valueType attribute
    // FIXME: this should be a reference, not an ident.
    db = db.add(
        Record::addition(
            Entity(3),
            Entity(3),
            Value::Ident("db:type:ident".into()),
            Entity(0),
        ),
    )?;


    // Idents for primitive types
    db = db.add(
        Record::addition(
            Entity(4),
            Entity(1),
            Value::Ident("db:type:ident".into()),
            Entity(0),
        ),
    )?;
    db = db.add(
        Record::addition(
            Entity(5),
            Entity(1),
            Value::Ident("db:type:string".into()),
            Entity(0),
        ),
    )?;
    db = db.add(
        Record::addition(
            Entity(6),
            Entity(1),
            Value::Ident("db:type:timestamp".into()),
            Entity(0),
        ),
    )?;
    db = db.add(
        Record::addition(
            Entity(7),
            Entity(1),
            Value::Ident("db:type:entity".into()),
            Entity(0),
        ),
    )?;

    Ok(db)
}
