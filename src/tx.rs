use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};

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
    recv: Receiver<Event>,
    send: Sender<Event>,
}

enum Event {
    Tx(Tx, Sender<TxReport>),
}

/// TxHandle is a wrapper over Transactor that provides a thread-safe
/// interface for submitting transactions and receiving their results,
/// abstracting away the implementation of the thread-safety.
#[derive(Clone)]
pub struct TxHandle {
    chan: Sender<Event>,
}

impl TxHandle {
    pub fn new(transactor: &mut Transactor) -> TxHandle {
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
                        db = add(&db, record)?;
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
                })
            }
            Err(_) => {
                let mut tx = Transactor {
                    next_id: 8,
                    store: store.clone(),
                    latest_tx: 0,
                    last_indexed_tx: -1,
                    current_db: create_db(store)?,
                    send,
                    recv,
                };

                tx.rebuild_indices()?;
                Ok(tx)
            }
        }
    }

    /// Builds a new set of durable indices by combining the existing
    /// durable indices and the in-memory indices.
    pub fn rebuild_indices(&mut self) -> Result<()> {
        let new_db = {
            let Db {
                ref eav,
                ref ave,
                ref aev,
                ref vae,
                ..
            } = self.current_db;

            let new_eav = eav.rebuild();
            let new_ave = ave.rebuild();
            let new_aev = aev.rebuild();
            let new_vae = vae.rebuild();

            Db {
                eav: new_eav,
                ave: new_ave,
                aev: new_aev,
                vae: new_vae,
                idents: self.current_db.idents.clone(),
                schema: self.current_db.schema.clone(),
                store: self.current_db.store.clone(),
            }
        };

        // FIXME: Make all this async-safe.
        save_contents(&new_db, self.next_id, self.latest_tx)?;
        self.current_db = new_db;

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
                    add($db, rec)
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
        save_contents(&db_after, self.next_id, self.last_indexed_tx)?;
        self.current_db = db_after;

        if self.current_db.mem_index_size() > 10000 {
            println!("Rebuilding indices...");
            self.rebuild_indices()?;
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
                    let result = self.process_tx(tx)?;
                    // We don't actually care whether the client gets
                    // the report or not.
                    let _ = cb_chan.send(result);
                }
                // TODO: implement event for reindexing.
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

pub fn add(db: &Db, record: Record) -> Result<Db> {
    let new_eav = db.eav.insert(record.clone());
    let new_ave = db.ave.insert(record.clone());
    let new_aev = db.aev.insert(record.clone());
    let new_vae = db.vae.insert(record.clone());

    // If the record has a db:ident, we need to add it to the ident map.
    let new_idents = if record.attribute == db.idents.get_entity("db:ident").unwrap() {
        match record.value {
            Value::Ident(ref s) => db.idents.add(s.clone(), record.entity),
            _ => return Err("db:ident value must be an identifier".into()), // unreachable?
        }
    } else {
        db.idents.clone()
    };

    let new_schema = if record.attribute == db.idents.get_entity("db:valueType").unwrap() {
        let value_type = match record.value {
            Value::Ident(s) => {
                match s.as_str() {
                    "db:type:string" => ValueType::String,
                    "db:type:ident" => ValueType::Ident,
                    "db:type:timestamp" => ValueType::Timestamp,
                    "db:type:entity" => ValueType::Entity,
                    _ => return Err(format!("{} is not a valid primitive type", s).into()),
                }
            }
            _ => return Err("db:valueType must be an identifier".into()),
        };
        let mut new_schema = db.schema.clone();
        new_schema.insert(record.entity, value_type);
        new_schema
    } else {
        db.schema.clone()
    };

    Ok(Db {
        eav: new_eav,
        ave: new_ave,
        aev: new_aev,
        vae: new_vae,
        idents: new_idents,
        schema: new_schema,
        store: db.store.clone(),
    })
}

fn create_db(store: Arc<KVStore>) -> Result<Db> {
    use durable_tree;

    let empty_root: durable_tree::Node<Record> = durable_tree::Node::Interior {
        keys: vec![],
        links: vec![],
    };

    let node_store = durable_tree::NodeStore::new(store.clone());
    let eav_root = node_store.add_node(&empty_root)?;
    let aev_root = node_store.add_node(&empty_root)?;
    let ave_root = node_store.add_node(&empty_root)?;
    let vae_root = node_store.add_node(&empty_root)?;

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
    db = add(
        &db,
        Record::addition(
            Entity(0),
            Entity(2),
            Value::Timestamp(Utc::now()),
            Entity(0),
        ),
    )?;

    // Entity for the db:ident attribute
    db = add(
        &db,
        Record::addition(
            Entity(1),
            Entity(1),
            Value::Ident("db:ident".into()),
            Entity(0),
        ),
    )?;

    // Entity for the db:txInstant attribute
    db = add(
        &db,
        Record::addition(
            Entity(2),
            Entity(1),
            Value::Ident("db:txInstant".into()),
            Entity(0),
        ),
    )?;

    // Entity for the db:valueType attribute
    db = add(
        &db,
        Record::addition(
            Entity(3),
            Entity(1),
            Value::Ident("db:valueType".into()),
            Entity(0),
        ),
    )?;

    // Value type for the db:ident attribute
    // FIXME: this should be a reference, not an ident.
    db = add(
        &db,
        Record::addition(
            Entity(1),
            Entity(3),
            Value::Ident("db:type:ident".into()),
            Entity(0),
        ),
    )?;

    // Value type for the db:valueType attribute
    // FIXME: this should be a reference, not an ident.
    db = add(
        &db,
        Record::addition(
            Entity(3),
            Entity(3),
            Value::Ident("db:type:ident".into()),
            Entity(0),
        ),
    )?;


    // Idents for primitive types
    db = add(
        &db,
        Record::addition(
            Entity(4),
            Entity(1),
            Value::Ident("db:type:ident".into()),
            Entity(0),
        ),
    )?;
    db = add(
        &db,
        Record::addition(
            Entity(5),
            Entity(1),
            Value::Ident("db:type:string".into()),
            Entity(0),
        ),
    )?;
    db = add(
        &db,
        Record::addition(
            Entity(6),
            Entity(1),
            Value::Ident("db:type:timestamp".into()),
            Entity(0),
        ),
    )?;
    db = add(
        &db,
        Record::addition(
            Entity(7),
            Entity(1),
            Value::Ident("db:type:entity".into()),
            Entity(0),
        ),
    )?;

    Ok(db)
}
