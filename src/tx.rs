use std::sync::Arc;

use chrono::prelude::UTC;

use backends::KVStore;
use index::IndexNode;
use db::{add_node, Db, DbContents};
use {Tx, TxReport, Entity, Record, Value, TxItem, Result, IdentMap};

pub struct Transactor {
    next_id: u64,
    current_db: Db
}

impl Transactor {
    pub fn new(store: Arc<KVStore>) -> Result<Transactor> {
        let contents: DbContents = store.get_contents()?;

        Ok(Transactor {
            next_id: contents.next_id,
            current_db: Db::new(contents, store.clone())
        })
    }

    pub fn process_tx(&mut self, tx: Tx) -> Result<TxReport> {
        let mut new_entities = vec![];
        let tx_entity = Entity(self.get_id());
        let attr = self.current_db.idents.get_entity("db:txInstant".to_string()).unwrap();
        let mut db_after = add(&self.current_db,
                               Record::addition(tx_entity, attr, Value::Timestamp(UTC::now()), tx_entity))?;
        for item in tx.items {
            match item {
                TxItem::Addition(f) => {
                    let attr = match db_after.idents.get_entity(f.attribute) {
                        Some(attr) => attr,
                        None => return Ok(TxReport::Failure("invalid attribute".into()))
                    };
                    db_after = add(&db_after, Record::addition(f.entity, attr, f.value, tx_entity))?;
                }
                TxItem::NewEntity(ht) => {
                    let entity = Entity(self.get_id());
                    for (k, v) in ht {
                        let attr = match db_after.idents.get_entity(k) {
                            Some(attr) => attr,
                            None => return Ok(TxReport::Failure("invalid attribute".into()))
                        };

                        db_after = add(&db_after, Record::addition(entity, attr, v, tx_entity))?;
                    }
                    new_entities.push(entity);
                }
                TxItem::Retraction(f) => {
                    let attr = match db_after.idents.get_entity(f.attribute) {
                        Some(attr) => attr,
                        None => return Ok(TxReport::Failure("invalid attribute".into()))
                    };
                    db_after = add(&db_after, Record::retraction(f.entity, attr, f.value, tx_entity))?;
                }
            }
        }

        save_contents(&db_after, self.next_id)?;
        self.current_db = db_after;
        Ok(TxReport::Success { new_entities })
    }

    fn get_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id +=1 ;
        id
    }
}

pub fn create_db(store: Arc<KVStore>) -> Result<()> {
    let empty_root: IndexNode<Record> = IndexNode::Leaf { items: vec![] };

    let eav_root = add_node(&(*store), empty_root.clone())?;
    let aev_root = add_node(&(*store), empty_root.clone())?;
    let ave_root = add_node(&(*store), empty_root.clone())?;

    let contents = DbContents {
        next_id: 0,
        idents: IdentMap::default(),
        eav: eav_root,
        ave: ave_root,
        aev: aev_root,
    };

    let mut db = Db::new(contents, store.clone());
    db.idents = db.idents.add("db:ident".to_string(), Entity(1));

    // Bootstrap some attributes we need to run transactions,
    // because they need to reference one another.

    // Initial transaction entity
    db = add(&db, Record::addition(Entity(0),
                            Entity(2),
                            Value::Timestamp(UTC::now()),
                            Entity(0)))?;

    // Entity for the db:ident attribute
    db = add(&db, Record::addition(Entity(1),
                            Entity(1),
                            Value::Ident("db:ident".into()),
                            Entity(0)))?;

    // Entity for the db:txInstant attribute
    db = add(&db, Record::addition(Entity(2),
                            Entity(1),
                            Value::Ident("db:txInstant".into()),
                            Entity(0)))?;

    save_contents(&db, 3)?;
    Ok(())
}

/// Saves the db metadata (index root nodes, entity ID state) to
/// storage, when implemented by the storage backend (i.e. when
/// not using in-memory storage).
fn save_contents(db: &Db, next_id: u64) -> Result<()> {
    let contents = DbContents {
        next_id,
        idents: db.idents.clone(),
        eav: db.eav.root_ref.clone(),
        aev: db.aev.root_ref.clone(),
        ave: db.ave.root_ref.clone(),
    };

    db.store.set_contents(&contents)?;
    Ok(())
}

fn add(db: &Db, record: Record) -> Result<Db> {
    let new_eav = db.eav.insert(record.clone())?;
    let new_ave = db.ave.insert(record.clone())?;
    let new_aev = db.aev.insert(record.clone())?;

    // If the record has a db:ident, we need to add it to the ident map.
    let new_idents = if record.attribute == db.idents.get_entity("db:ident".to_string()).unwrap() {
        match record.value {
            Value::Ident(s) => db.idents.add(s.clone(), record.entity),
            _ => unimplemented!(), // FIXME: type error
        }
    } else {
        db.idents.clone()
    };

    Ok(Db {
        eav: new_eav,
        ave: new_ave,
        aev: new_aev,
        idents: new_idents,
        store: db.store.clone()
    })
}
