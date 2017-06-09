use std::path::Path;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::ser::Serialize;
use serde::de::Deserialize;
use rmp_serde::{Deserializer, Serializer};

use rusqlite as sql;
use uuid::Uuid;

use btree::{IndexNode, KVStore, DbContents};

#[derive(Clone)]
pub struct SqliteStore<V> {
    phantom: PhantomData<V>,
    conn: Arc<sql::Connection>,
}

#[derive(Debug)]
pub struct Error(String);

impl From<sql::Error> for Error {
    fn from(err: sql::Error) -> Error {
        Error(err.to_string())
    }
}

impl From<String> for Error {
    fn from(err: String) -> Error {
        Error(err)
    }
}



impl<'de, V> SqliteStore<V>
    where V: Serialize + Deserialize<'de> + Clone
{
    /// Sets the DbContents struct to point to a new set of indices,
    /// both in memory and durably.


    pub fn new<P: AsRef<Path>>(path: P) -> Result<SqliteStore<V>, Error> {
        let conn = sql::Connection::open(path)?;

        // Set up SQLite tables to track index data
        conn.execute("CREATE TABLE IF NOT EXISTS logos_kvs (key TEXT NOT NULL PRIMARY KEY, val BLOB)",
                     &[])?;

        let store = SqliteStore {
            conn: Arc::new(conn),
            phantom: PhantomData,
        };

        // If the table is new, we need to set up index roots.
        let result: Result<Vec<u8>, sql::Error> = store
            .conn
            .query_row("SELECT val FROM logos_kvs WHERE key = 'db_contents'",
                       &[],
                       |row| row.get(0));

        match result {
            Ok(_) => {
                // The indices exist already; they'll be retrieved by the Db when
                // it calls get_contents() on the store.
            }
            Err(_) => {
                // The indices do NOT exist and we need to create root nodes for them.
                let empty_root: IndexNode<V> = IndexNode::Leaf {
                    items: vec![],
                };
                let eav_root = store.add(empty_root.clone())?;
                let aev_root = store.add(empty_root.clone())?;
                let ave_root = store.add(empty_root.clone())?;

                store.set_contents(&DbContents {
                    next_id: 1,
                    eav: eav_root,
                    ave: ave_root,
                    aev: aev_root,
                })?;
            }
        }

        Ok(store)
    }
}

impl<'de, V> KVStore for SqliteStore<V>
    where V: Serialize + Deserialize<'de> + Clone
{
    type Item = V;

    fn get(&self, key: &str) -> Result<IndexNode<Self::Item>, String> {
        let mut stmt = self.conn
            .prepare("SELECT val FROM logos_kvs WHERE key = ?1")
            .unwrap();
        match stmt.query_row(&[&key], |row| {
            let s: Vec<u8> = row.get(0);
            s
        }) {
            Ok(val) => {
                let mut de = Deserializer::new(&val[..]);
                match Deserialize::deserialize(&mut de) {
                    Ok(node) => {
                        let node: IndexNode<_> = node;
                        Ok(node)
                    }
                    Err(err) => Err(err.to_string()),
                }
            }
            Err(err) => Err(err.to_string()),
        }
    }

    fn add(&self, value: IndexNode<Self::Item>) -> Result<String, String> {
        let key = Uuid::new_v4().to_string();
        let mut buf = Vec::new();
        value.serialize(&mut Serializer::new(&mut buf)).unwrap();

        let mut stmt = self.conn
            .prepare("INSERT INTO logos_kvs (key, val) VALUES (?1, ?2)")
            .unwrap();
        match stmt.execute(&[&key, &buf]) {
            Ok(_) => Ok(key),
            Err(e) => Err(e.to_string()),
        }
    }

    fn set_contents(&self, contents: &DbContents) -> Result<(), String> {
        let mut buf = Vec::new();
        contents.serialize(&mut Serializer::new(&mut buf)).unwrap();

        let mut stmt = self.conn
            .prepare("INSERT OR REPLACE INTO logos_kvs (key, val) VALUES ('db_contents', ?1)")
            .unwrap();
        stmt.execute(&[&buf]).map_err(|e| e.to_string())?;

        Ok(())
    }

    fn get_contents(&self) -> Result<DbContents, String> {
        let mut stmt = self.conn
            .prepare("SELECT val FROM logos_kvs WHERE key = 'db_contents'")
            .unwrap();
        stmt.query_row(&[], |row| {
            let val: Vec<u8> = row.get(0);
            let mut de = Deserializer::new(&val[..]);
            match Deserialize::deserialize(&mut de) {
                Ok(contents) => {
                    Ok(contents)
                }
                Err(err) => Err(err.to_string()),
            }
        }).map_err(|e| e.to_string())?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    extern crate test;
    use self::test::Bencher;

    #[test]
    fn test_kv_store() {
        let root: IndexNode<String> = IndexNode::Leaf {
            items: vec![],
        };
        let store = SqliteStore::new("/tmp/logos.db").unwrap();
        let key = store.add(root.clone()).unwrap();

        assert_eq!(store.get(&key).unwrap(), root)
    }

    #[bench]
    fn bench_kv_insert(b: &mut Bencher) {
        let root: IndexNode<String> = IndexNode::Leaf {
            items: vec![],
        };
        let store = SqliteStore::new("/tmp/logos.db").unwrap();

        b.iter(|| {
                   let key = store.add(root.clone()).unwrap();
                   assert_eq!(store.get(&key).unwrap(), root)
               })
    }
}
