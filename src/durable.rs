use std::path::Path;
use serde::ser::Serialize;
use serde::de::Deserialize;
use rmp_serde::{Deserializer, Serializer};
use rusqlite as sql;
use uuid::Uuid;

/// Trait abstracting over anything that can be used as a disk-backed
/// KV store, where keys can only be added, not modified.
trait KVStore<V>
    where Self: Sized
{
    type Node;
    type Error;
    fn get(&self, key: &str) -> Result<Self::Node, Self::Error>;
    fn add(&self, value: &Self::Node) -> Result<String, Self::Error>;
}

/// Representation of a B-tree node that is serializable to disk.
/// Contains a vector of keys (i.e. the contents of the B-tree) and a
/// vector of links, which are strings that correspond to keys in the
/// KV store.
// FIXME: overloaded use of `key`
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct DurableNode<V> {
    keys: Vec<V>,
    links: Vec<String>,
}

struct SqliteStore {
    conn: sql::Connection,
    db_contents: DbContents,
}

#[derive(Debug)]
struct Error(String);

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

/// A structure designed to be stored in the index that enables
/// a process to locate the indexes, tx log, etc.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DbContents {
    eav_index: String,
    ave_index: String,
    aev_index: String,
}

impl SqliteStore {
    /// Sets the DbContents struct to point to a new set of indices,
    /// both in memory and durably.
    fn set_contents(&mut self, contents: DbContents) -> Result<(), Error> {
        let mut buf = Vec::new();
        contents.serialize(&mut Serializer::new(&mut buf)).unwrap();

        let mut stmt = self.conn
            .prepare("INSERT INTO logos_kvs (key, val) VALUES (?1, ?2)")
            .unwrap();
        stmt.execute(&[&"db_contents", &buf])?;
        self.db_contents = contents;
        Ok(())
    }

    fn new<P: AsRef<Path>>(path: P) -> Result<SqliteStore, Error> {
        let conn = sql::Connection::open(path)?;

        // Set up SQLite tables to track index data
        conn.execute("CREATE TABLE IF NOT EXISTS logos_kvs (key TEXT NOT NULL, val BLOB)",
                     &[])?;

        let mut store = SqliteStore {
            conn: conn,
            // This DbContents will be overridden no matter what, but we need to create
            // the Store struct now in order to use its `add` method.
            db_contents: DbContents {
                eav_index: "dummy".to_string(),
                ave_index: "dummy".to_string(),
                aev_index: "dummy".to_string()
            }
        };

        // If the table is new, we need to set up index roots.
        let result: Result<Vec<u8>, sql::Error> = store
            .conn
            .query_row("SELECT val FROM logos_kvs WHERE key = 'db_contents'",
                       &[],
                       |row| row.get(0));

        match result {
            Ok(val) => {
                let mut de = Deserializer::new(&val[..]);
                let res: Result<DbContents, ::rmp_serde::decode::Error> =
                    Deserialize::deserialize(&mut de);
                match res {
                    Ok(_) => (),
                    _ => {
                        return Err(Error("corrupt index; could not deserialize db_contents"
                                             .to_string()))
                    }
                }
            }
            Err(err) => {
                println!("{}", err.to_string());
                let empty_root: DurableNode<super::Fact> = DurableNode {
                    keys: vec![],
                    links: vec![],
                };
                let eav_root = store.add(&empty_root)?;
                let aev_root = store.add(&empty_root)?;
                let ave_root = store.add(&empty_root)?;

                store.set_contents(DbContents {
                    eav_index: eav_root,
                    ave_index: ave_root,
                    aev_index: aev_root,
                })?;

            }
        }

        Ok(store)
    }
}

impl<'de, V> KVStore<V> for SqliteStore
    where V: Serialize + Deserialize<'de>
{
    type Node = DurableNode<V>;
    type Error = String;

    fn get(&self, key: &str) -> Result<Self::Node, Self::Error> {
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
                        let node: DurableNode<_> = node;
                        Ok(node)
                    }
                    Err(err) => Err(err.to_string()),
                }
            }
            Err(err) => Err(err.to_string()),
        }
    }

    fn add(&self, value: &Self::Node) -> Result<String, Self::Error> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    extern crate test;
    use self::test::Bencher;

    #[test]
    fn test_kv_store() {
        let root: DurableNode<String> = DurableNode {
            links: vec![],
            keys: vec![],
        };
        let store = SqliteStore::new("/tmp/logos.db").unwrap();
        let key = store.add(&root).unwrap();

        assert_eq!(store.get(&key).unwrap(), root)
    }

    #[bench]
    fn bench_kv_insert(b: &mut Bencher) {
        let root: DurableNode<String> = DurableNode {
            links: vec![],
            keys: vec![],
        };
        let store = SqliteStore::new("/tmp/logos.db").unwrap();

        b.iter(|| {
                   let key = store.add(&root).unwrap();
                   assert_eq!(store.get(&key).unwrap(), root)
               })
    }
}
