use std::path::Path;
use serde::ser::Serialize;
use serde::de::{Deserialize};
use rmp_serde::{Deserializer, Serializer};
use rusqlite as sql;
use uuid::Uuid;

/// Trait abstracting over anything that can be used as a disk-backed
/// KV store, where keys can only be added, not modified.
trait KVStore<V> where Self: Sized {
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
    links: Vec<String>
}

struct SqliteStore {
    conn: sql::Connection
}

#[derive(Debug)]
struct Error(String);

impl From<sql::Error> for Error {
    fn from(err: sql::Error) -> Error {
        Error(err.to_string())
    }
}

impl SqliteStore {
    fn new<P: AsRef<Path>>(path: P) -> Result<SqliteStore, Error> {
        let conn = sql::Connection::open(path)?;

        conn.execute("CREATE TABLE IF NOT EXISTS logos_kvs (key TEXT NOT NULL, val BLOB)", &[])?;

        Ok(SqliteStore { conn: conn })
    }
}

impl<'de, V> KVStore<V> for SqliteStore
    where V: Serialize + Deserialize<'de>
{
    type Node = DurableNode<V>;
    type Error = String;

    fn get(&self, key: &str) -> Result<Self::Node, Self::Error> {
        let mut stmt = self.conn.prepare("SELECT val FROM logos_kvs WHERE key = ?1").unwrap();
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
                    Err(err) => Err(err.to_string())
                }
            }
            Err(err) => Err(err.to_string())
        }
    }

    fn add(&self, value: &Self::Node) -> Result<String, Self::Error> {
        let key = Uuid::new_v4().to_string();
        let mut buf = Vec::new();
        value.serialize(&mut Serializer::new(&mut buf)).unwrap();

        let mut stmt = self.conn.prepare("INSERT INTO logos_kvs (key, val) VALUES (?1, ?2)").unwrap();
        match stmt.execute(&[&key, &buf]) {
            Ok(_) => Ok(key),
            Err(e) => Err(e.to_string())
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
        let root: DurableNode<String> = DurableNode { links: vec![], keys: vec![]};
        let store = SqliteStore::new("/tmp/logos.db").unwrap();
        let key = store.add(&root).unwrap();

        assert_eq!(store.get(&key).unwrap(), root)
    }

    #[bench]
    fn bench_kv_insert(b: &mut Bencher) {
        let root: DurableNode<String> = DurableNode { links: vec![], keys: vec![]};
        let store = SqliteStore::new("/tmp/logos.db").unwrap();

        b.iter(|| {
            let key = store.add(&root).unwrap();
            assert_eq!(store.get(&key).unwrap(), root)
        })
    }
}
