use std::ops::Deref;
use serde::ser::Serialize;
use serde::de::{Deserialize};

use std::marker::PhantomData;

use rmp_serde::{Deserializer, Serializer};
use rusqlite as sql;

trait KVStore<V> where Self: Sized {
    type Node;
    type Error;
    fn get(&self, key: &str) -> Result<Self::Node, Self::Error>;

    fn set(&self, key: &str, value: &Self::Node) -> Result<(), Self::Error>;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct DurableNode<V> {
    keys: Vec<V>,
    links: Vec<String>
}

struct SqliteStore {
    conn: sql::Connection
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

    fn set(&self, key: &str, value: &Self::Node) -> Result<(), Self::Error> {
        let mut buf = Vec::new();
        value.serialize(&mut Serializer::new(&mut buf)).unwrap();

        let mut stmt = self.conn.prepare("INSERT INTO logos_kvs (key, val) VALUES (?1, ?2)").unwrap();
        match stmt.execute(&[&key, &buf]) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite as sql;

    #[test]
    fn test_kv_store() {
        let mut store: SqliteStore = SqliteStore { conn: sql::Connection::open("/tmp/logos.db").unwrap()};

//        store.conn.execute("CREATE TABLE IF NOT EXISTS logos_kvs (key TEXT NOT NULL, val BLOB)", &[]).unwrap();
        let mut root: DurableNode<String> = DurableNode { links: vec![], keys: vec![]};
        store.set("key1", &root);

        assert_eq!(store.get("key1").unwrap(), root)
    }
}
