use std::path::Path;
use std::sync::Arc;

use rusqlite as sql;

use Result;
use super::KVStore;

#[derive(Clone)]
pub struct SqliteStore {
    conn: Arc<sql::Connection>,
}

impl SqliteStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<SqliteStore> {
        let conn = sql::Connection::open(path)?;

        // Set up SQLite tables to track index data
        conn.execute("CREATE TABLE IF NOT EXISTS logos_kvs (key TEXT NOT NULL PRIMARY KEY, val BLOB)",
                     &[])?;

        let store = SqliteStore { conn: Arc::new(conn) };

        Ok(store)
    }
}

impl KVStore for SqliteStore {
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        let mut stmt = self.conn
            .prepare("SELECT val FROM logos_kvs WHERE key = ?1")
            .unwrap();
        stmt.query_row(&[&key], |row| {
                let s: Vec<u8> = row.get(0);
                s
            }).map_err(|e| e.into())
    }

    fn set(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut stmt = self.conn
            // We can't assume the key isn't already set, so need INSERT OR REPLACE.
            .prepare("INSERT OR REPLACE INTO logos_kvs (key, val) VALUES (?1, ?2)")
            .unwrap();
        stmt.execute(&[&key, &value])?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    extern crate test;
    use self::test::Bencher;
    use btree::IndexNode;

    use rmp_serde::Serializer;
    use serde::{Serialize, Deserialize};

    #[test]
    fn test_kv_store() {
        let root: IndexNode<String> = IndexNode::Leaf { items: vec![] };
        let store = SqliteStore::new("/tmp/logos.db").unwrap();
        let mut buf = Vec::new();
        root.serialize(&mut Serializer::new(&mut buf));
        store.set("my_key", &buf).unwrap();

        assert_eq!(store.get("my_key").unwrap(), buf)
    }
}
