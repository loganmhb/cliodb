use std::path::Path;

use rusqlite as sql;

use serde::{Serialize, Deserialize};
use rmp_serde::{Serializer, Deserializer};

use {Result, KVStore, Record};
use tx::TxRaw;

pub struct SqliteStore {
    conn: sql::Connection,
}

// FIXME: this is irresponsible!!
unsafe impl ::std::marker::Sync for SqliteStore {}

impl SqliteStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<SqliteStore> {
        let conn = sql::Connection::open(path)?;

        // Set up SQLite tables to track index data
        conn.execute(
            "CREATE TABLE IF NOT EXISTS logos_kvs (key TEXT NOT NULL PRIMARY KEY, val BLOB)",
            &[],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS logos_txs (id INTEGER NOT NULL PRIMARY KEY, val BLOB)",
            &[],
        )?;


        let store = SqliteStore { conn: conn };

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

    fn get_txs(&self, from: i64) -> Result<Vec<TxRaw>> {
        let mut stmt = self.conn
            .prepare("SELECT id, val FROM logos_txs WHERE id > ?1")
            .unwrap();
        let results: Vec<Result<TxRaw>> = stmt.query_map(&[&from], |ref row| {
            let bytes: Vec<u8> = row.get(1);
            let mut de = Deserializer::new(&bytes[..]);
            let res: Vec<Record> = Deserialize::deserialize(&mut de)?;
            let id: i64 = row.get(0);
            Ok(TxRaw {
                id: id,
                records: res,
            })
            // FIXME: why does this end up as a nested result?
        }).unwrap()
            .map(|r| r.unwrap())
            .collect();

        let mut txs = vec![];
        for result in results {
            txs.push(result?);
        }
        Ok(txs)
    }

    fn add_tx(&self, tx: &TxRaw) -> Result<()> {
        let mut serialized: Vec<u8> = vec![];
        tx.records.serialize(&mut Serializer::new(&mut serialized))?;

        let mut stmt = self.conn
            .prepare("INSERT INTO logos_txs (id, val) VALUES (?1, ?2)")
            .unwrap();

        stmt.execute(&[&tx.id, &serialized])?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    extern crate test;

    use rmp_serde::Serializer;
    use serde::Serialize;
    use durable_tree::Node;

    #[test]
    fn test_kv_store() {
        let root: Node<String> = Node::Leaf { items: vec![] };
        let store = SqliteStore::new("/tmp/logos.db").unwrap();
        let mut buf = Vec::new();
        root.serialize(&mut Serializer::new(&mut buf)).unwrap();
        store.set("my_key", &buf).unwrap();

        assert_eq!(store.get("my_key").unwrap(), buf)
    }
}
