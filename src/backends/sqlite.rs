use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite as sql;

use rmp_serde;

use {Result, KVStore, Record};
use tx::TxRaw;

pub struct SqliteStore {
    conn: Arc<Mutex<sql::Connection>>,
}

impl SqliteStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<SqliteStore> {
        let conn = sql::Connection::open(path)?;

        // Set up SQLite tables to track index data
        conn.execute(
            "CREATE TABLE IF NOT EXISTS cliodb_kvs (key TEXT NOT NULL PRIMARY KEY, val BLOB)",
            sql::NO_PARAMS,
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS cliodb_txs (id INTEGER NOT NULL PRIMARY KEY, val BLOB)",
            sql::NO_PARAMS,
        )?;

        let store = SqliteStore { conn: Arc::new(Mutex::new(conn)) };
        Ok(store)
    }
}

impl KVStore for SqliteStore {
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT val FROM cliodb_kvs WHERE key = ?1")
            .unwrap();
        let mut rows = stmt.query_map(sql::params![key], |row| {
            let r: Option<Vec<u8>> = row.get(0).unwrap();
            Ok(r.unwrap())
        })?;
        match rows.next() {
            Some(row) => Ok(row?),
            None => Err("key not found".into())
        }
    }

    fn set(&self, key: &str, value: &[u8]) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        // We can't assume the key isn't already set, so need INSERT OR REPLACE.
        let mut stmt = conn.prepare(
            "INSERT OR REPLACE INTO cliodb_kvs (key, val) VALUES (?1, ?2)",
        ).unwrap();
        stmt.execute(sql::params![key, value])?;
        Ok(())
    }

    fn get_txs(&self, from: i64) -> Result<Vec<TxRaw>> {
        // FIXME: handle errors
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id, val FROM cliodb_txs WHERE id > ?1")
            .unwrap();
        let results: Vec<TxRaw> = stmt.query_map(sql::params![&from], |ref row| {
            let maybe_bytes: Option<Vec<u8>> = row.get(1).unwrap();
            let bytes = maybe_bytes.unwrap();
            let res: Vec<Record> = rmp_serde::from_read_ref(&bytes).expect("corrupt data");
            let id: i64 = row.get(0).unwrap();
            Ok(TxRaw {
                id: id,
                records: res,
            })
        }).unwrap()
            .map(|r| r.unwrap())
            .collect();

        Ok(results)
    }

    fn add_tx(&self, tx: &TxRaw) -> Result<()> {
        let serialized: Vec<u8> = rmp_serde::to_vec(&tx.records)?;

        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("INSERT INTO cliodb_txs (id, val) VALUES (?1, ?2)")
            .unwrap();

        stmt.execute(sql::params![tx.id, &serialized])?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    extern crate test;

    use durable_tree::{Node, LeafNode};

    #[test]
    fn test_kv_store() {
        let root: Node<String> = Node::Leaf(LeafNode { items: vec![] });
        let store = SqliteStore::new("/tmp/cliodb.db").unwrap();
        let buf = rmp_serde::to_vec(&root).unwrap();
        store.set("my_key", &buf).unwrap();

        assert_eq!(store.get("my_key").unwrap(), buf)
    }
}
