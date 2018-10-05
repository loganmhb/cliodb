use mysql;

use serde::{Serialize, Deserialize};
use rmp_serde::{Serializer, Deserializer};

use {Result, KVStore, Record};
use tx::TxRaw;

pub struct MysqlStore {
    pool: mysql::Pool,
}

impl MysqlStore {
    pub fn new(uri: &str) -> Result<MysqlStore> {

        let pool = mysql::Pool::new(uri)?;

        let empty_params: Vec<String> = vec![];
        // Set up tables to track index data
        pool.prep_exec(
            "CREATE TABLE IF NOT EXISTS logos_kvs (`key` VARCHAR(36) NOT NULL PRIMARY KEY, val BLOB)",
            empty_params.clone()
        )?;
        pool.prep_exec(
            "CREATE TABLE IF NOT EXISTS logos_txs (id INTEGER NOT NULL PRIMARY KEY, val BLOB)",
            empty_params
        )?;

        let store = MysqlStore { pool };

        Ok(store)
    }
}

impl KVStore for MysqlStore {
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.pool.first_exec(
            "SELECT val FROM logos_kvs WHERE `key` = :key",
            vec![("key", key)]
        )
            .map_err(|e| e.to_string())
            .map(|r| r.ok_or("vale does not exist"))
            .and_then(|row| {
                // FIXME: this function should return a Result<Option<Vec<u8>>> instead
                // of using a result as an option
                match row?.get(0) {
                    Some(val) => Ok(val),
                    None => Err("val does not exist".into())
                }
            })
            .map_err(|e| e.into())

    }

    fn set(&self, key: &str, value: &[u8]) -> Result<()> {
        // We can't assume the key isn't already set, so need INSERT OR REPLACE.
        self.pool.prep_exec(
            "INSERT INTO logos_kvs (`key`, val) VALUES (?, ?) ON DUPLICATE KEY UPDATE val = ?",
            (key, value, value)
        ) .map(|_| ()).map_err(|e| e.into())
    }

    fn get_txs(&self, from: i64) -> Result<Vec<TxRaw>> {
        let results = self.pool.prep_exec("SELECT id, val FROM logos_txs WHERE id > ?", (from,))?
            .map(|row_result| {
                row_result
                    .map_err(|e| e.to_string())
                    .and_then(|row| {
                        let id: i64 = row.get(0).unwrap();
                        let bytes: Vec<u8> = row.get(1).unwrap();
                        let mut de = Deserializer::new(&bytes[..]);
                        let res: Vec<Record> = Deserialize::deserialize(&mut de)
                            .map_err(|e| e.to_string())?;

                        Ok(TxRaw {
                            id: id,
                            records: res,
                        })
                    }).map_err(|e| e.to_string())
            });
        let mut txs = vec![];
        for result in results {
            txs.push(result?);
        }
        Ok(txs)
    }

    fn add_tx(&self, tx: &TxRaw) -> Result<()> {
        let mut serialized: Vec<u8> = vec![];
        tx.records.serialize(&mut Serializer::new(&mut serialized))?;

        self.pool.prep_exec("INSERT INTO logos_txs (id, val) VALUES (?, ?)", (tx.id, serialized))?;
        Ok(())
    }
}
