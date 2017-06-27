use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::fmt::Debug;

use super::KVStore;
use db::TxClient;
use tx;
use Result;

// HashMap pretending to be a database
#[derive(Clone, Debug)]
pub struct HeapStore {
    index: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    log: Arc<Mutex<Vec<tx::TxRaw>>>,
}

impl HeapStore {
    pub fn new<T: Debug + Ord + Clone>() -> HeapStore {
        let store = HeapStore {
            index: Arc::new(Mutex::new(HashMap::default())),
            log: Arc::new(Mutex::new(Vec::new())),
        };

        store.set_transactor(&TxClient::Local).unwrap();
        store
    }
}

impl KVStore for HeapStore {
    fn set(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut guard = self.index.lock()?;

        match (*guard).insert(key.to_string(), value.to_vec()) {
            Some(_) => Ok(()),
            None => Ok(()),
        }
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.index
            .lock()
            .unwrap()
            .get(key)
            .map(|v| v.clone())
            .ok_or(format!("invalid reference: {}", key).into())
    }

    fn add_tx(&self, tx: &tx::TxRaw) -> Result<()> {
        self.log.lock().unwrap().push(tx.clone());
        Ok(())
    }

    fn get_txs(&self, after: i64) -> Result<Vec<tx::TxRaw>> {
        Ok((*self.log.lock().unwrap())
               .iter()
               .filter(|tx| tx.id >= after)
               .cloned()
               .collect::<Vec<_>>())
    }
}
