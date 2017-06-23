use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::fmt::Debug;

use super::{KVStore};
use db::TxClient;
use tx;
use Result;

// HashMap pretending to be a database
#[derive(Clone, Debug)]
pub struct HeapStore {
    inner: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl HeapStore {
    pub fn new<T: Debug + Ord + Clone>() -> HeapStore {
        let store = HeapStore { inner: Arc::new(Mutex::new(HashMap::default())) };

        let s = Arc::new(store.clone());

        store.set_transactor(&TxClient::Local).unwrap();
        store
    }
}

impl KVStore for HeapStore {
    fn set(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut guard = self.inner.lock()?;

        match (*guard).insert(key.to_string(), value.to_vec()) {
            Some(_) => Ok(()),
            None => Ok(()),
        }
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.inner
            .lock()
            .unwrap()
            .get(key)
            .map(|v| v.clone())
            .ok_or("invalid reference".into())
    }
}
