use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use super::{KVStore, DbContents};
use btree;
use ident::IdentMap;
use Result;

// HashMap pretending to be a database
#[derive(Clone, Debug)]
pub struct HeapStore {
    inner: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl HeapStore {
    pub fn new() -> HeapStore {
        let store = HeapStore { inner: Arc::new(Mutex::new(HashMap::default())) };
        let node_store = btree::NodeStore { backing_store: Arc::new(store.clone()) };

        let empty_root: btree::IndexNode<String> = btree::IndexNode::Leaf { items: vec![] };
        let contents = DbContents {
            next_id: 0,
            idents: IdentMap::default(),
            eav: node_store.add_node(empty_root.clone()).unwrap(),
            ave: node_store.add_node(empty_root.clone()).unwrap(),
            aev: node_store.add_node(empty_root).unwrap()
        };

        store.set_contents(&contents).unwrap();
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
