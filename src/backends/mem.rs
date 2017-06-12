use uuid::Uuid;
use std::sync::{Arc, Mutex};
use std::fmt::Debug;
use std::collections::HashMap;

use ident::IdentMap;
use btree::IndexNode;
use super::{KVStore, DbContents};

// HashMap pretending to be a database
#[derive(Clone, Debug)]
pub struct HeapStore<T: Debug + Ord + Clone> {
    inner: Arc<Mutex<HashMap<String, IndexNode<T>>>>,
}

impl<T: Ord + Debug + Clone> HeapStore<T> {
    pub fn new() -> HeapStore<T> {
        HeapStore { inner: Arc::new(Mutex::new(HashMap::default())) }
    }
}

impl<T: Debug + Ord + Clone> KVStore for HeapStore<T> {
    type Item = T;

    fn add(&self, value: IndexNode<T>) -> Result<String, String> {
        let key = Uuid::new_v4().to_string();
        let mut guard = self.inner.lock().map_err(|e| e.to_string())?;

        match (*guard).insert(key.clone(), value) {
            Some(_) => Err("duplicate uuid?!?".to_string()),
            None => Ok(key),
        }
    }

    fn set_contents(&self, _contents: &DbContents) -> Result<(), String> {
        // HeapStore can't survive a restart, so it doesn't need to set
        // the DbContents.
        Ok(())
    }

    fn get_contents(&self) -> Result<DbContents, String> {
        // We don't bother storing contents in a HeapStore, so we just make
        // a new one.
        let empty_root = IndexNode::Leaf { items: vec![] };

        Ok(DbContents {
            next_id: 0,
            idents: IdentMap::default(),
            eav: self.add(empty_root.clone())?,
            ave: self.add(empty_root.clone())?,
            aev: self.add(empty_root)?
        })
    }

    fn get(&self, key: &str) -> Result<IndexNode<T>, String> {
        self.inner
            .lock()
            .unwrap()
            .get(key)
            .map(|v| v.clone())
            .ok_or("invalid reference".to_string())
    }
}
