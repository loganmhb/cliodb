use std::cmp::Ordering;
use std::fmt::Debug;
use std::sync::Arc;

use serde::{Serialize, Deserialize};
use itertools::Itertools;

use backends::KVStore;
use durable_tree::{DurableTree, NodeStore};
use rbtree::RBTree;

pub trait Comparator: Clone {
    type Item;
    fn compare(a: &Self::Item, b: &Self::Item) -> Ordering;
}

#[derive(Clone)]
pub struct Index<T, C>
where
    T: Debug + Ord + Clone,
    C: Comparator<Item = T>,
{
    mem_index: RBTree<T, C>,
    _comparator: C,
    store: NodeStore<T>,
    durable_index: DurableTree<T, C>,
}

impl<'de, T, C> Index<T, C>
where
    T: Debug + Ord + Clone + Serialize + Deserialize<'de>,
    C: Comparator<Item = T> + Copy,
{
    pub fn new(root_ref: String, store: Arc<KVStore>, comparator: C) -> Index<T, C> {
        let node_store = NodeStore::new(store);
        Index {
            _comparator: comparator,
            store: node_store.clone(),
            mem_index: RBTree::new(comparator),
            durable_index: DurableTree::from_ref(root_ref, node_store, comparator),
        }
    }

    pub fn mem_index_size(&self) -> usize {
        self.mem_index.size()
    }

    pub fn range_from(&self, range_start: T) -> impl Iterator<Item = T> {
        self.mem_index.range_from(range_start.clone()).merge_by(
            self.durable_index
                .range_from(range_start)
                .unwrap()
                .map(|r| r.unwrap()),
            |a, b| C::compare(a, b) == Ordering::Less,
        )
    }

    pub fn iter(&self) -> impl Iterator<Item = T> {
        // FIXME: signature should allow returning Result instead of unwrapping
        self.mem_index.iter().merge_by(
            self.durable_index.iter().unwrap().map(
                |r| r.unwrap(),
            ),
            |a, b| {
                C::compare(a, b) == Ordering::Less
            },
        )
    }

    pub fn durable_root(&self) -> String {
        use durable_tree::Link;

        match self.durable_index.root {
            Link::DbKey(ref s) => s.clone(),
            _ => panic!("root reference has a boxed pointer"),
        }
    }

    pub fn insert(&self, item: T) -> Index<T, C> {
        Index {
            mem_index: self.mem_index.insert(item),
            ..self.clone()
        }
    }

    pub fn rebuild(&self) -> Index<T, C> {
        // FIXME: return a Result to avoid unwrapping
        Index {
            durable_index: DurableTree::build_from_iter(
                self.store.clone(),
                self.iter(),
                self._comparator,
            ).unwrap(),
            mem_index: RBTree::new(self._comparator),
            ..self.clone()
        }
    }
}



#[cfg(test)]
#[derive(Clone, Default, Copy)]
pub struct NumComparator;

#[cfg(test)]
impl Comparator for NumComparator {
    type Item = i64;

    fn compare(a: &i64, b: &i64) -> Ordering {
        a.cmp(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use itertools::assert_equal;
    use backends::mem::HeapStore;

    #[test]
    fn test_rebuild() {
        use durable_tree::{Node, InteriorNode};

        let store = Arc::new(HeapStore::new::<i64>());
        let ns: NodeStore<i64> = NodeStore::new(store.clone());
        let root_node = InteriorNode {
            keys: vec![],
            links: vec![],
        };
        let root_ref = ns.add_node(&Node::Interior(root_node)).unwrap();
        let mut index = Index::new(root_ref, store, NumComparator);

        for i in 0..1000 {
            index = index.insert(i);
        }

        assert_equal(index.iter(), index.rebuild().iter());
    }
}
