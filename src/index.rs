use std::cmp::Ordering;
use std::fmt::Debug;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use itertools::Itertools;

use backends::KVStore;
use durable_tree::{DurableTree};
use rbtree::RBTree;

pub trait Comparator: Copy + Debug {
    type Item;
    fn compare(a: &Self::Item, b: &Self::Item) -> Ordering;
}

/// The Equivalent trait is used to deduplicate facts in the
/// database. It is like Eq, but makes no guarantees about the
/// relationship between equivalence and ordering (i.e. A can be less
/// than B and also equivalent to it).
pub trait Equivalent {
    fn equivalent(&self, other: &Self) -> bool;
}

#[derive(Clone)]
pub struct Index<T, C>
where
    T: Equivalent + Debug + Ord + Clone,
    C: Comparator<Item = T>,
{
    mem_index: RBTree<T, C>,
    _comparator: C,
    durable_index: DurableTree<T, C>,
}

impl<T, C> Index<T, C>
where
    T: Equivalent + Debug + Ord + Clone + Serialize + DeserializeOwned,
    C: Comparator<Item = T> + Copy,
{
    pub fn new(root_ref: String, store: Arc<dyn KVStore>, comparator: C) -> Index<T, C> {
        Index {
            _comparator: comparator,
            mem_index: RBTree::new(comparator),
            durable_index: DurableTree::from_ref(root_ref, store, comparator),
        }
    }

    pub fn mem_index_size(&self) -> usize {
        self.mem_index.size()
    }

    pub fn range_from(&self, range_start: T) -> impl Iterator<Item = T> {
        self.mem_index.range_from(range_start.clone()).merge_by(
            self.durable_index
                .range_from(range_start)
                // FIXME: handle all these errors
                .unwrap()
                .map(|r| r.unwrap())
                // deduplicate equivalent facts which may be in both the in-memory and durable index
                .coalesce(|x, y| { if x.equivalent(&y) { Ok(x) } else { Err((x, y))} }),
            |a, b| C::compare(a, b) == Ordering::Less,
        )
    }

    pub fn durable_root(&self) -> String {
        self.durable_index.root.clone()
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

    pub fn insert(&self, item: T) -> Index<T, C> {
        Index {
            mem_index: self.mem_index.insert(item),
            ..self.clone()
        }
    }

    pub fn rebuild(&self) -> Index<T, C> {
        // FIXME: return a Result to avoid unwrapping
        Index {
            durable_index: self.durable_index.rebuild_with_novelty(
                self.mem_index.iter()
            ).expect("error rebuilding durable index"),
            mem_index: RBTree::new(self._comparator),
            ..self.clone()
        }
    }
}



#[cfg(test)]
#[derive(Clone, Default, Copy, Debug)]
pub struct NumComparator;

#[cfg(test)]
impl Comparator for NumComparator {
    type Item = i64;

    fn compare(a: &i64, b: &i64) -> Ordering {
        a.cmp(b)
    }
}

#[cfg(test)]
impl Equivalent for i64 {
    fn equivalent(&self, other: &i64) -> bool {
        self.eq(other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use itertools::assert_equal;
    use backends::sqlite::SqliteStore;
    use durable_tree::{DurableTree};

    #[test]
    fn test_rebuild() {
        let store = Arc::new(SqliteStore::new(":memory:").unwrap());
        let root_ref = DurableTree::create(store.clone(), NumComparator).unwrap().root;
        let mut index = Index::new(root_ref, store, NumComparator);

        for i in 0..1000 {
            index = index.insert(i);
        }

        let rebuilt = index.rebuild();
        assert_equal(index.iter(), rebuilt.iter());
    }

    #[test]
    fn test_deduplication() {
        let store = Arc::new(SqliteStore::new(":memory:").unwrap());
        let root_ref = DurableTree::create(store.clone(), NumComparator).unwrap().root;
        let index = Index::new(root_ref, store, NumComparator)
            .insert(1)
            .insert(2)
            .insert(2)
            .insert(3);

        assert_equal(index.range_from(1), 1..4)
    }
}
