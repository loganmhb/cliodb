use std::sync::Arc;
use std::fmt::Debug;
use std::ops::RangeFrom;
use std::collections::HashMap;
use std::marker::PhantomData;
use uuid::Uuid;

use btree::{Node, Insertion, Iter, IterState, KVStore, iter_range_start};

#[derive(Clone, Debug)]
pub struct Index<T> {
    root: String,
    phantom: PhantomData<T>
}

impl<T: Ord + Clone + Debug> Index<T> {
    pub fn new(store: &HeapStore<T>) -> Index<T> {
        let root = store.add(&HeapNode::Leaf { keys: vec![] }).unwrap();
        Index { root: root, phantom: PhantomData }
    }

    pub fn insert(&self, store: &HeapStore<T>, item: T) -> Index<T> {
        match store.get(&self.root).unwrap().insert(store, item.clone()) {
            Insertion::Duplicate => self.clone(),
            Insertion::Inserted(new_root) => {
                // FIXME: handle errors
                let root_ref = store.add(&new_root).unwrap();
                // FIXME: update the db contents
                Index { root: root_ref, phantom: PhantomData }
            }
            Insertion::NodeFull => {
                // Need to make a new root; the whole tree is full.
                // FIXME: error handling
                let (left, sep, right) = store.get(&self.root).unwrap().split();
                let left_ref = store.add(&left).unwrap();
                let right_ref = store.add(&right).unwrap();
                let new_root_links = vec![left_ref, right_ref];
                let new_root_keys = vec![sep];

                let new_root = HeapNode::Directory {
                    links: new_root_links,
                    keys: new_root_keys,
                };

                match new_root.insert(store, item) {
                    Insertion::Inserted(root) => Index {
                        root: store.add(&root).unwrap(),
                        phantom: PhantomData
                    },
                    _ => unreachable!(),
                }
            }
        }
    }

    pub fn iter(&self) -> Iter<HeapNode<T>> {
        let mut stack = Vec::new();
        stack.push(IterState {
            node_ref: self.root.clone(),
            link_idx: 0,
            item_idx: 0,
        });
        Iter { stack: stack }
    }

    pub fn iter_range_from(&self, store: HeapStore<T>, range: RangeFrom<T>) -> Iter<HeapNode<T>> {
        iter_range_start(store, self.root.clone(), range)
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum HeapNode<T: Ord + Clone + Debug> {
    Directory {
        keys: Vec<T>,
        links: Vec<String>,
    },
    Leaf { keys: Vec<T> },
}

#[derive(Debug)]
pub struct HeapStore<T: Debug + Ord + Clone>(HashMap<String, HeapNode<T>>);

impl<T: Debug + Clone + Ord> Default for HeapStore<T> {
    fn default() -> HeapStore<T> {
        HeapStore(HashMap::default())
    }
}
impl<T: Debug + Ord + Clone> KVStore for HeapStore<T> {
    type Key = String;
    type Value = HeapNode<T>;
    type Error = String;

    fn get(&self, key: &Self::Key) -> Result<Self::Value, Self::Error> {
        self.0.get(key).map(|v| v.clone()).ok_or("Ref not found in store!".into())
    }

    fn add(&self, value: &Self::Value) -> Result<Self::Key, Self::Error> {
        let k = Uuid::new_v4().to_string();
        self.0.insert(k, value.clone()).map(|_| k).ok_or("Ref could not be added to store".into())
    }
}

impl<T: Ord + Clone + Debug> Node for HeapNode<T> {
    type Item = T;
    type Reference = String;
    type Store = HeapStore<T>;

    fn size(&self) -> usize {
        match self {
            &HeapNode::Leaf { ref keys, .. } => keys.len(),
            &HeapNode::Directory { ref keys, .. } => keys.len()
        }
    }

    fn items(&self) -> &[Self::Item] {
        match self {
            &HeapNode::Leaf { ref keys, .. } => keys,
            &HeapNode::Directory { ref keys, .. } => keys
        }
    }

    fn links(&self) -> &[Self::Reference] {
        match self {
            &HeapNode::Directory { ref links, .. } => links,
            _ => unimplemented!()
        }
    }

    fn save(&self, store: &Self::Store) -> Self::Reference {
        store.add(self).unwrap()
    }

    fn is_leaf(&self) -> bool {
        match self {
            &HeapNode::Leaf { .. } => true,
            &HeapNode::Directory { .. } => false
        }
    }

    fn new_leaf(items: Vec<Self::Item>) -> Self {
        HeapNode::Leaf { keys: items }
    }

    fn new_dir(items: Vec<Self::Item>, links: Vec<Self::Reference>) -> Self {
        HeapNode::Directory {
            keys: items,
            links: links
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate test;
    use self::test::Bencher;

    use itertools::*;
    use super::*;

    fn enumerate_node<T: Clone + Ord + ::std::fmt::Debug>(node: &HeapNode<T>) -> Vec<T> {
        match node {
            &HeapNode::Leaf { ref keys } => keys.clone(),
            &HeapNode::Directory {
                ref links,
                ref keys,
            } => {
                let mut result = vec![];
                for i in 0..keys.len() {
                    result.extend_from_slice(&enumerate_node(&links[i]));
                    result.push(keys[i].clone());
                }

                result.extend_from_slice(&enumerate_node(&links[keys.len()]));
                result
            }
        }
    }

    #[test]
    fn test_leaf_insert() {
        let mut idx: Index<u64> = Index::new();
        let range: ::std::ops::Range<u64> = 0..(16 * 16 + 1);
        for i in range {
            idx = idx.insert(i);
        }

        assert_eq!(enumerate_node(&idx.root),
                   (0..(16 * 16 + 1)).collect::<Vec<_>>());
    }

    #[test]
    fn test_tree_iter() {
        let mut idx: Index<usize> = Index::new();
        let range = 0..65535;
        for i in range.clone().rev().collect::<Vec<_>>() {
            idx = idx.insert(i);
        }
        assert_eq!(idx.iter().collect::<Vec<_>>(),
                   range.collect::<Vec<usize>>());
    }

    #[test]
    fn test_range_iter() {
        let mut idx = Index::new();
        let full_range = 0usize..10_000;
        let range = 1457usize..;

        for i in full_range.clone() {
            idx = idx.insert(i);
        }

        assert_equal(idx.iter_range_from(range.clone()),
                     range.start..full_range.end);
    }

    #[bench]
    fn bench_insert_sequence(b: &mut Bencher) {
        let mut tree = Index::new();
        let mut n = 0usize;
        b.iter(|| {
                   tree = tree.insert(n);
                   n += 1;
               });
    }

    #[bench]
    fn bench_insert_range(b: &mut Bencher) {
        let mut tree = Index::new();
        let mut n = 0usize;
        b.iter(|| {
                   tree = tree.insert(n);
                   n = (n + 1) % 512;
               });

    }

    #[bench]
    fn bench_iter(b: &mut Bencher) {
        let n = 10_000;
        let mut tree = Index::new();
        for i in 0..n {
            tree = tree.insert(i);
        }

        let mut iter = tree.iter();

        b.iter(|| if let None = iter.next() {
                   iter = tree.iter();
               });
    }
}
