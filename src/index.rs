use std::rc::Rc;
use std::fmt::Debug;
use std::ops::RangeFrom;

use btree::{Node, Insertion, Iter, IterState, iter_range_start};

#[derive(Clone, Debug)]
pub struct Index<T: Ord + Clone + Debug> {
    root: Rc<HeapNode<T>>,
}

impl<T: Ord + Clone + Debug> Index<T> {
    pub fn new() -> Index<T> {
        Index { root: Rc::new(HeapNode::Leaf { keys: vec![] }) }
    }

    pub fn insert(&self, item: T) -> Index<T> {
        match self.root.insert(item.clone()) {
            Insertion::Duplicate => self.clone(),
            Insertion::Inserted(new_root) => Index { root: Rc::new(new_root) },
            Insertion::NodeFull => {
                // Need to make a new root; the whole tree is full.
                let (left, sep, right) = self.root.split();
                let new_root_links = vec![Rc::new(left), Rc::new(right)];
                let new_root_keys = vec![sep];

                let new_root = HeapNode::Directory {
                    links: new_root_links,
                    keys: new_root_keys,
                };

                match new_root.insert(item) {
                    Insertion::Inserted(root) => Index { root: Rc::new(root) },
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

    pub fn iter_range_from(&self, range: RangeFrom<T>) -> Iter<HeapNode<T>> {
        iter_range_start(self.root.clone(), range)
    }
}

#[derive(Debug)]
pub enum HeapNode<T: Ord + Clone + Debug> {
    Directory {
        keys: Vec<T>,
        links: Vec<Rc<HeapNode<T>>>,
    },
    Leaf { keys: Vec<T> },
}

// When a node is cloned, we need to make sure that cloned vectors
// preserve their capacities again.
// FIXME: Sharing references (Arcs?) to vectors instead of cloning them might
// render this unnecessary.
impl<T: Clone + Ord + Debug> Clone for HeapNode<T> {
    fn clone(&self) -> HeapNode<T> {
        match self {
            &HeapNode::Leaf { ref keys } => HeapNode::Leaf { keys: keys.clone() },
            &HeapNode::Directory {
                ref keys,
                ref links,
            } => {
                HeapNode::Directory {
                    keys: keys.clone(),
                    links: links.clone()
                }
            }
        }
    }
}

impl<T: Ord + Clone + Debug> Node for HeapNode<T> {
    type Item = T;
    type Reference = Rc<HeapNode<T>>;

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

    fn save(self) -> Self::Reference {
        Rc::new(self)
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

    fn by_ref(reference: &Rc<HeapNode<T>>) -> Self {
        (**reference).clone()
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
