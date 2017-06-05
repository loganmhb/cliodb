use std::rc::Rc;
use std::fmt::Debug;
use std::ops::RangeFrom;

use btree::{Node, Insertion};

#[derive(Clone, Debug)]
pub struct Index<T: Ord + Clone + Debug> {
    root: HeapNode<T>,
}

impl<T: Ord + Clone + Debug> Index<T> {
    pub fn new() -> Index<T> {
        Index { root: HeapNode::Leaf { keys: vec![] } }
    }

    pub fn insert(&self, item: T) -> Index<T> {
        match self.root.insert(item.clone()) {
            Insertion::Duplicate => self.clone(),
            Insertion::Inserted(new_root) => Index { root: new_root },
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
                    Insertion::Inserted(root) => Index { root: root },
                    _ => unreachable!(),
                }
            }
        }
    }

    pub fn iter(&self) -> Iter<T> {
        let mut stack = Vec::new();
        stack.push(IterState {
                       node: &self.root,
                       link_idx: 0,
                       key_idx: 0,
                   });
        Iter { stack: stack }
    }

    pub fn iter_range_from(&self, range: RangeFrom<T>) -> Iter<T> {
        let mut stack = vec![
            IterState {
                node: &self.root,
                link_idx: 0,
                key_idx: 0,
            },
        ];

        // search for range.start
        loop {
            let state = stack.last().unwrap().clone();
            match state {
                IterState { node: &HeapNode::Leaf { ref keys }, .. } => {
                    match keys.binary_search(&range.start) {
                        Ok(idx) | Err(idx) => {
                            *stack.last_mut().unwrap() = IterState {
                                key_idx: idx,
                                ..state
                            };
                            return Iter { stack };
                        }
                    }
                }
                IterState {
                    node: &HeapNode::Directory {
                        ref keys,
                        ref links,
                    },
                    ..
                } => {
                    match keys.binary_search(&range.start) {
                        Ok(idx) => {
                            *stack.last_mut().unwrap() = IterState {
                                key_idx: idx,
                                link_idx: idx + 1,
                                ..state
                            };
                            return Iter { stack };
                        }
                        Err(idx) => {
                            *stack.last_mut().unwrap() = IterState {
                                key_idx: idx,
                                link_idx: idx+1,
                                ..state
                            };
                            stack.push(IterState {
                                           node: &links[idx],
                                           key_idx: 0,
                                           link_idx: 0,
                                       });
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
enum HeapNode<T: Ord + Clone + Debug> {
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

    fn child(&self, idx: usize) -> Self {
        match self {
            &HeapNode::Directory { ref links, .. } => (*links[idx]).clone(),
            _ => unimplemented!()
        }
    }
}

#[derive(Copy, Clone, Debug)]
struct IterState<'a, T: 'a + Ord + Clone + Debug> {
    node: &'a HeapNode<T>,
    link_idx: usize,
    key_idx: usize,
}

#[derive(Clone, Debug)]
pub struct Iter<'a, T: 'a + Ord + Clone + Debug> {
    stack: Vec<IterState<'a, T>>,
}

impl<'a, T: Ord + Clone + Debug> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let context = match self.stack.pop() {
                Some(frame) => frame,
                None => return None,
            };

            match context {
                IterState {
                    node: &HeapNode::Leaf { ref keys },
                    link_idx,
                    key_idx,
                } => {
                    if key_idx < keys.len() {
                        let res = &keys[key_idx];
                        self.stack
                            .push(IterState {
                                      node: context.node,
                                      link_idx,
                                      key_idx: key_idx + 1,
                                  });
                        return Some(res);
                    } else {
                        continue; // keep looking for a stack frame that will yield something
                    }
                }
                IterState {
                    node: &HeapNode::Directory {
                        ref links,
                        ref keys,
                    },
                    link_idx,
                    key_idx,
                } => {
                    // If link idx == key idx, push the child and continue.
                    // otherwise, yield the key idx and bump it.
                    if link_idx == key_idx {
                        self.stack
                            .push(IterState {
                                      node: context.node,
                                      link_idx: link_idx + 1,
                                      key_idx,
                                  });
                        self.stack
                            .push(IterState {
                                      node: &*links[link_idx],
                                      link_idx: 0,
                                      key_idx: 0,
                                  });
                        continue;
                    } else if key_idx < keys.len() {
                        let res = &keys[key_idx];
                        self.stack
                            .push(IterState {
                                      node: context.node,
                                      link_idx,
                                      key_idx: key_idx + 1,
                                  });
                        return Some(res);
                    } else {
                        // This node is done, so we don't re-push its stack frame.
                        continue;
                    }
                }
            }
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
        assert_eq!(idx.iter().cloned().collect::<Vec<_>>(),
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

        assert_equal(idx.iter_range_from(range.clone()).cloned(),
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
