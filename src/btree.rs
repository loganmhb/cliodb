use std::cmp::Ordering;
use std::fmt::Debug;
use std::ops::RangeFrom;
use backends::KVStore;

pub const CAPACITY: usize = 512;

pub trait Comparator : Clone {
    type Item;
    fn compare(a: &Self::Item, b: &Self::Item) -> Ordering;
}

#[derive(Clone)]
pub struct Index<T: Debug + Ord + Clone, S: KVStore<Item = T>, C: Comparator<Item=T>> {
    store: S,
    pub root_ref: String,
    comparator: C,
}


pub enum Insertion<N> {
    Inserted(N),
    Duplicate,
    NodeFull,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Serialize, Deserialize)]
pub enum IndexNode<T> {
    Dir { items: Vec<T>, links: Vec<String> },
    Leaf { items: Vec<T> },
}

impl<T, S, C> Index<T, S, C>
    where T: Debug + Ord + Clone, S: KVStore<Item=T>, C: Comparator<Item=T>
{
    pub fn new(root_ref: String, store: S, comparator: C) -> Result<Self, String> {
        Ok(Index { store, root_ref, comparator })
    }

    pub fn insert(&self, item: T) -> Result<Index<T, S, C>, String> {
        let new_root = self.store
            .get(&self.root_ref)
            .and_then(|root| root.insert(item.clone(), &self.store, &self.comparator));

        match new_root {
            Ok(Insertion::Inserted(root)) => {
                let root_ref = self.store.add(root)?;
                Ok(Index {
                    root_ref,
                    comparator: self.comparator.clone(),
                    store: self.store.clone(),
                })
            }

            Ok(Insertion::Duplicate) => Ok((*self).clone()),

            Ok(Insertion::NodeFull) => {
                // Need to split the root and create a new one.
                let root = self.store.get(&self.root_ref)?;

                let (left, sep, right) = root.split();
                let left_ref = self.store.add(left)?;
                let right_ref = self.store.add(right)?;

                let new_root_links = vec![left_ref, right_ref];
                let new_root_items = vec![sep];

                let new_root = IndexNode::Dir {
                    links: new_root_links,
                    items: new_root_items,
                };

                match new_root.insert(item, &self.store, &self.comparator)? {
                    Insertion::Inserted(root) => {
                        let root_ref = self.store.add(root)?;
                        Ok(Index {
                            store: self.store.clone(),
                            comparator: self.comparator.clone(),
                            root_ref,
                        })
                    }
                    _ => unreachable!(),
                }

            }
            Err(e) => Err(e),
        }
    }

    pub fn iter(&self) -> Iter<T, S> {
        Iter {
            store: self.store.clone(),
            stack: vec![
                IterState {
                    node_ref: self.root_ref.clone(),
                    link_idx: 0,
                    item_idx: 0,
                },
            ],
        }
    }

    // FIXME: Better would be to have this return either the Iter or,
    // if the store causes an error, to yield the error as the first iterator item.
    pub fn iter_range_from(&self, range: RangeFrom<T>) -> Result<Iter<T, S>, String> {
        let mut stack = vec![
            IterState {
                node_ref: self.root_ref.clone(),
                link_idx: 0,
                item_idx: 0,
            },
        ];

        // Search for the beginning of the range.
        loop {
            let state = stack.pop().unwrap();

            let node = self.store.get(&state.node_ref)?;

            match node {
                IndexNode::Leaf { items } => {
                    match items.binary_search_by(|other| C::compare(other, &range.start)) {
                        Ok(idx) => {
                            stack.push(IterState {
                                           item_idx: idx,
                                           link_idx: idx + 1,
                                           ..state
                                       });
                            return Ok(Iter {
                                          stack,
                                          store: self.store.clone(),
                                      });
                        }
                        Err(idx) => {
                            stack.push(IterState {
                                           item_idx: idx,
                                           ..state
                                       });
                            return Ok(Iter {
                                          stack,
                                          store: self.store.clone(),
                                      });
                        }
                    }
                }
                IndexNode::Dir { items, links } => {
                    match items.binary_search_by(|other| C::compare(other, &range.start)) {
                        Ok(idx) => {
                            stack.push(IterState {
                                           item_idx: idx,
                                           link_idx: idx + 1,
                                           ..state
                                       });
                            return Ok(Iter {
                                          stack,
                                          store: self.store.clone(),
                                      });
                        }
                        Err(idx) => {
                            stack.push(IterState {
                                           item_idx: idx,
                                           link_idx: idx + 1,
                                           ..state
                                       });
                            stack.push(IterState {
                                           node_ref: links[idx].clone(),
                                           item_idx: 0,
                                           link_idx: 0,
                                       });
                        }
                    }
                }
            }
        }
    }
}

impl<T: Debug + Ord + Clone> IndexNode<T> {
    fn insert<S, C>(&self, item: T, store: &S, comparator: &C) -> Result<Insertion<IndexNode<T>>, String>
        where S: KVStore<Item=T>, C: Comparator<Item=T>
    {
        use self::IndexNode::{Leaf, Dir};

        match self {
            &Leaf { ref items } => {
                if items.len() < CAPACITY {
                    let idx = match items.binary_search_by(|other| C::compare(other, &item)) {
                        Ok(_) => return Ok(Insertion::Duplicate),
                        Err(idx) => idx,
                    };

                    let mut new_items = items.clone();
                    new_items.insert(idx, item);

                    Ok(Insertion::Inserted(Leaf { items: new_items }))
                } else {
                    Ok(Insertion::NodeFull)
                }
            }

            &Dir {
                ref items,
                ref links,
            } => {
                let idx = match items.binary_search_by(|other| C::compare(other, &item)) {
                    Ok(_) => return Ok(Insertion::Duplicate),
                    Err(idx) => idx,
                };

                let child = store.get(&links[idx])?;
                let child_result = child.insert(item.clone(), store, comparator)?;

                match child_result {
                    Insertion::Duplicate => Ok(Insertion::Duplicate),
                    Insertion::Inserted(new_child) => {
                        let mut new_links = links.clone();
                        new_links[idx] = store.add(new_child)?;

                        Ok(Insertion::Inserted(Dir {
                                                   items: items.clone(),
                                                   links: new_links,
                                               }))
                    }

                    Insertion::NodeFull => {
                        // The child node needs to be split, if there's space in this node's links.
                        if items.len() < CAPACITY {
                            let (left, sep, right) = child.split();

                            let mut new_items = items.clone();
                            let mut new_links = links.clone();

                            let left_ref = store.add(left)?;
                            let right_ref = store.add(right)?;

                            new_items.insert(idx, sep);
                            new_links[idx] = right_ref;
                            new_links.insert(idx, left_ref);

                            let dir = Dir {
                                items: new_items,
                                links: new_links,
                            };

                            match dir.insert(item, store, comparator)? {
                                Insertion::Inserted(new_dir) => Ok(Insertion::Inserted(new_dir)),
                                // If it's a dup we wouldn't have gotten NodeFull; since we just split
                                // we won't get NodeFull again. Therefore anything else is unreachable.
                                _ => unreachable!(),
                            }
                        } else {
                            // No room - the split needs to be propagated up.
                            Ok(Insertion::NodeFull)
                        }
                    }
                }
            }
        }
    }

    fn split(&self) -> (Self, T, Self) {
        use self::IndexNode::{Dir, Leaf};

        match self {
            &Leaf { ref items } => {
                let split_idx = items.len() / 2;

                let (left_items, right_items_and_sep) = items.split_at(split_idx);
                let (sep, right_items) = right_items_and_sep.split_first().unwrap();

                let left = Leaf { items: left_items.to_vec() };
                let right = Leaf { items: right_items.to_vec() };

                (left, sep.clone(), right)
            }
            &Dir {
                ref items,
                ref links,
            } => {
                let split_idx = items.len() / 2;

                let (left_items, right_items_and_sep) = items.split_at(split_idx);
                let (sep, right_items) = right_items_and_sep.split_first().unwrap();

                let (left_links, right_links) = links.split_at(split_idx + 1);

                let left = Dir {
                    items: left_items.to_vec(),
                    links: left_links.to_vec(),
                };
                let right = Dir {
                    items: right_items.to_vec(),
                    links: right_links.to_vec(),
                };

                (left, sep.clone(), right)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct IterState {
    node_ref: String,
    link_idx: usize,
    item_idx: usize,
}

#[derive(Clone, Debug)]
pub struct Iter<T: Ord + Debug + Clone, S: KVStore<Item=T>> {
    store: S,
    pub stack: Vec<IterState>,
}

impl<T: Debug + Ord + Clone, S: KVStore<Item=T>> Iterator for Iter<T, S> {
    type Item = Result<T, String>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let IterState {
                node_ref,
                link_idx,
                item_idx,
                ..
            } = match self.stack.pop() {
                Some(frame) => frame,
                None => return None,
            };

            let node = match self.store.get(&node_ref) {
                Ok(n) => n,
                Err(e) => return Some(Err(e)),
            };
            match node {
                IndexNode::Leaf { items } => {
                    if item_idx < items.len() {
                        let res = items[item_idx].clone();
                        self.stack
                            .push(IterState {
                                      node_ref,
                                      link_idx,
                                      item_idx: item_idx + 1,
                                  });
                        return Some(Ok(res));
                    } else {
                        continue; // pop the frame and continue
                    }
                }
                IndexNode::Dir { items, links } => {
                    // If link idx == item idx, push the child and continue.
                    // otherwise, yield the item idx and bump it.
                    if link_idx == item_idx {
                        self.stack
                            .push(IterState {
                                      node_ref,
                                      link_idx: link_idx + 1,
                                      item_idx,
                                  });
                        self.stack
                            .push(IterState {
                                      node_ref: links[link_idx].clone(),
                                      link_idx: 0,
                                      item_idx: 0,
                                  });
                        continue;
                    } else if item_idx < items.len() {
                        let res = &items[item_idx];
                        self.stack
                            .push(IterState {
                                      node_ref,
                                      link_idx,
                                      item_idx: item_idx + 1,
                                  });
                        return Some(Ok(res.clone()));
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
    use super::*;
    use itertools::{assert_equal};

    extern crate test;
    use self::test::Bencher;
    use backends::mem::HeapStore;

    #[derive(Clone)]
    struct NumComparator;

    impl Comparator for NumComparator {
        type Item = u64;

        fn compare(a: &u64, b: &u64) -> Ordering {
            a.cmp(b)
        }
    }
    #[test]
    fn test_leaf_insert() {
        let store = HeapStore::new();
        let root_ref = store.add(IndexNode::Leaf { items: vec![]}).unwrap();
        let mut idx: Index<u64, HeapStore<u64>, NumComparator> =
            Index::new(root_ref, store, NumComparator).unwrap();
        let range: ::std::ops::Range<u64> = 0..(16 * 16 + 1);
        for i in range {
            idx = idx.insert(i).unwrap();
        }
    }

    #[test]
    fn test_tree_iter() {
        let store = HeapStore::new();
        let root_ref = store.add(IndexNode::Leaf { items: vec![]}).unwrap();
        let mut idx: Index<u64, HeapStore<u64>, NumComparator> =
            Index::new(root_ref, store, NumComparator).unwrap();
        let range = 0..65535;
        for i in range.clone().rev().collect::<Vec<_>>() {
            idx = idx.insert(i).unwrap();
        }
        assert_eq!(idx.iter().map(|x| x.unwrap()).collect::<Vec<_>>(),
                   range.collect::<Vec<u64>>());
    }

    #[test]
    fn test_range_iter() {
        let store = HeapStore::new();
        let root_ref = store.add(IndexNode::Leaf { items: vec![]}).unwrap();
        let mut idx = Index::new(root_ref, store, NumComparator).unwrap();
        let full_range = 0u64..10_000;
        let range = 1457u64..;

        for i in full_range.clone() {
            idx = idx.insert(i).unwrap();
        }

        // yuck
        assert_equal(idx.iter_range_from(range.clone()).unwrap().map(|item| item.unwrap()),
                     range.start..full_range.end);
    }

    #[bench]
    fn bench_insert_sequence(b: &mut Bencher) {
        let store = HeapStore::new();
        let root_ref = store.add(IndexNode::Leaf { items: vec![]}).unwrap();
        let mut tree = Index::new(root_ref, store, NumComparator).unwrap();
        let mut n = 0;
        b.iter(|| {
                   tree = tree.insert(n).unwrap();
                   n += 1;
               });
    }

    #[bench]
    fn bench_insert_range(b: &mut Bencher) {
        let store = HeapStore::new();
        let root_ref = store.add(IndexNode::Leaf { items: vec![]}).unwrap();
        let mut tree = Index::new(root_ref, store, NumComparator).unwrap();
        let mut n = 0;
        b.iter(|| {
                   tree = tree.insert(n).unwrap();
                   n = (n + 1) % 512;
               });

    }

    #[bench]
    fn bench_iter(b: &mut Bencher) {
        let n = 10_000;
        let store = HeapStore::new();
        let root_ref = store.add(IndexNode::Leaf { items: vec![]}).unwrap();
        let mut tree = Index::new(root_ref, store, NumComparator).unwrap();
        for i in 0..n {
            tree = tree.insert(i).unwrap();
        }

        let mut iter = tree.iter();

        b.iter(|| if let None = iter.next() {
                   iter = tree.iter();
               });
    }
}
