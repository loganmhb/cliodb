use serde::{Serialize, Deserialize};

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::RangeFrom;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use backends::KVStore;
use db;
use Result;

pub const CAPACITY: usize = 512;

pub trait Comparator: Clone {
    type Item;
    fn compare(a: &Self::Item, b: &Self::Item) -> Ordering;
}

#[derive(Clone)]
pub struct Index<T: Debug + Ord + Clone, C: Comparator<Item = T>> {
    store: NodeStore<T>,
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

impl<'de, T, C> Index<T, C>
    where T: Debug + Ord + Clone + Serialize + Deserialize<'de>,
          C: Comparator<Item = T>
{
    pub fn new(root_ref: String, store: NodeStore<T>, comparator: C) -> Self {
        Index {
            store,
            root_ref,
            comparator,
        }
    }

    pub fn insert(&self, item: T) -> Result<Index<T, C>> {
        let new_root =
            self.store
                .get_node(&self.root_ref)
                .and_then(|root| root.insert(item.clone(), &self.store, &self.comparator));

        match new_root {
            Ok(Insertion::Inserted(root)) => {
                let root_ref = self.store.add_node(root)?;
                Ok(Index {
                       root_ref,
                       comparator: self.comparator.clone(),
                       store: self.store.clone(),
                   })
            }

            Ok(Insertion::Duplicate) => Ok((*self).clone()),

            Ok(Insertion::NodeFull) => {
                // Need to split the root and create a new one.
                let root = self.store.get_node(&self.root_ref)?;

                let (left, sep, right) = root.split();
                let left_ref = self.store.add_node(left)?;
                let right_ref = self.store.add_node(right)?;

                let new_root_links = vec![left_ref, right_ref];
                let new_root_items = vec![sep];

                let new_root = IndexNode::Dir {
                    links: new_root_links,
                    items: new_root_items,
                };

                match new_root.insert(item, &self.store, &self.comparator)? {
                    Insertion::Inserted(root) => {
                        let root_ref = self.store.add_node(root)?;
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

    pub fn iter(&self) -> Iter<T> {
        Iter {
            phantom: PhantomData,
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
    pub fn iter_range_from(&self, range: RangeFrom<T>) -> Result<Iter<T>> {
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

            let node = self.store.get_node(&state.node_ref)?;

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
                                          phantom: PhantomData,
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
                                          phantom: PhantomData,
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
                                          phantom: PhantomData,
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

impl<'de, T> IndexNode<T>
    where T: Debug + Ord + Clone + Serialize + Deserialize<'de>
{
    fn insert<C>(&self,
                 item: T,
                 store: &NodeStore<T>,
                 comparator: &C)
                 -> Result<Insertion<IndexNode<T>>>
        where C: Comparator<Item = T>
    {
        use self::IndexNode::{Leaf, Dir};

        match self {
            &Leaf { ref items } => {
                let idx = match items.binary_search_by(|other| C::compare(other, &item)) {
                    Ok(_) => return Ok(Insertion::Duplicate),
                    Err(idx) => idx
                };

                if items.len() < CAPACITY {
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

                let child = store.get_node(&links[idx])?;
                let child_result = child.insert(item.clone(), store, comparator)?;

                match child_result {
                    Insertion::Duplicate => Ok(Insertion::Duplicate),
                    Insertion::Inserted(new_child) => {
                        let mut new_links = links.clone();
                        new_links[idx] = store.add_node(new_child)?;

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

                            let left_ref = store.add_node(left)?;
                            let right_ref = store.add_node(right)?;

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

#[derive(Clone)]
pub struct Iter<T: Ord + Debug + Clone> {
    store: NodeStore<T>,
    phantom: PhantomData<T>,
    pub stack: Vec<IterState>,
}

impl<'de, T: Debug + Ord + Clone + Deserialize<'de>> Iterator for Iter<T> {
    type Item = Result<T>;

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

            let node: IndexNode<T> = match self.store.get_node(&node_ref) {
                Ok(n) => n,
                Err(e) => return Some(Err(e)),
            };

            match node {
                IndexNode::Leaf { items } => {
                    if item_idx < items.len() {
                        let res: Result<T> = Ok(items.get(item_idx).unwrap().clone());
                        self.stack
                            .push(IterState {
                                      node_ref,
                                      link_idx,
                                      item_idx: item_idx + 1,
                                  });
                        return Some(res);
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

// FIXME: NodeStore is an awkward solution which could probably be avoided with
// lifetimes on Iter references to a KVStore
#[derive(Clone)]
pub struct NodeStore<T> {
    pub backing_store: Arc<KVStore + 'static>,
    cache: Arc<Mutex<HashMap<String, IndexNode<T>>>>,
}

impl<T: Debug> NodeStore<T> {
    /// Generates a unique ID and stores the given node at that ID,
    /// returning the ID if succsessful.
    pub fn add_node(&self, node: IndexNode<T>) -> Result<String>
        where T: Serialize
    {
        let node = db::add_node(&(*self.backing_store), node);
        node
    }

    /// Fetches and deserializes the node with the given key.
    fn get_node<'de>(&self, key: &str) -> Result<IndexNode<T>>
        where T: Deserialize<'de> + Clone
    {
        let store = &self.backing_store;
        let mut hm = self.cache.lock().unwrap();
        let node = hm.entry(key.to_string())
            .or_insert_with(|| db::get_node(&(**store), key).unwrap());
        Ok(node.clone())
    }

    pub fn new(store: Arc<KVStore>) -> NodeStore<T> {
        NodeStore {
            backing_store: store,
            cache: Arc::new(Mutex::new(HashMap::default())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::assert_equal;

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

    fn test_idx() -> Index<u64, NumComparator> {
        let ns: NodeStore<u64> = NodeStore::new(Arc::new(HeapStore::new::<u64>()));
        let root: IndexNode<u64> = IndexNode::Leaf { items: vec![] };
        let root_ref = ns.add_node(root).unwrap();
        Index::new(root_ref, ns, NumComparator)
    }

    #[test]
    fn test_leaf_insert() {
        let mut idx = test_idx();
        let range: ::std::ops::Range<u64> = 0..(16 * 16 + 1);
        for i in range {
            idx = idx.insert(i).unwrap();
        }
    }

    #[test]
    fn test_tree_iter() {
        let mut idx = test_idx();
        let range = 0..4096;
        for i in range.clone().rev().collect::<Vec<_>>() {
            idx = idx.insert(i).unwrap();
        }

        assert_eq!(idx.iter().map(|x| x.unwrap()).collect::<Vec<_>>(),
                   range.collect::<Vec<u64>>());
    }

    #[test]
    fn test_range_iter() {
        let mut idx = test_idx();

        let full_range = 0u64..10_000;
        let range = 1457u64..;

        for i in full_range.clone() {
            idx = idx.insert(i).unwrap();
        }

        // yuck
        assert_equal(idx.iter_range_from(range.clone())
                         .unwrap()
                         .map(|item| item.unwrap()),
                     range.start..full_range.end);
    }

    #[bench]
    fn bench_insert_sequence(b: &mut Bencher) {
        let mut tree = test_idx();
        let mut n = 0;
        b.iter(|| {
                   tree = tree.insert(n).unwrap();
                   n += 1;
               });
    }

    #[bench]
    fn bench_insert_range(b: &mut Bencher) {
        let mut tree = test_idx();

        let mut n = 0;
        b.iter(|| {
                   tree = tree.insert(n).unwrap();
                   n = (n + 1) % 512;
               });

    }

    #[bench]
    fn bench_iter(b: &mut Bencher) {
        let mut tree = test_idx();
        let n = 10_000;
        for i in 0..n {
            tree = tree.insert(i).unwrap();
        }

        let mut iter = tree.iter();

        b.iter(|| if let None = iter.next() {
                   iter = tree.iter();
               });
    }
}
