use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use itertools::Itertools;
use lru_cache::LruCache;
use serde::{Serialize, Deserialize};
use rmp_serde::{Serializer, Deserializer};
use uuid::Uuid;

use backends::KVStore;
use index::Comparator;
use Result;

///! This module defines a data structure for storing facts in the
///! backing store. It is intended to be constructed once in a batch
///! operation and then used until enough facts have accumulated in
///! the log to justify a new index.
///!
///! The structure is a variant of a B-tree. All data is stored in the
///! leaf nodes; interior nodes only store pointers to leaves and keys
///! for determining which pointer to follow.
///!
///! The tree is constructed from an iterator over all the data to be
///! indexed. Leaves are serialized as soon as enough data points have
///! accumulated, while interior nodes are held in memory and updated
///! in place until all leaves have been created, at which point the
///! interior nodes are converted from "draft nodes" in memory to
///! durable nodes in the backing store.

const NODE_CAPACITY: usize = 1024;

/// A link to another node of the tree. This can be either a string
/// key for retrieving the node from the backing store, or a pointer
/// to the node in memory. The pointers are used only during the
/// construction of the index.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Serialize, Deserialize)]
enum Link<T> {
    Pointer(Box<Node<T>>),
    DbKey(String),
}

/// A node of the tree. Leaf nodes store data only. Interior nodes
/// store links to other nodes (leaf or interior) and keys to
/// determine which pointer to follow in order to find an item (but
/// each key in an interior node is duplicated in a leaf node).
///
/// An empty tree is represented by an empty directory node (a node
/// with zero leaves and zero links). Otherwise, the number of keys in
/// the directory node is always exactly one less than the number of
/// links.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Serialize, Deserialize)]
enum Node<T> {
    Leaf { items: Vec<T> },
    Interior { keys: Vec<T>, links: Vec<Link<T>> },
}

impl<'de, T> Node<T>
    where T: Serialize + Deserialize<'de> + Clone
{
    // FIXME: when the directory node reaches a certain size, split
    // and make a new parent
    fn add_leaf(&mut self, store: &mut NodeStore<T>, items: Vec<T>) -> Result<()> {
        match *self {
            Node::Leaf { .. } => panic!("add_leaf called on leaf node"),
            Node::Interior {
                ref mut keys,
                ref mut links,
            } => {
                let first_item = items[0].clone();
                let leaf = Node::Leaf { items };
                let leaf_link = Link::DbKey(store.add_node(&leaf)?);

                if links.len() == 0 {
                    // This is the first leaf.
                    links.push(leaf_link)
                } else {
                    // This is not the first leaf, so we need to add a
                    // key to determine which pointer to follow.
                    links.push(leaf_link);
                    keys.push(first_item);
                }

                Ok(())
            }
        }
    }

    /// Recursively persists the tree to the backing store, returning
    /// a string key referencing the root node.
    fn persist(self, store: &mut NodeStore<T>) -> Result<String> {
        match self {
            Node::Leaf { .. } => panic!("persist called on leaf node"),
            Node::Interior { links, keys } => {
                let mut new_links = vec![];
                for link in links {
                    match link {
                        Link::Pointer(ptr) => {
                            new_links.push(Link::DbKey(store.add_node(&ptr)?));
                        }
                        Link::DbKey(s) => {
                            // This happens when the link is to a leaf node.
                            new_links.push(Link::DbKey(s));
                        }
                    }
                }

                store.add_node(&Node::Interior {
                                   links: new_links,
                                   keys,
                               })
            }
        }
    }
}

struct DurableTree<T, C> {
    store: NodeStore<T>,
    root: Link<T>,
    _comparator: C
}

impl<'de, T, C> DurableTree<T, C>
    where T: Serialize + Deserialize<'de> + Clone,
          C: Comparator<Item = T>
{
    /// Builds the tree from an iterator by chunking it into an
    /// iterator of leaf nodes and then constructing the tree of
    /// directory nodes on top of that.
    fn build_from_iter<I>(mut store: NodeStore<T>, iter: I, _comparator: C)
                          -> DurableTree<T, C>
        where I: Iterator<Item = T>
    {
        let mut root: Node<T> = Node::Interior {
            keys: vec![],
            links: vec![],
        };

        let chunks = iter.chunks(NODE_CAPACITY);
        let leaf_item_vecs = chunks.into_iter().map(|chunk| chunk.collect::<Vec<_>>());

        for v in leaf_item_vecs {
            root.add_leaf(&mut store, v).unwrap();
        }

        let root_ref = root.persist(&mut store).unwrap();

        DurableTree {
            store: store,
            root: Link::DbKey(root_ref),
            _comparator
        }
    }

    fn iter(&self) -> Iter<T> {
        Iter {
            store: self.store.clone(),
            stack: vec![
                IterState {
                    node_ref: self.root.clone(),
                    link_idx: 0,
                    item_idx: 0,
                },
            ],
        }
    }

    fn range_from(&self, start: T) -> Result<Iter<T>> {
        let mut stack = vec![
            IterState {
                node_ref: self.root.clone(),
                link_idx: 0,
                item_idx: 0,
            },
        ];

        // Find the beginning of the range.
        loop {
            let state = stack.pop().unwrap();
            let node_ref = match state.node_ref {
                Link::Pointer(_) => unreachable!(),
                Link::DbKey(ref s) => s.clone(),
            };

            let node = self.store.get_node(&node_ref)?;

            match node {
                Node::Leaf { items } => {
                    match items.binary_search_by(|other| C::compare(other, &start)) {
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
                },
                Node::Interior { keys, links } => {
                    match keys.binary_search_by(|other| C::compare(other, &start)) {
                        Ok(idx) | Err(idx) => {
                            // If the key is found in an interior
                            // node, that means the actual item is the
                            // first one of the right child, so it
                            // doesn't actually make a difference if
                            // the key exists in this node or not.
                            stack.push(IterState {
                                item_idx: idx,
                                link_idx: idx + 1,
                                ..state
                            });
                            stack.push(IterState {
                                node_ref: links[idx].clone(),
                                item_idx: 0,
                                link_idx: 0
                            });
                        }
                    }
                }
            }
        }
    }
}

pub struct Iter<T> {
    store: NodeStore<T>,
    stack: Vec<IterState<T>>,
}

#[derive(Debug)]
struct IterState<T> {
    node_ref: Link<T>,
    link_idx: usize,
    item_idx: usize,
}

impl<'de, T> Iterator for Iter<T>
    where T: Clone + Deserialize<'de> + Serialize + Debug
{
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

            let db_ref = match node_ref {
                Link::DbKey(ref s) => s.clone(),
                Link::Pointer(_) => panic!("can't iterate using Link::Pointer"),
            };

            let node = match self.store.get_node(&db_ref) {
                Ok(n) => n,
                // FIXME: Re-push stack frame on error?
                Err(e) => return Some(Err(e)),
            };

            match node {
                Node::Leaf { ref items } => {
                    if item_idx < items.len() {
                        let item: &T = items.get(item_idx).unwrap();
                        let res: Self::Item = Ok(item.clone());
                        self.stack
                            .push(IterState {
                                      node_ref: node_ref,
                                      link_idx,
                                      item_idx: item_idx + 1,
                                  });
                        return Some(res);

                    }
                }
                Node::Interior { links, .. } => {
                    if link_idx < links.len() {
                        // Re-push own dir for later.
                        self.stack
                            .push(IterState {
                                      node_ref,
                                      link_idx: link_idx + 1,
                                      item_idx,
                                  });
                        // Push next child dir.
                        self.stack
                            .push(IterState {
                                      node_ref: links[link_idx].clone(),
                                      link_idx: 0,
                                      item_idx: 0,
                                  });
                        continue;
                    }
                }
            }
        }
    }
}

/// Structure to cache lookups into the backing store, avoiding both
/// network and deserialization overhead.
#[derive(Clone)]
pub struct NodeStore<T> {
    cache: Arc<Mutex<LruCache<String, Node<T>>>>,
    store: Arc<KVStore>,
}

impl<'de, T> NodeStore<T>
    where T: Serialize + Deserialize<'de> + Clone
{
    fn add_node(&self, node: &Node<T>) -> Result<String> {
        let mut buf = Vec::new();
        node.serialize(&mut Serializer::new(&mut buf))?;

        let key: String = Uuid::new_v4().to_string();
        self.store.set(&key, &buf)?;
        Ok(key)
    }

    /// Fetches and deserializes the node with the given key.
    fn get_node(&self, key: &str) -> Result<Node<T>> {
        let mut cache = self.cache.lock().unwrap();
        let res = cache.get_mut(key).map(|n| n.clone());
        match res {
            Some(node) => Ok(node.clone()),
            None => {
                println!("getting node: {}", key);
                let serialized = self.store.get(key)?;
                let mut de = Deserializer::new(&serialized[..]);
                let node: Node<T> = Deserialize::deserialize(&mut de)?;
                cache.insert(key.to_string(), node.clone());
                Ok(node.clone())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use backends::mem::HeapStore;
    use itertools::assert_equal;
    use index::NumComparator;

    fn test_tree<I: Clone + Iterator<Item = u64>>(iter: I) -> DurableTree<u64, NumComparator> {
        let store = Arc::new(HeapStore::new::<u64>());
        let node_store = NodeStore {
            cache: Arc::new(Mutex::new(LruCache::new(1024))),
            store: store.clone(),
        };

        DurableTree::build_from_iter(node_store.clone(), iter.clone(), NumComparator)
    }

    #[test]
    fn test_build_and_iter() {
        let iter = 0..10_000;
        let tree = test_tree(iter.clone());

        assert_equal(tree.iter().map(|r| r.unwrap()), iter);
    }

    #[test]
    fn test_range_from() {
        use std::ops::Range;

        let tree = test_tree(0..10_000);
        let first_range: Range<u64> = 500..10_000;
        assert_equal(tree.range_from(500).unwrap().map(|r| r.unwrap()), first_range);
        let second_range: Range<u64> = 8459..10_000;
        assert_equal(tree.range_from(8459).unwrap().map(|r| r.unwrap()), second_range);
     }

    // When new parents are implemented (comes into play circa 10e5-10e6 datoms) this should pass:
    // #[test]
    // #[ignore]
    // fn test_node_height() {
    //     let store = Arc::new(HeapStore::new::<u64>());
    //     let mut node_store = NodeStore {
    //         cache: LruCache::new(1024),
    //         store: store.clone(),
    //     };

    //     let iter = 0..10_000_000;
    //     let tree = DurableTree::build_from_iter(node_store.clone(), iter.clone());

    //     let root_ref = match tree.root {
    //         Link::DbKey(s) => s,
    //         _ => unreachable!(),
    //     };

    //     let root_node_links: Vec<Link<u64>> = match node_store.get_node(&root_ref).unwrap() {
    //         Node::Interior { links, .. } => links,
    //         _ => unreachable!(),
    //     };

    //     assert!(root_node_links.len() <= NODE_CAPACITY)
    // }
}
