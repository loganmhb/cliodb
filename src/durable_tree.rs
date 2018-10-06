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
pub enum Link<T> {
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
pub enum Node<T> {
    Leaf(LeafNode<T>),
    Interior(InteriorNode<T>),
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Serialize, Deserialize)]
pub struct LeafNode<T> {
    pub items: Vec<T>
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Serialize, Deserialize)]
pub struct InteriorNode<T> {
    pub keys: Vec<T>,
    pub links: Vec<Link<T>>
}

impl <'de, T> InteriorNode<T>
    where T: Serialize + Deserialize<'de> + Clone
{
    // FIXME: when the directory node reaches a certain size, split
    // and make a new parent
    fn add_leaf(&mut self, store: &mut NodeStore<T>, items: Vec<T>) -> Result<()> {
        let first_item = items[0].clone();
        let leaf = LeafNode { items };
        let leaf_link = Link::DbKey(store.add_node(&Node::Leaf(leaf))?);

        if self.links.len() == 0 {
            // This is the first leaf.
            self.links.push(leaf_link)
        } else {
            // This is not the first leaf, so we need to add a
            // key to determine which pointer to follow.
            self.links.push(leaf_link);
            self.keys.push(first_item);
        }

        Ok(())
    }

    /// Recursively persists the tree to the backing store, returning
    /// a string key referencing the root node.
    fn persist(self, store: &mut NodeStore<T>) -> Result<String> {
        let mut new_links = vec![];
        for link in self.links {
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

        store.add_node(&Node::Interior(InteriorNode {
            links: new_links,
            keys: self.keys,
        }))
    }
}

#[derive(Clone)]
pub struct DurableTree<T, C> {
    pub root: Link<T>,
    store: NodeStore<T>,
    _comparator: C,
}

impl<'de, T, C> DurableTree<T, C>
where
    T: Serialize + Deserialize<'de> + Clone + Debug,
    C: Comparator<Item = T>,
{
    /// Builds the tree from an iterator by chunking it into an
    /// iterator of leaf nodes and then constructing the tree of
    /// directory nodes on top of that.
    pub fn build_from_iter<I>(mut store: NodeStore<T>, iter: I, _comparator: C) -> DurableTree<T, C>
    where
        I: Iterator<Item = T>,
    {
        let mut root: InteriorNode<T> = InteriorNode {
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
            _comparator,
        }
    }

    pub fn from_ref(db_ref: String, node_store: NodeStore<T>, _comparator: C) -> DurableTree<T, C> {
        DurableTree {
            root: Link::DbKey(db_ref),
            store: node_store,
            _comparator,
        }
    }

    fn iter_leaves(&self) -> LeafIter<T> {
        LeafIter {
            store: self.store.clone(),
            stack: vec![LeafIterState {
                node_ref: self.root.clone(),
                link_idx: 0
            }]
        }
    }

    pub fn iter(&self) -> Result<ItemIter<T>> {
        ItemIter::from_leaves(self.iter_leaves(), 0)
    }

    pub fn range_from(&self, start: T) -> Result<ItemIter<T>> {
        println!("ranging from {:?}", start);
        let mut stack = vec![
            LeafIterState {
                node_ref: self.root.clone(),
                link_idx: 0,
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

            match *node {
                Node::Leaf(LeafNode { ref items }) => {
                    match items.binary_search_by(|other| C::compare(other, &start)) {
                        Ok(idx) => {
                            stack.push(LeafIterState {
                                link_idx: idx + 1,
                                ..state
                            });

                            println!("stack {:?}, index {:?}", stack, idx);
                            return ItemIter::from_leaves(
                                LeafIter { store: self.store.clone(), stack: stack },
                                idx
                            );
                        }
                        Err(idx) => {
                            return ItemIter::from_leaves(
                                LeafIter { stack, store: self.store.clone() },
                                idx
                            );
                        }
                    }
                }
                Node::Interior(InteriorNode {
                    ref keys,
                    ref links,
                }) => {
                    match keys.binary_search_by(|other| C::compare(other, &start)) {
                        Ok(idx) | Err(idx) => {
                            // If the key is found in an interior
                            // node, that means the actual item is the
                            // first one of the right child, so it
                            // doesn't actually make a difference if
                            // the key exists in this node or not.
                            if idx == 0 && links.len() == 0 {
                                // Hack: empty interior node only
                                // happens when the root is empty and
                                // there are no leaves.
                                // FIXME: Initialize the tree better to avoid this special case.
                                return Ok(ItemIter {
                                    leaves: LeafIter {
                                        stack,
                                        store: self.store.clone()
                                    },
                                    current_leaf: None,
                                    item_idx: 0,
                                });
                            }

                            stack.push(LeafIterState {
                                link_idx: idx + 1,
                                ..state
                            });
                            stack.push(LeafIterState {
                                node_ref: links[idx].clone(),
                                link_idx: 0,
                            });
                        }
                    }
                }
            }
        }
    }
}

struct LeafIter<T> {
    store: NodeStore<T>,
    stack: Vec<LeafIterState<T>>,
}

#[derive(Debug)]
struct LeafIterState<T> {
    node_ref: Link<T>,
    link_idx: usize,
}

impl<'de, T> Iterator for LeafIter<T>
where T: Clone + Deserialize<'de> + Serialize + Debug,
{
    type Item = Result<LeafNode<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        println!("iteration");
        loop {
            println!("stack {:?}", self.stack);
            let LeafIterState { node_ref, link_idx } = match self.stack.pop() {
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

            match *node {
                Node::Leaf(ref leaf) => {
                    // FIXME(perf): should not be necessary to clone the node
                    println!("leaf starting with {:?}", leaf.items[0]);
                    return Some(Ok(leaf.clone()));
                }
                Node::Interior(InteriorNode { ref links, .. }) => {
                    if links.len() == 0 {
                        // Special case: empty root node
                        return None;
                    }
                    println!("link {:?}", link_idx);
                    let next_link_idx = link_idx + 1;
                    if next_link_idx < links.len() {
                        // Re-push own dir for later.
                        self.stack.push(LeafIterState {
                            node_ref,
                            link_idx: next_link_idx,
                        });
                    }
                    // Push next child node and keep looking for leaves.
                    self.stack.push(LeafIterState {
                        node_ref: links[link_idx].clone(),
                        link_idx: 0,
                    });
                    continue;
                }
            }

        }
    }

}

pub struct ItemIter<T>
{
    leaves: LeafIter<T>,
    current_leaf: Option<LeafNode<T>>,
    item_idx: usize,
}

impl<'de, T> ItemIter<T> where T: Clone + Deserialize<'de> + Serialize + Debug {
    fn from_leaves(mut leaves: LeafIter<T>, idx_in_leaf: usize) -> Result<ItemIter<T>> {
        let first_leaf = match leaves.next() {
            Some(Ok(leaf)) => Some(leaf),
            Some(Err(e)) => return Err(e),
            None => None,
        };
        return Ok(ItemIter {
            leaves,
            current_leaf: first_leaf,
            item_idx: idx_in_leaf,
        })
    }
}

impl<'de, T> Iterator for ItemIter<T>
where T: Clone + Deserialize<'de> + Serialize + Debug,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let items = match self.current_leaf.clone() {
            Some(LeafNode { items }) => items,
            None => return None,
        };

        if self.item_idx < items.len() {
            let item = items[self.item_idx].clone();
            self.item_idx += 1;
            return Some(Ok(item));
        } else {
            self.current_leaf = match self.leaves.next() {
                Some(Ok(leaf)) => Some(leaf),
                Some(Err(e)) => return Some(Err(e)),
                None => None,
            };
            self.item_idx = 0;
            return self.next();
        }
    }
}

/// Structure to cache lookups into the backing store, avoiding both
/// network and deserialization overhead.
#[derive(Clone)]
pub struct NodeStore<T> {
    cache: Arc<Mutex<LruCache<String, Arc<Node<T>>>>>,
    store: Arc<KVStore>,
}

impl<'de, T> NodeStore<T>
where
    T: Serialize + Deserialize<'de> + Clone,
{
    pub fn new(store: Arc<KVStore>) -> NodeStore<T> {
        NodeStore {
            // TODO make size configurable
            cache: Arc::new(Mutex::new(LruCache::new(1024))),
            store: store,
        }
    }

    pub fn add_node(&self, node: &Node<T>) -> Result<String> {
        let mut buf = Vec::new();
        node.serialize(&mut Serializer::new(&mut buf))?;

        let key: String = Uuid::new_v4().to_string();
        self.store.set(&key, &buf)?;
        Ok(key)
    }

    /// Fetches and deserializes the node with the given key.
    fn get_node(&self, key: &str) -> Result<Arc<Node<T>>> {
        let mut cache = self.cache.lock().unwrap();
        let res = cache.get_mut(key).map(|n| n.clone());
        match res {
            Some(node) => Ok(node.clone()),
            None => {
                let serialized = self.store.get(key)?;
                let mut de = Deserializer::new(&serialized[..]);
                let node: Arc<Node<T>> = Arc::new(Deserialize::deserialize(&mut de)?);
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

    fn test_tree<I: Clone + Iterator<Item = i64>>(iter: I) -> DurableTree<i64, NumComparator> {
        let store = Arc::new(HeapStore::new::<i64>());
        let node_store = NodeStore::new(store.clone());

        DurableTree::build_from_iter(node_store.clone(), iter.clone(), NumComparator)
    }

    #[test]
    fn test_leaf_iter() {
        let iter = 0..10_000;
        let tree = test_tree(iter.clone());

        assert_equal(
            tree.iter_leaves().map(|r| r.unwrap()).map(|l| l.items[0]),
            vec![0, 1024, 2048, 3072, 4096, 5120, 6144, 7168, 8192, 9216]
        );
    }

    #[test]
    fn test_build_and_iter() {
        let iter = 0..10_000;
        let tree = test_tree(iter.clone());

        assert_equal(tree.iter().unwrap().map(|r| r.unwrap()), iter);
    }

    #[test]
    fn test_range_from() {
        use std::ops::Range;

        let tree = test_tree(0..10_000);
        let first_range: Range<i64> = 500..10_000;
        assert_equal(
            tree.range_from(500).unwrap().map(|r| r.unwrap()),
            first_range,
        );
        let second_range: Range<i64> = 8459..10_000;
        assert_equal(
            tree.range_from(8459).unwrap().map(|r| r.unwrap()),
            second_range,
        );
    }

    // When new parents are implemented (comes into play circa 10e5-10e6 datoms) this should pass:
    // #[test]
    // #[ignore]
    // fn test_node_height() {
    //     let store = Arc::new(HeapStore::new::<i64>());
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

    //     let root_node_links: Vec<Link<i64>> = match node_store.get_node(&root_ref).unwrap() {
    //         Node::Interior { links, .. } => links,
    //         _ => unreachable!(),
    //     };

    //     assert!(root_node_links.len() <= NODE_CAPACITY)
    // }
}
