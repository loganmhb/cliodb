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


/// A node of the tree -- either leaf or interior. An empty tree is
/// represented by an empty directory node.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Serialize, Deserialize)]
pub enum Node<T> {
    Leaf(LeafNode<T>),
    Interior(InteriorNode<T>),
}


/// A leaf node is just an array of items.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Serialize, Deserialize)]
pub struct LeafNode<T> {
    pub items: Vec<T>
}


/// An interior node doesn't contain any data itself, but contains
/// information for navigating to a leaf node. This information is a
/// vector of keys (the first item of each child node) and links to
/// those children.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Serialize, Deserialize)]
pub struct InteriorNode<T> {
    pub keys: Vec<T>,
    pub links: Vec<Link<T>>
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
    pub fn build_from_iter<I>(store: NodeStore<T>, iter: I, _comparator: C) -> Result<DurableTree<T, C>>
    where
        I: Iterator<Item = T>,
    {
        // As we build up the tree, we need to keep track of the
        // rightmost interior node on each level, so that we can
        // append to it. At the beginning, that's just the empty root.
        // The levels are ordered from highest to lowest, so the root
        // of the tree is always last.
        let mut open_nodes: Vec<InteriorNode<T>> = vec![];

        // The items need to be chunked into leaf nodes.
        let chunks = iter.chunks(NODE_CAPACITY);
        let leaf_item_vecs = chunks.into_iter().map(|chunk| chunk.collect::<Vec<_>>());

        // The leaves themselves need to be chunked into directory nodes.
        let closure_store = store.clone();
        let leaf_node_links = leaf_item_vecs.map(|items| {
            let key = items[0].clone();
            let leaf = LeafNode { items };
            let leaf_link: Result<(T, Link<T>)> = closure_store.add_node(&Node::Leaf(leaf))
                .map(|db_ref| (key, Link::DbKey(db_ref)));
            leaf_link
        });

        // error handling makes this a bit awkward; we need to process
        // the leaf links lazily, but return an error if we encounter
        // an error in the iterator, so instead of folding or
        // something we have to use for loops and a bunch of mutable
        // references
        for result in leaf_node_links {
            let (mut key, mut link) = result?;

            let mut layer = 0;
            loop {
                if open_nodes.len() < layer + 1 {
                    // The tree is full. We need to add a new root node before proceeding.
                    open_nodes.push(InteriorNode { links: vec![], keys: vec![] });
                }


                let mut parent = &mut open_nodes[layer];
                parent.keys.push(key);
                parent.links.push(link);

                if parent.links.len() == NODE_CAPACITY {
                    // This node is full, so we need to replace it
                    // with a new empty one, persist it, and add a
                    // link to it to its own parent.
                    let old_node = std::mem::replace(parent, InteriorNode { links: vec![], keys: vec![] });
                    key = old_node.keys[0].clone();
                    link = Link::DbKey(store.add_node(&Node::Interior(old_node))?);
                    layer += 1;
                    continue;
                } else {
                    break;
                }
            }
        }

        // Now that the tree is built, we need to persist the remaining open nodes.
        let mut open_node_iter = open_nodes.into_iter();
        let first_open_node = open_node_iter.next().unwrap();

        if first_open_node.keys.len() == 0 {
            // an empty directory node means a root
            let link = Link::DbKey(store.add_node(&Node::Interior(first_open_node))?);
            return Ok(
                DurableTree {
                    store: store,
                    root: link,
                    _comparator,
                }
            )
        }

        let mut key = first_open_node.keys[0].clone();
        // FIXME: should be able to avoid this clone, I think, maybe requiring
        // a change in the signature of add_node.
        let mut link = Link::DbKey(store.add_node(&Node::Interior(first_open_node.clone()))?);

        for mut node in open_node_iter {
            if node.keys.len() == 0 {
                // nothing ever got added to this node so it's not needed
                continue;
            }
            node.keys.push(key.clone());
            node.links.push(link);
            key = (&node.keys[0]).clone();
            link = Link::DbKey(store.add_node(&Node::Interior(node))?);
        }

        Ok(DurableTree {
            store: store,
            root: link,
            _comparator,
        })
    }

    pub fn rebuild_with_novelty<I>(
        &self,
        mut store: NodeStore<T>,
        iter: I,
        _comparator: C
    ) -> DurableTree<T, C>
        where I: Iterator<Item = T>
    {
        // iterate over nodes.
        // if a node can be saved (i.e. the next item in the iterator is past the node), reuse it and move to the next node.
        //
        unimplemented!()
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
                    // If the key is found in an interior node, that
                    // means the actual item is the first one of the
                    // child at that index, so it doesn't actually make a
                    // difference if the key exists in this node or
                    // not, except for the off-by-one error.
                    let link_idx = match keys.binary_search_by(|other| C::compare(other, &start)) {
                        Ok(idx) => idx,
                        // This is not elegant, but I think it can
                        // happen when the key doesn't exist and sorts
                        // between this node and the previous one.
                        Err(0) => 0,
                        Err(idx) => idx - 1,
                    };

                    if link_idx == 0 && links.len() == 0 {
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
                        link_idx: link_idx + 1,
                        ..state
                    });
                    stack.push(LeafIterState {
                        node_ref: links[link_idx].clone(),
                        link_idx: 0,
                    });
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
        loop {
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
                    return Some(Ok(leaf.clone()));
                }
                Node::Interior(InteriorNode { ref links, .. }) => {
                    if links.len() == 0 {
                        // Special case: empty root node
                        return None;
                    }
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

        DurableTree::build_from_iter(node_store.clone(), iter.clone(), NumComparator).unwrap()
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
    #[test]
    #[ignore]
    fn test_node_height() {
        let store = Arc::new(HeapStore::new::<i64>());
        let node_store = NodeStore {
            cache: Arc::new(Mutex::new(LruCache::new(1024))),
            store: store.clone(),
        };

        let iter = 0..10_000_000;
        let tree = DurableTree::build_from_iter(node_store.clone(), iter.clone(), NumComparator).unwrap();

        let root_ref = match tree.root {
            Link::DbKey(ref s) => s.clone(),
            _ => unreachable!(),
        };

        let root_node_links_len: usize = match *node_store.get_node(&root_ref).unwrap() {
            Node::Interior(InteriorNode { ref links, .. }) => links.len(),
            _ => unreachable!(),
        };

        assert!(root_node_links_len <= NODE_CAPACITY);
        assert_equal(
            tree.iter().unwrap().map(|r| r.unwrap()),
            iter
        );
    }
}
