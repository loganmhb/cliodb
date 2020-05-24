use std::fmt::Debug;
use std::iter::Peekable;
use std::cmp::Ordering;
// TODO: replace mutex with futures::lock
use std::sync::{Arc, Mutex};
use log::{error};

use itertools::Itertools;
use lru_cache::LruCache;
use serde::{Serialize, Deserialize};
use serde::de::DeserializeOwned;
use uuid::Uuid;

use backends::KVStore;
use index::{Equivalent, Comparator};
use {Result};

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

// TODO: leaf max size should be in bytes, not records, in order to comply
// with backing kv store size limits (e.g. 65kb mysql blobs)
const LEAF_CAPACITY: usize = 16384;

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
    pub root: String,
    store: NodeStore<T>,
    _comparator: C,
}

impl<T, C> DurableTree<T, C>
where
    T: Equivalent + Serialize + DeserializeOwned + Clone + Debug,
    C: Comparator<Item = T>,
{
    pub fn create(store: Arc<dyn KVStore>, comparator: C) -> Result<DurableTree<T, C>> {
        let empty_root = Node::Interior(InteriorNode { links: vec![], keys: vec![] });
        let node_store = NodeStore::new(store.clone());
        let root_ref = node_store.add_node(&empty_root)?;
        Ok(DurableTree {
            root: root_ref,
            store: node_store,
            _comparator: comparator,
        })
    }

    /// Builds the tree from an iterator by chunking it into an
    /// iterator of leaf nodes and then constructing the tree of
    /// directory nodes on top of that.
    // TODO: remove
    #[cfg(test)]
    fn build_from_iter<I>(store: NodeStore<T>, iter: I, comparator: C) -> Result<DurableTree<T, C>>
    where
        I: Iterator<Item = T>,
    {
        // The items need to be chunked into leaf nodes.
        let chunks = iter.chunks(LEAF_CAPACITY);
        let leaves = chunks.into_iter()
            .map(|chunk| chunk.collect::<Vec<_>>())
            .map(|items| LeafNode { items });
        let closure_store = store.clone();
        let leaf_node_links = leaves.map(|leaf| {
            let leaf_link = closure_store.add_node(&Node::Leaf(leaf.clone()))
                .map(|db_key| LeafRef { node: leaf, db_key });
            leaf_link
        });

        Self::build_from_leaves(leaf_node_links, store, comparator)
    }

    /// Builds a new durable store from an iterator of leaf nodes by
    /// constructing a new directory tree on top.  The leaves iterator
    /// must return items of type Result<(T, Link<T>)>, where the
    /// first element of the tuple is the first item in the leaf and
    /// the second element is a link to the persisted leaf. This
    /// allows unchanged leaves to be preserved if rebuilding a tree.
    fn build_from_leaves<I: Iterator<Item = Result<LeafRef<T>>>>(
        leaves: I,
        store: NodeStore<T>,
        comparator: C
    ) -> Result<DurableTree<T, C>> {
        // As we build up the tree, we need to keep track of the
        // rightmost interior node on each level, so that we can
        // append to it. At the beginning, that's just the empty root.
        // The levels are ordered from highest to lowest, so the root
        // of the tree is always last.
        let mut open_nodes: Vec<InteriorNode<T>> = vec![InteriorNode { links: vec![], keys: vec![] }];

        let leaves = leaves.collect::<Vec<_>>();
        // error handling makes this a bit awkward; we need to process
        // the leaf links lazily, but return an error if we encounter
        // an error in the iterator, so instead of folding or
        // something we have to use for loops and a bunch of mutable
        // references
        // TODO: failable iterators?
        for result in leaves {
            let LeafRef { node, mut db_key } = result.expect("no leaf ref");
            let mut key = node.items[0].clone();
            let mut layer = 0;
            loop {
                if open_nodes.len() < layer + 1 {
                    // The tree is full. We need to add a new root node before proceeding.
                    open_nodes.push(InteriorNode { links: vec![], keys: vec![] });
                }


                let parent = &mut open_nodes[layer];
                parent.keys.push(key);
                parent.links.push(Link::DbKey(db_key));

                if parent.links.len() == NODE_CAPACITY {
                    // This node is full, so we need to replace it
                    // with a new empty one, persist it, and add a
                    // link to it to its own parent.
                    let old_node = std::mem::replace(parent, InteriorNode { links: vec![], keys: vec![] });
                    key = old_node.keys[0].clone();
                    db_key = store.add_node(&Node::Interior(old_node)).expect("could not add node");
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
            let root_ref = store.add_node(&Node::Interior(first_open_node))?;
            return Ok(
                DurableTree {
                    store: store,
                    root: root_ref,
                    _comparator: comparator,
                }
            )
        }

        let mut key = first_open_node.keys[0].clone();
        // FIXME: should be able to avoid this clone, I think, maybe requiring
        // a change in the signature of add_node.
        let mut link = store.add_node(&Node::Interior(first_open_node.clone()))?;

        for mut node in open_node_iter {
            if node.keys.len() == 0 {
                // nothing ever got added to this node so it's not needed
                continue;
            }
            node.keys.push(key.clone());
            node.links.push(Link::DbKey(link));
            key = (&node.keys[0]).clone();
            link = store.add_node(&Node::Interior(node))?;
        }

        Ok(DurableTree {
            store: store,
            root: link,
            _comparator: comparator,
        })
    }

    pub fn rebuild_with_novelty<I>(
        &self,
        novelty: I,
    ) -> Result<DurableTree<T, C>>
        where I: Iterator<Item = T>
    {
        let rebuild_iterator = RebuildIter::new(
            self.iter_leaves(),
            novelty,
            self.store.clone(),
            self._comparator,
        ).expect("could not construct RebuildIter");
        Self::build_from_leaves(rebuild_iterator, self.store.clone(), self._comparator)
    }

    pub fn from_ref(db_ref: String, store: Arc<dyn KVStore>, _comparator: C) -> DurableTree<T, C> {
        DurableTree {
            root: db_ref,
            store: NodeStore::new(store),
            _comparator,
        }
    }

    fn iter_leaves(&self) -> LeafIter<T> {
        LeafIter {
            store: self.store.clone(),
            stack: vec![LeafIterState {
                node_ref: Link::DbKey(self.root.clone()),
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
                node_ref: Link::DbKey(self.root.clone()),
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
                            stack.push(LeafIterState {
                                link_idx: 0,
                                ..state
                            });
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
                        // This is not elegant, but it happens when
                        // the key doesn't exist and sorts between
                        // this node and the previous one.
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

                    if link_idx + 1 < links.len() {
                        stack.push(LeafIterState {
                            link_idx: link_idx + 1,
                            ..state
                        });
                    }
                    stack.push(LeafIterState {
                        node_ref: links[link_idx].clone(),
                        link_idx: 0,
                    });
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct LeafRef<T> {
    db_key: String,
    // FIXME: don't pull the whole leaf into memory?
    node: LeafNode<T>,
}

#[derive(Clone)]
struct LeafIter<T> {
    store: NodeStore<T>,
    stack: Vec<LeafIterState<T>>,
}

#[derive(Debug, Clone)]
struct LeafIterState<T> {
    node_ref: Link<T>,
    link_idx: usize,
}

impl<T> Iterator for LeafIter<T>
where T: Clone + DeserializeOwned + Serialize + Debug,
{
    type Item = Result<LeafRef<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let LeafIterState { node_ref, link_idx } = match self.stack.pop() {
                Some(frame) => frame,
                None => return None,
            };

            let db_key = match node_ref {
                Link::DbKey(ref s) => s.clone(),
                Link::Pointer(_) => panic!("can't iterate using Link::Pointer"),
            };

            let node = match self.store.get_node(&db_key) {
                Ok(n) => n,
                // FIXME: Re-push stack frame on error?
                Err(e) => {
                    error!("Error calling get_node: {:?}", e);
                    return Some(Err(e));
                },
            };

            match *node {
                Node::Leaf(ref leaf) => {
                    // FIXME(perf): should not be necessary to clone the node
                    return Some(Ok(LeafRef { db_key, node: leaf.clone()}));
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

impl<T> ItemIter<T> where T: Clone + DeserializeOwned + Serialize + Debug {
    fn from_leaves(mut leaves: LeafIter<T>, idx_in_leaf: usize) -> Result<ItemIter<T>> {
        let first_leaf = match leaves.next() {
            Some(Ok(LeafRef { node: leaf, .. })) => Some(leaf),
            Some(Err(e)) => {
                error!("Error in from_leaves {:?}", e);
                return Err(e);
            },
            None => None,
        };
        return Ok(ItemIter {
            leaves,
            current_leaf: first_leaf,
            item_idx: idx_in_leaf,
        })
    }
}

impl<T> Iterator for ItemIter<T>
where T: Clone + DeserializeOwned + Serialize + Debug,
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
                Some(Ok(LeafRef { node: leaf, .. })) => Some(leaf),
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
struct NodeStore<T> {
    cache: Arc<Mutex<LruCache<String, Arc<Node<T>>>>>,
    store: Arc<dyn KVStore>,
}

impl<T> NodeStore<T>
where
    T: Serialize + DeserializeOwned + Clone,
{
    fn new(store: Arc<dyn KVStore>) -> NodeStore<T> {
        NodeStore {
            // TODO make size configurable
            cache: Arc::new(Mutex::new(LruCache::new(1024))),
            store: store,
        }
    }

    fn add_node(&self, node: &Node<T>) -> Result<String> {
        let buf = rmp_serde::to_vec(node)?;
        let mut encoded = Vec::new();

        {
            let mut encoder = snap::write::FrameEncoder::new(&mut encoded);
            std::io::copy(&mut &buf[..], &mut encoder)?;
        }

        let key: String = Uuid::new_v4().to_string();
        self.store.set(&key, &encoded)?;
        Ok(key)
    }

    /// Fetches and deserializes the node with the given key.
    fn get_node(&self, key: &str) -> Result<Arc<Node<T>>> {
        let mut cache = self.cache.lock().unwrap();
        let res = cache.get_mut(key).map(|n| n.clone());
        match res {
            Some(node) => Ok(node.clone()),
            None => {
                let compressed = self.store.get(key)?;
                let mut serialized = Vec::new();
                let mut decoder = snap::read::FrameDecoder::new(&compressed[..]);
                std::io::copy(&mut decoder, &mut serialized)?;
                let value: Node<T> = rmp_serde::from_read_ref(&serialized)?;
                let node: Arc<Node<T>> = Arc::new(value);
                cache.insert(key.to_string(), node.clone());
                Ok(node.clone())
            }
        }
    }
}


struct RebuildIter<T, L: Iterator<Item = Result<LeafRef<T>>>, I: Iterator<Item = T>, C: Comparator> {
    current_leaf: Option<Result<LeafRef<T>>>,
    next_leaf: Option<Result<LeafRef<T>>>,
    following_leaves: L,
    // A stack of new leaves to supply, so they're sorted backwards
    new_leaves: Vec<Result<LeafRef<T>>>,
    novelty: Peekable<I>,
    store: NodeStore<T>,
    _comparator: C,
}

impl <T, L: Iterator<Item = Result<LeafRef<T>>>, I: Iterator<Item = T>, C: Comparator> RebuildIter<T, L, I, C>
where T: Clone + Debug {
    fn new(mut leaves: L, novelty: I, store: NodeStore<T>, comparator: C) -> Result<RebuildIter<T, L, I, C>> {
        let first = leaves.next();
        let second = leaves.next();
        Ok(RebuildIter {
            current_leaf: first,
            next_leaf: second,
            following_leaves: leaves,
            new_leaves: vec![],
            novelty: novelty.peekable(),
            store,
            _comparator: comparator,
        })
    }
}

impl <T, L, I, C> Iterator for RebuildIter<T, L, I, C>
where T: Equivalent + Clone + Debug + DeserializeOwned + Serialize,
      L: Iterator<Item = Result<LeafRef<T>>>,
      I: Iterator<Item = T>,
      C: Comparator<Item = T> {
    type Item = Result<LeafRef<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        // If we've already generated some new leaves, return the next one.
        if let Some(new_leaf) = self.new_leaves.pop() {
            return Some(new_leaf)
        }

        // Otherwise, we need to generate the next set.
        let next_leaf = std::mem::replace(&mut self.next_leaf, self.following_leaves.next());
        let next_leaf_first_item = match next_leaf {
            Some(Ok(LeafRef { ref node, .. })) => Some(node.items[0].clone()),
            Some(Err(e)) => return Some(Err(e)),
            None => None
        };
        let current_leaf = std::mem::replace(&mut self.current_leaf, next_leaf);
        match current_leaf {
            None => {
                // There's an edge case where there are zero leaves which we have to handle here.
                match self.novelty.peek().cloned() {
                    None => return None,
                    Some(_) => {
                        let mut remaining_novelty = vec![];
                        while let Some(item) = self.novelty.next() {
                            remaining_novelty.push(item);
                        }
                        let mut created_leaves = remaining_novelty.into_iter().chunks(LEAF_CAPACITY).into_iter().map(|items| {
                            let node = LeafNode { items: items.collect() };
                            self.store.add_node(&Node::Leaf(node.clone())).map(|db_key| LeafRef { node, db_key })
                        }).collect::<Vec<_>>();
                        while let Some(new_leaf) = created_leaves.pop() {
                            self.new_leaves.push(new_leaf);
                        }
                    }
                }
            },
            Some(Err(e)) => return Some(Err(e)),
            Some(Ok(LeafRef { node, db_key })) => {
                let last_item = &node.items[node.items.len() - 1].clone();
                match self.novelty.peek().cloned() {
                    None => self.new_leaves.push(Ok(LeafRef { node, db_key })),
                    Some(first_novel_item) => {
                        if C::compare(&first_novel_item, &last_item) == Ordering::Greater {
                            // we can reuse this leaf, since it doesn't overlap with the novelty
                            // TODO: check for reusability the other way as well?
                            self.new_leaves.push(Ok(LeafRef { node, db_key }));
                        } else {
                            // There's overlapping novelty, so we can't reuse this leaf -- we have to rebuild a new one.
                            // This implementation greedily takes all possible novelty before the next leaf's first item.
                            // FIXME: the use of chunks() here can result in leafs smaller than half size, which is not ideal
                            // (but not critical for balancing the tree because they're leaves)
                            // There's an edge case for the last leaf, when we need to take all remaining novelty.

                            // this is just take_while(|i| C::compare(&i, &next_first_item) == Ordering::Less), but take_while
                            // consumes the rest of its iterator which we don't want
                            let mut overlapping_novelty = vec![];
                            // FIXME: tortured logic
                            while self.novelty.peek().map(|i| match next_leaf_first_item.clone() {
                                None => true,
                                Some(item) => C::compare(&i, &item) == Ordering::Less
                            }) == Some(true) {
                                overlapping_novelty.push(self.novelty.next().unwrap());
                            }

                            let mut created_leaves = node.items.into_iter()
                                .merge_by(overlapping_novelty, |a, b| C::compare(a, b) == Ordering::Less)
                                .coalesce(|x, y| if x.equivalent(&y) { Ok(x) } else { Err((x, y)) })
                                .chunks(LEAF_CAPACITY)
                                .into_iter()
                                .map(|items| {
                                    let node = LeafNode { items: items.collect() };
                                    self.store.add_node(&Node::Leaf(node.clone())).map(|db_key| LeafRef { node, db_key })
                                }).collect::<Vec<_>>();

                            // Push new leaves onto the new leaves stack
                            // FIXME: this shouldn't be a stack, it should be a queue of some sort
                            while let Some(new_leaf) = created_leaves.pop() {
                                self.new_leaves.push(new_leaf);
                            }
                        }
                    }
                }
            }

        }

        return self.new_leaves.pop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::assert_equal;
    use index::NumComparator;
    use backends::sqlite::SqliteStore;
    extern crate test;
    use self::test::{Bencher};

    fn test_tree<I: Clone + Iterator<Item = i64>>(iter: I) -> DurableTree<i64, NumComparator> {
        let store = Arc::new(SqliteStore::new(":memory:").unwrap());
        let node_store = NodeStore::new(store.clone());

        DurableTree::build_from_iter(node_store.clone(), iter.clone(), NumComparator).unwrap()
    }

    #[test]
    fn test_leaf_iter() {
        let iter = 0..100_000;
        let tree = test_tree(iter.clone());

        assert_equal(
            tree.iter_leaves().map(|r| r.unwrap()).map(|l| l.node.items[0]),
            vec![0, 16384, 32768, 49152, 65536, 81920, 98304]
        );
    }

    #[test]
    fn test_build_and_iter() {
        let iter = 0..10_000;
        let tree = test_tree(iter.clone());

        assert_equal(tree.iter().unwrap().map(|r| r.unwrap()), iter);
    }

    #[test]
    fn test_range_from_present_item() {
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

    #[test]
    fn test_range_from_absent_item() {
        use std::ops::Range;

        let even_numbers = || (0..10_000).filter(|i| i % 2 == 0);
        let tree = test_tree(even_numbers());

        let first_range = even_numbers().skip_while(|i| i < &501);
        assert_equal(
            tree.range_from(501).unwrap().map(|r| r.unwrap()),
            first_range,
        );

        let second_range = even_numbers().skip_while(|i| i < &8459);
        assert_equal(
            tree.range_from(8459).unwrap().map(|r| r.unwrap()),
            second_range,
        );
    }


    #[test]
    fn test_rebuild_with_novelty_builds_correct_iterator() {
        let tree = test_tree((0..32767).filter(|i| i % 2 == 0));
        let rebuild = tree.rebuild_with_novelty((0..32767).filter(|i| i % 2 != 0)).unwrap();
        assert_equal(
            rebuild.iter().unwrap().map(|r| r.unwrap()),
            0..32767
        )
    }

    #[test]
    fn test_rebuild_with_novelty_reuses_leaves() {
        let tree = test_tree(0..32767);
        let rebuild = tree.rebuild_with_novelty(32767..40000).unwrap();
        assert_equal(
            rebuild.iter().unwrap().map(|r| r.unwrap()),
            0..40000
        );
        assert_equal(
            tree.iter_leaves().take(2).map(|r| r.unwrap()),
            rebuild.iter_leaves().take(2).map(|r| r.unwrap())
        )
    }

    #[test]
    fn test_rebuild_with_novelty_avoids_duplicates() {
        let tree = test_tree(0..1000);
        let rebuild = tree.rebuild_with_novelty(900..1200).unwrap();
        assert_equal(
            rebuild.iter().unwrap().map(|r| r.unwrap()),
            0..1200
        );
    }

    #[test]
    #[ignore]
    fn test_node_height() {
        let store = Arc::new(SqliteStore::new(":memory:").unwrap());
        let node_store = NodeStore {
            cache: Arc::new(Mutex::new(LruCache::new(1024))),
            store: store.clone(),
        };

        let iter = 0..10_000_000;
        let tree = DurableTree::build_from_iter(node_store.clone(), iter.clone(), NumComparator).unwrap();

        let root_node_links_len: usize = match *node_store.get_node(&tree.root).unwrap() {
            Node::Interior(InteriorNode { ref links, .. }) => links.len(),
            _ => unreachable!(),
        };

        assert!(root_node_links_len <= NODE_CAPACITY);
        assert_equal(
            tree.iter().unwrap().map(|r| r.unwrap()),
            iter
        );
    }


    #[bench]
    fn bench_build_from_iter(b: &mut Bencher) {
        use super::super::backends::sqlite::SqliteStore;
        let store = Arc::new(SqliteStore::new("/tmp/cliodb_bench.db").unwrap());
        let node_store: NodeStore<i64> = NodeStore {
            cache: Arc::new(Mutex::new(LruCache::new(1024))),
            store: store.clone()
        };
        b.iter(|| DurableTree::build_from_iter(node_store.clone(), 0..1_000_000, NumComparator))
    }

    #[bench]
    fn bench_rebuild_with_novelty(b: &mut Bencher) {
        use super::super::backends::sqlite::SqliteStore;
        let store = Arc::new(SqliteStore::new("/tmp/cliodb_bench.db").unwrap());
        let node_store: NodeStore<i64> = NodeStore {
            cache: Arc::new(Mutex::new(LruCache::new(1024))),
            store: store.clone()
        };
        let tree = DurableTree::build_from_iter(node_store.clone(), 0..1_000_000, NumComparator).unwrap();
        b.iter(|| tree.rebuild_with_novelty(500_000..510_000).unwrap())
    }

    #[bench]
    fn bench_rebuild_with_novelty_mostly_novelty(b: &mut Bencher) {
        use super::super::backends::sqlite::SqliteStore;
        let store = Arc::new(SqliteStore::new("/tmp/cliodb_bench.db").unwrap());
        let node_store: NodeStore<i64> = NodeStore {
            cache: Arc::new(Mutex::new(LruCache::new(1024))),
            store: store.clone()
        };
        let tree = DurableTree::build_from_iter(node_store.clone(), 0..100_000, NumComparator).unwrap();
        b.iter(|| tree.rebuild_with_novelty(0..1_000_000).unwrap())
    }
}
