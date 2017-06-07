use std::ops::RangeFrom;
use std::fmt::Debug;

pub const CAPACITY: usize = 512;

/// Trait abstracting over anything that can be used as a
/// KV store, where keys can only be added, not modified.
pub trait KVStore
    where Self: Sized
{
    type Key;
    type Value;
    type Error;
    fn get(&self, key: &Self::Key) -> Result<Self::Value, Self::Error>;
    fn add(&self, value: &Self::Value) -> Result<String, Self::Error>;
}

pub enum Insertion<N: Node> {
    Inserted(N),
    Duplicate,
    NodeFull,
}

pub trait Node where Self: Sized {
    type Item: Ord + Clone + Debug;
    type Reference: Clone;
    type Store: KVStore<Key=Self::Reference, Value=Self>;

    /// Return the number of items in the node.
    fn size(&self) -> usize;
    /// Allocate the node, however applicable, and return a reference.
    fn save(&self, store: &Self::Store) -> Self::Reference;
    fn is_leaf(&self) -> bool;
    fn items(&self) -> &[Self::Item];
    fn links(&self) -> &[Self::Reference];

    fn new_leaf(items: Vec<Self::Item>) -> Self;
    fn new_dir(items: Vec<Self::Item>, links: Vec<Self::Reference>) -> Self;

    fn insert(&self, store: &Self::Store, item: Self::Item) -> Insertion<Self> {
        if self.is_leaf() {
            if self.size() < CAPACITY {
                let idx = match self.items().binary_search(&item) {
                    Ok(_) => return Insertion::Duplicate,
                    Err(idx) => idx
                };

                let mut new_items = self.items().to_vec();
                new_items.insert(idx, item);

                Insertion::Inserted(Self::new_leaf(new_items))
            } else {
                Insertion::NodeFull
            }
        } else {
            let idx = match self.items().binary_search(&item) {
                Ok(_) => return Insertion::Duplicate,
                Err(idx) => idx
            };

            let child = store.get(&self.links()[idx]).unwrap();
            let child_result = child.insert(item.clone());

            match child_result {
                Insertion::Duplicate => Insertion::Duplicate,
                Insertion::Inserted(new_child) => {
                    let mut new_links = self.links().to_vec();
                    new_links[idx] = new_child.save();

                    Insertion::Inserted(Self::new_dir(self.items().to_vec(), new_links))
                },

                Insertion::NodeFull => {
                    // The child node needs to be split, if there's space in this node's links.
                    if self.size() < CAPACITY {
                        let (left, sep, right) = child.split();

                        let mut new_items = self.items().to_vec();
                        let mut new_links = self.links().to_vec();

                        new_items.insert(idx, sep);
                        new_links[idx] = right.save();
                        new_links.insert(idx, left.save());

                        let dir = Self::new_dir(new_items, new_links);

                        match dir.insert(item) {
                            Insertion::Inserted(new_dir) => Insertion::Inserted(new_dir),
                            // If it's a dup we wouldn't have gotten NodeFull; since we just split
                            // we won't get NodeFull again. Therefore anything else is unreachable.
                            _ => unreachable!()
                        }
                    } else {
                        // No room - the split needs to be propagated up.
                        Insertion::NodeFull
                    }
                }
            }
        }
    }

    fn split(&self) -> (Self, Self::Item, Self) {
        let split_idx = self.size() / 2;

        // Regardless of leaf or dir, we need to split the items.
        let (left_items, right_items_and_sep) = self.items().split_at(split_idx);
        let (sep, right_items) = right_items_and_sep.split_first().unwrap();

        if self.is_leaf() {
            let left = Self::new_leaf(left_items.to_vec());
            let right = Self::new_leaf(right_items.to_vec());

            (left, sep.clone(), right)
        } else {
            // For a dir, we also need to split the links.
            let (left_links, right_links) = self.links().split_at(split_idx + 1);

            let left = Self::new_dir(left_items.to_vec(), left_links.to_vec());
            let right = Self::new_dir(right_items.to_vec(), right_links.to_vec());

            (left, sep.clone(), right)
        }
    }
}


pub fn iter_range_start<N: Node, S: KVStore>(store: S, node_ref: N::Reference, range: RangeFrom<N::Item>) -> Iter<N> {
    let mut stack = vec![
        IterState {
            node_ref,
            link_idx: 0,
            item_idx: 0
        }
    ];

    // Search for the beginning of the range.
    loop {
        let state = stack.pop().unwrap();

        let node: N = store.get(&state.node_ref);

        match node.items().binary_search(&range.start) {
            Ok(idx) => {
                stack.push(IterState {
                    item_idx: idx,
                    link_idx: idx + 1,
                    ..state
                });
                return Iter { stack }
            },
            Err(idx) if node.is_leaf() => {
                stack.push(IterState {
                    item_idx: idx,
                    ..state
                });
                return Iter { stack };
            }
            Err(idx) => {
                stack.push(IterState {
                    item_idx: idx,
                    link_idx: idx+1,
                    ..state
                });
                stack.push(IterState {
                    node_ref: node.links()[idx].clone(),
                    item_idx: 0,
                    link_idx: 0,
                });
            }
        }
    }
}

// The structs necessary to implement iter() for an index are provided here, but
// iter() cannot be implemented for Node directly because a Node doesn't have a
// reference to itself, which is needed for creating the Iter struct's stack.
#[derive(Copy, Clone, Debug)]
pub struct IterState<N: Node> {
    pub node_ref: N::Reference,
    pub link_idx: usize,
    pub item_idx: usize,
}

#[derive(Clone)]
pub struct Iter<N: Node> {
    pub stack: Vec<IterState<N>>,
}

impl<N: Node> Iterator for Iter<N> {
    type Item = Result<N::Item, String>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let state @ IterState { node_ref, link_idx, item_idx} = match self.stack.pop() {
                Some(frame) => frame,
                None => return None,
            };

            let node = match self.get_ref(&node_ref) {
                Ok(n) => n,
                Err(e) => {
                    // Push the old state back on the stack so a retry will get
                    // the same item.
                    self.stack.push(state);
                    return Err(e)
                }
            };

            if node.is_leaf() {
                if item_idx < node.size() {
                    let res = node.items()[item_idx].clone();
                    self.stack.push(IterState {
                        node_ref,
                        link_idx,
                        item_idx: item_idx + 1,
                    });
                    return Some(Ok(res));
                } else {
                    continue; // pop the frame and continue
                }
            } else {
                // If link idx == item idx, push the child and continue.
                // otherwise, yield the item idx and bump it.
                if link_idx == item_idx {
                    self.stack.push(IterState {
                        node_ref,
                        link_idx: link_idx + 1,
                        item_idx,
                    });
                    self.stack.push(IterState {
                        node_ref: node.links()[link_idx].clone(),
                        link_idx: 0,
                        item_idx: 0,
                    });
                    continue;
                } else if item_idx < node.size() {
                    let res = &node.items()[item_idx];
                    self.stack.push(IterState {
                        node_ref,
                        link_idx,
                        item_idx: item_idx + 1,
                    });
                    return Some((*res).clone());
                } else {
                    // This node is done, so we don't re-push its stack frame.
                    continue;
                }
            }
        }
    }
}
