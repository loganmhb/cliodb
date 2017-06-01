use std::sync::Arc;
use std::fmt::Debug;

const KEY_CAPACITY: usize = 512;
const LINK_CAPACITY: usize = KEY_CAPACITY + 1;

// Cloning a vector doesn't preserve its capacity, which we rely on
// for the B-tree node size.
macro_rules! clone_vec {
    ($other:expr, $capacity:expr) => {
        {
            let mut new_vec = Vec::with_capacity($capacity);
            new_vec.extend_from_slice(&$other);
            new_vec
        }
    };
}

#[derive(Debug)]
struct Index<T: Ord + Clone + Debug> {
    root: Node<T>,
}

impl<T: Ord + Clone + Debug> Index<T> {
    fn new() -> Index<T> {
        Index { root: Node::Leaf { keys: Vec::with_capacity(KEY_CAPACITY) } }
    }

    fn insert(&self, item: T) -> Index<T> {
        match self.root.insert(item.clone()) {
            Ok(new_root) => Index { root: new_root },
            Err(InsertError::NodeFull) => {
                // Need to make a new root; the whole tree is full.
                let (left, sep, right) = self.root.split();
                let mut new_root_links = Vec::with_capacity(self.root.capacity() + 1);
                let mut new_root_keys: Vec<T> = Vec::with_capacity(self.root.capacity());
                new_root_links.push(Arc::new(left));
                new_root_links.push(Arc::new(right));
                new_root_keys.push(sep);
                Index {
                    root: Node::Directory {
                            links: new_root_links,
                            keys: new_root_keys,
                        }
                        .insert(item)
                        .unwrap(),
                }
            }
        }
    }

    fn iter(&self) -> Iter<T> {
        let mut stack = Vec::new();
        stack.push((&self.root, 0, 0));
        Iter {
            index: self,
            stack: stack,
        }
    }
}

struct Iter<'a, T: 'a + Ord + Clone + Debug> {
    index: &'a Index<T>,
    stack: Vec<(&'a Node<T>, usize, usize)>,
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
                (&Node::Leaf { ref keys }, link_idx, key_idx) => {
                    if key_idx < keys.len() {
                        let res = &keys[key_idx];
                        self.stack.push((context.0, link_idx, key_idx + 1));
                        return Some(res);
                    } else {
                        continue; // keep looking for a stack frame that will yield something
                    }
                },
                (&Node::Directory { ref links, ref keys }, link_idx, key_idx) => {
                    // If link idx == key idx, push the child and continue.
                    // otherwise, yield the key idx and bump it.
                    if link_idx == key_idx {
                        self.stack.push((context.0, link_idx+1, key_idx));
                        self.stack.push((&*links[link_idx], 0, 0));
                        continue;
                    } else if key_idx < keys.len() {
                        let res = &keys[key_idx];
                        self.stack.push((context.0, link_idx, key_idx + 1));
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

#[derive(Debug)]
enum Node<T: Ord + Clone + Debug> {
    Directory {
        keys: Vec<T>,
        links: Vec<Arc<Node<T>>>,
    },
    Leaf { keys: Vec<T> },
}

// When a node is cloned, we need to make sure that cloned vectors
// preserve their capacities again.
// FIXME: Sharing references (Arcs?) to vectors instead of cloning them might
// render this unnecessary.
impl<T: Clone + Ord + Debug> Clone for Node<T> {
    fn clone(&self) -> Node<T> {
        match self {
            &Node::Leaf { ref keys } => Node::Leaf { keys: clone_vec!(keys, KEY_CAPACITY) },
            &Node::Directory {
                 ref keys,
                 ref links,
             } => {
                Node::Directory {
                    keys: clone_vec!(keys, KEY_CAPACITY),
                    links: clone_vec!(links, LINK_CAPACITY),
                }
            }
        }
    }
}

#[derive(Debug)]
enum InsertError {
    NodeFull,
}

impl<T: Ord + Clone + Debug> Node<T> {
    /// Returns the number of keys in the node. For directory nodes,
    /// the number of links is always one greater than the number of
    /// keys.
    fn size(&self) -> usize {
        match self {
            &Node::Leaf { ref keys } => keys.len(),
            &Node::Directory { ref keys, .. } => keys.len(),
        }
    }

    fn capacity(&self) -> usize {
        match self {
            &Node::Leaf { ref keys } => keys.capacity(),
            &Node::Directory { ref keys, .. } => keys.capacity(),
        }
    }

    /// Splits a node in half, returning a tuple of the first half, separator key
    /// and the second half. Should only be called on full nodes, because it assumes
    /// that there are enough items in the node to create two new legal nodes.
    fn split(&self) -> (Node<T>, T, Node<T>) {
        assert_eq!(self.capacity(), KEY_CAPACITY);
        // It's a logic error to invoke this when the node isn't full.
        assert!(self.size() == KEY_CAPACITY);

        let split_idx = self.size() / 2;
        match self {
            &Node::Leaf { ref keys } => {
                let (left_keys_slice, right_keys_and_sep) = keys.split_at(split_idx);
                // Pop the separator off to be inserted into the parent.
                let (sep, right_keys_slice) = right_keys_and_sep.split_first().unwrap();

                let mut left_keys = Vec::with_capacity(KEY_CAPACITY);
                let mut right_keys = Vec::with_capacity(KEY_CAPACITY);

                left_keys.extend_from_slice(left_keys_slice);
                right_keys.extend_from_slice(right_keys_slice);

                let left = Node::Leaf { keys: left_keys };

                let right = Node::Leaf { keys: right_keys };
                (left, sep.clone(), right)
            }
            &Node::Directory {
                 ref keys,
                 ref links,
             } => {
                let (left_keys_slice, right_keys_and_sep) = keys.split_at(split_idx);
                let (left_links_slice, right_links_slice) = links.split_at(split_idx + 1);
                let (sep, right_keys_slice) = right_keys_and_sep.split_first().unwrap();

                let mut left_keys = Vec::with_capacity(KEY_CAPACITY);
                let mut right_keys = Vec::with_capacity(KEY_CAPACITY);

                let mut left_links = Vec::with_capacity(LINK_CAPACITY);
                let mut right_links = Vec::with_capacity(LINK_CAPACITY);

                left_keys.extend_from_slice(left_keys_slice);
                right_keys.extend_from_slice(right_keys_slice);

                left_links.extend_from_slice(left_links_slice);
                right_links.extend_from_slice(right_links_slice);

                let left = Node::Directory {
                    keys: left_keys,
                    links: left_links,
                };

                let right = Node::Directory {
                    keys: right_keys,
                    links: right_links,
                };

                (left, sep.clone(), right)
            }
        }
    }

    fn insert(&self, item: T) -> Result<Node<T>, InsertError> {
        match self {
            &Node::Leaf { ref keys } => {
                if keys.len() < keys.capacity() {
                    let idx = match keys.binary_search(&item) {
                        Ok(_) => return Ok(self.clone()), // idempotent insertion?
                        Err(idx) => idx,
                    };

                    let mut new_keys = clone_vec!(keys, KEY_CAPACITY);
                    new_keys.insert(idx, item);

                    Ok(Node::Leaf { keys: new_keys })
                } else {
                    Err(InsertError::NodeFull)
                }
            }
            &Node::Directory {
                 ref keys,
                 ref links,
             } => {

                assert!(keys.len() + 1 == links.len());

                let idx = match keys.binary_search(&item) {
                    Ok(_) => return Ok(self.clone()), // idempotent insertion?
                    Err(idx) => idx,
                };

                let child = links.get(idx).unwrap();
                let result = child.insert(item.clone());

                match result {
                    Ok(new_child) => {
                        let mut new_links = clone_vec!(links, LINK_CAPACITY);
                        new_links[idx] = Arc::new(new_child);

                        Ok(Node::Directory {
                               keys: keys.clone(),
                               links: new_links,
                           })
                    }
                    Err(InsertError::NodeFull) => {
                        // Child needs to be split, if we have space
                        // for an extra link.
                        if links.len() < links.capacity() {
                            let (left, sep, right) = child.split();

                            let mut new_keys = keys.clone();
                            let mut new_links = links.clone();

                            new_keys.insert(idx, sep);
                            // FIXME: not at all sure this isn't off by 1 or 2
                            new_links[idx] = Arc::new(right);
                            new_links.insert(idx, Arc::new(left));

                            Ok(Node::Directory {
                                       links: new_links,
                                       keys: new_keys,
                                   }
                                   .insert(item)
                                   .unwrap())
                        } else {
                            Err(InsertError::NodeFull)
                        }
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

    use super::*;

    fn enumerate_node<T: Clone + Ord + ::std::fmt::Debug>(node: &Node<T>) -> Vec<T> {
        match node {
            &Node::Leaf { ref keys } => keys.clone(),
            &Node::Directory {
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
        assert_eq!(idx.iter().cloned().collect::<Vec<_>>(), range.collect::<Vec<usize>>());
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
}
