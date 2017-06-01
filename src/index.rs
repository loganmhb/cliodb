use std::sync::Arc;
use std::fmt::Debug;

#[derive(Debug)]
struct Index<T: Ord + Clone + Debug> {
    root: Node<T>,
}

impl<T: Ord + Clone + Debug> Index<T> {
    fn new() -> Index<T> {
        let cap = 16;
        Index {
            root: Node::Leaf {
                keys: Vec::with_capacity(cap),
                capacity: cap,
            },
        }
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
                        capacity: self.root.capacity(),
                        links: new_root_links,
                        keys: new_root_keys,
                    }.insert(item).unwrap()
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
enum Node<T: Ord + Clone + Debug> {
    Directory {
        capacity: usize,
        keys: Vec<T>,
        links: Vec<Arc<Node<T>>>,
    },
    Leaf { capacity: usize, keys: Vec<T> },
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
            &Node::Leaf { capacity, ref keys } => keys.len(),
            &Node::Directory {
                 capacity,
                 ref keys,
                 ref links,
             } => keys.len(),
        }
    }

    fn capacity(&self) -> usize {
        match self {
            &Node::Leaf { capacity, ref keys } => capacity,
            &Node::Directory {
                 capacity,
                 ref keys,
                 ref links,
             } => capacity,
        }
    }

    /// Splits a node in half, returning a tuple of the first half, separator key
    /// and the second half.
    /// SHOULD ONLY BE CALLED ON FULL NODES. (Unsafe?)
    fn split(&self) -> (Node<T>, T, Node<T>) {
        // It's a logic error to invoke this when the node isn't full.
        assert!(self.size() == self.capacity());

        let split_idx = self.size() / 2;
        match self {
            &Node::Leaf { capacity, ref keys } => {
                let (left_keys, right_keys_and_sep) = keys.split_at(split_idx);
                // Pop the separator off to be inserted into the parent.
                let (sep, right_keys) = right_keys_and_sep.split_first().unwrap();

                let left = Node::Leaf {
                    capacity: capacity,
                    keys: left_keys.to_owned(),
                };

                let right = Node::Leaf {
                    capacity: capacity,
                    keys: right_keys.to_owned(),
                };
                (left, sep.clone(), right)
            }
            &Node::Directory {
                 capacity,
                 ref keys,
                 ref links,
             } => {
                let (left_keys, right_keys_and_sep) = keys.split_at(split_idx);
                let (left_links, right_links) = links.split_at(split_idx + 1);
                let (sep, right_keys) = right_keys_and_sep.split_first().unwrap();

                let left = Node::Directory {
                    capacity: capacity,
                    keys: left_keys.to_vec(),
                    links: left_links.to_vec(),
                };

                let right = Node::Directory {
                    capacity: capacity,
                    keys: right_keys.to_vec(),
                    links: right_links.to_vec(),
                };

                (left, sep.clone(), right)
            }
        }
    }

    fn insert(&self, item: T) -> Result<Node<T>, InsertError> {
        match self {
            &Node::Leaf { capacity, ref keys } => {
                if keys.len() < capacity {
                    let idx = match keys.binary_search(&item) {
                        Ok(_) => return Ok(self.clone()), // idempotent insertion?
                        Err(idx) => idx,
                    };

                    let mut new_keys = keys.clone();
                    new_keys.insert(idx, item);
                    Ok(Node::Leaf {
                           capacity: capacity,
                           keys: new_keys,
                       })
                } else {
                    Err(InsertError::NodeFull)
                }
            }
            &Node::Directory {
                 capacity,
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
                        let mut new_links = links.clone();
                        new_links[idx] = Arc::new(new_child);
                        Ok(Node::Directory {
                            capacity,
                            keys: keys.clone(),
                            links: new_links
                        })
                    },
                    Err(InsertError::NodeFull) => {
                        // Child needs to be split, if we have space
                        // for an extra link.
                        if links.len() < capacity+1 {
                            let (left, sep, right) = child.split();

                            let mut new_keys = keys.clone();
                            let mut new_links = links.clone();

                            new_keys.insert(idx, sep);
                            // FIXME: not at all sure this isn't off by 1 or 2
                            new_links[idx] = Arc::new(right);
                            new_links.insert(idx, Arc::new(left));

                            Ok(Node::Directory {
                                   capacity: capacity,
                                   links: new_links,
                                   keys: new_keys,
                               }.insert(item).unwrap())
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
    use self::test::{Bencher};

    use super::*;

    fn enumerate_node<T: Clone + Ord + ::std::fmt::Debug>(node: &Node<T>) -> Vec<T> {
        match node {
            &Node::Leaf { capacity, ref keys } => {
                keys.clone()
            },
            &Node::Directory { capacity, ref links, ref keys } => {
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
        let range: ::std::ops::Range<u64> = 0..(16*16+1);
        for i in range {
            idx = idx.insert(i);
        }

        assert_eq!(enumerate_node(&idx.root), (0..(16*16+1)).collect::<Vec<_>>());
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
            n  = (n + 1) % 512;
        });
    }
}
