use std::sync::Arc;
use std::cell::RefCell;

struct Index<T: Ord + Clone> {
    root: Node<T>,
}

impl<T: Ord + Clone> Index<T> {
    fn new() -> Index<T> {
        let cap = 512;
        Index {
            root: Node::Leaf { keys: Vec::with_capacity(cap), capacity: cap },
        }
    }
}

#[derive(Debug, Clone)]
enum Node<T: Ord + Clone> {
    Directory {
        capacity: usize,
        keys: Vec<T>,
        links: Vec<Arc<RefCell<Node<T>>>>,
    },
    Leaf { capacity: usize, keys: Vec<T> },
}

enum InsertError {
    NodeFull,
}

impl<T: Ord + Clone> Node<T> {
    fn with_capacity(capacity: usize) -> Node<T> {
        Node::Directory {
            capacity: capacity,
            links: Vec::with_capacity(capacity + 1),
            keys: Vec::with_capacity(capacity),
        }
    }

    /// Returns the number of keys in the node. For directory nodes,
    /// the number of links is always one greater than the number of
    /// keys.
    fn size(&self) -> usize {
        match self {
            &Node::Leaf {capacity, ref keys} => keys.len(),
            &Node::Directory {capacity, ref keys, ref links} => keys.len()
        }
    }

    fn capacity(&self) -> usize {
        match self {
            &Node::Leaf {capacity, ref keys} => capacity,
            &Node::Directory {capacity, ref keys, ref links} => capacity
        }
    }

    /// Splits a node in half, returning a tuple of the separator key
    /// and the new node (which has the larger keys). The old node gets mutated.
    /// SHOULD ONLY BE CALLED ON FULL NODES. (Unsafe?)
    fn split(&mut self) -> (T, Node<T>) {
        println!("Creating a new node.");
        // It's a logic error to invoke this when the node isn't full.
        assert!(self.size() == self.capacity());

        let split_idx = self.size() / 2;
        match self {
            &mut Node::Leaf {
                     capacity,
                     ref mut keys,
                 } => {
                let mut new_keys = keys.split_off(split_idx);
                // Pop the separator off to be inserted into the parent.
                let sep = new_keys.pop().unwrap();

                (sep, Node::Leaf {
                    capacity: capacity,
                    keys: new_keys,
                })
            }
            &mut Node::Directory {
                     capacity,
                     ref mut keys,
                     ref mut links,
                 } => {
                let mut new_keys = keys.split_off(split_idx);
                let new_links = links.split_off(split_idx+1);
                let sep = new_keys.pop().unwrap();
                (sep, Node::Directory {
                    capacity: capacity,
                    keys: new_keys,
                    links: new_links,
                })
            }
        }
    }

    fn insert(&mut self, item: T) -> Result<(), InsertError> {
        println!("Inserting item. Capacity is {}, size is {}", self.capacity(), self.size());
        let cap = self.capacity();
        match self {
            &mut Node::Leaf {
                     capacity,
                     ref mut keys,
                 } => {
                // FIXME: safe to rely on keys.capacity()?
                if keys.len() < capacity {
                    let idx = match keys.binary_search(&item) {
                        Ok(_) => return Ok(()), // idempotent insertion?
                        Err(idx) => idx,
                    };

                    keys.insert(idx, item);
                    Ok(())
                } else {
                    Err(InsertError::NodeFull)
                }
            }
            &mut Node::Directory {
                     capacity,
                     ref mut keys,
                     ref mut links,
                 } => {

                assert!(keys.len() + 1 == links.len());

                let idx = match keys.binary_search(&item) {
                    Ok(_) => return Ok(()), // idempotent insertion?
                    Err(idx) => idx,
                };

                let child = links.get(idx).unwrap().clone();
                let result = child.borrow_mut().insert(item);

                match result {
                    Ok(()) => return Ok(()), // success!
                    Err(InsertError::NodeFull) => {
                        // Child needs to be split, if we have space
                        // for an extra link.
                        if links.len() < links.capacity() {
                            let (sep, new_child) = child.borrow_mut().split();
                            keys.insert(idx, sep);
                            links.insert(idx, Arc::new(RefCell::new(new_child)));
                            Ok(())
                        } else {
                            Err(InsertError::NodeFull)
                        }
                    }
                }
            }
        }
    }
}

impl<T: Ord + Clone> Index<T> {
    fn insert(&mut self, item: T) {
        let new_root_needed = self.root.insert(item).is_err();
        if new_root_needed {
            println!("Creating a new root node.");
            // Need to make a new root; the whole tree is full.
            let (sep, new_child) = self.root.split();
            let old_child = self.root.clone();
            let mut new_child_links = Vec::with_capacity(old_child.capacity() + 1);
            let mut new_child_keys: Vec<T> = Vec::with_capacity(old_child.capacity());
            let capacity = old_child.capacity();
            new_child_links.push(Arc::new(RefCell::new(old_child)));
            new_child_links.push(Arc::new(RefCell::new(new_child)));
            new_child_keys.push(sep);
            self.root = Node::Directory {
                capacity: capacity,
                links: new_child_links,
                keys: new_child_keys
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_leaf_insert() {
        let mut idx: Index<u64> = Index::new();
        let range: ::std::ops::Range<u64> = 0..10000;
        for i in range {
            idx.insert(i)
        }
    }
}
