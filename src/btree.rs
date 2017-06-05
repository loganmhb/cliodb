pub const CAPACITY: usize = 512;

pub enum Insertion<N: Node> {
    Inserted(N),
    Duplicate,
    NodeFull,
}

pub trait Node where Self: Sized {
    type Item: Ord + Clone;
    type Reference: Clone;

    /// Return the number of items in the node.
    fn size(&self) -> usize;
    /// Allocate the node, however applicable, and return a reference.
    fn save(self) -> Self::Reference;
    fn is_leaf(&self) -> bool;
    fn items(&self) -> &[Self::Item];
    fn links(&self) -> &[Self::Reference];

    /// For a directory node, returns the child at link index idx.
    /// It's a logic error to call this on a leaf node.
    fn child(&self, idx: usize) -> Self;

    fn new_leaf(items: Vec<Self::Item>) -> Self;
    fn new_dir(items: Vec<Self::Item>, links: Vec<Self::Reference>) -> Self;

    fn insert(&self, item: Self::Item) -> Insertion<Self> {
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

            let child = self.child(idx);
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
