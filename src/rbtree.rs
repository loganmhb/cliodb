//! Persistent red-black trees
use std::cmp::Ordering;
use std::sync::Arc;
use index::Comparator;

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
enum Color {
    Red,
    Black,
}

type Child<T> = Option<Arc<RBTreeNode<T>>>;

#[derive(Debug, Clone)]
struct RBTreeNode<T> {
    color: Color,
    item: T,
    left: Child<T>,
    right: Child<T>,
}

/// Helper function to insert an item into a red-black tree. It does
/// NOT enforce invariants in the base case (leaving that to `balance`
/// in the recursive cases), and it does not color the root of the
/// tree black.
fn ins<T: ::std::fmt::Debug, C>(tree: Child<T>, x: T, comparator: C) -> Arc<RBTreeNode<T>>
where
    T: Ord + Clone,
    C: Comparator<Item = T> + Copy,
{
    match tree {
        Some(ref t) => {
            match C::compare(&x, &t.item) {
                Ordering::Less => {
                    balance(Arc::new(RBTreeNode::new(
                        t.color,
                        Some(ins(t.left.clone(), x, comparator)),
                        t.item.clone(),
                        t.right.clone(),
                    )))
                }
                Ordering::Equal => {
                    t.clone() // duplicate
                }
                Ordering::Greater => {
                    balance(Arc::new(RBTreeNode::new(
                        t.color,
                        t.left.clone(),
                        t.item.clone(),
                        Some(ins(t.right.clone(), x, comparator)),
                    )))
                }
            }
        }
        None => Arc::new(RBTreeNode::new_red(None, x, None)),
    }
}

fn has_red_child<T>(tree: &RBTreeNode<T>) -> bool {
    tree.left.as_ref().map_or(false, |c| c.color == Color::Red) ||
        tree.right.as_ref().map_or(false, |c| c.color == Color::Red)
}

/// A tree needs balancing if it is a black node which has a red child
/// which itself has a red child.
fn needs_balancing<T: ::std::fmt::Debug>(tree: &RBTreeNode<T>) -> bool {
    tree.color == Color::Black &&
        (tree.left.as_ref().map_or(false, |t| {
            t.color == Color::Red && has_red_child(&*t)
        }) ||
             tree.right.as_ref().map_or(false, |t| {
                t.color == Color::Red && has_red_child(&*t)
            }))
}

/// Takes a potentially-unbalanced red-black tree and returns a
/// balanced equivalent tree, per Okasaki
/// (http://www.westpoint.edu/eecs/SiteAssets/SitePages/Faculty%20Publication
///  %20Documents/Okasaki/jfp99redblack.pdf).
fn balance<T: ::std::fmt::Debug + Ord + Clone>(tree: Arc<RBTreeNode<T>>) -> Arc<RBTreeNode<T>> {
    if needs_balancing(&tree) {
        if tree.left.clone().map(|ref c| c.color) == Some(Color::Red) &&
            has_red_child(&*tree.left.clone().unwrap())
        {
            // unwrap() is safe because of above pattern match on Some(Color::Red),
            // but we can't match through the Arc to actually eliminate it
            // within the type system (ノ°Д°）ノ︵ ┻━┻
            let left_child = tree.left.clone().unwrap();
            if left_child.left.clone().map(|gc| gc.color) == Some(Color::Red) {
                // Pattern one: left red child, left red grandchild.
                let left_gc = left_child.left.clone().unwrap();
                let new_left_child = RBTreeNode::new_black(
                    left_gc.left.clone(),
                    left_gc.item.clone(),
                    left_gc.right.clone(),
                );
                let new_right_child = RBTreeNode::new_black(
                    left_child.right.clone(),
                    tree.item.clone(),
                    tree.right.clone(),
                );

                Arc::new(RBTreeNode::new_red(
                    Some(Arc::new(new_left_child)),
                    left_child.item.clone(),
                    Some(Arc::new(new_right_child)),
                ))
            } else {
                // Because of the has_red_child clause in the if, we
                // know that in this else branch the left child's
                // right child must be red.
                //
                // Pattern two: left red child, right red grandchild.
                let right_gc = left_child.right.clone().unwrap();
                let new_left_child = RBTreeNode::new_black(
                    left_child.left.clone(),
                    left_child.item.clone(),
                    right_gc.left.clone(),
                );
                let new_right_child = RBTreeNode::new_black(
                    right_gc.right.clone(),
                    tree.item.clone(),
                    tree.right.clone(),
                );

                Arc::new(RBTreeNode::new_red(
                    Some(Arc::new(new_left_child)),
                    right_gc.item.clone(),
                    Some(Arc::new(new_right_child)),
                ))
            }
        } else {
            // Because of the needs_balancing guard, if the left child
            // wasn't red with a red child the right one must be.
            let right_child = tree.right.clone().unwrap();
            if right_child.left.clone().map(|gc| gc.color) == Some(Color::Red) {
                // Pattern three: right red child, left red grandchild.
                let left_gc = right_child.left.clone().unwrap();
                let new_left_child = RBTreeNode::new_black(
                    tree.left.clone(),
                    tree.item.clone(),
                    left_gc.left.clone(),
                );
                let new_right_child = RBTreeNode::new_black(
                    left_gc.right.clone(),
                    right_child.item.clone(),
                    right_child.right.clone(),
                );

                Arc::new(RBTreeNode::new_red(
                    Some(Arc::new(new_left_child)),
                    left_gc.item.clone(),
                    Some(Arc::new(new_right_child)),
                ))
            } else {
                // Pattern four: right red child, right red grandchild.
                let right_gc = right_child.right.clone().unwrap();
                let new_left_child = RBTreeNode::new_black(
                    tree.left.clone(),
                    tree.item.clone(),
                    right_child.left.clone(),
                );
                let new_right_child = RBTreeNode::new_black(
                    right_gc.left.clone(),
                    right_gc.item.clone(),
                    right_gc.right.clone(),
                );

                Arc::new(RBTreeNode::new_red(
                    Some(Arc::new(new_left_child)),
                    right_child.item.clone(),
                    Some(Arc::new(new_right_child)),
                ))
            }
        }
    } else {
        tree
    }
}

#[derive(Debug, Clone, Default)]
pub struct RBTree<T, C> {
    root: Child<T>,
    size: usize,
    comparator: C,
}

impl<T: ::std::fmt::Debug + Ord + Clone> RBTreeNode<T> {
    fn new(color: Color, left: Child<T>, item: T, right: Child<T>) -> RBTreeNode<T> {
        RBTreeNode {
            color,
            left,
            right,
            item,
        }
    }

    fn new_black(left: Child<T>, item: T, right: Child<T>) -> RBTreeNode<T> {
        RBTreeNode::new(Color::Black, left, item, right)
    }

    fn new_red(left: Child<T>, item: T, right: Child<T>) -> RBTreeNode<T> {
        RBTreeNode::new(Color::Red, left, item, right)
    }

    fn make_black(&self) -> Arc<RBTreeNode<T>> {
        Arc::new(RBTreeNode {
            color: Color::Black,
            left: self.left.clone(),
            right: self.right.clone(),
            item: self.item.clone(),
        })
    }
}

impl<T: ::std::fmt::Debug + Ord + Clone, C: Comparator<Item = T> + Copy> RBTree<T, C> {
    pub fn new(comparator: C) -> RBTree<T, C> {
        RBTree {
            root: None,
            size: 0,
            comparator,
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn insert(&self, x: T) -> RBTree<T, C> {
        let tree = RBTree {
            root: Some(ins(self.root.clone(), x, self.comparator).make_black()),
            size: self.size + 1,
            comparator: self.comparator,
        };
        tree
    }

    pub fn iter(&self) -> Iter<T> {
        let mut stack = Vec::new();
        let mut node = self.root.clone();

        // Push left children onto the stack to initialize the search.
        while let Some(node_ptr) = node {
            stack.push(node_ptr.clone());
            node = node_ptr.left.clone();
            continue;
        }

        Iter { stack }
    }

    pub fn range_from(&self, start: T) -> Iter<T> {
        let mut stack = Vec::new();
        let mut node = self.root.clone();

        while let Some(node_ptr) = node.clone() {
            match C::compare(&node_ptr.item, &start) {
                Ordering::Greater => {
                    node = node_ptr.left.clone();
                    stack.push(node_ptr);
                    continue;
                }
                Ordering::Equal => {
                    stack.push(node_ptr);
                    break;
                }
                Ordering::Less => {
                    // This node is too small and should be skipped, but
                    // we might still need to start in its right subtree.
                    node = node_ptr.right.clone();
                    continue;
                }
            }
        }

        Iter { stack }
    }
}

pub struct Iter<T> {
    stack: Vec<Arc<RBTreeNode<T>>>,
}

impl<T: Clone> Iterator for Iter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        // The node at the top of the stack, if any, contains the value to yield.
        // But before yielding, we need to push the node's right child (if any)
        // and all of its left children.
        if let Some(node) = self.stack.pop() {
            let val = node.item.clone();
            let mut next_node = node.right.clone();

            while let Some(child) = next_node {
                next_node = child.left.clone();
                self.stack.push(child);
            }

            Some(val)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use test::{Bencher};
    use index::NumComparator;

    fn enumerate_tree<T: Clone>(tree: Child<T>, mut vec: &mut Vec<T>) {
        match tree {
            Some(ref t) => {
                enumerate_tree(t.left.clone(), &mut vec);
                vec.push(t.item.clone());
                enumerate_tree(t.right.clone(), &mut vec);
            }
            None => (),
        }
    }

    #[test]
    fn test_needs_balancing() {
        let red_child = Some(Arc::new(RBTreeNode::new_red(None, 0, None)));

        let with_red_child = RBTreeNode::new_black(red_child.clone(), 0, red_child.clone());
        assert!(has_red_child(&with_red_child));

        let with_red_grandchild = RBTreeNode::new_black(
            Some(Arc::new(RBTreeNode::new_red(red_child.clone(), 0, None))),
            0,
            None,
        );

        assert!(needs_balancing(&with_red_grandchild));
        assert!(!needs_balancing(&with_red_child));
    }

    fn thousand_tree() -> RBTree<i64, NumComparator> {
        let mut t = RBTree::default();

        for i in 0..1000 {
            t = t.insert(999 - i);
        }

        t
    }

    #[test]
    fn test_inserts() {
        let t = thousand_tree();
        let mut enumerated = vec![];
        enumerate_tree(t.root.clone(), &mut enumerated);
        assert_eq!(enumerated, (0..1000).collect::<Vec<_>>());
    }

    #[test]
    fn test_pluggable_comparator() {
        use std::cmp::Ordering;
        use itertools::assert_equal;

        #[derive(Clone, Default, Copy, Debug)]
        struct RevComparator;

        impl Comparator for RevComparator {
            type Item = i64;

            fn compare(a: &i64, b: &i64) -> Ordering {
                b.cmp(a) // backwards!
            }
        }

        let mut t: RBTree<i64, RevComparator> = RBTree::default();

        for i in 0..1000 {
            t = t.insert(i);
        }

        let mut reversed: Vec<i64> = (0..1000).collect();
        reversed.reverse();

        assert_equal(t.iter(), reversed.into_iter())

    }

    #[test]
    fn test_iter() {
        let t = thousand_tree();

        assert_eq!((0..1000).collect::<Vec<_>>(), t.iter().collect::<Vec<_>>());
        assert_eq!(
            t.range_from(500).collect::<Vec<_>>(),
            (500..1000).collect::<Vec<_>>()
        );
    }

    fn assert_invariants<T>(root: &RBTreeNode<T>) {
        // Root must be black
        assert_eq!(root.color, Color::Black);

        // No red node with a red child
        fn assert_color_invariants<T>(node: &RBTreeNode<T>) {
            match node.left {
                Some(ref left) => assert_color_invariants(&left),
                None => (),
            }

            match node.right {
                Some(ref right) => assert_color_invariants(&right),
                None => (),
            }

            match node.color {
                Color::Red => assert!(!has_red_child(&node)),
                _ => (),
            }
        }

        assert_color_invariants(&root);

        // All branches have the same number of black nodes
        // (DFS saving the black depth of the first leaf seen)
        fn check_black_depth<T>(node: &RBTreeNode<T>) -> usize {
            let child_depth = match (&node.right, &node.left) {
                (&Some(ref right), &Some(ref left)) => {
                    let child_depth = check_black_depth(&right);
                    assert_eq!(child_depth, check_black_depth(&left));
                    child_depth
                }
                (&None, &Some(ref child)) |
                (&Some(ref child), &None) => {
                    let child_depth = check_black_depth(&child);
                    assert_eq!(child_depth, 0);
                    child_depth
                }
                _ => 0,
            };

            match node.color {
                Color::Black => 1 + child_depth,
                Color::Red => child_depth,
            }
        }

        check_black_depth(&root);
    }

    #[test]
    fn test_large_tree() {
        // This overflowed the stack when there was a bug that caused the tree
        // not to balance properly.
        let mut t: RBTree<i64, NumComparator> = RBTree::default();

        for i in 1..100000 {
            t = t.insert(i);
        }

        assert_invariants(&t.root.unwrap());
    }

    #[bench]
    fn bench_insert_elements(b: &mut Bencher) {
        let mut t: RBTree<i64, NumComparator> = RBTree::default();
        for i in 1..100000 {
            t = t.insert(i)
        }
        b.iter(|| t.insert(100001))
    }
}
