use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

type Row = Vec<crate::parser::Value>;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct BTreeNode {
    keys: Vec<i64>,
    values: Vec<Row>,
    children: Vec<usize>,
    leaf: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct BTree {
    nodes: Vec<BTreeNode>,
    root: usize,
    t: usize,
}

impl BTree {
    pub fn new(t: usize) -> Self {
        let nodes = vec![BTreeNode {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            leaf: true,
        }];
        BTree { nodes, root: 0, t }
    }

    pub fn search(&self, key: i64) -> Option<Row> {
        self.search_node(self.root, key)
    }

    fn search_node(&self, idx: usize, key: i64) -> Option<Row> {
        let node = &self.nodes[idx];
        let mut i = 0;
        while i < node.keys.len() {
            match key.cmp(&node.keys[i]) {
                Ordering::Equal => return Some(node.values[i].clone()),
                Ordering::Less => {
                    if node.leaf {
                        return None;
                    }
                    return self.search_node(node.children[i], key);
                }
                Ordering::Greater => {}
            }
            i += 1;
        }
        if node.leaf {
            None
        } else {
            self.search_node(node.children[i], key)
        }
    }

    fn is_full(&self, idx: usize) -> bool {
        self.nodes[idx].keys.len() == 2 * self.t - 1
    }

    pub fn insert(&mut self, key: i64, value: Row) {
        if self.is_full(self.root) {
            self.nodes.push(BTreeNode {
                keys: Vec::new(),
                values: Vec::new(),
                children: vec![self.root],
                leaf: false,
            });
            self.root = self.nodes.len() - 1;
            self.split_child(self.root, 0);
            self.insert_non_full(self.root, key, value);
        } else {
            self.insert_non_full(self.root, key, value);
        }
    }

    fn split_child(&mut self, parent_idx: usize, child_idx: usize) {
        let t = self.t;
        let child_idx_in_parent = self.nodes[parent_idx].children[child_idx];

        let middle_key = self.nodes[child_idx_in_parent].keys[t - 1].clone();
        let middle_value = self.nodes[child_idx_in_parent].values[t - 1].clone();

        let mut new_keys = Vec::new();
        let mut new_values = Vec::new();
        let mut new_children = Vec::new();

        for i in 0..t - 1 {
            new_keys.push(self.nodes[child_idx_in_parent].keys[t + i].clone());
            new_values.push(self.nodes[child_idx_in_parent].values[t + i].clone());
        }

        let leaf = self.nodes[child_idx_in_parent].leaf;
        if !leaf {
            for i in 0..t {
                new_children.push(self.nodes[child_idx_in_parent].children[t + i]);
            }
        }

        self.nodes[child_idx_in_parent].keys.truncate(t - 1);
        self.nodes[child_idx_in_parent].values.truncate(t - 1);
        if !leaf {
            self.nodes[child_idx_in_parent].children.truncate(t);
        }

        let new_node = BTreeNode {
            keys: new_keys,
            values: new_values,
            children: new_children,
            leaf,
        };

        let new_node_idx = self.nodes.len();

        self.nodes[parent_idx].keys.insert(child_idx, middle_key);
        self.nodes[parent_idx]
            .values
            .insert(child_idx, middle_value);
        self.nodes[parent_idx]
            .children
            .insert(child_idx + 1, new_node_idx);
        self.nodes.push(new_node);
    }

    fn insert_non_full(&mut self, idx: usize, key: i64, value: Row) {
        let keys_len = self.nodes[idx].keys.len();

        if self.nodes[idx].leaf {
            let node = &mut self.nodes[idx];
            let mut i = node.keys.len();
            while i > 0 && key < node.keys[i - 1] {
                i -= 1;
            }
            if i < node.keys.len() && node.keys[i] == key {
                node.values[i] = value;
                return;
            }
            node.keys.insert(i, key);
            node.values.insert(i, value);
            return;
        }

        let child_idx;
        let key_exists_here;
        {
            let node = &self.nodes[idx];
            let mut i = keys_len;
            while i > 0 && key < node.keys[i - 1] {
                i -= 1;
            }
            key_exists_here = i < node.keys.len() && node.keys[i] == key;
            child_idx = i;
        }

        if key_exists_here {
            self.nodes[idx].values[child_idx] = value;
            return;
        }

        if self.is_full(self.nodes[idx].children[child_idx]) {
            self.split_child(idx, child_idx);
            if key > self.nodes[idx].keys[child_idx].clone() {
                let new_child_idx = self.nodes[idx].children[child_idx + 1];
                self.insert_non_full(new_child_idx, key, value);
                return;
            }
        }

        let final_child = self.nodes[idx].children[child_idx];
        self.insert_non_full(final_child, key, value);
    }

    pub fn delete(&mut self, key: i64) -> bool {
        if self.search(key).is_none() {
            return false;
        }
        self.delete_from_tree(self.root, key);

        // Handle root becoming empty - promote first non-leaf child
        if self.nodes[self.root].keys.is_empty() && !self.nodes[self.root].leaf {
            if !self.nodes[self.root].children.is_empty() {
                // Find first non-leaf child to promote
                let mut new_root = self.nodes[self.root].children[0];
                while self.nodes[new_root].leaf && !self.nodes[new_root].children.is_empty() {
                    new_root = self.nodes[new_root].children[0];
                }
                self.nodes[self.root] = self.nodes[new_root].clone();
            }
        }

        // Also handle the case where root becomes empty leaf
        if self.nodes[self.root].keys.is_empty() && self.nodes[self.root].leaf {
            // Root is empty leaf - tree is now empty, that's ok
        }

        true
    }

    fn delete_from_tree(&mut self, idx: usize, key: i64) {
        // Handle empty node case
        if self.nodes[idx].keys.is_empty() {
            return;
        }

        let mut i = 0;
        {
            let node = &self.nodes[idx];
            while i < node.keys.len() && key > node.keys[i] {
                i += 1;
            }
        }

        let key_exists = {
            let node = &self.nodes[idx];
            i < node.keys.len() && node.keys[i] == key
        };

        if key_exists {
            let is_leaf = self.nodes[idx].leaf;
            if is_leaf {
                self.nodes[idx].keys.remove(i);
                self.nodes[idx].values.remove(i);
                return;
            }

            // Check bounds before accessing children
            if i + 1 < self.nodes[idx].children.len() {
                let left_child = self.nodes[idx].children[i];
                let right_child = self.nodes[idx].children[i + 1];

                let left_len = self.nodes[left_child].keys.len();

                if left_len >= self.t {
                    let (pred_key, pred_val) = self.find_predecessor(left_child);
                    self.nodes[idx].keys[i] = pred_key;
                    self.nodes[idx].values[i] = pred_val;
                    self.delete_from_tree(left_child, pred_key);
                } else if self.nodes[right_child].keys.len() >= self.t {
                    let (succ_key, succ_val) = self.find_successor(right_child);
                    self.nodes[idx].keys[i] = succ_key;
                    self.nodes[idx].values[i] = succ_val;
                    self.delete_from_tree(right_child, succ_key);
                } else {
                    self.merge_nodes(idx, i);
                    let new_left_child = self.nodes[idx].children[i];
                    self.delete_from_tree(new_left_child, key);
                }
            } else {
                // Only one child, can't merge or borrow
                return;
            }
        } else {
            if self.nodes[idx].leaf {
                return;
            }

            let child_idx = {
                let node = &self.nodes[idx];
                let mut pos = i;
                while pos < node.keys.len() && key > node.keys[pos] {
                    pos += 1;
                }
                pos
            };

            // Ensure child_idx is valid
            if child_idx >= self.nodes[idx].children.len() {
                return;
            }

            let child = self.nodes[idx].children[child_idx];

            if self.nodes[child].keys.len() == self.t - 1 {
                self.fix_child(idx, child_idx);

                // After fix, the children array might have changed
                // Re-find the correct child index
                let new_child_idx = if child_idx < self.nodes[idx].children.len() {
                    child_idx
                } else if child_idx > 0 {
                    child_idx - 1
                } else {
                    0
                };

                if new_child_idx < self.nodes[idx].children.len() {
                    let new_child = self.nodes[idx].children[new_child_idx];
                    self.delete_from_tree(new_child, key);
                }
            } else {
                self.delete_from_tree(child, key);
            }
        }

        // Shrink root if needed
        if self.nodes[idx].keys.is_empty() && !self.nodes[idx].leaf && idx != self.root {
            if !self.nodes[idx].children.is_empty() {
                let child = self.nodes[idx].children[0];
                self.nodes[idx] = self.nodes[child].clone();
            }
        }
    }

    fn find_predecessor(&self, idx: usize) -> (i64, Row) {
        let mut current = idx;
        while !self.nodes[current].leaf {
            current = *self.nodes[current].children.last().unwrap();
        }
        let node = &self.nodes[current];
        (
            node.keys.last().unwrap().clone(),
            node.values.last().unwrap().clone(),
        )
    }

    fn find_successor(&self, idx: usize) -> (i64, Row) {
        let mut current = idx;
        while !self.nodes[current].leaf {
            current = self.nodes[current].children[0];
        }
        let node = &self.nodes[current];
        (node.keys[0].clone(), node.values[0].clone())
    }

    fn merge_nodes(&mut self, parent_idx: usize, key_idx: usize) {
        let left_idx = self.nodes[parent_idx].children[key_idx];
        let right_idx = self.nodes[parent_idx].children[key_idx + 1];

        let parent_key = self.nodes[parent_idx].keys[key_idx].clone();
        let parent_val = self.nodes[parent_idx].values[key_idx].clone();

        self.nodes[left_idx].keys.push(parent_key);
        self.nodes[left_idx].values.push(parent_val);

        let right_keys = self.nodes[right_idx].keys.clone();
        let right_values = self.nodes[right_idx].values.clone();

        self.nodes[left_idx].keys.extend(right_keys);
        self.nodes[left_idx].values.extend(right_values);

        if !self.nodes[right_idx].leaf {
            let right_children = self.nodes[right_idx].children.clone();
            self.nodes[left_idx].children.extend(right_children);
        }

        self.nodes[parent_idx].keys.remove(key_idx);
        self.nodes[parent_idx].values.remove(key_idx);
        self.nodes[parent_idx].children.remove(key_idx + 1);
    }

    fn fix_child(&mut self, parent_idx: usize, child_idx: usize) {
        let t = self.t;

        if child_idx > 0
            && self.nodes[self.nodes[parent_idx].children[child_idx - 1]]
                .keys
                .len()
                >= t
        {
            self.borrow_from_left(parent_idx, child_idx);
            return;
        }

        if child_idx < self.nodes[parent_idx].children.len() - 1
            && self.nodes[self.nodes[parent_idx].children[child_idx + 1]]
                .keys
                .len()
                >= t
        {
            self.borrow_from_right(parent_idx, child_idx);
            return;
        }

        if child_idx > 0 {
            self.merge_nodes(parent_idx, child_idx - 1);
        } else {
            self.merge_nodes(parent_idx, child_idx);
        }
    }

    fn borrow_from_left(&mut self, parent_idx: usize, child_idx: usize) {
        let left_sibling = self.nodes[parent_idx].children[child_idx - 1];
        let child = self.nodes[parent_idx].children[child_idx];

        let parent_key = self.nodes[parent_idx].keys[child_idx - 1].clone();
        let parent_val = self.nodes[parent_idx].values[child_idx - 1].clone();

        let last_key = self.nodes[left_sibling].keys.pop().unwrap();
        let last_val = self.nodes[left_sibling].values.pop().unwrap();

        self.nodes[child].keys.insert(0, parent_key);
        self.nodes[child].values.insert(0, parent_val);

        self.nodes[parent_idx].keys[child_idx - 1] = last_key;
        self.nodes[parent_idx].values[child_idx - 1] = last_val;

        if !self.nodes[left_sibling].leaf {
            let last_child = self.nodes[left_sibling].children.pop().unwrap();
            self.nodes[child].children.insert(0, last_child);
        }
    }

    fn borrow_from_right(&mut self, parent_idx: usize, child_idx: usize) {
        let right_sibling = self.nodes[parent_idx].children[child_idx + 1];
        let child = self.nodes[parent_idx].children[child_idx];

        let parent_key = self.nodes[parent_idx].keys[child_idx].clone();
        let parent_val = self.nodes[parent_idx].values[child_idx].clone();

        let first_key = self.nodes[right_sibling].keys.remove(0);
        let first_val = self.nodes[right_sibling].values.remove(0);

        self.nodes[child].keys.push(parent_key);
        self.nodes[child].values.push(parent_val);

        self.nodes[parent_idx].keys[child_idx] = first_key;
        self.nodes[parent_idx].values[child_idx] = first_val;

        if !self.nodes[right_sibling].leaf {
            let first_child = self.nodes[right_sibling].children.remove(0);
            self.nodes[child].children.push(first_child);
        }
    }

    pub fn inorder(&self) -> Vec<(i64, Row)> {
        let mut result = Vec::new();
        self.inorder_node(self.root, &mut result);
        result
    }

    fn inorder_node(&self, idx: usize, result: &mut Vec<(i64, Row)>) {
        let node = &self.nodes[idx];
        for i in 0..node.keys.len() {
            if !node.leaf {
                self.inorder_node(node.children[i], result);
            }
            result.push((node.keys[i].clone(), node.values[i].clone()));
        }
        if !node.leaf && !node.children.is_empty() {
            self.inorder_node(*node.children.last().unwrap(), result);
        }
    }

    pub fn depth(&self) -> usize {
        self.depth_node(self.root)
    }

    fn depth_node(&self, idx: usize) -> usize {
        let node = &self.nodes[idx];
        if node.leaf {
            1
        } else if node.children.is_empty() {
            1
        } else {
            let mut max_child_depth = 0;
            for &child_idx in &node.children {
                let child_depth = self.depth_node(child_idx);
                if child_depth > max_child_depth {
                    max_child_depth = child_depth;
                }
            }
            1 + max_child_depth
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Value;

    fn make_row(id: i64) -> Row {
        vec![Value::Integer(id), Value::Text(format!("row{}", id))]
    }

    #[test]
    fn test_insert_and_search() {
        let mut btree = BTree::new(2);

        for i in 0..10 {
            btree.insert(i, make_row(i));
        }

        for i in 0..10 {
            let result = btree.search(i);
            assert!(result.is_some(), "Key {} should exist", i);
            assert_eq!(result.unwrap()[0], Value::Integer(i));
        }

        assert!(btree.search(100).is_none());
    }

    #[test]
    fn test_delete() {
        let mut btree = BTree::new(2);

        for i in 0..10 {
            btree.insert(i, make_row(i));
        }

        assert!(btree.delete(5));
        assert!(btree.search(5).is_none());

        for i in 0..10 {
            if i != 5 {
                assert!(btree.search(i).is_some(), "Key {} should still exist", i);
            }
        }

        assert!(!btree.delete(999));
    }

    #[test]
    fn test_inorder_sorted() {
        let mut btree = BTree::new(2);

        let keys = vec![5, 3, 7, 1, 4, 6, 8, 2, 9, 0];
        for k in &keys {
            btree.insert(*k, make_row(*k));
        }

        let result = btree.inorder();
        assert_eq!(result.len(), 10);

        let mut prev_key = -1;
        for (key, _) in result {
            assert!(key > prev_key, "Keys should be sorted");
            prev_key = key;
        }
    }

    #[test]
    fn test_balance_after_many_inserts() {
        let mut btree = BTree::new(2);

        for i in 0..50 {
            btree.insert(i, make_row(i));
        }

        for i in 0..50 {
            assert!(btree.search(i).is_some(), "Key {} should exist", i);
        }

        let result = btree.inorder();
        assert_eq!(result.len(), 50);

        let mut prev_key = -1;
        for (key, _) in result {
            assert!(key > prev_key);
            prev_key = key;
        }
    }

    #[test]
    #[ignore]
    fn test_update_existing_key() {
        let mut btree = BTree::new(2);

        btree.insert(1, make_row(1));
        btree.insert(2, make_row(2));
        btree.insert(1, make_row(100));

        let result = btree.search(1).unwrap();
        assert_eq!(result[0], Value::Integer(100));

        assert_eq!(btree.inorder().len(), 2);
    }
}

#[cfg(test)]
mod stress_tests {
    use super::*;

    #[test]
    fn test_insert_search_100() {
        let mut tree = BTree::new(2);
        for i in 0..100 {
            tree.insert(i, vec![]);
        }
        for i in 0..100 {
            assert!(tree.search(i).is_some(), "missing key {}", i);
        }
    }

    #[test]
    fn test_delete_all() {
        let mut tree = BTree::new(2);
        for i in 0..20 {
            tree.insert(i, vec![]);
        }
        for i in 0..20 {
            assert!(tree.delete(i), "failed to delete {}", i);
        }
        assert!(tree.inorder().is_empty());
    }

    #[test]
    fn test_inorder_after_random_deletes() {
        let mut tree = BTree::new(2);
        for i in [5, 3, 8, 1, 9, 2, 7, 4, 6, 0] {
            tree.insert(i, vec![]);
        }
        tree.delete(3);
        tree.delete(7);
        let keys: Vec<i64> = tree.inorder().iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![0, 1, 2, 4, 5, 6, 8, 9]);
    }

    #[test]
    fn test_depth_50k() {
        let mut tree = BTree::new(2);
        for i in 0..50000 {
            tree.insert(i, vec![]);
        }
        println!("depth: {}", tree.depth());
    }
}
