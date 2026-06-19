use crate::disk::{PageId, RecordId, PAGE_SIZE};
use serde::{Deserialize, Serialize};

pub const MAX_KEYS: usize = 100;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DiskBTreeNode {
    pub page_id: PageId,
    pub is_leaf: bool,
    pub keys: Vec<i64>,
    pub values: Vec<RecordId>,
    pub children: Vec<PageId>,
}

impl DiskBTreeNode {
    pub fn new(page_id: PageId, is_leaf: bool) -> Self {
        Self {
            page_id,
            is_leaf,
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
        }
    }

    pub fn encode(&self) -> [u8; PAGE_SIZE] {
        let mut buffer = [0u8; PAGE_SIZE];
        let serialized = bincode::serialize(self).expect("Failed to serialize node");
        assert!(
            serialized.len() <= PAGE_SIZE,
            "Node exceeds 4KB page size! Data too large."
        );
        buffer[..serialized.len()].copy_from_slice(&serialized);
        buffer
    }

    pub fn decode(buffer: &[u8; PAGE_SIZE]) -> Self {
        bincode::deserialize(buffer).expect("Failed to deserialize node")
    }
}

use crate::buffer::BufferPoolManager;
use std::sync::{Arc, Mutex};

pub struct DiskBTree {
    pub buffer_pool: Arc<Mutex<BufferPoolManager>>,
    pub root_page_id: PageId,
}

impl DiskBTree {
    pub fn new(buffer_pool: Arc<Mutex<BufferPoolManager>>, root_page_id: PageId) -> Self {
        Self {
            buffer_pool,
            root_page_id,
        }
    }

    pub fn get_node(&self, page_id: PageId) -> DiskBTreeNode {
        let mut pool = self.buffer_pool.lock().unwrap();
        let page = pool.fetch_page(page_id).unwrap().unwrap();
        let node = DiskBTreeNode::decode(&page.data);
        pool.unpin_page(page_id, false);
        node
    }

    pub fn save_node(&self, node: &DiskBTreeNode) {
        let mut pool = self.buffer_pool.lock().unwrap();
        let page = pool.fetch_page(node.page_id).unwrap().unwrap();
        page.data = node.encode();
        pool.unpin_page(node.page_id, true);
    }

    pub fn search(&self, key: i64) -> Option<RecordId> {
        let mut current_page_id = self.root_page_id;

        loop {
            let node = self.get_node(current_page_id);

            let mut i = 0;
            while i < node.keys.len() && key > node.keys[i] {
                i += 1;
            }

            if i < node.keys.len() && key == node.keys[i] {
                return Some(node.values[i]);
            }

            if node.is_leaf {
                return None;
            }

            current_page_id = node.children[i];
        }
    }

    pub fn insert(&mut self, key: i64, value: RecordId) {
        let root = self.get_node(self.root_page_id);

        if root.keys.len() >= MAX_KEYS {
            let new_root_id = {
                let mut pool = self.buffer_pool.lock().unwrap();
                let new_page = pool.new_page().unwrap().unwrap();
                let id = new_page.id;
                pool.unpin_page(id, true);
                id
            };

            let new_root = DiskBTreeNode {
                page_id: new_root_id,
                is_leaf: false,
                keys: Vec::new(),
                values: Vec::new(),
                children: vec![self.root_page_id],
            };
            self.save_node(&new_root);

            self.root_page_id = new_root_id;
            self.split_child(new_root_id, 0);
        }

        self.insert_non_full(self.root_page_id, key, value);
    }

    fn insert_non_full(&mut self, current_page_id: PageId, key: i64, value: RecordId) {
        let mut node = self.get_node(current_page_id);

        if node.is_leaf {
            let mut insert_idx = 0;
            while insert_idx < node.keys.len() && key > node.keys[insert_idx] {
                insert_idx += 1;
            }

            if insert_idx < node.keys.len() && key == node.keys[insert_idx] {
                node.values[insert_idx] = value;
            } else {
                node.keys.insert(insert_idx, key);
                node.values.insert(insert_idx, value);
            }
            self.save_node(&node);
        } else {
            let mut i = 0;
            while i < node.keys.len() && key > node.keys[i] {
                i += 1;
            }

            let child = self.get_node(node.children[i]);
            if child.keys.len() >= MAX_KEYS {
                self.split_child(current_page_id, i);
                node = self.get_node(current_page_id);
                if key > node.keys[i] {
                    i += 1;
                }
            }

            self.insert_non_full(node.children[i], key, value);
        }
    }

    fn split_child(&mut self, parent_page_id: PageId, child_index: usize) {
        let mut parent = self.get_node(parent_page_id);
        let mut full_child = self.get_node(parent.children[child_index]);

        let mut pool = self.buffer_pool.lock().unwrap();
        let new_page = pool.new_page().unwrap().unwrap();
        let new_sibling_id = new_page.id;
        pool.unpin_page(new_sibling_id, true);
        drop(pool);

        let mid_index = full_child.keys.len() / 2;
        let median_key = full_child.keys[mid_index];
        let median_val = full_child.values[mid_index];

        let right_sibling = DiskBTreeNode {
            page_id: new_sibling_id,
            is_leaf: full_child.is_leaf,
            keys: full_child.keys.split_off(mid_index + 1),
            values: full_child.values.split_off(mid_index + 1),
            children: if full_child.is_leaf {
                Vec::new()
            } else {
                full_child.children.split_off(mid_index + 1)
            },
        };

        full_child.keys.pop();
        full_child.values.pop();

        parent.keys.insert(child_index, median_key);
        parent.values.insert(child_index, median_val);
        parent.children.insert(child_index + 1, new_sibling_id);

        self.save_node(&full_child);
        self.save_node(&right_sibling);
        self.save_node(&parent);
    }


    /// Delete a key from the B-Tree. Returns true if the key was found and deleted.
    pub fn delete(&mut self, key: i64) -> bool {
        if self.search(key).is_none() {
            return false;
        }
        self.delete_entry(self.root_page_id, key);

        // Handle root shrinking: if root is empty internal node, promote first child
        let root = self.get_node(self.root_page_id);
        if root.keys.is_empty() && !root.is_leaf && !root.children.is_empty() {
            self.root_page_id = root.children[0];
        }

        true
    }

    fn delete_entry(&mut self, page_id: PageId, key: i64) {
        let node = self.get_node(page_id);

        if node.keys.is_empty() {
            return;
        }

        // Find position of key
        let mut i = 0;
        while i < node.keys.len() && key > node.keys[i] {
            i += 1;
        }

        let key_found = i < node.keys.len() && node.keys[i] == key;

        if key_found {
            if node.is_leaf {
                // Case 1: key is in a leaf — remove directly
                let mut node = node;
                node.keys.remove(i);
                node.values.remove(i);
                self.save_node(&node);
            } else {
                // Case 2: key is in an internal node — replace with predecessor
                let left_child_id = node.children[i];
                let (pred_key, pred_val) = self.find_predecessor(left_child_id);

                let mut node = self.get_node(page_id);
                node.keys[i] = pred_key;
                node.values[i] = pred_val;
                self.save_node(&node);

                // Delete predecessor from the left subtree
                self.delete_entry(left_child_id, pred_key);

                // After recursive delete, fix underflow in the left child
                let child = self.get_node(left_child_id);
                if child.keys.len() < MAX_KEYS / 2 {
                    self.fix_child(page_id, i);
                }
            }
        } else {
            // Key not in this node
            if node.is_leaf {
                return;
            }

            let child_idx = i; // key would be in children[i]

            if child_idx >= node.children.len() {
                return;
            }

            // Ensure the child we descend into has enough keys
            let child = self.get_node(node.children[child_idx]);
            if child.keys.len() <= MAX_KEYS / 2 {
                self.fix_child(page_id, child_idx);

                // After fix, re-read parent and re-find child index
                let node = self.get_node(page_id);
                let mut new_i = 0;
                while new_i < node.keys.len() && key > node.keys[new_i] {
                    new_i += 1;
                }
                let new_child_idx = new_i;
                if new_child_idx < node.children.len() {
                    self.delete_entry(node.children[new_child_idx], key);
                }
            } else {
                self.delete_entry(node.children[child_idx], key);
            }
        }
    }

    /// Find the predecessor (largest key in the subtree rooted at page_id).
    fn find_predecessor(&self, page_id: PageId) -> (i64, RecordId) {
        let mut current_id = page_id;
        loop {
            let node = self.get_node(current_id);
            if node.is_leaf {
                let last = node.keys.len() - 1;
                return (node.keys[last], node.values[last]);
            }
            current_id = *node.children.last().unwrap();
        }
    }

    /// Fix an underfull child at `child_idx` by borrowing from a sibling or merging.
    fn fix_child(&mut self, parent_page_id: PageId, child_idx: usize) {
        let parent = self.get_node(parent_page_id);

        // Try to borrow from left sibling
        if child_idx > 0 {
            let left_sibling = self.get_node(parent.children[child_idx - 1]);
            if left_sibling.keys.len() > MAX_KEYS / 2 {
                self.borrow_from_left(parent_page_id, child_idx);
                return;
            }
        }

        // Try to borrow from right sibling
        if child_idx < parent.children.len() - 1 {
            let right_sibling = self.get_node(parent.children[child_idx + 1]);
            if right_sibling.keys.len() > MAX_KEYS / 2 {
                self.borrow_from_right(parent_page_id, child_idx);
                return;
            }
        }

        // Merge: prefer merging with left sibling, otherwise with right
        if child_idx > 0 {
            self.merge_children(parent_page_id, child_idx - 1);
        } else {
            self.merge_children(parent_page_id, child_idx);
        }
    }

    /// Borrow one key from the left sibling through the parent.
    fn borrow_from_left(&mut self, parent_page_id: PageId, child_idx: usize) {
        let mut parent = self.get_node(parent_page_id);
        let mut left_sibling = self.get_node(parent.children[child_idx - 1]);
        let mut child = self.get_node(parent.children[child_idx]);

        // Move parent key down to child
        child.keys.insert(0, parent.keys[child_idx - 1]);
        child.values.insert(0, parent.values[child_idx - 1]);

        // Move last key of left sibling up to parent
        parent.keys[child_idx - 1] = left_sibling.keys.pop().unwrap();
        parent.values[child_idx - 1] = left_sibling.values.pop().unwrap();

        // Move last child pointer if internal
        if !left_sibling.is_leaf {
            let last_child = left_sibling.children.pop().unwrap();
            child.children.insert(0, last_child);
        }

        self.save_node(&parent);
        self.save_node(&left_sibling);
        self.save_node(&child);
    }

    /// Borrow one key from the right sibling through the parent.
    fn borrow_from_right(&mut self, parent_page_id: PageId, child_idx: usize) {
        let mut parent = self.get_node(parent_page_id);
        let mut right_sibling = self.get_node(parent.children[child_idx + 1]);
        let mut child = self.get_node(parent.children[child_idx]);

        // Move parent key down to child
        child.keys.push(parent.keys[child_idx]);
        child.values.push(parent.values[child_idx]);

        // Move first key of right sibling up to parent
        parent.keys[child_idx] = right_sibling.keys.remove(0);
        parent.values[child_idx] = right_sibling.values.remove(0);

        // Move first child pointer if internal
        if !right_sibling.is_leaf {
            let first_child = right_sibling.children.remove(0);
            child.children.push(first_child);
        }

        self.save_node(&parent);
        self.save_node(&right_sibling);
        self.save_node(&child);
    }

    /// Merge children[key_idx] and children[key_idx+1], pulling down the parent key.
    fn merge_children(&mut self, parent_page_id: PageId, key_idx: usize) {
        let mut parent = self.get_node(parent_page_id);
        let mut left = self.get_node(parent.children[key_idx]);
        let right = self.get_node(parent.children[key_idx + 1]);

        // Pull down the separator key from parent
        left.keys.push(parent.keys[key_idx]);
        left.values.push(parent.values[key_idx]);

        // Append all keys/values/children from right into left
        left.keys.extend(right.keys);
        left.values.extend(right.values);
        if !right.is_leaf {
            left.children.extend(right.children);
        }

        // Remove the separator and right child pointer from parent
        parent.keys.remove(key_idx);
        parent.values.remove(key_idx);
        parent.children.remove(key_idx + 1);

        self.save_node(&left);
        self.save_node(&parent);
    }

    /// Return all (key, value) pairs in sorted order via in-order traversal.
    pub fn inorder(&self) -> Vec<(i64, RecordId)> {
        let mut result = Vec::new();
        self.inorder_helper(self.root_page_id, &mut result);
        result
    }

    fn inorder_helper(&self, page_id: PageId, result: &mut Vec<(i64, RecordId)>) {
        let node = self.get_node(page_id);
        for i in 0..node.keys.len() {
            if !node.is_leaf {
                self.inorder_helper(node.children[i], result);
            }
            result.push((node.keys[i], node.values[i]));
        }
        if !node.is_leaf && !node.children.is_empty() {
            self.inorder_helper(*node.children.last().unwrap(), result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::RecordId;

    fn make_record_id(page_id: PageId, slot: u16) -> RecordId {
        RecordId {
            page_id,
            slot_id: slot,
        }
    }

    /// Helper to create a DiskBTree backed by a temp file with the given buffer pool size.
    fn setup_tree(pool_size: usize) -> (DiskBTree, tempfile::NamedTempFile) {
        use crate::buffer::BufferPoolManager;
        use crate::disk::DiskManager;
        use tempfile::NamedTempFile;

        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();
        let bpm = BufferPoolManager::new(pool_size, dm);
        let pool = Arc::new(Mutex::new(bpm));

        let root_page_id = {
            let mut p = pool.lock().unwrap();
            let page = p.new_page().unwrap().unwrap();
            let page_id = page.id;
            let node = DiskBTreeNode::new(page_id, true);
            page.data = node.encode();
            p.unpin_page(page_id, true);
            page_id
        };

        let tree = DiskBTree::new(pool, root_page_id);
        (tree, temp_file)
    }

    #[test]
    fn test_encode_decode_empty_node() {
        let node = DiskBTreeNode::new(0, true);
        let encoded = node.encode();
        let decoded = DiskBTreeNode::decode(&encoded);
        assert_eq!(decoded.page_id, 0);
        assert!(decoded.is_leaf);
        assert!(decoded.keys.is_empty());
        assert!(decoded.values.is_empty());
    }

    #[test]
    fn test_encode_decode_with_data() {
        let mut node = DiskBTreeNode::new(5, false);
        node.keys = vec![10, 20, 30];
        node.values = vec![
            make_record_id(100, 0),
            make_record_id(100, 1),
            make_record_id(100, 2),
        ];
        node.children = vec![1, 2, 3, 4];

        let encoded = node.encode();
        let decoded = DiskBTreeNode::decode(&encoded);

        assert_eq!(decoded.page_id, 5);
        assert!(!decoded.is_leaf);
        assert_eq!(decoded.keys, vec![10, 20, 30]);
        assert_eq!(decoded.values.len(), 3);
        assert_eq!(decoded.children, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_encode_decode_leaf_node() {
        let mut node = DiskBTreeNode::new(42, true);
        node.keys = vec![1, 2, 3];
        node.values = vec![
            make_record_id(50, 0),
            make_record_id(50, 1),
            make_record_id(50, 2),
        ];

        let encoded = node.encode();
        let decoded = DiskBTreeNode::decode(&encoded);

        assert_eq!(decoded.page_id, 42);
        assert!(decoded.is_leaf);
        assert!(decoded.children.is_empty());
    }

    #[test]
    fn test_roundtrip_many_keys() {
        let mut node = DiskBTreeNode::new(100, true);
        for i in 0..100 {
            node.keys.push(i as i64);
            node.values.push(make_record_id(200, i as u16));
        }

        let encoded = node.encode();
        let decoded = DiskBTreeNode::decode(&encoded);

        assert_eq!(decoded.keys.len(), 100);
        assert_eq!(decoded.values.len(), 100);
        for i in 0..100 {
            assert_eq!(decoded.keys[i], i as i64);
            assert_eq!(decoded.values[i].page_id, 200);
            assert_eq!(decoded.values[i].slot_id, i as u16);
        }
    }

    #[test]
    fn test_disk_btree_search_insert() {
        let (mut tree, _tmp) = setup_tree(10);

        tree.insert(10, make_record_id(100, 1));
        tree.insert(20, make_record_id(100, 2));
        tree.insert(5, make_record_id(100, 0));

        let result1 = tree.search(10);
        assert!(result1.is_some());
        assert_eq!(result1.unwrap().slot_id, 1);

        let result2 = tree.search(20);
        assert!(result2.is_some());
        assert_eq!(result2.unwrap().slot_id, 2);

        let result3 = tree.search(5);
        assert!(result3.is_some());
        assert_eq!(result3.unwrap().slot_id, 0);

        let result_none = tree.search(99);
        assert!(result_none.is_none());
    }

    #[test]
    fn test_disk_btree_node_splitting() {
        let (mut tree, _tmp) = setup_tree(50);

        for i in 0..300 {
            tree.insert(i as i64, make_record_id(100, i as u16));
        }

        for i in 0..300 {
            let result = tree.search(i as i64);
            assert!(result.is_some(), "Key {} should exist", i);
            assert_eq!(result.unwrap().slot_id, i as u16);
        }
    }

    #[test]
    fn test_disk_btree_delete() {
        let (mut tree, _tmp) = setup_tree(50);

        // Insert 10 keys: 0..10
        for i in 0..10 {
            tree.insert(i as i64, make_record_id(100, i as u16));
        }

        // Delete 3 keys
        let deleted_keys = [2, 5, 8];
        for &k in &deleted_keys {
            assert!(tree.delete(k), "Should have deleted key {}", k);
        }

        // Verify deleted keys are gone
        for &k in &deleted_keys {
            assert!(tree.search(k).is_none(), "Key {} should be deleted", k);
        }

        // Verify remaining 7 keys are still present
        let remaining: Vec<i64> = (0..10).filter(|k| !deleted_keys.contains(k)).collect();
        for k in &remaining {
            assert!(tree.search(*k).is_some(), "Key {} should still exist", k);
        }
        assert_eq!(remaining.len(), 7);

        // Deleting a non-existent key returns false
        assert!(!tree.delete(999));
    }

    #[test]
    fn test_disk_btree_delete_all() {
        let (mut tree, _tmp) = setup_tree(50);

        // Insert 20 keys
        for i in 0..20 {
            tree.insert(i as i64, make_record_id(100, i as u16));
        }

        // Delete all 20
        for i in 0..20 {
            assert!(tree.delete(i as i64), "Should have deleted key {}", i);
        }

        // Verify all are gone
        for i in 0..20 {
            assert!(
                tree.search(i as i64).is_none(),
                "Key {} should not exist after deletion",
                i
            );
        }
    }

    #[test]
    fn test_disk_btree_inorder() {
        let (mut tree, _tmp) = setup_tree(50);

        // Insert keys in a shuffled order
        let keys = [7, 3, 9, 1, 5, 0, 8, 2, 6, 4];
        for &k in &keys {
            tree.insert(k, make_record_id(100, k as u16));
        }

        let result = tree.inorder();
        assert_eq!(result.len(), keys.len());

        // Verify keys are sorted
        let result_keys: Vec<i64> = result.iter().map(|(k, _)| *k).collect();
        let mut sorted_keys = keys.to_vec();
        sorted_keys.sort();
        assert_eq!(result_keys, sorted_keys);

        // Also verify values match
        for (k, v) in &result {
            assert_eq!(v.slot_id, *k as u16);
        }
    }
}
