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
        let serialized = serde_json::to_vec(self).expect("Failed to serialize node");
        assert!(
            serialized.len() <= PAGE_SIZE,
            "Node exceeds 4KB page size! Data too large."
        );
        buffer[..serialized.len()].copy_from_slice(&serialized);
        buffer
    }

    pub fn decode(buffer: &[u8; PAGE_SIZE]) -> Self {
        let len = buffer.iter().position(|&b| b == 0).unwrap_or(PAGE_SIZE);
        serde_json::from_slice(&buffer[..len]).expect("Failed to deserialize node")
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
        use crate::buffer::BufferPoolManager;
        use crate::disk::DiskManager;
        use tempfile::NamedTempFile;

        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();
        let bpm = BufferPoolManager::new(10, dm);
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

        let mut tree = DiskBTree::new(pool.clone(), root_page_id);

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
        use crate::buffer::BufferPoolManager;
        use crate::disk::DiskManager;
        use tempfile::NamedTempFile;

        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();
        let bpm = BufferPoolManager::new(50, dm);
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

        let mut tree = DiskBTree::new(pool.clone(), root_page_id);

        for i in 0..300 {
            tree.insert(i as i64, make_record_id(100, i as u16));
        }

        for i in 0..300 {
            let result = tree.search(i as i64);
            assert!(result.is_some(), "Key {} should exist", i);
            assert_eq!(result.unwrap().slot_id, i as u16);
        }
    }
}
