use crate::disk::{PageId, PAGE_SIZE};
use crate::storage::Row;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DiskBTreeNode {
    pub page_id: PageId,
    pub is_leaf: bool,
    pub keys: Vec<i64>,
    pub values: Vec<Row>,
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
use std::cell::RefCell;
use std::rc::Rc;

pub struct DiskBTree {
    pub buffer_pool: Rc<RefCell<BufferPoolManager>>,
    pub root_page_id: PageId,
}

impl DiskBTree {
    pub fn new(buffer_pool: Rc<RefCell<BufferPoolManager>>, root_page_id: PageId) -> Self {
        Self {
            buffer_pool,
            root_page_id,
        }
    }

    pub fn get_node(&self, page_id: PageId) -> DiskBTreeNode {
        let mut pool = self.buffer_pool.borrow_mut();
        let page = pool.fetch_page(page_id).unwrap().unwrap();
        let node = DiskBTreeNode::decode(&page.data);
        pool.unpin_page(page_id, false);
        node
    }

    pub fn save_node(&self, node: &DiskBTreeNode) {
        let mut pool = self.buffer_pool.borrow_mut();
        let page = pool.fetch_page(node.page_id).unwrap().unwrap();
        page.data = node.encode();
        pool.unpin_page(node.page_id, true);
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
        node.values = vec![make_row(10), make_row(20), make_row(30)];
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
        node.values = vec![make_row(1), make_row(2), make_row(3)];

        let encoded = node.encode();
        let decoded = DiskBTreeNode::decode(&encoded);

        assert_eq!(decoded.page_id, 42);
        assert!(decoded.is_leaf);
        assert!(decoded.children.is_empty());
    }

    #[test]
    fn test_roundtrip_large_node() {
        let mut node = DiskBTreeNode::new(100, true);
        for i in 0..50 {
            node.keys.push(i as i64);
            node.values.push(make_row(i as i64));
        }

        let encoded = node.encode();
        let decoded = DiskBTreeNode::decode(&encoded);

        assert_eq!(decoded.keys.len(), 50);
        assert_eq!(decoded.values.len(), 50);
        for i in 0..50 {
            assert_eq!(decoded.keys[i], i as i64);
        }
    }
}
