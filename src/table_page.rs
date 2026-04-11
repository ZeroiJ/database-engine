use crate::disk::{PageId, PAGE_SIZE};
use crate::storage::Row;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TablePage {
    pub page_id: PageId,
    pub rows: BTreeMap<u16, Row>,
    pub next_slot_id: u16,
    pub next_page_id: Option<PageId>,
}

impl TablePage {
    pub fn new(page_id: PageId) -> Self {
        Self {
            page_id,
            rows: BTreeMap::new(),
            next_slot_id: 0,
            next_page_id: None,
        }
    }

    pub fn encode(&self) -> [u8; PAGE_SIZE] {
        let mut buffer = [0u8; PAGE_SIZE];
        let serialized = serde_json::to_vec(self).expect("Failed to serialize TablePage");
        assert!(
            serialized.len() <= PAGE_SIZE,
            "TablePage exceeds 4KB limit!"
        );
        buffer[..serialized.len()].copy_from_slice(&serialized);
        buffer
    }

    pub fn decode(buffer: &[u8; PAGE_SIZE]) -> Self {
        let len = buffer.iter().position(|&b| b == 0).unwrap_or(PAGE_SIZE);
        serde_json::from_slice(&buffer[..len]).expect("Failed to deserialize TablePage")
    }

    pub fn insert_row(&mut self, row: Row) -> Result<u16, &'static str> {
        let mut temp = self.clone();
        temp.rows.insert(temp.next_slot_id, row.clone());
        temp.next_slot_id += 1;

        let serialized = serde_json::to_vec(&temp).map_err(|_| "Serialization error")?;

        if serialized.len() > PAGE_SIZE {
            return Err("Page Full");
        }

        let slot = self.next_slot_id;
        self.rows.insert(slot, row);
        self.next_slot_id += 1;
        Ok(slot)
    }

    pub fn get_row(&self, slot_id: u16) -> Option<&Row> {
        self.rows.get(&slot_id)
    }

    pub fn delete_row(&mut self, slot_id: u16) -> bool {
        self.rows.remove(&slot_id).is_some()
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
    fn test_insert_single_row() {
        let mut page = TablePage::new(0);
        let result = page.insert_row(make_row(1));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_get_row() {
        let mut page = TablePage::new(0);
        page.insert_row(make_row(1)).unwrap();
        let row = page.get_row(0);
        assert!(row.is_some());
        assert_eq!(row.unwrap()[0], Value::Integer(1));
    }

    #[test]
    fn test_delete_row() {
        let mut page = TablePage::new(0);
        page.insert_row(make_row(1)).unwrap();
        let deleted = page.delete_row(0);
        assert!(deleted);
        assert!(page.get_row(0).is_none());
    }

    #[test]
    fn test_page_full_rejection() {
        let mut page = TablePage::new(0);

        page.insert_row(make_row(1)).unwrap();

        let large_row: Row = vec![Value::Text("x".repeat(5000))];
        let result = page.insert_row(large_row);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Page Full");
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let mut page = TablePage::new(5);
        page.insert_row(make_row(10)).unwrap();
        page.insert_row(make_row(20)).unwrap();

        let encoded = page.encode();
        let decoded = TablePage::decode(&encoded);

        assert_eq!(decoded.page_id, 5);
        assert_eq!(decoded.rows.len(), 2);
        assert_eq!(decoded.next_slot_id, 2);
    }
}
