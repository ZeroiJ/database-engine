use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Result, Seek, SeekFrom, Write};
use std::path::Path;

pub const PAGE_SIZE: usize = 4096;
pub type PageId = u32;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordId {
    pub page_id: PageId,
    pub slot_id: u16,
}

#[derive(Clone)]
pub struct Page {
    pub id: PageId,
    pub data: [u8; PAGE_SIZE],
    pub is_dirty: bool,
    pub pin_count: u32,
}

impl Page {
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            data: [0; PAGE_SIZE],
            is_dirty: false,
            pin_count: 0,
        }
    }
}

pub struct DiskManager {
    file: File,
    next_page_id: PageId,
}

impl DiskManager {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let next_page_id = (file_size / PAGE_SIZE as u64) as PageId;

        Ok(DiskManager { file, next_page_id })
    }

    pub fn read_page(&mut self, page_id: PageId, page_data: &mut [u8; PAGE_SIZE]) -> Result<()> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(page_data)?;
        Ok(())
    }

    pub fn write_page(&mut self, page_id: PageId, page_data: &[u8; PAGE_SIZE]) -> Result<()> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(page_data)?;
        self.file.flush()?;
        Ok(())
    }

    pub fn allocate_page(&mut self) -> PageId {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        page_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_disk_manager_create() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();
        assert_eq!(dm.next_page_id, 0);
    }

    #[test]
    fn test_read_write_page() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::new(temp_file.path()).unwrap();

        let page_id = dm.allocate_page();
        assert_eq!(page_id, 0);

        let mut write_data = [0u8; PAGE_SIZE];
        write_data[0] = 0xDE;
        write_data[1] = 0xAD;
        write_data[2] = 0xBE;
        write_data[3] = 0xEF;

        dm.write_page(page_id, &write_data).unwrap();

        let mut read_data = [0u8; PAGE_SIZE];
        dm.read_page(page_id, &mut read_data).unwrap();

        assert_eq!(write_data, read_data);
    }

    #[test]
    fn test_allocate_multiple_pages() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::new(temp_file.path()).unwrap();

        let id1 = dm.allocate_page();
        let id2 = dm.allocate_page();
        let id3 = dm.allocate_page();

        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id3, 2);
        assert_eq!(dm.next_page_id, 3);
    }
}
