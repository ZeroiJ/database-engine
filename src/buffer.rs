use crate::disk::{DiskManager, Page, PageId};
use std::collections::{HashMap, VecDeque};
use std::io::Result;

pub struct BufferPoolManager {
    disk_manager: DiskManager,
    pool: Vec<Page>,
    page_table: HashMap<PageId, usize>,
    free_list: Vec<usize>,
    replacer: VecDeque<usize>,
}

impl BufferPoolManager {
    pub fn new(pool_size: usize, disk_manager: DiskManager) -> Self {
        let mut pool = Vec::with_capacity(pool_size);
        let mut free_list = Vec::with_capacity(pool_size);

        for i in 0..pool_size {
            pool.push(Page::new(0));
            free_list.push(i);
        }

        let mut replacer = VecDeque::new();
        for i in 0..pool_size {
            replacer.push_back(i);
        }

        BufferPoolManager {
            disk_manager,
            pool,
            page_table: HashMap::new(),
            free_list,
            replacer,
        }
    }

    pub fn fetch_page(&mut self, page_id: PageId) -> Result<Option<&mut Page>> {
        if let Some(&idx) = self.page_table.get(&page_id) {
            let page = &mut self.pool[idx];
            page.pin_count += 1;
            if page.pin_count == 1 {
                self.replacer.retain(|&x| x != idx);
            }
            return Ok(Some(&mut self.pool[idx]));
        }

        let idx = if let Some(free_idx) = self.free_list.pop() {
            free_idx
        } else {
            if let Some(evict_idx) = self.replacer.pop_front() {
                let evict_page_id = self.pool[evict_idx].id;
                if self.pool[evict_idx].is_dirty {
                    self.disk_manager
                        .write_page(evict_page_id, &self.pool[evict_idx].data)?;
                }
                self.page_table.remove(&evict_page_id);
                evict_idx
            } else {
                return Ok(None);
            }
        };

        let mut page = Page::new(page_id);
        self.disk_manager.read_page(page_id, &mut page.data)?;
        page.pin_count = 1;

        self.pool[idx] = page;
        self.page_table.insert(page_id, idx);

        Ok(Some(&mut self.pool[idx]))
    }

    pub fn new_page(&mut self) -> Result<Option<&mut Page>> {
        let page_id = self.disk_manager.allocate_page();

        let idx = if let Some(free_idx) = self.free_list.pop() {
            free_idx
        } else {
            if let Some(evict_idx) = self.replacer.pop_front() {
                let evict_page_id = self.pool[evict_idx].id;
                if self.pool[evict_idx].is_dirty {
                    self.disk_manager
                        .write_page(evict_page_id, &self.pool[evict_idx].data)?;
                }
                self.page_table.remove(&evict_page_id);
                evict_idx
            } else {
                return Ok(None);
            }
        };

        let mut page = Page::new(page_id);
        page.pin_count = 1;
        page.is_dirty = true;

        self.pool[idx] = page;
        self.page_table.insert(page_id, idx);

        Ok(Some(&mut self.pool[idx]))
    }

    pub fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) {
        if let Some(&idx) = self.page_table.get(&page_id) {
            let page = &mut self.pool[idx];
            if page.pin_count > 0 {
                page.pin_count -= 1;
            }
            if is_dirty {
                page.is_dirty = true;
            }
            if page.pin_count == 0 {
                self.replacer.push_back(idx);
            }
        }
    }

    pub fn flush_page(&mut self, page_id: PageId) -> Result<()> {
        if let Some(&idx) = self.page_table.get(&page_id) {
            let page = &self.pool[idx];
            if page.is_dirty {
                self.disk_manager.write_page(page_id, &page.data)?;
            }
        }
        Ok(())
    }

    pub fn flush_all_pages(&mut self) -> Result<()> {
        let page_ids: Vec<PageId> = self.page_table.keys().cloned().collect();
        for page_id in page_ids {
            self.flush_page(page_id)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::PAGE_SIZE;
    use tempfile::NamedTempFile;

    #[test]
    fn test_buffer_pool_basic() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();
        let mut bpm = BufferPoolManager::new(10, dm);

        let page = bpm.new_page().unwrap();
        assert!(page.is_some());
        let page = page.unwrap();
        assert_eq!(page.pin_count, 1);
        assert_eq!(page.id, 0);
    }

    #[test]
    fn test_buffer_pool_modify_and_fetch() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();
        let mut bpm = BufferPoolManager::new(10, dm);

        let page = bpm.new_page().unwrap();
        let page = page.unwrap();
        page.data[0] = 0xAB;
        page.data[1] = 0xCD;
        let page_id = page.id;

        bpm.unpin_page(page_id, true);

        let fetched = bpm.fetch_page(page_id).unwrap();
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched.data[0], 0xAB);
        assert_eq!(fetched.data[1], 0xCD);
    }

    #[test]
    fn test_buffer_pool_unpin_reuse() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();
        let mut bpm = BufferPoolManager::new(2, dm);

        let page1 = bpm.new_page().unwrap().unwrap();
        let page1_id = page1.id;
        bpm.unpin_page(page1_id, false);

        let page2 = bpm.new_page().unwrap().unwrap();
        let page2_id = page2.id;
        bpm.unpin_page(page2_id, false);

        let _page1_refetched = bpm.fetch_page(page1_id).unwrap().unwrap();
        let _page2_refetched = bpm.fetch_page(page2_id).unwrap().unwrap();
    }

    #[test]
    fn test_flush_pages() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();
        let mut bpm = BufferPoolManager::new(5, dm);

        let page = bpm.new_page().unwrap().unwrap();
        page.data[0] = 0x11;
        let page_id = page.id;
        bpm.unpin_page(page_id, true);

        bpm.flush_all_pages().unwrap();

        let mut dm2 = DiskManager::new(temp_file.path()).unwrap();
        let mut read_data = [0u8; PAGE_SIZE];
        dm2.read_page(page_id, &mut read_data).unwrap();
        assert_eq!(read_data[0], 0x11);
    }
}
