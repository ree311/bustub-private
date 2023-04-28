//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}
/**
 * TODO(P1): Add implementation
 *
 * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
 * are currently in use and not evictable (in another word, pinned).
 *
 * You should pick the replacement frame from either the free list or the replacer (always find from the free list
 * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
 * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
 *
 * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
 * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
 * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
 *
 * @param[out] page_id id of created page
 * @return nullptr if no new pages could be created, otherwise pointer to new page
 */
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  LOG_INFO("# [NewPgImp] Now creating new page %d", *page_id);
  frame_id_t free_frame;
  if (!free_list_.empty()) {
    LOG_DEBUG("# [NewPgImp] Get frame from free list");
    free_frame = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&free_frame)) {
    LOG_DEBUG("# [NewPgImp] Free list is full , now evicted %d", free_frame);
    if (pages_[free_frame].IsDirty()) {
      disk_manager_->WritePage(pages_[free_frame].GetPageId(), pages_[free_frame].GetData());
    }

  } else {
    LOG_INFO("# [NewPgImp] Free list is full and can't evict, create new page failed");
    return nullptr;
  }
  latch_.lock();
  *page_id = AllocatePage();
  latch_.unlock();
  pages_[free_frame].ResetMemory();
  pages_[free_frame].page_id_ = *page_id;
  pages_[free_frame].pin_count_++;
  replacer_->RecordAccess(free_frame);
  replacer_->SetEvictable(free_frame, false);

  return &pages_[free_frame];
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
 * but all frames are currently in use and not evictable (in another word, pinned).
 *
 * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
 * the replacer (always find from the free list first), read the page from disk by calling disk_manager_->ReadPage(),
 * and replace the old page in the frame. Similar to NewPgImp(), if the old page is dirty, you need to write it back
 * to disk and update the metadata of the new page
 *
 * In addition, remember to disable eviction and record the access history of the frame like you did for NewPgImp().
 *
 * @param page_id id of page to be fetched
 * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
 */
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPageId() == page_id) {
      frame_id_t frame_id;
      page_table_->Find(page_id, frame_id);
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);
      pages_[i].pin_count_++;
      return &pages_[frame_id];
    }
  }
  frame_id_t old_frame;
  if (!free_list_.empty()) {
    old_frame = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&old_frame)) {
    if (pages_[old_frame].IsDirty()) {
      latch_.lock();
      disk_manager_->WritePage(pages_[old_frame].GetPageId(), pages_[old_frame].GetData());
      latch_.unlock();
    }
  } else {
    return nullptr;
  }

  latch_.lock();
  disk_manager_->ReadPage(page_id, pages_[old_frame].data_);
  latch_.unlock();
  pages_[old_frame].page_id_ = page_id;
  pages_[old_frame].pin_count_ = 1;
  replacer_->RecordAccess(old_frame);
  replacer_->SetEvictable(old_frame, false);

  return &pages_[old_frame];
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
 * 0, return false.
 *
 * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
 * Also, set the dirty flag on the page to indicate if the page was modified.
 *
 * @param page_id id of page to be unpinned
 * @param is_dirty true if the page should be marked as dirty, false otherwise
 * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
 */
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPageId() == page_id) {
      pages_[i].is_dirty_ = is_dirty;

      if (pages_[i].GetPinCount() <= 0) {
        // LOG_DEBUG("# [UnpinPgImp] page_id%d, pinCount<=0", page_id);
        return false;
      }
      if (pages_[i].GetPinCount() == 1) {
        // LOG_DEBUG("# [UnpinPgImp] page_id%d, pinCount==1", page_id);
        pages_[i].pin_count_--;
        replacer_->SetEvictable(i, true);
      } else {
        // LOG_DEBUG("# [UnpinPgImp] page_id%d, pinCount>1", page_id);
        pages_[i].pin_count_--;
      }
      return true;
    }
  }
  return false;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Flush the target page to disk.
 *
 * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
 * Unset the dirty flag of the page after flushing.
 *
 * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
 * @return false if the page could not be found in the page table, true otherwise
 */
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    throw "Page Can't be INVALID_PAGE_ID";
  }

  for (size_t i = 0; i < pool_size_; i++) {
    LOG_INFO("# [FlushPgImp] Now check page_id %d ", pages_[i].GetPageId());
    if (pages_[i].GetPageId() == page_id) {
      pages_[i].is_dirty_ = false;
      latch_.lock();
      disk_manager_->WritePage(page_id, pages_[i].GetData());
      latch_.unlock();
      LOG_INFO("# [FlushPgImp] Page %d flushed", page_id);
      return true;
    }
  }
  LOG_INFO("# [FlushPgImp] Can't find page %d, flush failed", page_id);
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPageId() != INVALID_PAGE_ID) {
      pages_[i].is_dirty_ = false;
      latch_.lock();
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      latch_.unlock();
    }
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
 * page is pinned and cannot be deleted, return false immediately.
 *
 * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
 * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
 * imitate freeing the page on the disk.
 *
 * @param page_id id of page to be deleted
 * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
 */
auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    throw "Page Can't be INVALID_PAGE_ID";
  }
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPageId() == page_id) {
      if (pages_[i].GetPinCount() > 0) {
        return false;
      }
      pages_[i].ResetMemory();
      pages_[i].page_id_ = INVALID_PAGE_ID;
      pages_[i].is_dirty_ = false;
      page_table_->Remove(page_id);
      replacer_->Remove(i);
      free_list_.emplace_back(i);
      latch_.lock();
      DeallocatePage(page_id);
      latch_.unlock();
      return true;
    }
  }
  return false;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
