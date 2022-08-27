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

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

// use the page_id to find the corresponding frame_id
int BufferPoolManagerInstance::find_frame_id(page_id_t page_id){
  auto it = page_table_.find(page_id);
  if (it == page_table_.cend()){
    return -1;
  }

  return it->second;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lck(latch_);

  if (page_id == INVALID_PAGE_ID)
    return false;
  
  int frame_id = find_frame_id(page_id);
  if (frame_id == -1)
    return false;

  flush_pg(page_id, frame_id);

  // if (pages_[frame_id].is_dirty_ == true){
  pages_[frame_id].is_dirty_ = false;
  // }

  return true;
}

void BufferPoolManagerInstance::flush_pg(page_id_t page_id, int frame_id){
  if (pages_[frame_id].IsDirty()){
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  }
  return;
}



void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  // page_id_t page_id;
  for (const auto &page : page_table_){
    FlushPgImp(page.first);
  }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> lck (latch_);
  *page_id = AllocatePage();
  int frame_id;
  
  if (!free_list_.empty()){
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!free_list_.front()) {

    if (replacer_->Victim(&frame_id)){     
      page_id_t old_page_id = pages_[frame_id].page_id_;
      FlushPgImp(old_page_id);

      // erase it from the page table to get space for the new one
      page_table_.erase(old_page_id);
    } else{
      return nullptr;
    }
  }

  // add P to page table
  page_table_.insert({*page_id, frame_id});
  
  // update p's metadata
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ ++;

  // zero out the memory
  memset(pages_[frame_id].GetData(), 0, PAGE_SIZE);

  return &pages_[frame_id];
  // return nullptr;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  std::lock_guard<std::mutex> lck (latch_);

  // page_id_t page_id;
  
  frame_id_t frame_id = find_frame_id(page_id);
  if  (frame_id>=0){
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  } else {
    NewPgImp(&page_id);
    return &pages_[find_frame_id(page_id)];
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lck (latch_);

  DeallocatePage(page_id);
  frame_id_t frame_id = find_frame_id(page_id);
  if (frame_id < 0){
    return true;
  } else if (frame_id >= 0){
    if (pages_[frame_id].pin_count_ !=0){
      return false;
    }

    // flush the content
    FlushPgImp(page_id);
    // remove it fron the page table
    page_table_.erase(page_id);
    // reset metadata
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 0;
    memset(pages_[frame_id].GetData(), 0, PAGE_SIZE);
    // return it to the free list;
    free_list_.push_back(frame_id);
  }
  return false;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lck (latch_);
  auto frame_id = find_frame_id(page_id);
  if (frame_id < 0 || pages_[frame_id].pin_count_ <= 0){
    return false;
  }
  pages_[frame_id].is_dirty_ = is_dirty;
  if (--pages_[frame_id].pin_count_ == 0){
    replacer_->Unpin(frame_id);
    FlushPgImp(page_id);
  }
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
