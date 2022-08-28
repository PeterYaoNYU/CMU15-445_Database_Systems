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

  printf("basic info: pool size: %d\n num_instances: %d\n, instance_index: %d\n", (int)pool_size, (int)num_instances, (int)instance_index);
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

// use the page_id to find the corresponding frame_id
int BufferPoolManagerInstance::find_frame_id(page_id_t page_id) {
  // std::lock_guard<std::mutex> lock_guard(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return -1;
  }

  return it->second;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lck(latch_);
  printf("flushing %d\n", (int)page_id);

  if (page_id == INVALID_PAGE_ID) return false;

  int frame_id = find_frame_id(page_id);
  if (frame_id == -1) return false;

  flush_pg(page_id, frame_id);

  // if (pages_[frame_id].is_dirty_ == true){
  pages_[frame_id].is_dirty_ = false;
  // }

  return true;
}

void BufferPoolManagerInstance::flush_pg(page_id_t page_id, int frame_id) {
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  }
  return;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  // page_id_t page_id;
  printf("flushing all\n");

  for (const auto &page : page_table_) {
    FlushPage(page.first);
  }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> lck(latch_);

  printf("new pg imp\n");

  bool all_pinned = true;
  for (size_t i =0; i<pool_size_; ++i){
    if (pages_[i].GetPinCount() <= 0){
      all_pinned = false;
      break;
    }
  }

  if (all_pinned){
    return nullptr;
  }

  frame_id_t frame_id_;

  if (!free_list_.empty()){
    frame_id_ = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Victim(&frame_id_)){
    auto old_page_id_ = pages_[frame_id_].GetPageId();
    if (pages_[frame_id_].IsDirty()){
      disk_manager_->WritePage(old_page_id_, pages_[frame_id_].GetData());
      pages_[frame_id_].is_dirty_ = false;
    }
    page_table_.erase(old_page_id_);
  } else {
    return nullptr;
  }

  replacer_->Pin(frame_id_);

  auto new_page_id_ = AllocatePage();

  *page_id = new_page_id_;

  memset(pages_[frame_id_].GetData(), 0, PAGE_SIZE);
  pages_[frame_id_].page_id_ = new_page_id_;
  pages_[frame_id_].pin_count_ =1;
  pages_[frame_id_].is_dirty_ = false;

  page_table_.emplace(new_page_id_, frame_id_);

  printf("done new paging and the new page id is %d, frame id is %d\n", new_page_id_, frame_id_);

  return &pages_[frame_id_];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  std::lock_guard<std::mutex> lck(latch_);

  printf("fetching page %d\n", page_id);
  // page_id_t page_id;

  frame_id_t frame_id = find_frame_id(page_id);
  if (frame_id >= 0) {
    replacer_->Pin(frame_id);
    pages_[frame_id].pin_count_++;
    printf("fetching: page %d found in the page table\n", page_id);
    // pages_[frame_id].is_dirty_=true;
    return &pages_[frame_id];
  } else {
    if (!free_list_.empty()){
      frame_id = free_list_.front();
      free_list_.pop_front();    
      printf("fetching page %d: get from freelist \n", page_id);
    } else if (replacer_->Victim(&frame_id)) {
      printf("fetching page %d: got from lru waitlist, frame id: %d\n", page_id, frame_id);
      auto old_page_id = pages_[frame_id].GetPageId();
      if (pages_[frame_id].IsDirty()){
        disk_manager_->WritePage(old_page_id, pages_[frame_id].GetData());
        pages_[frame_id].is_dirty_ = false;
      }
      page_table_.erase(old_page_id);
      // pages_[frame_id].is_dirty_ = false;
    } else{
      return nullptr;
    }

    replacer_->Pin(frame_id);
    page_table_.emplace(page_id, frame_id);
    pages_[frame_id].page_id_=page_id;
    pages_[frame_id].pin_count_++;
    pages_[frame_id].is_dirty_ = false;
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

    return &pages_[frame_id];
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lck(latch_);

  printf("deleting\n");


  frame_id_t frame_id = find_frame_id(page_id);
  if (frame_id < 0) {
    return true;
  } else if (frame_id >= 0) {
    if (pages_[frame_id].GetPinCount() > 0) {
      return false;
    }

    // flush the content
    if (pages_[frame_id].IsDirty()){
      disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    }
    // remove it fron the page table
    page_table_.erase(page_id);
    // reset metadata
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 0;
    DeallocatePage(page_id);

    memset(pages_[frame_id].GetData(), 0, PAGE_SIZE);
    // return it to the free list;
    free_list_.push_back(frame_id);
    return true;
  }
  return false;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lck(latch_);

  printf("unpinning %d\n", page_id);

  printf("finding\n");
  auto frame_id = find_frame_id(page_id);
  printf("return from find\n");
  printf("the frame id found: %d\n", frame_id);

  if (frame_id < 0 || pages_[frame_id].pin_count_ <= 0) {
    printf("test failed\n");
    return false;
  }

  // printf("check done\n");
  if (pages_[frame_id].pin_count_<=0){
    return false;
  }

  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->Unpin(frame_id);
    if (pages_[frame_id].is_dirty_){
      disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    }


  }

  pages_[frame_id].is_dirty_ = is_dirty;
  printf("done setting the is dirty bit to %d\n", is_dirty);
  printf("done unpinning\n");
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
