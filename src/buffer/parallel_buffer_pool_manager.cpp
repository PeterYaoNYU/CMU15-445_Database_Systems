//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : num_instances_(num_instances) {
  // Allocate and create individual BufferPoolManagerInstances
  BufferPoolManagerInstance *buffer_pool_manager_;

  for (size_t i = 0; i < num_instances; i++) {
    buffer_pool_manager_ = new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager);
    managers_.push_back(buffer_pool_manager_);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  printf("destructing...\n");
  for (size_t i = 0; i < num_instances_; i++) {
    delete managers_[i];
  }
  printf("done destructing each manager instance\n");
  // delete &managers_;
  // printf("done destructing all \n");
}

auto ParallelBufferPoolManager::GetPoolSize() -> size_t {
  // Get size of all BufferPoolManagerInstances
  return managers_[0]->GetPoolSize();
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManager * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return managers_[page_id % num_instances_];
  // return nullptr;
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *manager_;
  manager_ = GetBufferPoolManager(page_id);
  return manager_->FetchPage(page_id);
  return nullptr;
}

auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *manager_;
  manager_ = GetBufferPoolManager(page_id);
  return manager_->UnpinPage(page_id, is_dirty);
  return false;
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *manager_;
  manager_ = GetBufferPoolManager(page_id);
  return manager_->FlushPage(page_id);
  return false;
}

auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  printf("calling new page imp\n");
  // return nullptr;
  size_t trials = 0;
  Page *page_ptr;
  while (trials++ < num_instances_) {
    // starting_index = (starting_index + 1) % num_instances_;
    if ((page_ptr = managers_[starting_index]->NewPage(page_id)) != nullptr) {
      return page_ptr;
    } else {
      starting_index = (starting_index + 1) % num_instances_;
    }
  }
  return nullptr;
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  // Delete page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->DeletePage(page_id);
  // return false;
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (auto &i : managers_) {
    i->FlushAllPages();
  }
}

}  // namespace bustub
