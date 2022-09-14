// //===----------------------------------------------------------------------===//
// //
// //                         BusTub
// //
// // lru_replacer.cpp
// //
// // Identification: src/buffer/lru_replacer.cpp
// //
// // Copyright (c) 2015-2019, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #include "buffer/lru_replacer.h"

// namespace bustub {

// LRUReplacer::LRUReplacer(size_t num_pages) {}

// LRUReplacer::~LRUReplacer() = default;

// auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
//   if (lru_replacer.empty()) {
//     return false;
//   }

//   replacer_mutex.lock();

//   frame_id_t frame_id_min = lru_replacer.begin()->first;
//   int timestamp_min = lru_replacer.begin()->second;

//   for (auto it = lru_replacer.begin(); it != lru_replacer.end(); ++it) {
//     if (it->second < timestamp_min) {
//       timestamp_min = it->second;
//       frame_id_min = it->first;
//     }
//   }
//   *frame_id = frame_id_min;
//   lru_replacer.erase(frame_id_min);

//   replacer_mutex.unlock();
//   return true;
// }

// void LRUReplacer::Pin(frame_id_t frame_id) {
//   replacer_mutex.lock();
//   lru_replacer.erase(frame_id);
//   replacer_mutex.unlock();
// }

// void LRUReplacer::Unpin(frame_id_t frame_id) {
//   replacer_mutex.lock();
//   lru_replacer.insert({frame_id, timestamp++});
//   replacer_mutex.unlock();
// }

// auto LRUReplacer::Size() -> size_t { return lru_replacer.size(); }

// }  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : page2iter_{num_pages} {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (wait_list_.empty()) {
    *frame_id = INVALID_PAGE_ID;
    return false;
  }
  *frame_id = wait_list_.back();
  wait_list_.pop_back();
  page2iter_[*frame_id] = std::list<frame_id_t>::iterator{};
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (!IsInReplacer(frame_id)) {
    return;
  }
  wait_list_.erase(page2iter_[frame_id]);
  page2iter_[frame_id] = std::list<frame_id_t>::iterator{};
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (IsInReplacer(frame_id)) {
    return;
  }
  wait_list_.push_front(frame_id);
  page2iter_[frame_id] = wait_list_.begin();
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock_guard(latch_);
  return wait_list_.size();
}

bool LRUReplacer::IsInReplacer(frame_id_t frame_id) {
  return page2iter_[frame_id] != std::list<frame_id_t>::iterator{};
}

}  // namespace bustub
