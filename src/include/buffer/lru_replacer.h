//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  auto Victim(frame_id_t *frame_id) -> bool override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

 private:
  bool IsInReplacer(frame_id_t frame_id);

  /** A linked list of pages waiting to be evicted. */
  std::list<frame_id_t> wait_list_{};

  /** The iterator in this vector points to wait_list_. */
  std::vector<std::list<frame_id_t>::iterator> page2iter_;

  std::mutex latch_{};
  // TODO(student): implement me!
  // int timestamp;
  // std::unordered_map<frame_id_t, int> lru_replacer;
  // std::mutex replacer_mutex;
};

}  // namespace bustub
