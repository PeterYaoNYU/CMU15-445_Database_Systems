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

LRUReplacer::LRUReplacer(size_t num_pages) {

    
}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
    if (lru_replacer.empty()){
        return false;
    }

    replacer_mutex.lock();
    
    int frame_id_min = lru_replacer.begin()->first;
    frame_id_t timestamp_min = lru_replacer.begin()->second;

    for (auto it = lru_replacer.begin(); it!=lru_replacer.end(); ++it){
        if (it->second < timestamp_min){
            timestamp_min = it->second;
            frame_id_min = it->first;
        }
    }
    * frame_id = frame_id_min;
    lru_replacer.erase(frame_id_min);

    replacer_mutex.unlock();
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    replacer_mutex.lock();
    lru_replacer.erase(frame_id);
    replacer_mutex.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    replacer_mutex.lock();
    lru_replacer.insert({frame_id, timestamp++});
    replacer_mutex.unlock();
}

auto LRUReplacer::Size() -> size_t { return lru_replacer.size(); }

}  // namespace bustub
