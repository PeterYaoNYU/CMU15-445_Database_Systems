//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) -> bool {
  // return false;
  bool flag_ = false;
  for (int i = 0; i < (int)BUCKET_ARRAY_SIZE; i++){
    if (cmp(key, array_[i].first) == 0){
      (*result).push_back(array_[i].second);
    }
  }
  return flag_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  // return true;
  int char_num_;
  int bit_num_;
  char mask = 1;
  bool found_flag_ = false;
  int i;
  for (i = 0; i < (int)BUCKET_ARRAY_SIZE; i++){
    char_num_ = i / 8;
    bit_num_ = i % 8;
    mask = (1<<bit_num_);
    if (!(mask & readable_[char_num_])){
      found_flag_ = true;
      break;
    }
  }
  if (! found_flag_){
    return false;
  }
  array_[i] = std::make_pair(key, value);
  occupied_[i / 8] |= mask;
  readable_[i / 8] |= mask;
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  printf("removing\n");
  int char_offset_;
  int bit_offset_;
  char mask = 0;
  bool found_flag_ = false;
  int i =0;
  for (; i < (int)BUCKET_ARRAY_SIZE; i++){
    if (cmp(array_[i].first, key) == 0 && IsReadable(i) && array_[i].second == value){
      char_offset_ = i / 8;
      bit_offset_ = i % 8;
      mask = (1<<bit_offset_);
      found_flag_ = true;
      // if (mask & occupied_[char_offset_]){
      //   found_flag_ = true;
      //   break;
      // }
    }
  }
  if (found_flag_){
    mask = (1 << bit_offset_);
    readable_[char_offset_] ^= mask;
    return true;
  }else {
    return false;
  }
  // did not reset the readable bits;
  // did not reset the array_ content!;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const -> KeyType {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const -> ValueType {
  return array_[bucket_idx].second;
}

// did not implememt the remove on readable_ and array_
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  int char_offset_ = bucket_idx / 8;
  int bit_offset_ = bucket_idx % 8;
  char mask = 1<<bit_offset_;
  if (mask & occupied_[char_offset_]){
    occupied_[char_offset_] ^= mask;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const -> bool {
  printf("checking avability of bucket_id: %d\n", bucket_idx);
  int char_offset_ = bucket_idx / 8;
  int bit_offset_ = bucket_idx % 8;
  char mask = 1<<bit_offset_;
  if (mask & occupied_[char_offset_]){
    return true;
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  int char_offset_ = bucket_idx / 8;
  int bit_offset_ = bucket_idx % 8;
  char mask = 1<<bit_offset_;
  occupied_[char_offset_] |= mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const -> bool {
  printf("checking readibility of bucket_id: %d\n", bucket_idx);
  int char_offset_ = bucket_idx / 8;
  int bit_offset_ = bucket_idx % 8;
  char mask = 1<<bit_offset_;
  if (mask & readable_[char_offset_]){
    return true;
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  int char_offset_ = bucket_idx / 8;
  int bit_offset_ = bucket_idx % 8;
  char mask = 1<<bit_offset_;
  readable_[char_offset_] |= mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsFull() -> bool {
  bool full_flag_ = true;
  for (int i=0; i<(int)BUCKET_ARRAY_SIZE; i++){
    if (! IsOccupied(i)){
      full_flag_ = false;
      break;
    }
  }
  return full_flag_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::NumReadable() -> uint32_t {
  int count_  = 0;
  for (int i= 0; i<(int)BUCKET_ARRAY_SIZE; i++){
    if (IsReadable(i)){
      count_++;
    }
  }
  return count_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsEmpty() -> bool {
  return !IsFull();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
