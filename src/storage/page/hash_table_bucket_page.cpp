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
  bool FoundFlag = false;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i) && (cmp(key, KeyAt(i)) == 0)) {
      result->push_back(ValueAt(i));
      FoundFlag = true;
    }
  }
  return FoundFlag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsDuplicate(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i) && (cmp(key, KeyAt(i)) == 0) && (value == ValueAt(i))) {
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  if (IsFull()) {
    return false;
  }
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i) && (cmp(key, KeyAt(i)) == 0) && (ValueAt(i) == value)) {
      return false;
    }
  }

  uint32_t idx;
  for (idx = 0; idx < BUCKET_ARRAY_SIZE; idx++) {
    if (!IsReadable(idx)) {
      break;
    }
  }
  SetOccupied(idx);
  SetReadable(idx);
  array_[idx].first = key;
  array_[idx].second = value;
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i) && (cmp(array_[i].first, key) == 0) && (ValueAt(i) == value)) {
      RemoveAt(i);
      SetUnReadable(i);
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetUnReadable(uint32_t bucket_idx) {
  if (!IsReadable(bucket_idx)) {
    return;
  }
  int ArrayOffset = bucket_idx / 8;
  int CharOffset = bucket_idx % 8;
  char Mask = 1 << CharOffset;
  readable_[ArrayOffset] ^= Mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const -> KeyType {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const -> ValueType {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  SetUnReadable(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const -> bool {
  int ArrayOffset = bucket_idx / 8;
  int CharOffset = bucket_idx % 8;
  char Mask = 1 << CharOffset;
  bool OccupiedFlag = Mask & occupied_[ArrayOffset];
  return OccupiedFlag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  int ArrayOffset = bucket_idx / 8;
  int CharOffset = bucket_idx % 8;
  char Mask = 1 << CharOffset;
  occupied_[ArrayOffset] |= Mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const -> bool {
  int ArrayOffset = bucket_idx / 8;
  int CharOffset = bucket_idx % 8;
  char Mask = 1 << CharOffset;
  bool ReadableFlag = Mask & readable_[ArrayOffset];
  return ReadableFlag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  int ArrayOffset = bucket_idx / 8;
  int CharOffset = bucket_idx % 8;
  char Mask = 1 << CharOffset;
  readable_[ArrayOffset] |= Mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsFull() -> bool {
  bool FullFlag = true;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsReadable(i)) {
      FullFlag = false;
      break;
    }
  }
  return FullFlag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::NumReadable() -> uint32_t {
  int ReadableCount = 0;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      ReadableCount++;
    }
  }
  return ReadableCount;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsEmpty() -> bool {
  // bool EmptyFlag = true;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      return false;
    }
  }
  return true;
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
