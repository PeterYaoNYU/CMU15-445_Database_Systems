//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <math.h>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!

  // dir_page_ is a pointer to the hash table directory page!
  Page *dir_page_ = buffer_pool_manager_->NewPage(&directory_page_id_);
  auto dir_page_data_ = reinterpret_cast<HashTableDirectoryPage *>(dir_page_->GetData());
  dir_page_data_->SetPageId(directory_page_id_);

  dir_page_data_->InitDirectoryPage();

  page_id_t bucket_0_page_id_, bucket_1_page_id_;
  // pointer to the bubcket 1 page!
  buffer_pool_manager_->NewPage(&bucket_0_page_id_);
  // pointer to the bucket 2 page
  buffer_pool_manager_->NewPage(&bucket_1_page_id_);

  dir_page_data_->SetBucketPageId(0, bucket_0_page_id_);
  dir_page_data_->SetBucketPageId(1, bucket_1_page_id_);

  dir_page_data_->SetLocalDepth(0, 1);
  dir_page_data_->SetLocalDepth(1, 1);

  dir_page_data_->IncrGlobalDepth();

  buffer_pool_manager_->UnpinPage(directory_page_id_, 1);
  buffer_pool_manager_->UnpinPage(bucket_0_page_id_, 0);
  buffer_pool_manager_->UnpinPage(bucket_1_page_id_, 0);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  uint32_t mask_ = dir_page->GetGlobalDepthMask();
  uint32_t index_now_ = Hash(key) & mask_;
  // printf("mask: %d, index_now: %d\n", mask_, index_now_);
  // printf("finding the directory index\n");
  while (!dir_page->GetBucketPageId(index_now_)) {
    printf("recursing\n");
    index_now_ = Hash(key) & (mask_ >> 1);
  }
  // printf("done finding the directory index\n");
  return index_now_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  uint32_t bucket_idx_ = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(bucket_idx_);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  // return nullptr;
  // printf("fetching directory page\n");
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
  // printf("done fetching directory page\n");
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> std::pair<Page *, HASH_TABLE_BUCKET_TYPE *> {
  // in order to use the write latch_, we may need to return a pair instead
  auto bucket_page_ = buffer_pool_manager_->FetchPage(bucket_page_id);
  auto bucket_page_data_ = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page_->GetData());

  return std::make_pair(bucket_page_, bucket_page_data_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  table_latch_.RLock();
  // printf("get value...............................table_latch reader_count: %d\n", table_latch_.get_reader_count());

  std::cout << "getting value: " << key << std::endl;

  auto dir_page_data_ = FetchDirectoryPage();
  auto bucket_page_id_ = KeyToPageId(key, dir_page_data_);
  auto [bucket_page_, bucket_page_data_] = FetchBucketPage(bucket_page_id_);

  bucket_page_->RLatch();
  bool found_flag_ = bucket_page_data_->GetValue(key, comparator_, result);
  // not sure about the right place to call unlatch_
  // maybe unpinning will cause a reace condition since there is pincount -- involved!
  // but then again what about fetching
  // bucket_page_->RUnlatch();

  buffer_pool_manager_->UnpinPage(bucket_page_id_, 0);
  buffer_pool_manager_->UnpinPage(directory_page_id_, 0);

  bucket_page_->RUnlatch();

  table_latch_.RUnlock();
  // printf("get value...............................table_latch reader_count: %d\n", table_latch_.get_reader_count());

  return found_flag_;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  // printf("Insert...............................table_latch reader_count: %d\n", table_latch_.get_reader_count());

  std::cout << "inserting kv pair: " << key << " " << value << std::endl;

  auto dir_page_data_ = FetchDirectoryPage();
  // printf("done fetching the directory page\n");
  auto bucket_page_id_ = KeyToPageId(key, dir_page_data_);
  // printf("bucket page id found\n");
  auto [bucket_page_, bucket_page_data_] = FetchBucketPage(bucket_page_id_);

  std::vector<ValueType> result;
  GetValue(transaction, key, &result);
  for (auto it = result.begin(); it != result.end(); it++) {
    if (*it == value) {
      // shit!!!!!!!! forget to unlock table_latch_ here, took me 30 mins to find this fucking bug ou
      // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      table_latch_.RUnlock();
      return false;
    }
  }

  bucket_page_->WLatch();
  // printf("inserting...\n");
  // bucket_page_->WUnlatch();
  if (bucket_page_data_->IsFull()) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, 0);
    buffer_pool_manager_->UnpinPage(bucket_page_id_, 0);
    bucket_page_->WUnlatch();
    table_latch_.RUnlock();
    // printf("Insert...............................table_latch reader_count: %d\n", table_latch_.get_reader_count());

    return SplitInsert(transaction, key, value);
  }

  bool success_flag_ = bucket_page_data_->Insert(key, value, comparator_);

  // not sure if the directory page should be considered dirty or not
  buffer_pool_manager_->UnpinPage(directory_page_id_, 0);
  buffer_pool_manager_->UnpinPage(bucket_page_id_, success_flag_);
  bucket_page_->WUnlatch();
  table_latch_.RUnlock();
  // printf("Insert...............................table_latch reader_count: %d\n", table_latch_.get_reader_count());

  return success_flag_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.WLock();
  // printf("split insert...............................table_latch reader_count: %d\n",
  // table_latch_.get_reader_count());

  std::cout << "split inserting kv: " << key << ' ' << value << std::endl;
  auto dir_page_data_ = FetchDirectoryPage();
  auto bucket_page_id_ = KeyToPageId(key, dir_page_data_);
  auto [bucket_page_, bucket_page_data_] = FetchBucketPage(bucket_page_id_);
  auto bucket_idx_ = KeyToDirectoryIndex(key, dir_page_data_);
  bool split_flag_ = false;
  page_id_t new_bucket_page_id_;

  // here comes the splitting !
  if (bucket_page_data_->IsFull()) {
    split_flag_ = true;
    auto new_bucket_page_ = (buffer_pool_manager_->NewPage(&new_bucket_page_id_));
    auto new_bucket_idx_ = std::pow(2, dir_page_data_->GetLocalDepth(bucket_idx_));

    dir_page_data_->SetBucketPageId(new_bucket_idx_, new_bucket_page_id_);
    dir_page_data_->SetLocalDepth(new_bucket_idx_, dir_page_data_->GetLocalDepth(bucket_idx_));

    // auto new_bucket_page_data_ = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_bucket_page_->GetData());
    dir_page_data_->IncrLocalDepth(new_bucket_idx_);
    dir_page_data_->IncrLocalDepth(bucket_idx_);
    if (dir_page_data_->GetLocalDepth(new_bucket_idx_) > dir_page_data_->GetGlobalDepth()) {
      dir_page_data_->IncrGlobalDepth();
    }
    // reinsert everything that is in the old bucket idx
    for (int i = 0; i < static_cast<int>(BUCKET_ARRAY_SIZE); i++) {
      if (bucket_page_data_->IsReadable(i)) {
        bucket_page_->WLatch();
        new_bucket_page_->WLatch();
        KeyType old_key = bucket_page_data_->KeyAt(i);
        ValueType old_val = bucket_page_data_->ValueAt(i);
        Insert(transaction, old_key, old_val);
        bucket_page_->WUnlatch();
        new_bucket_page_->WUnlatch();
      }
    }
  } else {
    table_latch_.WUnlock();
    // printf("split insert...............................table_latch reader_count: %d\n",
    // table_latch_.get_reader_count());

    return bucket_page_data_->Insert(key, value, comparator_);
  }

  if (split_flag_) {
    buffer_pool_manager_->UnpinPage(new_bucket_page_id_, 1);
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, split_flag_);
  buffer_pool_manager_->UnpinPage(bucket_page_id_, 1);
  table_latch_.WUnlock();
  // printf("split insert...............................table_latch reader_count: %d\n",
  // table_latch_.get_reader_count());

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  // printf("remove...............................table_latch reader_count: %d\n", table_latch_.get_reader_count());
  std::cout << "removing kv pair " << key << " " << value << std::endl;
  auto dir_page_data_ = FetchDirectoryPage();
  auto bucket_idx_ = KeyToDirectoryIndex(key, dir_page_data_);
  auto bucket_page_id_ = KeyToPageId(key, dir_page_data_);
  auto [bucket_page_, bucket_page_data_] = FetchBucketPage(bucket_page_id_);

  bucket_page_->WLatch();
  bucket_page_data_->print_bucket();
  // printf(".........................calling bucket_page_data_ remove function\n");
  bool success_flag_ = bucket_page_data_->Remove(key, value, comparator_);
  // bool merge_flag_ = false;

  // potentially merge the empty bucket together!
  if (bucket_page_data_->IsEmpty() && dir_page_data_->GetLocalDepth(bucket_idx_) != 0) {
    printf("entering the merging branch\n");
    std::cout << "is empty bucket? " << bucket_page_data_->IsEmpty() << std::endl;
    bucket_page_data_->print_bucket();

    buffer_pool_manager_->UnpinPage(bucket_page_id_, success_flag_);
    buffer_pool_manager_->UnpinPage(directory_page_id_, 0);
    bucket_page_->WUnlatch();
    table_latch_.RUnlock();

    printf("done unlocking all latches\n");
    // merge_flag_ = true;
    Merge(transaction, key, value);
    return success_flag_;
  }

  // printf("start unpinning all\n");

  buffer_pool_manager_->UnpinPage(bucket_page_id_, success_flag_);
  buffer_pool_manager_->UnpinPage(directory_page_id_, 0);

  // printf("done unpinning all\n");
  bucket_page_->WUnlatch();
  // printf("done write unlatching...\n");
  table_latch_.RUnlock();
  // printf("remove...............................table_latch reader_count: %d\n", table_latch_.get_reader_count());

  // printf("done read unlocking...\n");
  return success_flag_;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // printf("waiting for wlock()\n");
  table_latch_.WLock();
  // printf("merge...............................table_latch reader_count: %d\n", table_latch_.get_reader_count());

  printf("merging........\n");
  auto dir_page_data_ = FetchDirectoryPage();
  auto bucket_idx_ = KeyToDirectoryIndex(key, dir_page_data_);
  // auto bucket_page_id_ = KeyToPageId(key, dir_page_data_);
  // auto bucket_page_ = FetchBucketPage(bucket_page_id_);
  uint32_t smaller_bucket_idx_, bigger_bucket_idx_;

  auto bucket_local_depth_ = dir_page_data_->GetLocalDepth(bucket_idx_);
  if (bucket_idx_ >> (bucket_local_depth_ - 1)) {
    smaller_bucket_idx_ = bucket_idx_ - std::pow(2, bucket_local_depth_ - 1);
    bigger_bucket_idx_ = bucket_idx_;
  } else {
    bigger_bucket_idx_ = bucket_idx_ + std::pow(2, bucket_local_depth_ - 1);
    smaller_bucket_idx_ = bucket_idx_;
  }

  std::cout << "bucket idxs " << smaller_bucket_idx_ << " " << bigger_bucket_idx_ << std::endl;

  if (dir_page_data_->GetLocalDepth(smaller_bucket_idx_) != dir_page_data_->GetLocalDepth(bigger_bucket_idx_)) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, 0);
    table_latch_.WUnlock();
    // printf("merge...............................table_latch reader_count: %d\n", table_latch_.get_reader_count());

    // buffer_pool_manager_->UnpinPage(bucket_page_);
    return;
  }

  dir_page_data_->DecrLocalDepth(smaller_bucket_idx_);
  dir_page_data_->DecrLocalDepth(bigger_bucket_idx_);

  bool decr_global_depth_flag_ = true;
  for (uint32_t i = 0; i < DIRECTORY_ARRAY_SIZE; ++i) {
    if (dir_page_data_->GetLocalDepth(i) >= dir_page_data_->GetGlobalDepth()) {
      decr_global_depth_flag_ = false;
      break;
    }
  }
  if (decr_global_depth_flag_) {
    dir_page_data_->DecrGlobalDepth();
  }

  std::cout << "global depth " << dir_page_data_->GetGlobalDepth() << std::endl;

  auto [smaller_bucket_page_, smaller_bucket_page_data_] =
      FetchBucketPage(dir_page_data_->GetBucketPageId(smaller_bucket_idx_));
  // auto [bigger_bucket_page_, bigger_bucket_page_data_] = FetchBucketPage(bigger_bucket_idx_);
  smaller_bucket_page_->RLatch();
  if (smaller_bucket_page_data_->IsEmpty()) {
    auto smaller_bucket_page_id_ = dir_page_data_->GetBucketPageId(smaller_bucket_idx_);
    dir_page_data_->SetBucketPageId(smaller_bucket_idx_, dir_page_data_->GetBucketPageId(bigger_bucket_idx_));
    smaller_bucket_page_->RUnlatch();
    printf("after merging... delete the smaller idx\n");
    smaller_bucket_page_data_->print_bucket();

    buffer_pool_manager_->UnpinPage(smaller_bucket_page_id_, 0);
    buffer_pool_manager_->DeletePage(smaller_bucket_page_id_);
  } else {
    printf("after merging... delete the bigger index\n");
    smaller_bucket_page_data_->print_bucket();
    buffer_pool_manager_->DeletePage(dir_page_data_->GetBucketPageId(bigger_bucket_idx_));
    smaller_bucket_page_->RUnlatch();
  }
  dir_page_data_->SetLocalDepth(bigger_bucket_idx_, 0);
  dir_page_data_->SetBucketPageId(bigger_bucket_idx_, 0);

  buffer_pool_manager_->UnpinPage(directory_page_id_, 1);
  table_latch_.WUnlock();

  // printf("merge...............................table_latch reader_count: %d\n", table_latch_.get_reader_count());
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
