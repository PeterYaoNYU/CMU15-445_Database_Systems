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

#include <cmath>
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
  page_id_t dir_page_id;
  auto dir_page = buffer_pool_manager_->NewPage(&dir_page_id);
  directory_page_id_ = dir_page_id;

  auto dir_page_data = reinterpret_cast<HashTableDirectoryPage*>(dir_page->GetData());
  dir_page_data->SetPageId(dir_page_id);
  dir_page_data->SetLSN(0);
  dir_page_data->IncrGlobalDepth();

  // create two bucket pages from the buffer pool manager
  page_id_t bucket_one_page_id, bucket_two_page_id;
  // auto bucket_one = buffer_pool_manager_->NewPage(&bucket_one_page_id);
  // auto bucket_two = buffer_pool_manager_->NewPage(&bucket_two_page_id);
  buffer_pool_manager_->NewPage(&bucket_one_page_id);
  buffer_pool_manager_->NewPage(&bucket_two_page_id);

  dir_page_data->IncrLocalDepth(0);
  dir_page_data->IncrLocalDepth(1);
  dir_page_data->SetBucketPageId(0, bucket_one_page_id);
  dir_page_data->SetBucketPageId(1, bucket_two_page_id);
  
  buffer_pool_manager_->UnpinPage(bucket_one_page_id, true);
  buffer_pool_manager_->UnpinPage(bucket_two_page_id, true);
  buffer_pool_manager_->UnpinPage(dir_page_id, true);
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
  // uint32_t Depth = dir_page->GetGlobalDepth();
  uint32_t Mask = dir_page->GetGlobalDepthMask();
  //  uint32_t Key = static_cast<uint32_t>(key);
  uint32_t idx = Mask & Hash(key);
  // while (dir_page->GetBucketPageId(idx) == 0) {
  //   Depth--;
  //   Mask = std::pow(2, Depth) - 1;
  //   idx = Mask & Hash(key);
  // }
  return idx;
  // std::cout<<"the directory idx computed is "<<idx<<std::endl;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  uint32_t idx = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  auto DirectoryPage = buffer_pool_manager_->FetchPage(directory_page_id_);
  auto DirectoryPageData = reinterpret_cast<HashTableDirectoryPage *>(DirectoryPage->GetData());
  return DirectoryPageData;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  auto BucketPage = buffer_pool_manager_->FetchPage(bucket_page_id);
  auto BucketPageData = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(BucketPage->GetData());
  return BucketPageData;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  auto DirectoryPageData = FetchDirectoryPage();
  auto BucketPageId = KeyToPageId(key, DirectoryPageData);
  auto BucketPageData = FetchBucketPage(BucketPageId);
  bool FoundFlag = BucketPageData->GetValue(key, comparator_, result);
  // std::cout<<"finding kv pair "<<key<<" and the result is "<<FoundFlag<<std::endl;
  buffer_pool_manager_->UnpinPage(BucketPageId, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  return FoundFlag;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  auto DirectoryPageData = FetchDirectoryPage();
  // std::cout<<"dir page fetched!"<<std::endl;
  auto BucketPageId = KeyToPageId(key, DirectoryPageData);
  auto BucketPageData = FetchBucketPage(BucketPageId);

  // std::cout<<"inserting kv pair "<<key<<value<<std::endl;
  if (BucketPageData->IsDuplicate(key, value, comparator_)) {
    // std::cout<<"cannot insert cuz there is duplicate"<<std::endl;
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(BucketPageId, false);
    return false;
  }

  if (BucketPageData->IsFull()) {
    // std::cout<<"cannot insert because the bucket is full"<<std::endl;
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(BucketPageId, false);
    return SplitInsert(transaction, key, value);
  }

  bool SuccessFlag = BucketPageData->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(BucketPageId, true);
  return SuccessFlag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  auto DirectoryPageData = FetchDirectoryPage();
  auto BucketPageId = KeyToPageId(key, DirectoryPageData);
  auto BucketPageData = FetchBucketPage(BucketPageId);
  page_id_t NewPageId;
  // Page * NewPage = buffer_pool_manager_->NewPage(&NewPageId);
  buffer_pool_manager_->NewPage(&NewPageId);

  // auto NewPageData = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(NewPage->GetData());

  auto OldDirIdx = KeyToDirectoryIndex(key, DirectoryPageData);
  auto OldLocalDepth = DirectoryPageData->GetLocalDepth(OldDirIdx);
  // DirectoryPageData->IncrLocalDepth(OldDirIdx);
  // std::cout<<"the old dir idx, new dir idx"<<OldDirIdx<<" "<<NewDirIdx<<std::endl;

  // DirectoryPageData->SetLocalDepth(NewDirIdx, OldLocalDepth + 1);
  // DirectoryPageData->SetBucketPageId(NewDirIdx, NewPageId);


  if (OldLocalDepth ==  DirectoryPageData->GetGlobalDepth()) {
    // DirectoryPageData->IncrLocalDepth(OldDirIdx);
    DirectoryPageData->IncrGlobalDepth();
    DirectoryPageData->IncrLocalDepth(OldDirIdx);
    int gap = std::pow(2, OldLocalDepth);
    for (int i = std::pow(2, OldLocalDepth); i < std::pow(2, OldLocalDepth+1); i++){
      DirectoryPageData->SetBucketPageId(i, DirectoryPageData->GetBucketPageId(i-gap));
      // DirectoryPageData->bucket_page_ids_[i] = DirectoryPageData->bucket_page_ids_[i-gap];
      DirectoryPageData->SetLocalDepth(i, DirectoryPageData->GetLocalDepth(i-gap));
      // DirectoryPageData->local_depths_[i] = DirectoryPageData->local_depths_[i-gap];
    }
    auto NewDirIdx = OldDirIdx + std::pow(2, OldLocalDepth);
    DirectoryPageData->SetBucketPageId(NewDirIdx, NewPageId);
  } else {
    int mask = 0x1<<OldLocalDepth;
    for (uint32_t i = 0; i < std::pow(2, DirectoryPageData->GetGlobalDepth()); i++){
      if (DirectoryPageData->GetBucketPageId(i) == static_cast<int>(BucketPageId)){
        DirectoryPageData->IncrLocalDepth(i);
        if (mask & i){
          DirectoryPageData->SetBucketPageId(i, NewPageId);
        }
      }
    }
  }


  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (BucketPageData->IsReadable(i)) {
      auto OldKey = BucketPageData->KeyAt(i);
      auto OldValue = BucketPageData->ValueAt(i);
      BucketPageData->RemoveAt(i);
      Insert(transaction, OldKey, OldValue);
    }
  }

  bool SuccessFlag = Insert(transaction, key, value);

  DirectoryPageData->PrintDirectory();

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(BucketPageId, true);
  buffer_pool_manager_->UnpinPage(NewPageId, true);


  return SuccessFlag;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  auto dir_page_data = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page_data);
  auto bucket_page_data = FetchBucketPage(bucket_page_id);
  
  bool success_flag = bucket_page_data->Remove(key, value, comparator_);
  
  if (bucket_page_data->IsEmpty()){
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    Merge(transaction, key, value);
    return success_flag;
  }

  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);

  return success_flag;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // std::cout<<"start merging for kv pair "<< key<< " "<<value<<std::endl;
  auto dir_page_data = FetchDirectoryPage();
  auto bucket_dir_idx = KeyToDirectoryIndex(key, dir_page_data);
  auto bucket_page_id = KeyToPageId(key, dir_page_data);
  auto bucket_page_data = FetchBucketPage(bucket_page_id);
  auto old_local_depth = dir_page_data->GetLocalDepth(bucket_dir_idx);
  
  if ((!bucket_page_data->IsEmpty()) || old_local_depth<=0){
    // std::cout<<""
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return;
  }

  auto split_image_idx = dir_page_data->GetSplitImageIndex(bucket_dir_idx);
  auto split_image_local_depth = dir_page_data->GetLocalDepth(split_image_idx);
  if (old_local_depth != split_image_local_depth){
    // std::cout<< "Merge: two depths don;t match: "<<bucket_dir_idx, 
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return;
  }

  auto split_image_page_id = dir_page_data->GetBucketPageId(split_image_idx);
  // std::cout<<"old local depth, split image depth, old page id, split page id, old bucket idx, split idx: "<<old_local_depth<<" "<<split_image_local_depth<<" "<<bucket_page_id<<" "<<split_image_page_id<<" "<<bucket_dir_idx<<" "<<split_image_idx<<std::endl;

  for (uint32_t i = 0; i<dir_page_data->Size(); i++){
    if (static_cast<int>(bucket_page_id) == dir_page_data->GetBucketPageId(i)){
      dir_page_data->SetBucketPageId(i, split_image_page_id);
      dir_page_data->DecrLocalDepth(i);
      // std::cout<<"after decrementing, bucket_idx, new ld: "<<i<<" "<<dir_page_data->GetLocalDepth(i)<<std::endl;
    } else if (static_cast<int>(split_image_page_id) == dir_page_data->GetBucketPageId(i)){
      dir_page_data->DecrLocalDepth(i);
    }
  }

  // dir_page_data->DecrLocalDepth(split_image_idx);
  // std::cout<<"after decrementing, split image idx, new ld: "<<split_image_idx<<" "<<dir_page_data->GetLocalDepth(split_image_idx)<<std::endl;

  // std::cout<<"BUCKET NO 4 page id, ld: "<<dir_page_data->GetBucketPageId(4)<<" "<<dir_page_data->GetLocalDepth(4)<<std::endl;


  bool global_shrink_flag = true;
  for (uint32_t i = 0; i < DIRECTORY_ARRAY_SIZE; i++){
    if (dir_page_data->GetLocalDepth(i) >= dir_page_data->GetGlobalDepth()){
      global_shrink_flag = false;
      break;
    }
  }
  
  if (global_shrink_flag){
    dir_page_data->DecrGlobalDepth();
    // std::cout<<"shrinking"<<std::endl;
  }
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);

  buffer_pool_manager_->DeletePage(bucket_page_id);
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
