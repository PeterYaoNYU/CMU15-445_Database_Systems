//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key{array_[index].first};
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  int size = GetSize();

  for (int index = 0; index <= size; index++){
    if (ValueAt(index) == value){
      return index;
    }
  }

  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  int CompareResult = 0;
  // not sure whether it's <= or <
  for (int i = 1; i < GetSize(); i++){
    CompareResult = comparator(key, KeyAt(i));
    if (CompareResult >= 0){
      continue;
    } else if (CompareResult == -1){
      return ValueAt(i-1);
    }
  }
  return ValueAt(GetSize()-1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
                                                      SetSize(2);
                                                      array_[0].second= old_value;
                                                      array_[1].first = new_key;
                                                      array_[1].second = new_value;
                                                     }
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) -> int {
  int OldIndex = ValueIndex(old_value);
  if (OldIndex == -1){
    return GetSize();
  }
  // i didn't check if the new size is still less than the max size here in this function!
  int TotalSize = GetSize();
  // printf("the total size is %d\n", TotalSize);
  for (int i = TotalSize-1; i > OldIndex; i--){
    array_[i+1] = array_[i];
  }
  // printf("now the total size is %d\n", TotalSize);

  array_[OldIndex+1].first = new_key;
  array_[OldIndex+1].second = new_value;
  // printf("now 2 the total size is %d\n", TotalSize);

  SetSize(TotalSize+1);
  // printf("now 3 the total size is %d\n", TotalSize);

  return TotalSize+1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  // the tricky part is, how do you handle the invalid first location?
  int MySize = GetSize();
  int MovedSize = GetSize() / 2;
  // int OldSize = GetSize();
  // int StartingIdx = GetSize() - MovedSize;
  // if (recipient->GetSize() == 0){
  //   recipient->
  // }
  recipient->CopyNFrom(array_ + MySize - MovedSize, MovedSize, buffer_pool_manager);
  // delete half of the kv pairs. (not yet implemented)
  // set to the new size;
  // SetSize(OldSize - MovedSize);
  IncreaseSize(-MovedSize);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  // this function really needs some concurrency control  
  page_id_t PageId = GetPageId();
  int OldSize = GetSize();
  int InsertedIdx = GetSize();
  for (int i = 0; i < size; i++){
    array_[InsertedIdx+i].first = items->first;
    array_[InsertedIdx+i].second = items->second;
    auto CopiedPage = buffer_pool_manager->FetchPage(items->second);
    auto CopiedPageData = reinterpret_cast<BPlusTreePage *>(CopiedPage->GetData());
    CopiedPageData->SetParentPageId(PageId);
    buffer_pool_manager->UnpinPage(items->second, 1);
    items++;
  }

  SetSize(OldSize + size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  int OldSize = GetSize();

  for (int i = index; i < OldSize-1; i++){
    array_[i] = array_[i+1];
  }
  SetSize(OldSize-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() -> ValueType {
  if (GetSize() != 1){
    return INVALID_PAGE_ID;
  }
  ValueType V = ValueAt(0);
  SetSize(0);
  return V;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0, middle_key);
  recipient->CopyNFrom(array_, GetSize(), buffer_pool_manager);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager) {
  array_[0].first = middle_key;
  recipient->CopyLastFrom(array_[0], buffer_pool_manager);
  Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  int OldSize = GetSize();
  SetSize(OldSize+1);
  array_[OldSize] = pair;
  auto ChildPage = buffer_pool_manager->FetchPage(pair.second);
  auto ChildPageData = reinterpret_cast<BPlusTreePage *>(ChildPage->GetData());
  ChildPageData->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(pair.second, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0, middle_key);
  recipient->CopyFirstFrom(array_[GetSize()-1], buffer_pool_manager);
  SetSize(GetSize()-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  auto ParentPageId = GetParentPageId();
  
  for (int i = GetSize() - 1; i >= 0; i--){
    array_[i+1] = array_[i];
  }
  array_[0] = pair;
  
  auto NewPage = buffer_pool_manager->FetchPage(ValueAt(0));
  auto NewPageData = reinterpret_cast<BPlusTreePage*>(NewPage->GetData());
  NewPageData->SetParentPageId(ParentPageId);
  buffer_pool_manager->UnpinPage(NewPage->GetPageId(), 1);
  
  SetSize(GetSize()+1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
