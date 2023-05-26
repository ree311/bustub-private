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
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetPointerNums() const -> int{
//   return GetSize();
// }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::EraseAll() -> void{ 
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindBrotherPage(BPlusTreePage *page, int *key_index, page_id_t *bro_page_id_left, page_id_t *bro_page_id_right) const -> void {
  auto parent_page = reinterpret_cast<BPlusTreeInternalPage *>(page);
  bro_page_id_left = -1;
  bro_page_id_right = -1;
  for(int i=0; i<parent_page->GetSize(); i++){
    if(parent_page->ValueAt(i) == GetPageId()){
      if(i == 0){
        // if(parent_page->GetSize() == 1){
        //   return false;
        // }
        *bro_page_id_right = parent_page->ValueAt(i+1);
        // *key = parent_page->KeyAt(i+1);
        *key_index = i+1;
        return ;
      }
      *bro_page_id_left = parent_page->ValueAt(i-1);
      // *bro_page_id_right = parent_page->ValueAt(i+1);
      // *key = parent_page->KeyAt(i);
      *key_index = i;
      return ;
    }
  }
  return ;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KVInsert(int index, const KeyType &key, const ValueType &value) -> void {
  array_[index].first = key;
  array_[index].second = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAtFirst(const KeyType &key, const ValueType &value) -> void {
  int i = GetSize()+1;
  for( ; i>0; i--){
    array_[i] = array_[i-1];
  }
  if(array_.length()>1) {
    array_[1].first = key;
    array_[0].second = value;
  }
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAtEnd(const KeyType &key, const ValueType &value) -> void {
  int i = GetSize()+1;
  
  array_[i].first = key;
  array_[i].second = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InternalInsert(const KeyType &key, const ValueType &value, const KeyComparator &cmp) -> void{
  int low = 1, high = GetSize(), mid = 0, i = GetSize()+1;
  
  while(low <= high){
    mid = low + (high - low)/2;
    if(cmp(array_[mid].first, key) < 0){
      low = mid+1;
    }else{
      high = mid-1;
    }
  }
  
  for(; i>low; i--){
    array_[i] = array_[i-1];
  }

  array_[low].first = key;
  array_[low].second = value;
  IncreaseSize(1);
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) -> void{
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindSmallestBiggerKV(const KeyType &key, const KeyComparator &cmp) const -> int{
  int low = 0, high = GetSize()+1;
  while(low < high){
    if(cmp(KeyAt(low), key) >= 0){
      return low;
    }
    low++;
  }
  return -1;
}

// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetEndValue() const -> ValueType{
//   for(auto &value : array_){ }
//   return array_.back();
// }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteEndValue(KeyType *key, ValueType *value) -> void{
  int end = GetSize();

  *key = array_[end].first;
  *value = array_[end].second;
  
  IncreaseSize(-1);
  return ;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteFirstValue(KeyType *key, ValueType *value) -> void{
  *key = array_[1].first;
  *value = array_[0].second;
  int n = GetSize()+1;
  array_[0].second = array_[1].second;
  for(int i=1; i<n; i++){
    array_[i] = array_[i+1];
  }

  IncreaseSize(-1);
  return ;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
