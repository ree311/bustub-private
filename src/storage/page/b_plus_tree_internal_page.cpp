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
  page_id_ = page_id;
  parent_page_id_ = parent_id;
  page_type_ = IndexPageType::INTERNAL_PAGE;
  size_ = 0;
  max_size_ = max_size;
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

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetPointerNums() const -> int{
  return size_;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::EraseAll() -> void{ 
  auto it = array_.begin();
  while (it != array_.end()) {
    it = array_.erase(it);
  }
  size_ = 0;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindBrotherPage(BPlusTreePage *page, KeyType *key, page_id_t *bro_page_id_left = -1, page_id_t *bro_page_id_right = -1) const -> void {
  auto parent_page = reinterpret_cast<BPlusTreeInternalPage *>(page);
  for(int i=0; i<parent_page->GetSize(); i++){
    if(parent_page->ValueAt(i) == GetPageId()){
      if(i == 0){
        // if(parent_page->GetSize() == 1){
        //   return false;
        // }
        *bro_page_id_right = parent_page->ValueAt(i+1);
        *key = parent_page->KeyAt(i+1);
        return ;
      }else{
        *bro_page_id_left = parent_page->ValueAt(i-1);
        // *bro_page_id_right = parent_page->ValueAt(i+1);
        key = parent_page->KeyAt(i);
        return ;
      }
    }
  }
  return ;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetArray() const -> MappingType { return array_; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KVInsert(int index, const KeyType &key, const ValueType &value) -> bool {
  array_[index].first = key;
  array_[index].second = value;
  size_++;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InternalInsert(const KeyType &key, const ValueType &value) -> void{
  int low = 1, high = size_, mid = 0, i = size_+1;
  
  while(low <= high){
    mid = low + (high - low)/2;
    if(array_[mid].first < key){
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
  size_++;
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindSmallestBiggerKV(const KeyType &key) const -> int{
  int low = 0, high = array_.size();
  while(low < high){
    if(KeyAt(low) >= key){
      return low;
    }
    low++;
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetEndValue() const -> ValueType{
  for(auto &value : array_){ }
  return array_.back();
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteEndValue() -> void{
  auto it = array_.begin();
  while(std::next(it)!=array_.end()){
    it++;
  }
  array_.erase(it);
  size_--;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
