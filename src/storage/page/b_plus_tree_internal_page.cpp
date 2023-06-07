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
#include "common/logger.h"
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetPointerNums() const -> int{
//   return GetSize();
// }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::EraseAll() -> void { SetSize(0); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KVInsert(int index, const KeyType &key, const ValueType &value) -> void {
  array_[index].first = key;
  array_[index].second = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAtFirst(const KeyType &key, const ValueType &value) -> void {
  for (int i = GetSize(); i > 1; i--) {
    array_[i] = array_[i - 1];
  }

  SetKeyAt(1, key);
  SetValueAt(1, value);

  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAtEnd(const KeyType &key, const ValueType &value) -> void {
  int i = GetSize();

  array_[i].first = key;
  array_[i].second = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InternalInsert(const KeyType &key, const ValueType &value,
                                                    const KeyComparator &cmp) -> void {
  int low = 1;
  int high = GetSize();
  int mid = 0;
  int i = GetSize();

  while (low < high) {
    mid = low + (high - low) / 2;
    if (cmp(array_[mid].first, key) < 0) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  for (; i > low; i--) {
    array_[i] = array_[i - 1];
  }
  LOG_INFO("# [bpt InternalInsert] now got key:%ld at index:%d", key.ToString(), low);
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) -> void {
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindSmallestBiggerKV(const KeyType &key, const KeyComparator &cmp) const -> int {
  int low = 1;
  int high = GetSize();
  if (cmp(KeyAt(1), key) > 0) {
    return 0;
  }
  while (low < high) {
    if (cmp(KeyAt(low), key) < 0) {
      LOG_INFO("# [bpt FindSmall] now low is %d, which is smaller than key:%ld", low, key.ToString());
      low++;
    } else {
      return low;
    }
  }
  return GetSize() - 1;
}

// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetEndValue() const -> ValueType{
//   for(auto &value : array_){ }
//   return array_.back();
// }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteEndValue() -> void { IncreaseSize(-1); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteFirstValue() -> void {
  int n = GetSize();
  SetValueAt(1, array_[0].second);
  // array_[0].second = array_[1].second;
  for (int i = 1; i < n; i++) {
    array_[i] = array_[i + 1];
  }

  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteKey(const KeyType &key, const KeyComparator &cmp) -> void {
  int low = 1;
  int high = GetSize();
  int mid = 0;
  int i = GetSize();

  while (low < high) {
    mid = low + (high - low) / 2;
    if (cmp(array_[mid].first, key) < 0) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  LOG_INFO("# [bpt DeleteKey] now key:%ld at index:%d deleted", key.ToString(), low);
  for (; low < i; low++) {
    array_[low] = array_[low + 1];
  }

  IncreaseSize(-1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
