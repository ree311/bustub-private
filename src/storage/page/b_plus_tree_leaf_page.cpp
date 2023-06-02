//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) -> void { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) -> void { array_[index].first = key; }

// leaf_page is the direct page, pos is the start pos to copy
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNTo(BPlusTreeLeafPage *leaf_page, const int &pos) -> void {
  leaf_page->CopyNFrom(array_ + pos, GetSize() - pos);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *item, const int &size) -> void {
  std::copy(item, item + size + 1, array_ + GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KVInsert(int index, const KeyType &key, const ValueType &value) -> void {
  array_[index].first = key;
  array_[index].second = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAtFirst(const KeyType &key, const ValueType &value) -> void {
  int i = GetSize();
  for (; i > 0; i--) {
    array_[i] = array_[i - 1];
  }

  array_[0].first = key;
  array_[0].second = value;

  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAtEnd(const KeyType &key, const ValueType &value) -> void {
  int i = GetSize();

  array_[i].first = key;
  array_[i].second = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::LeafInsert(const KeyType &key, const ValueType &value, const KeyComparator &cmp)
    -> void {
  int low = 0, high = GetSize(), mid = 0, i = GetSize();

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
  LOG_INFO("# [bpt LeafInsert] now got key:%ld at index:%d", key.ToString(), low);
  array_[low].first = key;
  array_[low].second = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteKey(const KeyType &key, const KeyComparator &cmp) -> void {
  int low = 0, high = GetSize(), mid = 0, i = GetSize();

  while (low < high) {
    mid = low + (high - low) / 2;
    if (cmp(array_[mid].first, key) < 0) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  for (; low < i; low++) {
    array_[low] = array_[low + 1];
  }

  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindKey(const KeyType &key, ValueType *value, const KeyComparator &cmp) const -> bool {
  int low = 0, high = GetSize(), mid = 0;
  while (low < high) {
    mid = low + (high - low) / 2;
    if (cmp(array_[mid].first, key) < 0) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }
  if (cmp(array_[low].first, key) == 0) {
      LOG_INFO("# [bpt FindKey] find key:%ld at index:%d", key.ToString(), low);
      *value = array_[low].second;
      return true;
  }  
  LOG_INFO("# [bpt FindKey] can't find key:%ld", key.ToString());
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::EraseAll() -> void { SetSize(0); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteEndValue() -> void {
  IncreaseSize(-1);
  return;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteFirstValue() -> void {
  int n = GetSize();

  for (int i = 0; i < n; i++) {
    array_[i] = array_[i + 1];
  }

  IncreaseSize(-1);
  return;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
