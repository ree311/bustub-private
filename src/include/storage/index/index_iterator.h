//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(page_id_t leaf_page_id, int index, BufferPoolManager *buffer_pool_manager);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { 
    if(leaf_page_id_ == itr.leaf_page_id_ && index_ == itr.index_)  {
      return true;
    }
    return false;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { 
    if(leaf_page_id_ != itr.leaf_page_id_ || index_ != itr.index_)  {
      return true;
    }
    return false;
  }

 private:
  // add your own private member variables here
  page_id_t leaf_page_id_;
  int index_;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page_;
  BufferPoolManager *buffer_pool_manager_;
};

}  // namespace bustub
