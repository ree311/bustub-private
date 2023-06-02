#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  return false;
}

/*
 * Helper function to get the leaf node size
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetMaxSize() const -> size_t { return leaf_max_size_; }

/*
 * Helper function to find the leafnode int the current b+tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(BPlusTreePage *bpt_page, const KeyType &key, const KeyComparator &cmp) const -> BPlusTreePage *{
  if (bpt_page->IsLeafPage()) {
    auto leaf_page = reinterpret_cast<LeafPage *>(bpt_page);
    LOG_INFO("# [bpt FindLeaf]leaf finded!which has the first key:%ld", leaf_page->KeyAt(0).ToString());
    return bpt_page;
  }
  auto internal_page = reinterpret_cast<InternalPage *>(bpt_page);

  Page *node_page;
  int index = internal_page->FindSmallestBiggerKV(key, cmp);
  LOG_INFO("# [bpt FindLeaf]next index is %d", index);
  
  node_page = buffer_pool_manager_->FetchPage(internal_page->ValueAt(index));
  
  buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
  auto new_page = reinterpret_cast<BPlusTreePage *>(node_page->GetData());
  return FindLeaf(new_page, key, cmp);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *old_page, const KeyType &key, BPlusTreePage *new_page) -> void {
  LOG_INFO("# [bpt InsertInParent]key:%ld", key.ToString());
  // if(old_page->IsLeafPage()){
  //   auto internal_page = reinterpret_cast<InternalPage *>(old_page);
  //   auto new_internal_page = reinterpret_cast<InternalPage *>(new_page);
  // }else{
  //   auto internal_page = reinterpret_cast<LeafPage *>(old_page);
  //   auto new_internal_page = reinterpret_cast<LeafPage *>(new_page);
  // }
  auto internal_page = reinterpret_cast<InternalPage *>(old_page);
  auto new_internal_page = reinterpret_cast<InternalPage *>(new_page);

  if (internal_page->IsRootPage()) {
    
    page_id_t new_root_page_id;
    Page *new_root_page = buffer_pool_manager_->NewPage(&new_root_page_id);
    auto new_root_internal_page = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root_internal_page->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    root_page_id_ = new_root_page_id;
    new_root_internal_page->SetValueAt(0, internal_page->GetPageId());
    new_root_internal_page->SetKeyAt(1, key);
    new_root_internal_page->SetValueAt(1, new_internal_page->GetPageId());
    new_root_internal_page->IncreaseSize(2);
    UpdateRootPageId(0);
    LOG_INFO("# [bpt InsertInParent]create new root, key:%ld, root page id:%d", key.ToString(), root_page_id_);
    internal_page->SetParentPageId(root_page_id_);
    new_internal_page->SetParentPageId(root_page_id_);
    LOG_INFO("# [bpt InsertInParent]parent page id:%d", old_page->GetParentPageId());
    buffer_pool_manager_->UnpinPage(old_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    return;
  }
  page_id_t parent_page_id = internal_page->GetParentPageId();
  buffer_pool_manager_->UnpinPage(old_page->GetPageId(), true);
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  auto parent_bpt_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
  if (parent_bpt_page->GetSize() < internal_max_size_) {
    LOG_INFO("# [bpt InsertInParent]parent node is not full, do insert");
    parent_bpt_page->InternalInsert(key, new_page->GetPageId(), comparator_);
  } else {
    LOG_INFO("# [bpt InsertInParent]parent node is full, do split");
    page_id_t copy_page_id;
    Page *copy_page = buffer_pool_manager_->NewPage(&copy_page_id);
    auto internal_copy_page = reinterpret_cast<InternalPage *>(copy_page->GetData());
    internal_copy_page->Init(copy_page_id, parent_bpt_page->GetParentPageId(), internal_max_size_);

    for (int i = 0; i < parent_bpt_page->GetSize(); i++) {
      internal_copy_page->SetKeyAt(i, parent_bpt_page->KeyAt(i));
      internal_copy_page->SetValueAt(i, parent_bpt_page->ValueAt(i));
      internal_copy_page->IncreaseSize(1);
    }

    internal_copy_page->InternalInsert(key, new_page->GetPageId(), comparator_);
    parent_bpt_page->EraseAll();

    page_id_t new_parent_page_id;
    Page *new_parent_page = buffer_pool_manager_->NewPage(&new_parent_page_id);
    auto new_parent_internal_bpt_page = reinterpret_cast<InternalPage *>(new_parent_page->GetData());
    new_parent_internal_bpt_page->Init(new_parent_page_id, parent_bpt_page->GetParentPageId(), internal_max_size_);
    int i = 0, n = internal_max_size_;
    while (i < n / 2) {
      parent_bpt_page->InsertAtEnd(internal_copy_page->KeyAt(i), internal_copy_page->ValueAt(i));
      i++;
    }
    KeyType new_key = internal_copy_page->KeyAt(i);
    while (i < n) {
      new_parent_internal_bpt_page->InsertAtEnd(internal_copy_page->KeyAt(i), internal_copy_page->ValueAt(i));
      i++;
    }
    buffer_pool_manager_->DeletePage(copy_page_id);
    InsertInParent(parent_bpt_page, new_key, new_parent_internal_bpt_page);
  }
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    return false;
  }
  ValueType value;
  page_id_t current_page_id = GetRootPageId();
  Page *page = buffer_pool_manager_->FetchPage(current_page_id);
  auto bpt_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  auto leaf_page = reinterpret_cast<LeafPage *>(FindLeaf(bpt_page, key, comparator_));
  if (leaf_page->FindKey(key, &value, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    result->push_back(value);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  LOG_INFO("# [bpt Insert] ***********now insert key: %ld*************", key.ToString());
  if (IsEmpty()) {
    LOG_INFO("# [bpt Insert]tree is empty, now create root and insert");
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
    root_page_id_ = new_page_id;
    UpdateRootPageId(1);
    auto root_page = reinterpret_cast<LeafPage *>(new_page->GetData());
    root_page->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
    root_page->SetKeyAt(0, key);
    root_page->SetValueAt(0, value);
    root_page->IncreaseSize(1);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    return true;
  }
  page_id_t current_root_id = GetRootPageId();
  Page *page = buffer_pool_manager_->FetchPage(current_root_id);
  BPlusTreePage *bpt_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  // debug lines, wait to delete
  
  auto temp_leaf_page = reinterpret_cast<InternalPage *>(bpt_page);
  LOG_INFO("# [bpt Insert] Now the root's first key is:%ld", temp_leaf_page->KeyAt(1).ToString());

  auto leaf_page = reinterpret_cast<LeafPage *>(FindLeaf(bpt_page, key, comparator_));
  LOG_INFO("# [bpt Insert]parent_page_id:%d", leaf_page->GetParentPageId());
  if (leaf_page->GetSize() < leaf_max_size_-1) {
    // InsertInLeaf(bpt_page, key, value);
    LOG_INFO("# [bpt Insert]leaf node is not full, do insert");
    leaf_page->LeafInsert(key, value, comparator_);
  } else {
    LOG_INFO("# [bpt Insert]leaf node is full, now split");
    page_id_t new_parent_page_id;
    Page *new_parent_page = buffer_pool_manager_->NewPage(&new_parent_page_id);
    auto new_leaf_page = reinterpret_cast<LeafPage *>(new_parent_page->GetData());
    new_leaf_page->Init(new_parent_page_id, leaf_page->GetParentPageId(), leaf_max_size_);
    
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
    auto temp_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
    temp_leaf_page->Init(new_page_id, leaf_page->GetParentPageId(), leaf_max_size_);
    // MappingType copy_array[leaf_page->GetSize()+1];
    // // copy all array elements to a new array
    // leaf_page->CopyTo(copy_array);

    // leaf_page->CopyNTo(temp_leaf_page, 0);
    // if(leaf_page->IsRootPage()){
    //   LOG_INFO("# [bpt Insert] l is root, copy to 1 index");
    //   for (int i = 0; i < leaf_page->GetSize(); i++) {
    //     LOG_INFO("# [bpt Insert] now got key:%ld", leaf_page->KeyAt(i).ToString());
    //     temp_leaf_page->SetKeyAt(i+1, leaf_page->KeyAt(i));
    //     temp_leaf_page->SetValueAt(i+1, leaf_page->ValueAt(i));
    //     temp_leaf_page->IncreaseSize(1);
    //   }
    // }else{
      // LOG_INFO("# [bpt Insert] l is not root, copy to 0 index");
      for (int i = 0; i < leaf_page->GetSize(); i++) {
        //LOG_INFO("# [bpt Insert] now got key:%ld", leaf_page->KeyAt(i).ToString());
        temp_leaf_page->SetKeyAt(i, leaf_page->KeyAt(i));
        temp_leaf_page->SetValueAt(i, leaf_page->ValueAt(i));
        temp_leaf_page->IncreaseSize(1);
      }
    // }
    
    // InsertInLeaf(leaf_page, key, value);
    temp_leaf_page->LeafInsert(key, value, comparator_);

    new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
    leaf_page->SetNextPageId(new_leaf_page->GetPageId());
    leaf_page->EraseAll();

    LOG_INFO("# [bpt Insert]now move kvs");
    // move half
    int i = 0;
    for (; i < temp_leaf_page->GetSize() / 2; i++) {
      // todo::use setkeyat and setvalueat
      LOG_INFO("# [bpt Insert] first half, i:%d, key:%ld", i, temp_leaf_page->KeyAt(i).ToString());
      leaf_page->KVInsert(i, temp_leaf_page->KeyAt(i), temp_leaf_page->ValueAt(i));
    }
    // temp_leaf_page->CopyNTo(leaf_page, temp_n/2);
    // temp_leaf_page->CopyNTo(new_leaf_page, )
    int j = 0;
    for (; i < temp_leaf_page->GetSize(); i++) {
      // todo::use setkeyat and setvalueat
      LOG_INFO("# [bpt Insert] second half, i:%d, j:%d key:%ld", i, j, temp_leaf_page->KeyAt(i).ToString());
      new_leaf_page->KVInsert(j++, temp_leaf_page->KeyAt(i), temp_leaf_page->ValueAt(i));
    }
    buffer_pool_manager_->DeletePage(new_page_id);
    LOG_INFO("# [bpt Insert] new l, key:%ld, parent_page_id:%d", new_leaf_page->KeyAt(0).ToString(), new_leaf_page->GetParentPageId());
    InsertInParent(leaf_page, new_leaf_page->KeyAt(0), new_leaf_page);
    LOG_INFO("# [bpt Insert] after insert in parent, parent_page_id:%d", leaf_page->GetParentPageId());
  }
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  page_id_t current_root_id = GetRootPageId();
  Page *page = buffer_pool_manager_->FetchPage(current_root_id);
  BPlusTreePage *bpt_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  RemoveEntry(FindLeaf(bpt_page, key, comparator_), key);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveEntry(BPlusTreePage *bpt_page, const KeyType &key) {
  auto leaf_bpt_page = reinterpret_cast<LeafPage *>(bpt_page);
  leaf_bpt_page->DeleteKey(key, comparator_);
  if (leaf_bpt_page->IsRootPage() && leaf_bpt_page->GetSize() == 1) {
    // as the situation to find bro node in both leaf and internal is the same, I just turn the node to internal node to
    // avoid judgement on that
    auto bpt_internal_page = reinterpret_cast<InternalPage *>(leaf_bpt_page);
    root_page_id_ = bpt_internal_page->ValueAt(0);
    buffer_pool_manager_->DeletePage(bpt_internal_page->GetPageId());
    UpdateRootPageId(0);
  } else if ((leaf_bpt_page->IsLeafPage() && leaf_bpt_page->GetSize() < (leaf_max_size_ - 1) / 2) ||
             (!leaf_bpt_page->IsLeafPage() && leaf_bpt_page->GetSize() < internal_max_size_ / 2)) {
    page_id_t bro_page_id_left, bro_page_id_right;
    int parent_key_index;
    auto parent_page = reinterpret_cast<BPlusTreePage *>(
        (buffer_pool_manager_->FetchPage(leaf_bpt_page->GetParentPageId()))->GetData());
    // as the situation to find bro node in both leaf and internal is the same, I just turn the node to internal node to
    // avoid judgement on that
    auto bpt_internal_page = reinterpret_cast<InternalPage *>(leaf_bpt_page);
    bpt_internal_page->FindBrotherPage(parent_page, &parent_key_index, &bro_page_id_left, &bro_page_id_right);

    if (bro_page_id_left != INVALID_PAGE_ID) {
      auto left_page =
          reinterpret_cast<BPlusTreePage *>((buffer_pool_manager_->FetchPage(bro_page_id_left))->GetData());
      auto parent_internal_page = reinterpret_cast<InternalPage *>(parent_page);
      // auto right_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(bro_page_id_right));
      if (left_page->GetSize() + bpt_internal_page->GetSize() < static_cast<int>(GetMaxSize())) {
        CoalesceNodes(bpt_internal_page, left_page, true, parent_internal_page->KeyAt(parent_key_index));
        RemoveEntry(parent_internal_page, parent_internal_page->KeyAt(parent_key_index));
        buffer_pool_manager_->DeletePage(bpt_internal_page->GetPageId());
      } else {
        KeyType end_key =
            Redistribution(bpt_internal_page, left_page, false, parent_internal_page->KeyAt(parent_key_index));
        parent_internal_page->SetKeyAt(parent_key_index, end_key);
      }
    } else {
      auto right_page =
          reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(bro_page_id_right)->GetData());
      auto parent_internal_page = reinterpret_cast<InternalPage *>(parent_page);
      if (right_page->GetSize() + bpt_internal_page->GetSize() < static_cast<int>(GetMaxSize())) {
        CoalesceNodes(bpt_internal_page, right_page, false, parent_internal_page->KeyAt(parent_key_index));
        RemoveEntry(parent_internal_page, parent_internal_page->KeyAt(parent_key_index));
        buffer_pool_manager_->DeletePage(bpt_internal_page->GetPageId());
      } else {
        KeyType first_key =
            Redistribution(bpt_internal_page, right_page, true, parent_internal_page->KeyAt(parent_key_index));
        parent_internal_page->SetKeyAt(parent_key_index, first_key);
      }
    }
  }
}

// to do redistribution and return the end_key in bro so to replace the k' in parent node
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Redistribution(BPlusTreePage *bpt_page, BPlusTreePage *bro_page, bool is_predecessor,
                                    const KeyType &key) -> KeyType {
  KeyType move_key;
  if (!is_predecessor) {
    if (!bpt_page->IsLeafPage()) {
      auto bro_internal_page = reinterpret_cast<InternalPage *>(bro_page);
      auto bpt_internal_page = reinterpret_cast<InternalPage *>(bpt_page);
      move_key = bro_internal_page->KeyAt(bro_internal_page->GetSize());
      bpt_internal_page->InsertAtFirst(key, bro_internal_page->ValueAt(bro_internal_page->GetSize()));
      bro_internal_page->DeleteEndValue();
    } else {
      auto bro_internal_page = reinterpret_cast<LeafPage *>(bro_page);
      auto bpt_internal_page = reinterpret_cast<LeafPage *>(bpt_page);
      move_key = bro_internal_page->KeyAt(bro_internal_page->GetSize());
      bpt_internal_page->InsertAtFirst(key, bro_internal_page->ValueAt(bro_internal_page->GetSize()));
      bro_internal_page->DeleteEndValue();
    }
  } else {
    if (!bpt_page->IsLeafPage()) {
      auto bro_internal_page = reinterpret_cast<InternalPage *>(bro_page);
      auto bpt_internal_page = reinterpret_cast<InternalPage *>(bpt_page);
      move_key = bro_internal_page->KeyAt(1);
      bpt_internal_page->InsertAtEnd(key, bro_internal_page->ValueAt(0));
      bro_internal_page->DeleteFirstValue();
    } else {
      auto bro_internal_page = reinterpret_cast<LeafPage *>(bro_page);
      auto bpt_internal_page = reinterpret_cast<LeafPage *>(bpt_page);
      move_key = bro_internal_page->KeyAt(1);
      bpt_internal_page->InsertAtEnd(key, bro_internal_page->ValueAt(0));
      bro_internal_page->DeleteFirstValue();
    }
  }

  return move_key;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CoalesceNodes(BPlusTreePage *bpt_page, BPlusTreePage *bro_page, bool is_predecessor,
                                   const KeyType &key) {
  if (is_predecessor) {
    std::swap(bpt_page, bro_page);
  }
  if (!bpt_page->IsLeafPage()) {
    auto bpt_internal_page = reinterpret_cast<InternalPage *>(bpt_page);
    auto bro_internal_page = reinterpret_cast<InternalPage *>(bro_page);
    int n = bro_internal_page->GetSize();
    bro_internal_page->SetKeyAt(++n, key);
    bro_internal_page->SetValueAt(n, bpt_internal_page->ValueAt(0));
    for (int i = 1; i < bpt_internal_page->GetSize() + 1; i++) {
      bro_internal_page->SetKeyAt(i + n, bpt_internal_page->KeyAt(i));
      bro_internal_page->SetValueAt(i + n, bpt_internal_page->ValueAt(i));
      bro_internal_page->SetSize(bpt_internal_page->GetSize());
      n++;
    }
  } else {
    auto bpt_leaf_page = reinterpret_cast<InternalPage *>(bpt_page);
    auto bro_leaf_page = reinterpret_cast<InternalPage *>(bro_page);
    int n = bro_leaf_page->GetSize();
    for (int i = 1; i < bpt_leaf_page->GetSize() + 1; i++) {
      bro_leaf_page->SetKeyAt(i + n, bpt_leaf_page->KeyAt(i));
      bro_leaf_page->SetValueAt(i + n, bpt_leaf_page->ValueAt(i));
      bro_leaf_page->SetSize(bpt_leaf_page->GetSize());
      n++;
    }
  }
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
