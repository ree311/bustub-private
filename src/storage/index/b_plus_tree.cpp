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
  if(root_page_id_ == INVALID_PAGE_ID){
    return true;
  }
  return false;
}

/*
 * Helper function to get the leaf node size
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetSize() const -> size_t { return leaf_max_size_; }

/*
 * Helper function to find the leafnode int the current b+tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(BPlusTreePage *bpt_page, const KeyType &key) const -> void { 
  if(bpt_page->IsLeafPage()){
    return bpt_page;
  }
  auto internal_page = reinterpret_cast<BPlusTreeInternalPage *>(bpt_page);
  
  Page *node_page;
  int index = internal_page->FindSmallestBiggerKV(key);
  if(index = -1){
    node_page = buffer_pool_manager_->FetchPage(internal_page->EndValue());
  }else if(internal_page->KeyAt(index) == key){
    node_page = buffer_pool_manager_->FetchPage(internal_page->ValueAt(index+1));
  }else{
    node_page = buffer_pool_manager_->FetchPage(internal_page->ValueAt(index));
  }
  buffer_pool_manager_->UnpinPage(internal_page, false);
  bpt_page = reinterpret_cast<BPlusTreePage *>(node_page);
  FindLeaf(bpt_page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInParent(const page_id_t &old_page_id, const KeyType &key, const page_id_t &new_page_id) -> void { 
  Page *old_page = buffer_pool_manager_->FetchPage(old_page_id);
  auto internal_page = reinterpret_cast<BPlusTreeInternalPage *>(old_page);
  Page *new_page = buffer_pool_manager_->FetchPage(new_page_id);
  auto new_internal_page = reinterpret_cast<BPlusTreeInternalPage *>(new_page);
  if(internal_page->IsRootPage()){
    page_id_t new_root_page_id;
    Page *new_root_page = buffer_pool_manager_->NewPage(new_root_page_id);
    auto new_root_page = reinterpret_cast<BPlusTreeInternalPage *>(new_root_page);
    new_root_page->Init(new_root_page_id);
    new_root_page->SetValueAt(0, internal_page);
    new_root_page->SetKeyAt(1, key);
    new_root_page->SetValueAt(1, new_internal_page);
    UpdateRootPageId(1);
    buffer_pool_manager_->UnpinPage(old_page_id, true);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    return ;
  }
  page_id_t parent_page_id = internal_page->GetParentPageId();
  buffer_pool_manager_->UnpinPage(old_page_id, true);
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  auto parent_bpt_page = reinterpret_cast<BPlusTreeInternalPage *>(parent_page);
  if (parent_bpt_page->GetPointerNums() < GetSize()){
    parent_bpt_page->InternalInsert(key, new_page_id);
  } else {
    page_id_t copy_page_id;
    Page *copy_page = buffer_pool_manager_->FetchPage(copy_page_id);
    auto internal_copy_page = reinterpret_cast<BPlusTreeInternalPage *>(copy_page);
    internal_copy_page->IncreaseSize(1);
    for (auto const &p[k, v] : parent_bpt_page){
      internal_copy_page->KVInsert(k, v);
    }
    internal_copy_page->InternalInsert(key, new_page_id);
    parent_bpt_page->EraseAll();

    page_id_t new_parent_page_id;
    Page *new_parent_page = buffer_pool_manager_->NewPage(new_parent_page_id);
    auto new_parent_bpt_page = reinterpret_cast<BPlusTreeInternalPage *>(new_parent_bpt_page);
    int i = 0, n = GetSize();
    while (i < n/2) {
      parent_bpt_page->KVInsert(internal_copy_page->KeyAt(i), internal_copy_page->ValueAt(i));
      i++;
    }
    KeyType new_key = internal_copy_page->KeyAt(i);
    while (i < n) {
      new_parent_bpt_page->KVInsert(internal_copy_page->KeyAt(i), internal_copy_page->ValueAt(i));
    }
    buffer_pool_manager_->DeletePage(copy_page_id);
    InsertInParent(parent_bpt_page, new_key, new_parent_bpt_page);
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
  if(IsEmpty()){
    return false;
  }

  page_id_t current_page_id = GetRootPageId();
  Page *page = buffer_pool_manager_->FetchPage(current_page_id);
  BPlusTreePage *bpt_page = reinterpret_cast<BPlusTreePage *>(page);

  FindLeaf(bpt_page, key);

  auto leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(bpt_page);
  if(leaf_page->FindKey(key, result)){
    buffer_pool_manager_->UnpinPage(leaf_page, false);
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
  if(IsEmpty()){
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
    root_page_id_ = new_page_id;
    UpdateRootPageId(0);
    auto root_page = reinterpret_cast<BPlusTreeLeafPage *>(new_page);
    root_page->KVInsert(0, key, value);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    return true;
  }
  page_id_t current_root_id = GetRootPageId();
  Page *page = buffer_pool_manager_->FetchPage(current_root_id);
  BPlusTreePage *bpt_page = reinterpret_cast<BPlusTreePage *>(page);
  FindLeaf(bpt_page, key);

  if(bpt_page->GetKVNums() < GetSize()-1){
    //InsertInLeaf(bpt_page, key, value);
    bpt_page->LeafInsert(key, value);
  }else {
    page_id_t new_parent_page_id;
    Page *new_parent_page = buffer_pool_manager_->NewPage(new_parent_page_id);
    auto new_parent_bpt_page = reinterpret_cast<BPlusTreeInternalPage *>(new_parent_page);
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
    auto leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(new_page);
    for(auto const &p[k, v] : bpt_page->GetArray()){
      leaf_page->KVInsert(k, v);
    }
    // InsertInLeaf(leaf_page, key, value);
    leaf_page->LeafInsert(key, value);
    int temp_n = GetSize();
    new_parent_bpt_page->SetValueAt(temp_n-1, bpt_page->GetPageId());
    bpt_page->SetValueAt(temp_n-1, new_parent_bpt_page->GetPageId());
    bpt_page->EraseAll();

    int i = 0;
    for( ; i<temp_n/2; i++){
      bpt_page->KVInsert(leaf_page->KeyAt(i), leaf_page->ValueAt(i));
    }
    for( ; i<temp_n; i++){
      new_parent_bpt_page->KVInsert(leaf_page->KeyAt(i), leaf_page->ValueAt(i));
    }
    buffer_pool_manager_->DeletePage(new_page_id);
    InsertInParent(bpt_page, new_parent_page->KeyAt(0), new_parent_bpt_page);
  }
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

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
