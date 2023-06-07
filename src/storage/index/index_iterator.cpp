/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t leaf_page_id, int index, BufferPoolManager *buffer_pool_manager){
    leaf_page_id_ = leaf_page_id;
    Page *page = buffer_pool_manager->FetchPage(leaf_page_id);
    leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
    index_ = index;
    buffer_pool_manager_ = buffer_pool_manager;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator(){
    buffer_pool_manager_->UnpinPage(leaf_page_id_, false);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { 
    if(leaf_page_id_ == INVALID_PAGE_ID){
        return true;
    }
    return false;
 }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { 
    return leaf_page_->GetItem(index_);
 }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & { 
    if(index_ < leaf_page_->GetSize()-1){
        index_++;
        return *this;
    }
    buffer_pool_manager_->UnpinPage(leaf_page_id_, false);
    leaf_page_id_ = leaf_page_->GetNextPageId();
    if(leaf_page_id_ != INVALID_PAGE_ID){
        Page *page = buffer_pool_manager_->FetchPage(leaf_page_id_);
        leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
    }
    index_ = -1;
    return *this;
 }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
