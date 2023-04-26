//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cmath>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>
#include "common/logger.h"

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  if (dir_.empty()) {
    return 0;
  }
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IncreseGlobalDepth() -> void {
  std::scoped_lock<std::mutex> lock(latch_);
  return IncreseGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IncreseGlobalDepthInternal() -> void {
  global_depth_++;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch2_);
  auto dir_index = IndexOf(key);

  auto it = dir_[dir_index]->GetItems().begin();
  while (it != dir_[dir_index]->GetItems().end()) {
    if (it->first == key) {
      value = it->second;
      return true;
    }
    it++;
  }
  // for (auto const &p : dir_[dir_index]->GetItems()) {
  //   if (p.first == key) {
  //     value = p.second;
  //     return true;
  //   }
  // }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch2_);
  auto dir_index = IndexOf(key);

  auto it = dir_[dir_index]->GetItems().begin();
  while (it != dir_[dir_index]->GetItems().end()) {
    if (it->first == key) {
      it = dir_[dir_index]->GetItems().erase(it);
      LOG_INFO("# [KV Remove]Found key, now remove it");
      return true;
    }
    it++;
  }
  // for (auto const &p : dir_[dir_index]->GetItems()) {
  //   if (p.first == key) {
  //     LOG_INFO("# [KV Remove]Found key, now remove it");
  //     dir_[dir_index]->GetItems().remove(p);
  //     return true;
  //   }
  // }
  LOG_INFO("# [KV Remove]Can't find key");
  return false;
}

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief Insert the given key-value pair into the hash table.
 * If a key already exists, the value should be updated.
 * If the bucket is full and can't be inserted, do the following steps before retrying:
 *    1. If the local depth of the bucket is equal to the global depth,
 *        increment the global depth and double the size of the directory.
 *    2. Increment the local depth of the bucket.
 *    3. Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
 *
 * @param key The key to be inserted.
 * @param value The value to be inserted.
 */

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch2_);
  auto dir_index = IndexOf(key);
  LOG_DEBUG("# [KV Insert] dir_index is %lu", dir_index);
  // if (dir_.empty()) {
  //   LOG_DEBUG("# [KV Insert] dir_ is empty, now reserve");
  //   IncreseGlobalDepth();
  //   dir_index = IndexOf(key);
  //   LOG_DEBUG("# [KV Insert] dir_index is %lu", dir_index);
  //   std::shared_ptr<Bucket> buck0(new Bucket(bucket_size_, 1));
  //   dir_.push_back(buck0);
  //   std::shared_ptr<Bucket> buck1(new Bucket(bucket_size_, 1));
  //   dir_.push_back(buck1);
  //   dir_[dir_index]->GetItems().emplace_back(key, value);
  //   return;
  // }
  // latch_.lock();
  auto it = dir_[dir_index]->GetItems().begin();
  while (it != dir_[dir_index]->GetItems().end()) {
    if (it->first == key) {
      LOG_DEBUG("# [KV Insert]Found key, now to replace its value");
      it->second = value;
      return;
    }
    it++;
  }
  // for (auto &p : dir_[dir_index]->GetItems()) {
  //   if (p.first == key) {
  //     LOG_DEBUG("# [KV Insert]Found key, now to replace its value");
  //     p.second = value;
  //     return;
  //   }
  // }
  // latch_.unlock();

  auto pre_global_depth = GetGlobalDepth();
  auto pre_local_depth = GetLocalDepth(dir_index);
  size_t pre_global_size = dir_.size();
  auto pre_local_size = dir_[dir_index]->GetSize();
  LOG_DEBUG("# [KV Insert]Can't find key, try to insert");

  while (dir_[dir_index]->IsFull()) {
    LOG_DEBUG("# [KV Insert]Bucket is full");
    LOG_DEBUG("# [KV Insert]Pre Global depth is %d, Local depth is %d, Pre global size is %lu, pre local size is %zu",
              pre_global_depth, pre_local_depth, pre_global_size, pre_local_size);
    if (pre_global_depth == pre_local_depth) {
      LOG_DEBUG("# [KV Insert]Global depth is equal to local depth, now do as 1.");
      IncreseGlobalDepth();
      LOG_DEBUG("# [KV Insert]Global depth increased, now is %zu", dir_.size());
      dir_.resize(2 * pre_global_size);
      num_buckets_ *= 2;
      size_t i = pre_global_size;
      LOG_DEBUG("# [KV Insert]Now resize the dir_");

      // latch_.lock();
      while (i < 2 * pre_global_size) {
        dir_[i] = dir_[i - pre_global_size];
        // std::shared_ptr<Bucket> buck_new(new Bucket(bucket_size_, pre_local_depth));
        // auto it = dir_[i]->GetItems().begin();
        // LOG_DEBUG("# [KV Insert]Now redistribute the kv pairs for the bucket %lu", i);
        // while(std::next(it) != dir_[i]->GetItems().end()){
        //   if(IndexOf(std::next(it)->first) != i){
        //     LOG_DEBUG("# [KV Insert]Now move");
        //     buck_new->GetItems().splice(buck_new->GetItems().end(), dir_[i]->GetItems(), std::next(it));
        //   }
        //   it++;
        // }
        // if(IndexOf(dir_[i]->GetItems().begin()->first) != i){
        //   LOG_DEBUG("# [KV Insert]Now move");
        //   buck_new->GetItems().splice(buck_new->GetItems().end(), dir_[i]->GetItems(), dir_[i]->GetItems().begin());
        // }
        // dir_[i+pre_global_size] = buck_new;
        i++;
      }
      // latch_.unlock();

      LOG_DEBUG("# [KV Insert]Now the dir_ size is %zu", dir_.size());

      dir_index = IndexOf(key);
    }
    // do increment before redistribute, then both buckets's local depth can be increased
    LOG_DEBUG("# [KV Insert]Now Increase the local depth of index %lu", dir_index);
    dir_[dir_index]->IncrementDepth();

    std::shared_ptr<Bucket> buck_new(new Bucket(bucket_size_, pre_local_depth + 1));
    // latch_.lock();
    auto it = dir_[dir_index]->GetItems().begin();
    LOG_DEBUG("# [KV Insert]Now redistribute the kv pairs for the bucket %lu", dir_index);
    while (std::next(it) != dir_[dir_index]->GetItems().end()) {
      if (IndexOf(std::next(it)->first) == dir_index) {
        LOG_DEBUG("# [KV Insert]Move");
        buck_new->GetItems().splice(buck_new->GetItems().end(), dir_[dir_index]->GetItems(), std::next(it));
      }
      it++;
    }
    if (IndexOf(dir_[dir_index]->GetItems().begin()->first) == dir_index) {
      LOG_DEBUG("# [KV Insert]Move");
      buck_new->GetItems().splice(buck_new->GetItems().end(), dir_[dir_index]->GetItems(),
                                  dir_[dir_index]->GetItems().begin());
    }
    dir_[dir_index] = buck_new;
    // latch_.unlock();
    // LOG_DEBUG("# [KV Insert]Now redistribute the kv pairs in the bucket");
    // for (auto p :li){
    //   dir_[IndexOf(p.first)]->GetItems().emplace_back(p.first, p.second);
    //   LOG_DEBUG("# [KV Insert]Inserted one kv to index %lu", IndexOf(p.first));
    // }
  }
  LOG_DEBUG("# [KV Insert]Now insert the new key & value to index %lu", dir_index);
  // latch_.lock();
  dir_[dir_index]->GetItems().emplace_back(key, value);
  // latch_.unlock();
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  UNREACHABLE("not implemented");
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  UNREACHABLE("not implemented");
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  UNREACHABLE("not implemented");
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
