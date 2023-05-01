//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include <mutex>
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  std::vector<frame_id_t> inf_frames;
  size_t evict_frame_time = __INT_MAX__;
  frame_id_t evict_frame_id = __INT_MAX__;

  for (auto f : id_to_frames_) {
    if (f.second.evictable_) {
      if (f.second.k_value_ == __INT_MAX__) {
        inf_frames.push_back(f.first);
      }
      if (evict_frame_time > f.second.timestamps_.front()) {
        evict_frame_time = f.second.timestamps_.front();
        evict_frame_id = f.first;
      }
    }
  }
  if (inf_frames.size() == 1) {
    *frame_id = inf_frames[0];
    LOG_INFO("# [Evict]There is only one inf frame %d, now evict it", *frame_id);
    // id_to_frames_[inf_frames[0]].evictable = false;
    id_to_frames_.erase(inf_frames[0]);
    // curr_size_--;
  } else if (inf_frames.size() > 1) {
    size_t earliest_time = __INT_MAX__;
    frame_id_t earliest_time_id = inf_frames[0];
    for (auto i : inf_frames) {
      if (earliest_time > id_to_frames_[i].timestamps_.front()) {
        earliest_time_id = i;
        earliest_time = id_to_frames_[i].timestamps_.front();
      }
    }
    *frame_id = earliest_time_id;
    LOG_INFO("# [Evict]There are many inf frames, now evict the oldest one with id:%d", *frame_id);
    // id_to_frames_[*frame_id].evictable = false;
    id_to_frames_.erase(*frame_id);
    // curr_size_--;
  } else if (evict_frame_id != __INT_MAX__) {
    *frame_id = evict_frame_id;
    LOG_INFO("# [Evict]There is no inf frame, now evict the largest k-distance frame with id:%d", *frame_id);
    // id_to_frames_[*frame_id].evictable = false;
    id_to_frames_.erase(*frame_id);
    // curr_size_--;
  } else {
    return false;
  }

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  current_timestamp_++;

  if (id_to_frames_.find(frame_id) == id_to_frames_.end()) {
    LOG_INFO("# [RecordAccess]No frame %d, now make one new entry for it", frame_id);

    if (static_cast<int>(replacer_size_) < frame_id) {
      throw "frame id is INVALID";
    }
    std::queue<size_t> time_queue({current_timestamp_});
    struct Frame curr_frame = {true, time_queue, __INT_MAX__};
    id_to_frames_.emplace(frame_id, curr_frame);
    // curr_size_++;
    LOG_DEBUG("# [RecordAccess]Frame %d's timestamps_nums is %lu", frame_id,
              id_to_frames_[frame_id].timestamps_.size());
  } else if (id_to_frames_[frame_id].timestamps_.size() < k_ - 1) {
    LOG_DEBUG("# [RecordAccess]Frame %d's timestamps_nums less than k-1", frame_id);
    id_to_frames_[frame_id].timestamps_.push(current_timestamp_);
  } else if (id_to_frames_[frame_id].timestamps_.size() == k_ - 1) {
    LOG_DEBUG("# [RecordAccess]Frame %d's timestamps_nums is k-1", frame_id);
    id_to_frames_[frame_id].timestamps_.push(current_timestamp_);
    id_to_frames_[frame_id].k_value_ = id_to_frames_[frame_id].timestamps_.front();
  } else {
    LOG_DEBUG("# [RecordAccess]Frame %d's timestamps_nums is k", frame_id);
    id_to_frames_[frame_id].timestamps_.pop();
    id_to_frames_[frame_id].timestamps_.push(current_timestamp_);
    id_to_frames_[frame_id].k_value_ = id_to_frames_[frame_id].timestamps_.front();
  }
  LOG_INFO("# [RecordAccess]Frame %d got one new record at %ld", frame_id, current_timestamp_);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (id_to_frames_.find(frame_id) == id_to_frames_.end()) {
    LOG_INFO("# [SetEvictable]No frame %d", frame_id);
    return;
  }
  LOG_INFO("# [SetEvictable]Set frame %d as excepted", frame_id);
  id_to_frames_[frame_id].evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (id_to_frames_.find(frame_id) == id_to_frames_.end()) {
    LOG_INFO("# [Remove]Can't find the frame %d", frame_id);
    return;
  }
  if (!id_to_frames_[frame_id].evictable_) {
    throw "frame not evictable, can't remove!";
  }
  LOG_INFO("# [Remove]find the frame %d, now remove it!", frame_id);
  id_to_frames_.erase(frame_id);
  // curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t count = 0;
  for (auto const &frame : id_to_frames_) {
    if (frame.second.evictable_) {
      LOG_INFO("# [Size]Now we have one %d with k-value %d", frame.first, frame.second.k_value_);
      count++;
    }
  }
  LOG_INFO("# [Size]Now size is %ld", count);
  return count;
}

}  // namespace bustub
