//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis.h"

namespace blackwidow {

Redis::Redis()
    : lock_mgr_(new LockMgr(1000, 0, std::make_shared<MutexFactoryImpl>())),
      db_(nullptr) {
    scan_cursors_store_.max_size_ = 5000;
    default_compact_range_options_.exclusive_manual_compaction = false;
    default_compact_range_options_.change_level = true;
}

Redis::~Redis() {
  delete db_;
  delete lock_mgr_;
}

Status Redis::GetScanStartPoint(const Slice& key,
                                const Slice& pattern,
                                int64_t cursor,
                                std::string* start_point) {
  slash::MutexLock l(&scan_cursors_mutex_);
  std::string index_key =
    key.ToString() + "_" + pattern.ToString() + "_" + std::to_string(cursor);
  if (scan_cursors_store_.map_.find(index_key)
    == scan_cursors_store_.map_.end()) {
    return Status::NotFound();
  } else {
    *start_point = scan_cursors_store_.map_[index_key];
  }
  return Status::OK();
}

Status Redis::StoreScanNextPoint(const Slice& key,
                                 const Slice& pattern,
                                 int64_t cursor,
                                 const std::string& next_point) {
  slash::MutexLock l(&scan_cursors_mutex_);
  std::string index_key =
    key.ToString() + "_" + pattern.ToString() +  "_" + std::to_string(cursor);
  if (scan_cursors_store_.list_.size() > scan_cursors_store_.max_size_) {
    std::string tail = scan_cursors_store_.list_.back();
    scan_cursors_store_.map_.erase(tail);
    scan_cursors_store_.list_.pop_back();
  }

  scan_cursors_store_.map_[index_key] = next_point;
  scan_cursors_store_.list_.remove(index_key);
  scan_cursors_store_.list_.push_front(index_key);
  return Status::OK();
}

}  // namespace blackwidow
