//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis.h"

namespace blackwidow {

Redis::Redis()
    : lock_mgr_(new LockMgr(1000, 0, std::make_shared<MutexFactoryImpl>())),
      db_(nullptr),
      db_stats_(nullptr) {
    scan_cursors_store_.max_size_ = 5000;
    default_compact_range_options_.exclusive_manual_compaction = false;
    default_compact_range_options_.change_level = true;
}

Redis::~Redis() {
  delete db_;
  delete lock_mgr_;
}

void Redis::EnableDBStats(BlackwidowOptions& bw_options) {
    db_stats_ = rocksdb::CreateDBStatistics();
    bw_options.options.statistics = db_stats_;
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

void Redis::GetIntervalStats(std::map<std::string, uint64_t>& stats_val) {
    std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
    GetColumnFamilyHandles(cf_handles);
    GetIntervalStats(cf_handles, stats_val);
}

static uint64_t GetValueFromPropMap(const std::map<std::string, std::string> &props, const std::string &key) {
    std::map<std::string, std::string>::const_iterator iter = props.find(key);
    if (iter != props.end()) {
        return std::stoull(iter->second);
    } else {
        return 0;
    }
}


void Redis::GetIntervalStats(std::vector<rocksdb::ColumnFamilyHandle*>& cf_handles,
        std::map<std::string, uint64_t>& stats_val) {
    stats_val.clear();
    
    if (!db_ || !db_stats_ || cf_handles.empty()) {
        return;
    }
    
    auto UpdateStatsMap = [&stats_val] (const std::string& key, uint64_t val) {
        if (stats_val.find(key) == stats_val.end()) {
            stats_val.insert(std::make_pair(key, val));
        } else {
            stats_val[key] += val;
        }
    };
    for (auto& item : interval_stats_vec) {
        std::string str_val;
        uint64_t int_val = 0; 
        if (item.type == kProperityType) {
            for (auto cfp : cf_handles) {
                if (db_->GetProperty(cfp, rocksdb_prefix + item.key, &str_val)) {
                    int_val = std::strtoull(str_val.c_str(), NULL, 10);
                    UpdateStatsMap(item.key, int_val);
                }
            }
        } else if (item.type == kTickerType) {
            int_val = db_stats_->getTickerCount(tick_map.at(item.key));
            UpdateStatsMap(item.key, int_val);
        } else if (item.type == kProperityMapType) {
            for (auto cfp : cf_handles) {
                std::map<std::string, std::string> props;
                if (db_->GetMapProperty(cfp, rocksdb_prefix + item.key, &props)) {
                    const std::vector<std::string> keys = { "io_stalls.total_stop", "io_stalls.total_slowdown" };
                    for (auto & key : keys) {
                        auto v = GetValueFromPropMap(props, key);
                        UpdateStatsMap(key, v);
                    }
                }
            }
        }
    }
}

void Redis::GetProperty(const std::string& property, std::string& value) {
    std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
    GetColumnFamilyHandles(cf_handles);
    GetProperty(cf_handles, property, value);
}

void Redis::GetProperty(std::vector<rocksdb::ColumnFamilyHandle*>& cf_handles,
      const std::string& property, std::string& value) {
    value.clear();
    
    if (!db_ || !db_stats_ || cf_handles.empty()) {
        return;
    }

    for (auto cfp : cf_handles) {
        std::string str_val;
        if (db_->GetProperty(cfp, property, &str_val)) {
            value += str_val;
        }
    }
}

}  // namespace blackwidow
