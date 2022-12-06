//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_H_
#define SRC_REDIS_H_

#include <string>
#include <memory>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/slice.h"

#include "src/lock_mgr.h"
#include "src/mutex_impl.h"
#include "blackwidow/blackwidow.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/titandb/db.h"

namespace blackwidow {
using Status = rocksdb::Status;
using Slice = rocksdb::Slice;


enum StatType : int {
    kProperityType = 0,
    kProperityMapType,
    kTickerType,
};
struct StatItem {
    std::string key;
    StatType type;
};

static const std::string rocksdb_prefix = "rocksdb.";
const std::vector<StatItem> interval_stats_vec = {
    {"cfstats", kProperityMapType},
    {"STALL_MICROS", kTickerType},
    {"RATE_LIMIT_DELAY_MILLIS", kTickerType},
    {"MEMTABLE_HIT", kTickerType},
    {"MEMTABLE_MISS", kTickerType},
    {"GET_HIT_L0", kTickerType},
    {"GET_HIT_L1", kTickerType},
    {"GET_HIT_L2_AND_UP", kTickerType},
};
const std::map<std::string, rocksdb::Tickers> tick_map = {
    {"STALL_MICROS", rocksdb::STALL_MICROS},
    {"RATE_LIMIT_DELAY_MILLIS", rocksdb::RATE_LIMIT_DELAY_MILLIS},
    {"MEMTABLE_HIT", rocksdb::MEMTABLE_HIT},
    {"MEMTABLE_MISS", rocksdb::MEMTABLE_MISS},
    {"GET_HIT_L0", rocksdb::GET_HIT_L0},
    {"GET_HIT_L1", rocksdb::GET_HIT_L1},
    {"GET_HIT_L2_AND_UP", rocksdb::GET_HIT_L2_AND_UP},
};

class Redis {
 public:
  Redis();
  virtual ~Redis();

  rocksdb::DB* GetDB() {
    return db_;
  }

  // Common Commands
  void EnableDBStats(BlackwidowOptions& bw_options);
  virtual Status Open(BlackwidowOptions bw_options,
                      const std::string& db_path) = 0;
  virtual Status CompactRange(const rocksdb::Slice* begin,
                              const rocksdb::Slice* end) = 0;
  virtual Status GetProperty(const std::string& property, uint64_t* out) = 0;
  virtual Status ScanKeyNum(uint64_t* num) = 0;
  virtual Status ScanKeys(const std::string& pattern,
                          std::vector<std::string>* keys) = 0;

  // Keys Commands
  virtual Status Expire(const Slice& key, int32_t ttl) = 0;
  virtual Status Del(const Slice& key) = 0;
  virtual bool Scan(const std::string& start_key,
                    const std::string& pattern,
                    std::vector<std::string>* keys,
                    int64_t* count,
                    std::string* next_key) = 0;
  virtual Status Expireat(const Slice& key,
                          int32_t timestamp) = 0;
  virtual Status Persist(const Slice& key) = 0;
  virtual Status TTL(const Slice& key, int64_t* timestamp) = 0;
  virtual void GetColumnFamilyHandles(std::vector<rocksdb::ColumnFamilyHandle*>& handles) = 0;

  void SetDisableWAL(const bool disable_wal) {
    default_write_options_.disableWAL = disable_wal;
  }
  void GetIntervalStats(std::map<std::string, uint64_t>& stats_val);
  void GetProperty(const std::string& property, std::string& value);

 private:
  void GetIntervalStats(std::vector<rocksdb::ColumnFamilyHandle*>& cf_handles,
          std::map<std::string, uint64_t>& stats_val);
  void GetProperty(std::vector<rocksdb::ColumnFamilyHandle*>& cf_handles,
          const std::string& property, std::string& value);

 protected:
  LockMgr* lock_mgr_;
  rocksdb::DB* db_;
  rocksdb::WriteOptions default_write_options_;
  rocksdb::ReadOptions default_read_options_;
  rocksdb::CompactRangeOptions default_compact_range_options_;
  std::shared_ptr<rocksdb::Statistics> db_stats_;

  // For Scan
  slash::Mutex scan_cursors_mutex_;
  BlackWidow::LRU<std::string, std::string> scan_cursors_store_;

  Status GetScanStartPoint(const Slice& key, const Slice& pattern,
                           int64_t cursor, std::string* start_point);
  Status StoreScanNextPoint(const Slice& key, const Slice& pattern,
                            int64_t cursor, const std::string& next_point);
};

}  //  namespace blackwidow
#endif  //  SRC_REDIS_H_
