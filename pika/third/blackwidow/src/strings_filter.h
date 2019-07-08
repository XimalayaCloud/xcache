//  Copyright (c) 2017-present The blackwidow Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_STRINGS_FILTER_H_
#define SRC_STRINGS_FILTER_H_

#include <string>
#include <memory>

#include "src/strings_value_format.h"
#include "rocksdb/compaction_filter.h"
#include "src/debug.h"

namespace blackwidow {

class StringsFilter : public rocksdb::CompactionFilter {
 public:
  StringsFilter() = default;
  bool Filter(int level, const rocksdb::Slice& key,
              const rocksdb::Slice& value,
              std::string* new_value, bool* value_changed) const override {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    int32_t cur_time = static_cast<int32_t>(unix_time);
    ParsedStringsValue parsed_strings_value(value);
    Trace("==========================START==========================");
    Trace("[StringsFilter], key: %s, value = %s, timestamp: %d, cur_time: %d",
          key.ToString().c_str(),
          parsed_strings_value.value().ToString().c_str(),
          parsed_strings_value.timestamp(),
          cur_time);

    if (parsed_strings_value.timestamp() != 0
      && parsed_strings_value.timestamp() < cur_time) {
      Trace("Drop[Stale]");
      return true;
    } else {
      Trace("Reserve");
      return false;
    }
  }

  const char* Name() const override { return "StringsFilter"; }
};

class StringsFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  StringsFilterFactory() = default;
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
    const rocksdb::CompactionFilter::Context& context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(new StringsFilter());
  }
  const char* Name() const override {
    return "StringsFilterFactory";
  }
};

class TitanStringFilter : public rocksdb::CompactionFilter {
public:
  TitanStringFilter(rocksdb::titandb::TitanDB* db) : db_(db) {}

  bool Filter(int level, const Slice& key,
              const rocksdb::Slice& value,
              std::string* new_value, bool* value_changed) const override {
    std::string val;
    Status s = db_->Get(default_read_options_, key, &val);
    if (s.ok()) {
      ParsedStringsValue parsed_strings_value(&val);
      if (parsed_strings_value.IsStale()) {
        return true;
      } else {
        return false;
      }
    }
    return true;
  }

  Decision FilterV2(int level, const Slice& key, ValueType value_type,
                    const Slice& existing_value, std::string* new_value,
                    std::string* /*skip_until*/) const {
    switch (value_type) {
      case ValueType::kValue:
      case ValueType::kBlobIndex: {
        bool value_changed = false;
        bool rv = Filter(level, key, existing_value, new_value, &value_changed);
        if (rv) {
          return Decision::kRemove;
        }
        return value_changed ? Decision::kChangeValue : Decision::kKeep;
      }
      case ValueType::kMergeOperand: {
        bool rv = FilterMergeOperand(level, key, existing_value);
        return rv ? Decision::kRemove : Decision::kKeep;
      }
    }
    assert(false);
    return Decision::kKeep;
  }

  const char* Name() const override { return "TitanStringFilter"; }

private:
  rocksdb::titandb::TitanDB *db_;
  rocksdb::ReadOptions default_read_options_;
};

class TitanStringFilterFactory : public rocksdb::CompactionFilterFactory {
public:
  TitanStringFilterFactory(rocksdb::titandb::TitanDB** db_ptr) : db_ptr_(db_ptr) {}
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
    const rocksdb::CompactionFilter::Context& context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(
           new TitanStringFilter(*db_ptr_));
  }
  const char* Name() const override {
    return "TitanStringFilterFactory";
  }
private:
  rocksdb::titandb::TitanDB** db_ptr_;
};

}  //  namespace blackwidow
#endif  // SRC_STRINGS_FILTER_H_
