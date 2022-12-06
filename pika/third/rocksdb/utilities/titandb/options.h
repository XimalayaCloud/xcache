#pragma once
#include <atomic>

#include "rocksdb/options.h"

namespace rocksdb {
namespace titandb {

struct TitanDBOptions : public DBOptions {
  // The directory to store data specific to TitanDB alongside with
  // the base DB.
  //
  // Default: {dbname}/titandb
  std::string dirname;

  // Disable background GC
  //
  // Default: false
  bool disable_background_gc{false};

  // Max background GC thread
  //
  // Default: 1
  int32_t max_background_gc{1};

  TitanDBOptions() = default;
  explicit TitanDBOptions(const DBOptions& options) : DBOptions(options) {}

  TitanDBOptions& operator=(const DBOptions& options) {
    *dynamic_cast<DBOptions*>(this) = options;
    return *this;
  }
};

struct TitanCFOptions : public ColumnFamilyOptions {
  // The smallest value to store in blob files. Value smaller than
  // this threshold will be inlined in base DB.
  //
  // Default: 4096
  uint64_t min_blob_size{512};

  // The compression algorithm used to compress data in blob files.
  //
  // Default: kNoCompression
  CompressionType blob_file_compression{kSnappyCompression};

  // The desirable blob file size. This is not a hard limit but a wish.
  //
  // Default: 256MB
  uint64_t blob_file_target_size{256 << 20};

  // If non-NULL use the specified cache for blob records.
  //
  // Default: nullptr
  std::shared_ptr<Cache> blob_cache;

  // Max batch size for gc
  //
  // Default: 1GB
  std::atomic<uint64_t> max_gc_batch_size{1 << 30};

  // Min batch size for gc
  //
  // Default: 512MB
  std::atomic<uint64_t> min_gc_batch_size{512 << 20};

  // The ratio of how much discardable size of a blob file can be GC
  //
  // Default: 0.5
  std::atomic<float> blob_file_discardable_ratio{0.5};

  // The ratio of how much size of a blob file need to be sample before GC
  //
  // Default: 0.1
  float sample_file_size_ratio{0.1};

  // The blob file size less than this option will be mark gc
  //
  // Default: 8MB
  uint64_t merge_small_file_threshold{8 << 20};

  // If now - blob last sample time < gc_sample_cycle, we will sikp the file when gc pick file
  //
  // Default: 604800(7 day)
  std::atomic<int64_t> gc_sample_cycle{604800};

  // If gc jobs more then max_gc_queue_size, we will clear gc queue.
  //
  // Default: 64
  std::atomic<uint32_t> max_gc_queue_size{64};
  
  // If small file count more than max_gc_file_count, we will gc 
  //
  // Default: 1000
  std::atomic<uint32_t> max_gc_file_count{1000};



  TitanCFOptions(const TitanCFOptions &obj) : ColumnFamilyOptions(obj) {
    min_blob_size = obj.min_blob_size;
    blob_file_compression = obj.blob_file_compression;
    blob_file_target_size = obj.blob_file_target_size;
    blob_cache = obj.blob_cache;
    max_gc_batch_size = obj.max_gc_batch_size.load();
    min_gc_batch_size = obj.min_gc_batch_size.load();
    blob_file_discardable_ratio = obj.blob_file_discardable_ratio.load();
    sample_file_size_ratio = obj.sample_file_size_ratio;
    merge_small_file_threshold = obj.merge_small_file_threshold;
    gc_sample_cycle = obj.gc_sample_cycle.load();
    max_gc_queue_size = obj.max_gc_queue_size.load();
    max_gc_file_count = obj.max_gc_file_count.load();
  }

  TitanCFOptions& operator=(const TitanCFOptions &obj) {
    if (this == &obj) return *this;
    dynamic_cast<ColumnFamilyOptions&>(*this) = obj;
    min_blob_size = obj.min_blob_size;
    blob_file_compression = obj.blob_file_compression;
    blob_file_target_size = obj.blob_file_target_size;
    blob_cache = obj.blob_cache;
    max_gc_batch_size = obj.max_gc_batch_size.load();
    min_gc_batch_size = obj.min_gc_batch_size.load();
    blob_file_discardable_ratio = obj.blob_file_discardable_ratio.load();
    sample_file_size_ratio = obj.sample_file_size_ratio;
    merge_small_file_threshold = obj.merge_small_file_threshold;
    gc_sample_cycle = obj.gc_sample_cycle.load();
    max_gc_queue_size = obj.max_gc_queue_size.load();
    max_gc_file_count = obj.max_gc_file_count.load();
    return *this;
  }


  TitanCFOptions() = default;
  explicit TitanCFOptions(const ColumnFamilyOptions& options)
      : ColumnFamilyOptions(options) {}

  TitanCFOptions& operator=(const ColumnFamilyOptions& options) {
    *dynamic_cast<ColumnFamilyOptions*>(this) = options;
    return *this;
  }

  std::string ToString() const;
};

struct TitanOptions : public TitanDBOptions, public TitanCFOptions {
  TitanOptions() = default;
  explicit TitanOptions(const Options& options)
      : TitanDBOptions(options), TitanCFOptions(options) {}

  TitanOptions& operator=(const Options& options) {
    *dynamic_cast<TitanDBOptions*>(this) = options;
    *dynamic_cast<TitanCFOptions*>(this) = options;
    return *this;
  }

  operator Options() {
    Options options;
    *dynamic_cast<DBOptions*>(&options) = *dynamic_cast<DBOptions*>(this);
    *dynamic_cast<ColumnFamilyOptions*>(&options) =
        *dynamic_cast<ColumnFamilyOptions*>(this);
    return options;
  }
};

}  // namespace titandb
}  // namespace rocksdb
