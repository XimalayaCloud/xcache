#include "utilities/titandb/blob_gc_picker.h"

namespace rocksdb {
namespace titandb {


BasicBlobGCPicker::BasicBlobGCPicker(TitanDBOptions db_options,
                                     TitanCFOptions cf_options)
    : db_options_(db_options), cf_options_(cf_options) {}

BasicBlobGCPicker::~BasicBlobGCPicker() {}

std::unique_ptr<BlobGC> BasicBlobGCPicker::PickBlobGC(
    BlobStorage* blob_storage) {
  Status s;
  std::vector<BlobFileMeta*> blob_files;

  uint64_t batch_size = 0;
//  ROCKS_LOG_INFO(db_options_.info_log, "blob file num:%lu gc score:%lu",
//                 blob_storage->NumBlobFiles(), blob_storage->gc_score().size());

  int64_t unix_time;
  Env::Default()->GetCurrentTime(&unix_time);

  for (auto& gc_score : blob_storage->gc_score()) {
    auto blob_file = blob_storage->FindFile(gc_score.file_number).lock();
    assert(blob_file);

    //    ROCKS_LOG_INFO(db_options_.info_log,
    //                   "file number:%lu score:%f being_gc:%d pending:%d, "
    //                   "size:%lu discard:%lu mark_for_gc:%d
    //                   mark_for_sample:%d", blob_file->file_number_,
    //                   gc_score.score, blob_file->being_gc,
    //                   blob_file->pending, blob_file->file_size_,
    //                   blob_file->discardable_size_,
    //                   blob_file->marked_for_gc_,
    //                   blob_file->marked_for_sample);

    if (!CheckBlobFile(blob_file.get())) {
      ROCKS_LOG_INFO(db_options_.info_log, "file number:%lu no need gc",
                     blob_file->file_number());
      continue;
    }

    // if the file has sampled last time, but not gc, we will skip the file
    if (blob_file->GetDiscardableRatio() < cf_options_.blob_file_discardable_ratio) {
      if (0 != blob_file->last_sample_time() 
          && unix_time - blob_file->last_sample_time() < cf_options_.gc_sample_cycle) {
          ROCKS_LOG_DEBUG(db_options_.info_log, "Titan GC skip the file[%lu], cycle:%lld, discardable_ratio:%f, gc_batch_size:%llu",
                          gc_score.file_number,
                          cf_options_.gc_sample_cycle.load(),
                          cf_options_.blob_file_discardable_ratio.load(),
                          cf_options_.max_gc_batch_size.load());
          continue;
      }
    }

    blob_files.push_back(blob_file.get());

    batch_size += blob_file->file_size();
    if (batch_size >= cf_options_.max_gc_batch_size) break;
  }

  if (blob_files.empty() || batch_size < cf_options_.min_gc_batch_size) {
    ROCKS_LOG_INFO(db_options_.info_log, "Titan GC check to the end of all blob files, pick file size:%d, batch_size:%llu, blob file num:%lu, gc score:%lu",
                   blob_files.size(),
                   batch_size,
                   blob_storage->NumBlobFiles(),
                   blob_storage->gc_score().size());

    // reset file last_sample_time if we have checked out to the end of all blob files
    ResetAllBlobFileSampleTime(blob_storage);
    return nullptr;
  }

  return std::unique_ptr<BlobGC>(
      new BlobGC(std::move(blob_files), std::move(cf_options_)));
}

bool BasicBlobGCPicker::CheckBlobFile(BlobFileMeta* blob_file) const {
  assert(blob_file->file_state() != BlobFileMeta::FileState::kInit);
  if (blob_file->file_state() != BlobFileMeta::FileState::kNormal) return false;

  return true;
}

void BasicBlobGCPicker::ResetAllBlobFileSampleTime(BlobStorage* blob_storage) {
  for (auto& gc_score : blob_storage->gc_score()) {
    auto blob_file = blob_storage->FindFile(gc_score.file_number).lock();
    assert(blob_file);
    if (!CheckBlobFile(blob_file.get())) {
      continue;
    }
    blob_file->set_last_sample_time(0);
  }
}

}  // namespace titandb
}  // namespace rocksdb
