//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2012 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef ROCKSDB_LITE

#include "blackwidow/db_checkpoint.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include "rocksdb/db.h"
#include "util/file_util.h"

#if (ROCKSDB_MAJOR < 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR < 3))
#include "db/filename.h"
#else
#include "util/filename.h"
#endif

namespace rocksdb {

class DBCheckpointImpl : public DBCheckpoint {
 public:
  // Creates a DBCheckPoint object to be used for creating openable snapshots
  explicit DBCheckpointImpl(DB* db) : db_(db) {}

  // Builds an openable snapshot of RocksDB on the same disk, which
  // accepts an output directory on the same disk, and under the directory
  // (1) hard-linked SST files pointing to existing live SST files
  // SST files will be copied if output directory is on a different filesystem
  // (2) a copied manifest files and other files
  // The directory should not already exist and will be created by this API.
  // The directory will be an absolute path
  using DBCheckpoint::CreateCheckpoint;
  Status CreateCheckpoint(const std::string& checkpoint_dir) override;

  using DBCheckpoint::GetCheckpointFiles;
  Status GetCheckpointFiles(std::vector<std::string> &live_files,
                            VectorLogPtr &live_wal_files,
                            uint64_t &manifest_file_size,
                            uint64_t &sequence_number) override;

  using DBCheckpoint::CreateCheckpointWithFiles;
  Status CreateCheckpointWithFiles(const std::string& checkpoint_dir,
                                   std::vector<std::string> &live_files,
                                   VectorLogPtr &live_wal_files,
                                   uint64_t manifest_file_size,
                                   uint64_t sequence_number) override;

 private:
  Status BackupTitanFile(const std::string& titan_file,
                         const std::string& target_path,
                         bool& same_fs);

  DB* db_;
};

Status DBCheckpoint::Create(DB* db, DBCheckpoint** checkpoint_ptr) {
  *checkpoint_ptr = new DBCheckpointImpl(db);
  return Status::OK();
}

Status DBCheckpoint::CreateCheckpoint(const std::string& checkpoint_dir) {
  return Status::NotSupported("");
}

// Builds an openable snapshot of RocksDB
Status DBCheckpointImpl::CreateCheckpoint(const std::string& checkpoint_dir) {
  std::vector<std::string> live_files;
  VectorLogPtr live_wal_files;
  uint64_t manifest_file_size, sequence_number;
  Status s = GetCheckpointFiles(live_files,
      live_wal_files, manifest_file_size, sequence_number);
  if (s.ok()) {
    s = CreateCheckpointWithFiles(checkpoint_dir,
        live_files, live_wal_files, manifest_file_size, sequence_number);
  }
  return s;
}

Status DBCheckpointImpl::GetCheckpointFiles(
    std::vector<std::string> &live_files,
    VectorLogPtr &live_wal_files,
    uint64_t &manifest_file_size,
    uint64_t &sequence_number) {

  Status s;
  sequence_number = db_->GetLatestSequenceNumber();

  s = db_->DisableFileDeletions();
  if (s.ok()) {
    // this will return live_files prefixed with "/"
    s = db_->GetLiveFiles(live_files, &manifest_file_size);
  }

  // if we have more than one column family, we need to also get WAL files
  if (s.ok()) {
    s = db_->GetSortedWalFiles(live_wal_files);
  }

  if (!s.ok()) {
    db_->EnableFileDeletions(false);
  }

  return s;
}

Status DBCheckpointImpl::CreateCheckpointWithFiles(
    const std::string& checkpoint_dir,
    std::vector<std::string> &live_files,
    VectorLogPtr &live_wal_files,
    uint64_t manifest_file_size,
    uint64_t sequence_number) {
  bool same_fs = true;

  Status s = db_->GetEnv()->FileExists(checkpoint_dir);
  if (s.ok()) {
    return Status::InvalidArgument("Directory exists");
  } else if (!s.IsNotFound()) {
    assert(s.IsIOError());
    return s;
  }

  size_t wal_size = live_wal_files.size();
  Log(db_->GetOptions().info_log,
      "Started the snapshot process -- creating snapshot in directory %s",
      checkpoint_dir.c_str());

  std::string full_private_path = checkpoint_dir + ".tmp";

  // create snapshot directory
  s = db_->GetEnv()->CreateDir(full_private_path);

  // copy/hard link live_files
  std::string manifest_fname, current_fname;
  for (size_t i = 0; s.ok() && i < live_files.size(); ++i) {
    uint64_t number;
    FileType type;

    // backup titan file
    if (IsTitanFile(live_files[i])) {
      s = BackupTitanFile(live_files[i], full_private_path, same_fs);
      if (!s.ok()) {
        return s;
      }
      continue;
    }
    
    bool ok = ParseFileName(live_files[i], &number, &type);
    if (!ok) {
      s = Status::Corruption("Can't parse file name. This is very bad");
      break;
    }
    // we should only get sst, options, manifest and current files here
    assert(type == kTableFile || type == kDescriptorFile ||
           type == kCurrentFile || type == kOptionsFile);
    assert(live_files[i].size() > 0 && live_files[i][0] == '/');
    if (type == kCurrentFile) {
      // We will craft the current file manually to ensure it's consistent with
      // the manifest number. This is necessary because current's file contents
      // can change during checkpoint creation.
      current_fname = live_files[i];
      continue;
    } else if (type == kDescriptorFile) {
      manifest_fname = live_files[i];
    }
    std::string src_fname = live_files[i];

    // rules:
    // * if it's kTableFile, then it's shared
    // * if it's kDescriptorFile, limit the size to manifest_file_size
    // * always copy if cross-device link
    if ((type == kTableFile) && same_fs) {
      Log(db_->GetOptions().info_log, "Hard Linking %s", src_fname.c_str());
      s = db_->GetEnv()->LinkFile(db_->GetName() + src_fname,
                                  full_private_path + src_fname);
      if (s.IsNotSupported()) {
        same_fs = false;
        s = Status::OK();
      }
    }
    if ((type != kTableFile) || (!same_fs)) {
      Log(db_->GetOptions().info_log, "Copying %s", src_fname.c_str());
#if (ROCKSDB_MAJOR < 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR < 3))
      s = CopyFile(db_->GetEnv(), db_->GetName() + src_fname,
                   full_private_path + src_fname,
                   (type == kDescriptorFile) ? manifest_file_size : 0);
#else
      s = CopyFile(db_->GetEnv(), db_->GetName() + src_fname,
                   full_private_path + src_fname,
                   (type == kDescriptorFile) ? manifest_file_size : 0, false);
#endif
    }
  }
  if (s.ok() && !current_fname.empty() && !manifest_fname.empty()) {
    s = CreateFile(db_->GetEnv(), full_private_path + current_fname,
                   manifest_fname.substr(1) + "\n");
  }
  // Log(db_->GetOptions().info_log,
  //    "Number of log files %" ROCKSDB_PRIszt, live_wal_files.size());

  // Link WAL files. Copy exact size of last one because it is the only one
  // that has changes after the last flush.
  for (size_t i = 0; s.ok() && i < wal_size; ++i) {
    if ((live_wal_files[i]->Type() == kAliveLogFile) &&
        (live_wal_files[i]->StartSequence() >= sequence_number)) {
      if (i + 1 == wal_size) {
        Log(db_->GetOptions().info_log, "Copying %s",
            live_wal_files[i]->PathName().c_str());
#if (ROCKSDB_MAJOR < 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR < 3))
        s = CopyFile(db_->GetEnv(),
                     db_->GetOptions().wal_dir + live_wal_files[i]->PathName(),
                     full_private_path + live_wal_files[i]->PathName(),
                     live_wal_files[i]->SizeFileBytes());
#else
        s = CopyFile(db_->GetEnv(),
                     db_->GetOptions().wal_dir + live_wal_files[i]->PathName(),
                     full_private_path + live_wal_files[i]->PathName(),
                     live_wal_files[i]->SizeFileBytes(), false);
#endif
        break;
      }
      if (same_fs) {
        // we only care about live log files
        Log(db_->GetOptions().info_log, "Hard Linking %s",
            live_wal_files[i]->PathName().c_str());
        s = db_->GetEnv()->LinkFile(
            db_->GetOptions().wal_dir + live_wal_files[i]->PathName(),
            full_private_path + live_wal_files[i]->PathName());
        if (s.IsNotSupported()) {
          same_fs = false;
          s = Status::OK();
        }
      }
      if (!same_fs) {
        Log(db_->GetOptions().info_log, "Copying %s",
            live_wal_files[i]->PathName().c_str());
#if (ROCKSDB_MAJOR < 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR < 3))
        s = CopyFile(db_->GetEnv(),
                     db_->GetOptions().wal_dir + live_wal_files[i]->PathName(),
                     full_private_path + live_wal_files[i]->PathName(), 0);
#else
        s = CopyFile(db_->GetEnv(),
                     db_->GetOptions().wal_dir + live_wal_files[i]->PathName(),
                     full_private_path + live_wal_files[i]->PathName(),
                     0, false);
#endif
      }
    }
  }

  // we copied all the files, enable file deletions
  db_->EnableFileDeletions(false);

  if (s.ok()) {
    // move tmp private backup to real snapshot directory
    s = db_->GetEnv()->RenameFile(full_private_path, checkpoint_dir);
  }
  if (s.ok()) {
    unique_ptr<Directory> checkpoint_directory;
    db_->GetEnv()->NewDirectory(checkpoint_dir, &checkpoint_directory);
    if (checkpoint_directory != nullptr) {
      s = checkpoint_directory->Fsync();
    }
  }

  if (!s.ok()) {
    // clean all the files we might have created
    Log(db_->GetOptions().info_log, "Snapshot failed -- %s",
        s.ToString().c_str());
    // we have to delete the dir and all its children
    std::vector<std::string> subchildren;
    db_->GetEnv()->GetChildren(full_private_path, &subchildren);
    for (auto& subchild : subchildren) {
      std::string subchild_path = full_private_path + "/" + subchild;
      Status s1 = db_->GetEnv()->DeleteFile(subchild_path);
      Log(db_->GetOptions().info_log, "Delete file %s -- %s",
          subchild_path.c_str(), s1.ToString().c_str());
    }
    // finally delete the private dir
    Status s1 = db_->GetEnv()->DeleteDir(full_private_path);
    Log(db_->GetOptions().info_log, "Delete dir %s -- %s",
        full_private_path.c_str(), s1.ToString().c_str());
    return s;
  }

  // here we know that we succeeded and installed the new snapshot
  Log(db_->GetOptions().info_log, "Snapshot DONE. All is good");
  Log(db_->GetOptions().info_log, "Snapshot sequence number: %" PRIu64,
      sequence_number);

  return s;
}

Status DBCheckpointImpl::BackupTitanFile(const std::string& titan_file,
                                         const std::string& target_path,
                                         bool& same_fs) {

  std::string titan_path = target_path + "/titandb";
  Status s = db_->GetEnv()->FileExists(titan_path);
  if (s.IsNotFound()) {
    db_->GetEnv()->CreateDir(titan_path);
  }

  std::string file_name = titan_file.substr(titan_file.find_last_of('/'));
  uint64_t number;
  FileType type;
  bool ok = ParseFileName(file_name, &number, &type);
  if (!ok) {
    return Status::Corruption("DBCheckpointImpl::BackupTitanFile Can't parse file name. This is very bad");
  }
  assert(type == kBlobFile || type == kDescriptorFile || type == kCurrentFile);

  if ((type == kBlobFile) && same_fs) {
    s = db_->GetEnv()->LinkFile(db_->GetName() + titan_file, target_path + titan_file);
    if (s.IsNotSupported()) {
      same_fs = false;
      s = Status::OK();
    }
  }

  if ((type != kBlobFile) || (!same_fs)) {
      s = CopyFile(db_->GetEnv(), db_->GetName() + titan_file, target_path + titan_file, 0, false);
  }

  return s;
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
