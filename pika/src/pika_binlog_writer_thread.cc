// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <string>
#include <utility>
#include <sys/time.h>

#include "pink/include/pink_define.h"
#include "pika_binlog_writer_thread.h"
#include "pika_server.h"
#include "pika_conf.h"

extern PikaServer* g_pika_server;

PikaBinlogWriterThread::PikaBinlogWriterThread(int max_deque_size = 1000)
  : pink::Thread(),
    binlog_read_cond_(&binlog_mutex_protector_),
    binlog_write_cond_(&binlog_mutex_protector_),
    binlog_io_error_(false),
    is_binlog_writer_idle_(false),
    max_cmds_deque_size_(max_deque_size) {
  set_thread_name("BinlogWriterThread");
  LOG(INFO) << " PikaBinlogWriterThread start!! max_cmds_deque_size is " << max_deque_size ;
}

PikaBinlogWriterThread::~PikaBinlogWriterThread() {
  set_should_stop();
  if (is_running()) {
    binlog_read_cond_.Signal();
    StopThread();
  }

  LOG(INFO) << " PikaBinlogWriterThread " << pthread_self() << " exit!!!";
}

Status PikaBinlogWriterThread::WriteBinlog(const std::string &raw_args, bool is_sync) {
  if (!BinlogIoError()) {
    slash::Status s;

    if (is_sync) {
      g_pika_server->logger_->Lock();
      s = g_pika_server->logger_->Put(raw_args);
      g_pika_server->logger_->Unlock();
      if (!s.ok()) {
        LOG(WARNING) << "Write binlog IOError: " << raw_args;
        SetBinlogIoError(true);
      }
    } else {
      slash::MutexLock lm(&binlog_mutex_protector_);
      while (cmds_deque_.size() >= max_cmds_deque_size_) {
      	binlog_write_cond_.Wait();
      }

      cmds_deque_.push_back(raw_args);
      binlog_read_cond_.Signal();
      
      s = Status::OK();
    }

    return s;
  }
  return Status::IOError("BinlogIoError");
}

bool PikaBinlogWriterThread::BinlogIoError() {
  return binlog_io_error_;
}

void PikaBinlogWriterThread::SetBinlogIoError(bool error) {
  binlog_io_error_ = error;
}

bool PikaBinlogWriterThread::IsBinlogWriterIdle() {
  slash::MutexLock lm(&binlog_mutex_protector_);
  return is_binlog_writer_idle_;
}

void PikaBinlogWriterThread::SetMaxCmdsQueueSize(size_t max_size) {
  slash::MutexLock lm(&binlog_mutex_protector_);
  max_cmds_deque_size_ = max_size;
  LOG(INFO) << " PikaBinlogWriterThread: max_cmds_deque_size is set to " << max_size;
}

bool PikaBinlogWriterThread::IsCmdsDequeEmpty() {
  slash::MutexLock lm(&binlog_mutex_protector_);
  return cmds_deque_.empty();
}

/*bool PikaBinlogWriterThread::IsCmdsDequeFull() {
	slash::MutexLock lm(&binlog_mutex_protector_);
	return cmds_deque_.size() >= max_cmds_deque_size_;
}*/

void* PikaBinlogWriterThread::ThreadMain() {
  std::string cmd;
  while (!(should_stop() && IsCmdsDequeEmpty())) {
    {
      slash::MutexLock lm(&binlog_mutex_protector_);
      while (cmds_deque_.empty() && !should_stop()) {
        is_binlog_writer_idle_ = true;
        binlog_read_cond_.Wait();
      }
      is_binlog_writer_idle_ = false;
    }

    while (!IsCmdsDequeEmpty()) {
      cmd = "";
      {
        slash::MutexLock lm(&binlog_mutex_protector_);
        cmd = cmds_deque_.front();
        cmds_deque_.pop_front();
        // 如果所有的工作线程因缓冲区写满而阻塞，即使写binlog线程执行SignalALL,也只有一个工作线程可以正常操作缓冲区
        binlog_write_cond_.Signal();
      }

      if ("" == cmd) {
        continue;
      }

      if (BinlogIoError()) {
        LOG(WARNING) << "Write binlog IOError: " << cmd;
      } else {
        g_pika_server->logger_->Lock();
        slash::Status s = g_pika_server->logger_->Put(cmd);
        g_pika_server->logger_->Unlock();
        if (!s.ok()) {
          LOG(WARNING) << "Write binlog IOError: " << cmd;
          SetBinlogIoError(true);
        }
      }
    }
  }
  return NULL;
}
