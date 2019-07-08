// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef  PIKA_BINLOG_WRITER_THREAD_H_
#define  PIKA_BINLOG_WRITER_THREAD_H_
#include <map>
#include <set>
#include <atomic>
#include <list>
#include <deque>
#include <queue>

#include "pink/include/pink_thread.h"
#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"
#include "pika_client_conn.h"
#include "pika_define.h"

using slash::Status;

class PikaBinlogWriterThread : public pink::Thread {
 public:
  PikaBinlogWriterThread (int max_deque_size);
  virtual ~PikaBinlogWriterThread ();

  Status WriteBinlog (const std::string &raw_args, bool is_sync);
  bool BinlogIoError();
  bool IsBinlogWriterIdle();
  void SetMaxCmdsQueueSize(size_t max_size);
  void SetBinlogIoError(bool error);

 private:
  bool IsCmdsDequeEmpty();
  //bool IsCmdsDequeFull();

  slash::Mutex binlog_mutex_protector_;
  slash::CondVar binlog_read_cond_;         // 可读条件变量
  slash::CondVar binlog_write_cond_;        // 可写条件变量
  std::atomic<bool> binlog_io_error_;       // 标记写binlog是否出现异常
  std::atomic<bool> is_binlog_writer_idle_; // 标记线程是否处于空闲状态
  size_t max_cmds_deque_size_ ;             // 缓冲区最大值
  std::deque<std::string> cmds_deque_;      // 命令缓冲队列

  virtual void* ThreadMain();
};
#endif
