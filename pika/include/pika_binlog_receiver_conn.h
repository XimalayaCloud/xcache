// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_BINLOG_RECEIVER_CONN_H_
#define PIKA_BINLOG_RECEIVER_CONN_H_

#include "pink/include/pink_conn.h"
#include "pink/include/redis_parser.h"
#include "pika_command.h"

class PikaBinlogReceiverThread;

class PikaBinlogReceiverConn: public pink::PinkConn {
 public:
  PikaBinlogReceiverConn(int fd, std::string ip_port, void* worker_specific_data);
  virtual ~PikaBinlogReceiverConn();

  virtual pink::ReadStatus GetRequest() override;
  virtual pink::WriteStatus SendReply() override;

  bool ProcessBinlogData(const pink::RedisCmdArgsType& argv);

 private:
  static int ParserDealMessageCb(pink::RedisParser* parser, const pink::RedisCmdArgsType& argv);
  pink::ReadStatus ParseRedisParserStatus(pink::RedisParserStatus status);
  void RestoreArgs(const PikaCmdArgsType& argv);

  char* rbuf_;
  int rbuf_len_;
  int msg_peak_;

  // For Redis Protocol parser
  int last_read_pos_;
  long bulk_len_;
  std::string raw_args_;

  pink::RedisParser redis_parser_;
  PikaBinlogReceiverThread* binlog_receiver_;
};

#endif

