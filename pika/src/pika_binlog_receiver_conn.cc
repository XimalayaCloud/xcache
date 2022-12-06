// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>

#include "pika_binlog_receiver_conn.h"
#include "pika_server.h"
#include "pika_commonfunc.h"
#include "pika_conf.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

PikaBinlogReceiverConn::PikaBinlogReceiverConn(int fd,
                                               std::string ip_port,
                                               void* worker_specific_data)
    : PinkConn(fd, ip_port, NULL),
      rbuf_(nullptr),
      rbuf_len_(0),
      msg_peak_(0),
      last_read_pos_(-1),
      bulk_len_(-1) {
  binlog_receiver_ = reinterpret_cast<PikaBinlogReceiverThread*>(worker_specific_data);
  raw_args_.reserve(RAW_ARGS_LEN);
  pink::RedisParserSettings settings;
  settings.DealMessage = &(PikaBinlogReceiverConn::ParserDealMessageCb);
  redis_parser_.RedisParserInit(REDIS_PARSER_REQUEST, settings);
  redis_parser_.data = this;
}

PikaBinlogReceiverConn::~PikaBinlogReceiverConn() {
  free(rbuf_);
}

pink::ReadStatus PikaBinlogReceiverConn::ParseRedisParserStatus(pink::RedisParserStatus status) {
  if (status == pink::kRedisParserInitDone) {
    return pink::kOk;
  } else if (status == pink::kRedisParserHalf) {
    return pink::kReadHalf;
  } else if (status == pink::kRedisParserDone) {
    return pink::kReadAll;
  } else if (status == pink::kRedisParserError) {
    pink::RedisParserError error_code = redis_parser_.get_error_code();
    switch (error_code) {
      case pink::kRedisParserOk :
        // status is error cant be ok
        return pink::kReadError;
      case pink::kRedisParserInitError :
        return pink::kReadError;
      case pink::kRedisParserFullError :
        return pink::kFullError;
      case pink::kRedisParserProtoError :
        return pink::kParseError;
      case pink::kRedisParserDealError :
        return pink::kDealError;
      default :
        return pink::kReadError;
    }
  } else {
    return pink::kReadError;
  }
}

void PikaBinlogReceiverConn::RestoreArgs(const PikaCmdArgsType& argv) {
  raw_args_.clear();
  RedisAppendLen(raw_args_, argv.size(), "*");
  PikaCmdArgsType::const_iterator it = argv.begin();
  for ( ; it != argv.end(); ++it) {
    RedisAppendLen(raw_args_, (*it).size(), "$");
    RedisAppendContent(raw_args_, *it);
  }
}

pink::ReadStatus PikaBinlogReceiverConn::GetRequest() {
  ssize_t nread = 0;
  int next_read_pos = last_read_pos_ + 1;

  int remain = rbuf_len_ - next_read_pos;  // Remain buffer size
  int new_size = 0;
  if (remain == 0) {
    new_size = rbuf_len_ + REDIS_IOBUF_LEN;
    remain += REDIS_IOBUF_LEN;
  } else if (remain < bulk_len_) {
    new_size = next_read_pos + bulk_len_;
    remain = bulk_len_;
  }
  if (new_size > rbuf_len_) {
    if (new_size > REDIS_MAX_MESSAGE) {
      return pink::kFullError;
    }
    rbuf_ = static_cast<char*>(realloc(rbuf_, new_size));
    if (rbuf_ == nullptr) {
      return pink::kFullError;
    }
    rbuf_len_ = new_size;
  }

  nread = read(fd(), rbuf_ + next_read_pos, remain);
  if (nread == -1) {
    if (errno == EAGAIN) {
      nread = 0;
      return pink::kReadHalf; // HALF
    } else {
      // error happened, close client
      return pink::kReadError;
    }
  } else if (nread == 0) {
    // client closed, close client
    return pink::kReadClose;
  }

  // assert(nread > 0);
  last_read_pos_ += nread;
  msg_peak_ = last_read_pos_;

  int processed_len = 0;
  pink::RedisParserStatus ret = redis_parser_.ProcessInputBuffer(
      rbuf_ + next_read_pos, nread, &processed_len);
  pink::ReadStatus read_status = ParseRedisParserStatus(ret);
  if (read_status == pink::kReadAll || read_status == pink::kReadHalf) {
    last_read_pos_ = -1;
    bulk_len_ = redis_parser_.get_bulk_len();
  }
  return read_status;
}


pink::WriteStatus PikaBinlogReceiverConn::SendReply() {
  return pink::kWriteAll;
}

bool PikaBinlogReceiverConn::ProcessBinlogData(const pink::RedisCmdArgsType& argv) {
  //no reply
  //eq set_is_reply(false);
  g_pika_server->PlusThreadQuerynum();
  if (argv.empty()) {
    return false;
  }

  // Monitor related
  std::string monitor_message;
  bool is_monitoring = g_pika_server->HasMonitorClients();
  if (is_monitoring) {
    monitor_message = std::to_string(1.0*slash::NowMicros()/1000000) + " [" + this->ip_port() + "]";
    for (PikaCmdArgsType::const_iterator iter = argv.begin(); iter != argv.end(); iter++) {
      monitor_message += " " + slash::ToRead(*iter);
    }
    g_pika_server->AddMonitorMessage(monitor_message);
  }
  RestoreArgs(argv);

  bool is_readonly = g_pika_conf->readonly();

  // Here, the binlog dispatch thread, instead of the binlog bgthread takes on the task to write binlog
  // Only when the server is readonly
  uint64_t serial = binlog_receiver_->GetnPlusSerial();
  std::string cmd = argv.size() >= 1 ? argv[0] : "";
  if (is_readonly) {
    if (!g_pika_server->WaitTillBinlogBGSerial(serial)) {
      return false;
    }
    //g_pika_server->logger_->Lock();
    //g_pika_server->logger_->Put(raw_args_);
    //g_pika_server->logger_->Unlock();
    if (slash::StringToLower(cmd) != "slaveof") {
      std::string key = argv.size() >= 2 ? argv[1] : argv[0];
      uint32_t crc = PikaCommonFunc::CRC32Update(0, key.data(), (int)key.size());
      int thread_index =  (int)(crc % g_pika_conf->binlog_writer_num());
      g_pika_server->binlog_write_thread_[thread_index]->WriteBinlog(raw_args_, true);
    }
    g_pika_server->SignalNextBinlogBGSerial();
  }

  PikaCmdArgsType *v = new PikaCmdArgsType(argv);
  std::string dispatch_key = argv.size() >= 2 ? argv[1] : argv[0];
  g_pika_server->DispatchBinlogBG(dispatch_key, v, raw_args_,
      serial, is_readonly);
  //  memcpy(wbuf_ + wbuf_len_, res.data(), res.size());
  //  wbuf_len_ += res.size();
  return true;
}

int PikaBinlogReceiverConn::ParserDealMessageCb(pink::RedisParser* parser,
                                                const pink::RedisCmdArgsType& argv) {
  PikaBinlogReceiverConn* conn = reinterpret_cast<PikaBinlogReceiverConn*>(parser->data);
  return conn->ProcessBinlogData(argv) == true ? 0 : -1;
}
