// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#ifndef PINK_INCLUDE_REDIS_CLI_H_
#define PINK_INCLUDE_REDIS_CLI_H_

#include <vector>
#include <string>

#include "pink/include/pink_define.h"
#include "pink/include/pink_cli.h"

namespace pink {


typedef std::vector<std::string> RedisCmdArgsType;
// We can serialize redis command by 2 ways:
// 1. by variable argmuments;
//    eg.  RedisCli::Serialize(cmd, "set %s %d", "key", 5);
//        cmd will be set as the result string;
// 2. by a string vector;
//    eg.  RedisCli::Serialize(argv, cmd);
//        also cmd will be set as the result string.
extern int SerializeRedisCommand(std::string *cmd, const char *format, ...);
extern int SerializeRedisCommand(const RedisCmdArgsType &argv, std::string *cmd);

class RedisCli : public PinkCli {
 public:
  RedisCli();
  virtual ~RedisCli();

  // msg should have been parsed
  virtual Status Send(void *msg);

  // Read, parse and store the reply
  virtual Status Recv(void *result = NULL);

 private:
  RedisCmdArgsType argv_;   // The parsed result

  char *rbuf_;
  int32_t rbuf_size_;
  int32_t rbuf_pos_;
  int32_t rbuf_offset_;
  int elements_;    // the elements number of this current reply
  int err_;

  int GetReply();
  int GetReplyFromReader();

  int ProcessLineItem();
  int ProcessBulkItem();
  int ProcessMultiBulkItem();

  ssize_t BufferRead();
  char* ReadBytes(unsigned int bytes);
  char* ReadLine(int *_len);

  // No copyable
  RedisCli(const RedisCli&);
  void operator=(const RedisCli&);
};

}   // namespace pink

#endif  // PINK_INCLUDE_REDIS_CLI_H_
