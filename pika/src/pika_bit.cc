// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <limits>
#include "slash/include/slash_string.h"
#include "pika_bit.h"
#include "pika_server.h"
#include "pika_slot.h"

extern PikaServer *g_pika_server;

void BitSetCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBitSet);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &bit_offset_)) {
    res_.SetRes(CmdRes::kInvalidBitOffsetInt);
    return;
  }
  if (!slash::string2l(argv[3].data(), argv[3].size(), &on_)) {
    res_.SetRes(CmdRes::kInvalidBitInt);
    return;
  }
  if (bit_offset_ < 0) {
    res_.SetRes(CmdRes::kInvalidBitOffsetInt);
    return;
  }
  // value no bigger than 2^18
  if ( (bit_offset_ >> kMaxBitOpInputBit) > 0) {
    res_.SetRes(CmdRes::kInvalidBitOffsetInt);
    return;
  }
  if (on_ & ~1) {
    res_.SetRes(CmdRes::kInvalidBitInt);
    return;
  }
  return;
}

void BitSetCmd::Do() {
  std::string value;
  int32_t bit_val = 0;
  s_ = g_pika_server->db()->SetBit(key_, bit_offset_, on_, &bit_val);
  if (s_.ok()){
    res_.AppendInteger(bit_val);
    SlotKeyAdd("k", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void BitSetCmd::CacheDo() {
  Do();
}

void BitSetCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->SetBitIfKeyExist(key_, bit_offset_, on_);
  }
}

void BitGetCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBitGet);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &bit_offset_)) {
    res_.SetRes(CmdRes::kInvalidBitOffsetInt);
    return;
  }
  if (bit_offset_ < 0) {
    res_.SetRes(CmdRes::kInvalidBitOffsetInt);
    return;
  }
  return;
}

void BitGetCmd::Do() {
  int32_t bit_val = 0;
  s_ = g_pika_server->db()->GetBit(key_, bit_offset_, &bit_val);
  if (s_.ok()) {
    res_.AppendInteger(bit_val);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void BitGetCmd::PreDo() {
  long bit_val = 0;
  slash::Status s = g_pika_server->Cache()->GetBit(key_, bit_offset_, &bit_val);
  if (s.ok()) {
    res_.AppendInteger(bit_val);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void BitGetCmd::CacheDo() {
  res_.clear();
  Do();
}

void BitGetCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_KV, key_);
  }
}

void BitCountCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBitCount);
    return;
  }
  key_ = argv[1];
  if (argv.size() == 4) {
    count_all_ = false;
    if (!slash::string2l(argv[2].data(), argv[2].size(), &start_offset_)) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
    if (!slash::string2l(argv[3].data(), argv[3].size(), &end_offset_)) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
  } else if (argv.size() == 2) {
    count_all_ = true;
  } else {
    res_.SetRes(CmdRes::kSyntaxErr, kCmdNameBitCount);
  }
  return;
}

void BitCountCmd::Do() {
  int32_t count = 0;
  if (count_all_) {
    s_ = g_pika_server->db()->BitCount(key_, start_offset_, end_offset_, &count, false);
  } else {
    s_ = g_pika_server->db()->BitCount(key_, start_offset_, end_offset_, &count, true);
  }

  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void BitCountCmd::PreDo() {
  long count = 0;
  long start = static_cast<long>(start_offset_);
  long end = static_cast<long>(end_offset_);
  slash::Status s;
  if (count_all_) {
    s = g_pika_server->Cache()->BitCount(key_, start, end, &count, 0);
  } else {
    s = g_pika_server->Cache()->BitCount(key_, start, end, &count, 1);
  }

  if (s.ok()) {
    res_.AppendInteger(count);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void BitCountCmd::CacheDo() {
  res_.clear();
  Do();
}

void BitCountCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_KV, key_);
  }
}

void BitPosCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBitPos);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &bit_val_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (bit_val_ & ~1) {
    res_.SetRes(CmdRes::kInvalidBitPosArgument);
    return;
  }
  if (argv.size() == 3) {
    pos_all_ = true;
    endoffset_set_ = false;
  } else if (argv.size() == 4) {
    pos_all_ = false;
    endoffset_set_ = false;
    if (!slash::string2l(argv[3].data(), argv[3].size(), &start_offset_)) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    } 
  } else if (argv.size() == 5) {
    pos_all_ = false;
    endoffset_set_ = true;
    if (!slash::string2l(argv[3].data(), argv[3].size(), &start_offset_)) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    } 
    if (!slash::string2l(argv[4].data(), argv[4].size(), &end_offset_)) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }
  } else
    res_.SetRes(CmdRes::kSyntaxErr, kCmdNameBitPos);
  return;
}

void BitPosCmd::Do() {
  int64_t pos = 0;
  if (pos_all_) {
    s_ = g_pika_server->db()->BitPos(key_, bit_val_, &pos);
  } else if (!pos_all_ && !endoffset_set_) {
    s_ = g_pika_server->db()->BitPos(key_, bit_val_, start_offset_, &pos);
  } else if (!pos_all_ && endoffset_set_) {
    s_ = g_pika_server->db()->BitPos(key_, bit_val_, start_offset_, end_offset_, &pos);
  }
  if (s_.ok()) {
    res_.AppendInteger(pos);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void BitPosCmd::PreDo() {
  long pos = 0;
  slash::Status s;
  long bit = static_cast<long>(bit_val_);
  long start = static_cast<long>(start_offset_);
  long end = static_cast<long>(end_offset_);
  if (pos_all_) {
    s = g_pika_server->Cache()->BitPos(key_, bit, &pos);
  } else if (!pos_all_ && !endoffset_set_) {
    s = g_pika_server->Cache()->BitPos(key_, bit, start, &pos);
  } else if (!pos_all_ && endoffset_set_) {
    s = g_pika_server->Cache()->BitPos(key_, bit, start, end, &pos);
  }
  if (s.ok()) {
    res_.AppendInteger(pos);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void BitPosCmd::CacheDo() {
  res_.clear();
  Do();
}

void BitPosCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_KV, key_);
  }
}

void BitOpCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameBitOp);
    return;
  }

  if (!strcasecmp(argv[1].data(), "not")) {
    op_ = blackwidow::kBitOpNot;
  } else if (!strcasecmp(argv[1].data(), "and")) {
    op_ = blackwidow::kBitOpAnd;
  } else if (!strcasecmp(argv[1].data(), "or")) {
    op_ = blackwidow::kBitOpOr;
  } else if (!strcasecmp(argv[1].data(), "xor")) {
    op_ = blackwidow::kBitOpXor;
  } else {
    res_.SetRes(CmdRes::kSyntaxErr, kCmdNameBitOp);
    return;
  }
  if (op_ == blackwidow::kBitOpNot && argv.size() != 4) {
      res_.SetRes(CmdRes::kWrongBitOpNotNum, kCmdNameBitOp);
      return;
  } else if (op_ != blackwidow::kBitOpNot && argv.size() < 4) {
      res_.SetRes(CmdRes::kWrongNum, kCmdNameBitOp);
      return;
  } else if (argv.size() >= kMaxBitOpInputKey) {
      res_.SetRes(CmdRes::kWrongNum, kCmdNameBitOp);
      return;
  }

  dest_key_ = argv[2].data();
  for(unsigned int i = 3; i <= argv.size() - 1; i++) {
      src_keys_.push_back(argv[i].data());
  }
  return;
}

void BitOpCmd::Do() {
  int64_t result_length;
  s_ = g_pika_server->db()->BitOp(op_, dest_key_, src_keys_, &result_length);
  if (s_.ok()) {
    SlotKeyAdd("k", dest_key_);
    res_.AppendInteger((int)result_length);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void BitOpCmd::CacheDo() {
  Do();
}

void BitOpCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->Del(dest_key_);
  }
}
