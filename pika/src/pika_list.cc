// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <vector>

#include "slash/include/slash_string.h"
#include "pika_list.h"
#include "pika_server.h"
#include "pika_slot.h"

extern PikaServer *g_pika_server;

void LIndexCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLIndex);
    return;
  }
  key_ = argv[1];
  std::string index = argv[2];
  if (!slash::string2l(index.data(), index.size(), &index_)) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
  return;
}

void LIndexCmd::Do() {
  std::string value;
  s_ = g_pika_server->db()->LIndex(key_, index_, &value);
  if (s_.ok()) {
    res_.AppendString(value);
  } else if (s_.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void LIndexCmd::PreDo() {
  std::string value;
  slash::Status s = g_pika_server->Cache()->LIndex(key_, index_, &value);
  if (s.ok()) {
    res_.AppendString(value);
  } else if (s.IsItemNotExist()) {
    res_.AppendStringLen(-1);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LIndexCmd::CacheDo() {
  res_.clear();
  Do();
}

void LIndexCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_LIST, key_);
  }
}

void LInsertCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLInsert);
    return;
  }
  key_ = argv[1];
  std::string dir = slash::StringToLower(argv[2]);
  if (dir == "before" ) {
    dir_ = blackwidow::Before;
  } else if (dir == "after") {
    dir_ = blackwidow::After;
  } else {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  pivot_ = argv[3];
  value_ = argv[4];
}

void LInsertCmd::Do() {
  int64_t llen = 0;
  s_ = g_pika_server->db()->LInsert(key_, dir_, pivot_, value_, &llen);
  if (s_.ok()) {
    res_.AppendInteger(llen);
    SlotKeyAdd("l", key_);
  } else if (s_.IsNotFound()) {
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void LInsertCmd::CacheDo() {
  Do();
}

void LInsertCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->LInsert(key_, dir_, pivot_, value_);
  }
}

void LLenCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLLen);
    return;
  }
  key_ = argv[1];
}

void LLenCmd::Do() {
  uint64_t llen = 0;
  s_ = g_pika_server->db()->LLen(key_, &llen);
  if (s_.ok() || s_.IsNotFound()){
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void LLenCmd::PreDo() {
  uint64_t llen = 0;
  slash::Status s = g_pika_server->Cache()->LLen(key_, &llen);
  if (s.ok()){
    res_.AppendInteger(llen);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LLenCmd::CacheDo() {
  res_.clear();
  Do();
}

void LLenCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_LIST, key_);
  }
}

void LPushCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLPush);
    return;
  }
  key_ = argv[1];
  size_t pos = 2;
  while (pos < argv.size()) {
    values_.push_back(argv[pos++]);
  }
}

void LPushCmd::Do() {
  uint64_t llen = 0;
  s_ = g_pika_server->db()->LPush(key_, values_, &llen);
  if (s_.ok()) {
    res_.AppendInteger(llen);
    SlotKeyAdd("l", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void LPushCmd::CacheDo() {
  Do();
}

void LPushCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->LPushx(key_, values_);
  }
}

void LPopCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLPop);
    return;
  }
  key_ = argv[1];
}

void LPopCmd::Do() {
  std::string value;
  s_ = g_pika_server->db()->LPop(key_, &value);
  if (s_.ok()) {
    res_.AppendString(value);
    KeyNotExistsRem("l", key_);
  } else if (s_.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void LPopCmd::CacheDo() {
  Do();
}

void LPopCmd::PostDo() {
  if (s_.ok()) {
    std::string value;
    g_pika_server->Cache()->LPop(key_, &value);
  }
}

void LPushxCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLPushx);
    return;
  }
  key_ = argv[1];
  value_ = argv[2];
}

void LPushxCmd::Do() {
  uint64_t llen = 0;
  s_ = g_pika_server->db()->LPushx(key_, value_, &llen);
  if (s_.ok()) {
    res_.AppendInteger(llen);
    SlotKeyAdd("l", key_);
  } else if (s_.IsNotFound()) {
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void LPushxCmd::CacheDo() {
  Do();
}

void LPushxCmd::PostDo() {
  if (s_.ok()) {
    std::vector<std::string> values;
    values.push_back(value_);
    g_pika_server->Cache()->LPushx(key_, values);
  }
}

void LRangeCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLRange);
    return;
  }
  key_ = argv[1];
  std::string left = argv[2];
  if (!slash::string2l(left.data(), left.size(), &left_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  std::string right = argv[3];
  if (!slash::string2l(right.data(), right.size(), &right_)) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
  return;
}

void LRangeCmd::Do() {
  std::vector<std::string> values;
  s_ = g_pika_server->db()->LRange(key_, left_, right_, &values);
  if (s_.ok()) {
    res_.AppendArrayLen(values.size());
    for (const auto& value : values) {
      res_.AppendString(value);
    }
  } else if (s_.IsNotFound()) {
    res_.AppendArrayLen(0);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void LRangeCmd::PreDo() {
  std::vector<std::string> values;
  slash::Status s = g_pika_server->Cache()->LRange(key_, left_, right_, &values);
  if (s.ok()) {
    res_.AppendArrayLen(values.size());
    for (const auto& value : values) {
      res_.AppendString(value);
    }
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void LRangeCmd::CacheDo() {
  res_.clear();
  Do();
}

void LRangeCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_LIST, key_);
  }
}

void LRemCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLRem);
    return;
  }
  key_ = argv[1];
  std::string count = argv[2];
  if (!slash::string2l(count.data(), count.size(), &count_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv[3];
}

void LRemCmd::Do() {
  uint64_t res = 0;
  s_ = g_pika_server->db()->LRem(key_, count_, value_, &res);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(res);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  KeyNotExistsRem("l", key_);
}

void LRemCmd::CacheDo() {
  Do();
}

void LRemCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->LRem(key_, count_, value_);
  }
}

void LSetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLSet);
    return;
  }
  key_ = argv[1];
  std::string index = argv[2];
  if (!slash::string2l(index.data(), index.size(), &index_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  value_ = argv[3];
}

void LSetCmd::Do() {
    s_ = g_pika_server->db()->LSet(key_, index_, value_);
    if (s_.ok()) {
      res_.SetRes(CmdRes::kOk);
      SlotKeyAdd("l", key_);
    } else if (s_.IsNotFound()) {
      res_.SetRes(CmdRes::kNotFound);
    } else if (s_.IsCorruption() && s_.ToString() == "Corruption: index out of range") {
      //TODO refine return value
      res_.SetRes(CmdRes::kOutOfRange);
    } else {
      res_.SetRes(CmdRes::kErrOther, s_.ToString());
    }
}

void LSetCmd::CacheDo() {
  Do();
}

void LSetCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->LSet(key_, index_, value_);
  }
}

void LTrimCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLTrim);
    return;
  }
  key_ = argv[1];
  std::string start = argv[2];
  if (!slash::string2l(start.data(), start.size(), &start_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  std::string stop = argv[3];
  if (!slash::string2l(stop.data(), stop.size(), &stop_)) {
    res_.SetRes(CmdRes::kInvalidInt);
  }
  return;
}

void LTrimCmd::Do() {
  s_ = g_pika_server->db()->LTrim(key_, start_, stop_);
  if (s_.ok() || s_.IsNotFound()) {
    KeyNotExistsRem("l", key_);
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void LTrimCmd::CacheDo() {
  Do();
}

void LTrimCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->LTrim(key_, start_, stop_);
  }
}

void RPopCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameRPop);
    return;
  }
  key_ = argv[1];
}

void RPopCmd::Do() {
  std::string value;
  s_ = g_pika_server->db()->RPop(key_, &value);
  if (s_.ok()) {
    res_.AppendString(value);
    KeyNotExistsRem("l", key_);
  } else if (s_.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void RPopCmd::CacheDo() {
  Do();
}

void RPopCmd::PostDo() {
  if (s_.ok()) {
    std::string value;
    g_pika_server->Cache()->RPop(key_, &value);
  }
}

void RPopLPushCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameRPopLPush);
    return;
  }
  source_ = argv[1];
  receiver_ = argv[2];
}

void RPopLPushCmd::Do() {
  std::string value;
  s_ = g_pika_server->db()->RPoplpush(source_, receiver_, &value);
  if (s_.ok()) {
	KeyNotExistsRem("l", source_);
    SlotKeyAdd("l", receiver_);
    res_.AppendString(value);
  } else if (s_.IsNotFound()) {
    res_.AppendStringLen(-1);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void RPopLPushCmd::PreDo() {
  res_.SetRes(CmdRes::kErrOther, "the command is not support in cache mode");
}

void RPushCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameRPush);
    return;
  }
  key_ = argv[1];
  size_t pos = 2;
  while (pos < argv.size()) {
    values_.push_back(argv[pos++]);
  }
}

void RPushCmd::Do() {
  uint64_t llen = 0;
  s_ = g_pika_server->db()->RPush(key_, values_, &llen);
  if (s_.ok()) {
    res_.AppendInteger(llen);
	SlotKeyAdd("l", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void RPushCmd::CacheDo() {
  Do();
}

void RPushCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->RPushx(key_, values_);
  }
}

void RPushxCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameRPushx);
    return;
  }
  key_ = argv[1];
  value_ = argv[2];
}

void RPushxCmd::Do() {
  uint64_t llen = 0;
  s_ = g_pika_server->db()->RPushx(key_, value_, &llen);
  if (s_.ok()) {
    res_.AppendInteger(llen);
    SlotKeyAdd("l", key_);
  } else if (s_.IsNotFound()) {
    res_.AppendInteger(llen);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void RPushxCmd::CacheDo() {
  Do();
}

void RPushxCmd::PostDo() {
  if (s_.ok()) {
    std::vector<std::string> values;
    values.push_back(value_);
    g_pika_server->Cache()->RPushx(key_, values);
  }
}
