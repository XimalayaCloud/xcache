// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "slash/include/slash_string.h"
#include "pika_hash.h"
#include "pika_server.h"
#include "pika_slot.h"

extern PikaServer *g_pika_server;

void HDelCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHDel);
    return;
  }
  key_ = argv[1];
  PikaCmdArgsType::const_iterator iter = argv.begin();
  iter++; 
  iter++;
  fields_.assign(iter, argv.end());
  return;
}

void HDelCmd::Do() {
  s_ = g_pika_server->db()->HDel(key_, fields_, &deleted_);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(deleted_);
	  KeyNotExistsRem("h", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void HDelCmd::CacheDo() {
  Do();
}

void HDelCmd::PostDo() {
  if (s_.ok() && 0 < deleted_) {
    g_pika_server->Cache()->HDel(key_, fields_);
  }
}

void HSetCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHSet);
    return;
  }
  key_ = argv[1];
  field_ = argv[2];
  value_ = argv[3];
  return;
}

void HSetCmd::Do() {
  int32_t ret = 0;
  s_ = g_pika_server->db()->HSet(key_, field_, value_, &ret);
  if (s_.ok()) {
    res_.AppendContent(":" + std::to_string(ret));
    SlotKeyAdd("h", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void HSetCmd::CacheDo() {
  Do();
}

void HSetCmd::PostDo()
{
  if (s_.ok()) {
    g_pika_server->Cache()->HSetIfKeyExist(key_, field_, value_);
  }
}

void HGetCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHGet);
    return;
  }
  key_ = argv[1];
  field_ = argv[2];
  return;
}

void HGetCmd::Do() {
  std::string value;
  s_ = g_pika_server->db()->HGet(key_, field_, &value);
  if (s_.ok()) {
    res_.AppendStringLen(value.size());
    res_.AppendContent(value);
  } else if (s_.IsNotFound()) {
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HGetCmd::PreDo() {
  std::string value;
  slash::Status s = g_pika_server->Cache()->HGet(key_, field_, &value);
  if (s.ok()) {
    res_.AppendStringLen(value.size());
    res_.AppendContent(value);
  } else if (s.IsItemNotExist()) {
    res_.AppendContent("$-1");
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void HGetCmd::CacheDo() {
  res_.clear();
  Do();
}

void HGetCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_);
  }
}

void HGetallCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHGetall);
    return;
  }
  key_ = argv[1];
  return;
}

void HGetallCmd::Do() {
  std::vector<blackwidow::FieldValue> fvs;
  s_ = g_pika_server->db()->HGetall(key_, &fvs);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLen(fvs.size() * 2);
    for (const auto& fv : fvs) {
      res_.AppendStringLen(fv.field.size());
      res_.AppendContent(fv.field);
      res_.AppendStringLen(fv.value.size());
      res_.AppendContent(fv.value);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void HGetallCmd::PreDo() {
  std::vector<blackwidow::FieldValue> fvs;
  slash::Status s = g_pika_server->Cache()->HGetall(key_, &fvs);
  if (s.ok()) {
    res_.AppendArrayLen(fvs.size() * 2);
    for (const auto& fv : fvs) {
      res_.AppendStringLen(fv.field.size());
      res_.AppendContent(fv.field);
      res_.AppendStringLen(fv.value.size());
      res_.AppendContent(fv.value);
    }
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void HGetallCmd::CacheDo() {
  res_.clear();
  Do();
}

void HGetallCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_);
  }
}

void HExistsCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHExists);
    return;
  }
  key_ = argv[1];
  field_ = argv[2];
  return;
}

void HExistsCmd::Do() {
  s_ = g_pika_server->db()->HExists(key_, field_);
  if (s_.ok()) {
    res_.AppendContent(":1");
  } else if (s_.IsNotFound()) {
    res_.AppendContent(":0");
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HExistsCmd::PreDo() {
  slash::Status s = g_pika_server->Cache()->HExists(key_, field_);
  if (s.ok()) {
    res_.AppendContent(":1");
  } else if (s.IsItemNotExist()) {
    res_.AppendContent(":0");
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void HExistsCmd::CacheDo() {
  res_.clear();
  Do();
}

void HExistsCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_);
  }
}

void HIncrbyCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHIncrby);
    return;
  }
  key_ = argv[1];
  field_ = argv[2];
  if (argv[3].find(" ") != std::string::npos || !slash::string2l(argv[3].data(), argv[3].size(), &by_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void HIncrbyCmd::Do() {
  int64_t new_value;
  s_ = g_pika_server->db()->HIncrby(key_, field_, by_, &new_value);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendContent(":" + std::to_string(new_value));
    SlotKeyAdd("h", key_);
  } else if (s_.IsCorruption() && s_.ToString() == "Corruption: hash value is not an integer") {
    res_.SetRes(CmdRes::kInvalidInt);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void HIncrbyCmd::CacheDo() {
  Do();
}

void HIncrbyCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->HIncrbyxx(key_, field_, by_);
  }
}

void HIncrbyfloatCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHIncrbyfloat);
    return;
  }
  key_ = argv[1];
  field_ = argv[2];
  by_ = argv[3];
  return;
}

void HIncrbyfloatCmd::Do() {
  std::string new_value;
  s_ = g_pika_server->db()->HIncrbyfloat(key_, field_, by_, &new_value);
  if (s_.ok()) {
    res_.AppendStringLen(new_value.size());
    res_.AppendContent(new_value);
    SlotKeyAdd("h", key_);
  } else if (s_.IsCorruption() && s_.ToString() == "Corruption: value is not a vaild float") {
    res_.SetRes(CmdRes::kInvalidFloat);
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kOverFlow);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void HIncrbyfloatCmd::CacheDo() {
  Do();
}

void HIncrbyfloatCmd::PostDo() {
  if (s_.ok()) {
    long double long_double_by;
    if (blackwidow::StrToLongDouble(by_.data(), by_.size(), &long_double_by) != -1) {
      g_pika_server->Cache()->HIncrbyfloatxx(key_, field_, long_double_by);
    }
  }
}

void HKeysCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHKeys);
    return;
  }
  key_ = argv[1];
  return;
}

void HKeysCmd::Do() {
  std::vector<std::string> fields;
  s_ = g_pika_server->db()->HKeys(key_, &fields);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLen(fields.size());
    for (const auto& field : fields) {
      res_.AppendString(field);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void HKeysCmd::PreDo() {
  std::vector<std::string> fields;
  slash::Status s = g_pika_server->Cache()->HKeys(key_, &fields);
  if (s.ok()) {
    res_.AppendArrayLen(fields.size());
    for (const auto& field : fields) {
      res_.AppendString(field);
    }
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void HKeysCmd::CacheDo() {
  res_.clear();
  Do();
}

void HKeysCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_);
  }
}

void HLenCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHLen);
    return;
  }
  key_ = argv[1];
  return;
}

void HLenCmd::Do() {
  int32_t len = 0;
  s_ = g_pika_server->db()->HLen(key_, &len);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(len);
  } else {
    res_.SetRes(CmdRes::kErrOther, "something wrong in hlen");
  }
  return;
}

void HLenCmd::PreDo() {
  unsigned long len = 0;
  slash::Status s = g_pika_server->Cache()->HLen(key_, &len);
  if (s.ok()) {
    res_.AppendInteger(len);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, "something wrong in hlen");
  }
}

void HLenCmd::CacheDo() {
  res_.clear();
  Do();
}

void HLenCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_);
  }
}

void HMgetCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHMget);
    return;
  }
  key_ = argv[1];
  PikaCmdArgsType::const_iterator iter = argv.begin();
  iter++;
  iter++;
  fields_.assign(iter, argv.end()); 
  return;
}

void HMgetCmd::Do() {
  std::vector<blackwidow::ValueStatus> vss;
  s_ = g_pika_server->db()->HMGet(key_, fields_, &vss);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLen(vss.size());
    for (const auto& vs : vss) {
      if (vs.status.ok()) {
        res_.AppendStringLen(vs.value.size());
        res_.AppendContent(vs.value);
      } else {
        res_.AppendContent("$-1");
      }
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void HMgetCmd::PreDo() {
  std::vector<blackwidow::ValueStatus> vss;
  slash::Status s = g_pika_server->Cache()->HMGet(key_, fields_, &vss);
  if (s.ok()) {
    res_.AppendArrayLen(vss.size());
    for (const auto& vs : vss) {
      if (vs.status.ok()) {
        res_.AppendStringLen(vs.value.size());
        res_.AppendContent(vs.value);
      } else {
        res_.AppendContent("$-1");
      }
    }
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void HMgetCmd::CacheDo() {
  res_.clear();
  Do();
}

void HMgetCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_);
  }
}

void HMsetCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHMset);
    return;
  }
  key_ = argv[1];
  size_t argc = argv.size();
  if (argc % 2 != 0) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHMset);
    return;
  }
  size_t index = 2;
  fvs_.clear();
  for (; index < argc; index += 2) {
    fvs_.push_back({argv[index], argv[index + 1]});
  }
  return;
}

void HMsetCmd::Do() {
  s_ = g_pika_server->db()->HMSet(key_, fvs_);
  if (s_.ok()) {
    res_.SetRes(CmdRes::kOk);
    SlotKeyAdd("h", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void HMsetCmd::CacheDo() {
  Do();
}

void HMsetCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->HMSetxx(key_, fvs_);
  }
}

void HSetnxCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHSetnx);
    return;
  }
  key_ = argv[1];
  field_ = argv[2];
  value_ = argv[3];
  return;
}

void HSetnxCmd::Do() {
  int32_t ret = 0;
  s_ = g_pika_server->db()->HSetnx(key_, field_, value_, &ret);
  if (s_.ok()) {
    res_.AppendContent(":" + std::to_string(ret));
	SlotKeyAdd("h", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void HSetnxCmd::CacheDo() {
  Do();
}

void HSetnxCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->HSetIfKeyExistAndFieldNotExist(key_, field_, value_);
  }
}

void HStrlenCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHStrlen);
    return;
  }
  key_ = argv[1];
  field_ = argv[2];
  return;
}

void HStrlenCmd::Do() {
  int32_t len = 0;
  s_ = g_pika_server->db()->HStrlen(key_, field_, &len);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(len);
  } else {
    res_.SetRes(CmdRes::kErrOther, "something wrong in hstrlen");
  }
  return;
}

void HStrlenCmd::PreDo() {
  unsigned long len = 0;
  slash::Status s = g_pika_server->Cache()->HStrlen(key_, field_, &len);
  if (s.ok()) {
    res_.AppendInteger(len);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, "something wrong in hstrlen");
  }
  return;
}

void HStrlenCmd::CacheDo() {
  res_.clear();
  Do();
}

void HStrlenCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_);
  }
}

void HValsCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHVals);
    return;
  }
  key_ = argv[1];
  return;
}

void HValsCmd::Do() {
  std::vector<std::string> values;
  s_ = g_pika_server->db()->HVals(key_, &values);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLen(values.size());
    for (const auto& value : values) {
      res_.AppendStringLen(value.size());
      res_.AppendContent(value);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void HValsCmd::PreDo() {
  std::vector<std::string> values;
  slash::Status s = g_pika_server->Cache()->HVals(key_, &values);
  if (s.ok()) {
    res_.AppendArrayLen(values.size());
    for (const auto& value : values) {
      res_.AppendStringLen(value.size());
      res_.AppendContent(value);
    }
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void HValsCmd::CacheDo() {
  res_.clear();
  Do();
}

void HValsCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_HASH, key_);
  }
}

void HScanCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameHScan);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &cursor_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  size_t index = 3, argc = argv.size();

  while (index < argc) {
    std::string opt = argv[index]; 
    if (!strcasecmp(opt.data(), "match") || !strcasecmp(opt.data(), "count")) {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!strcasecmp(opt.data(), "match")) {
        pattern_ = argv[index];
      } else if (!slash::string2l(argv[index].data(), argv[index].size(), &count_)) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
  if (count_ < 0) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  return;
}

void HScanCmd::Do() {
  int64_t next_cursor = 0;
  std::vector<blackwidow::FieldValue> field_values;
  s_ = g_pika_server->db()->HScan(key_, cursor_, pattern_, count_, &field_values, &next_cursor);

  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendContent("*2");
    char buf[32];
    int32_t len = slash::ll2string(buf, sizeof(buf), next_cursor);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);

    res_.AppendArrayLen(field_values.size()*2);
    for (const auto& field_value : field_values) {
      res_.AppendString(field_value.field);
      res_.AppendString(field_value.value);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}
