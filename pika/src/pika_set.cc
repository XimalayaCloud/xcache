// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "slash/include/slash_string.h"
#include "pika_set.h"
#include "pika_server.h"
#include "pika_slot.h"

extern PikaServer *g_pika_server;

void SAddCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSAdd);
    return;
  }
  key_ = argv[1];
  PikaCmdArgsType::iterator iter = argv.begin();
  iter++; 
  iter++;
  members_.assign(iter, argv.end());
  return;
}

void SAddCmd::Do() {
  int32_t count = 0;
  s_ = g_pika_server->db()->SAdd(key_, members_, &count);
  if (!s_.ok()) {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
    return;
  }
  SlotKeyAdd("s", key_);
  res_.AppendInteger(count);
  return;
}

void SAddCmd::CacheDo() {
  Do();
}

void SAddCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->SAddIfKeyExist(key_, members_);
  }
}

void SPopCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSPop);
    return;
  }
  key_ = argv[1];
  return;
}

void SPopCmd::Do() {
  s_ = g_pika_server->db()->SPop(key_, &member_);
  if (s_.ok()) {
    res_.AppendStringLen(member_.size());
    res_.AppendContent(member_);
    KeyNotExistsRem("s", key_);
  } else if (s_.IsNotFound()) {
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void SPopCmd::CacheDo() {
  Do();
}

void SPopCmd::PostDo() {
  if (s_.ok()) {
    std::vector<std::string> members;
    members.push_back(member_);
    g_pika_server->Cache()->SRem(key_, members);
  }
}

void SCardCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSCard);
    return;
  }
  key_ = argv[1];
  return;
}

void SCardCmd::Do() {
  int32_t card = 0;
  s_ = g_pika_server->db()->SCard(key_, &card);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(card);
  } else {
    res_.SetRes(CmdRes::kErrOther, "scard error");
  }
  return;
}

void SCardCmd::PreDo() {
  unsigned long card = 0;
  slash::Status s = g_pika_server->Cache()->SCard(key_, &card);
  if (s.ok()) {
    res_.AppendInteger(card);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, "scard error");
  }
}

void SCardCmd::CacheDo() {
  res_.clear();
  Do();
}

void SCardCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_SET, key_);
  }
}

void SMembersCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSMembers);
    return;
  }
  key_ = argv[1];
  return;
}

void SMembersCmd::Do() {
  std::vector<std::string> members;
  s_ = g_pika_server->db()->SMembers(key_, &members);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendArrayLen(members.size());
    for (const auto& member : members) {
      res_.AppendStringLen(member.size());
      res_.AppendContent(member);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void SMembersCmd::PreDo() {
  std::vector<std::string> members;
  slash::Status s = g_pika_server->Cache()->SMembers(key_, &members);
  if (s.ok()) {
    res_.AppendArrayLen(members.size());
    for (const auto& member : members) {
      res_.AppendStringLen(member.size());
      res_.AppendContent(member);
    }
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void SMembersCmd::CacheDo() {
  res_.clear();
  Do();
}

void SMembersCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_SET, key_);
  }
}

void SScanCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSScan);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &cursor_)) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSScan);
    return;
  }
  size_t argc = argv.size(), index = 3;
  while (index < argc) {
    std::string opt = slash::StringToLower(argv[index]); 
    if (opt == "match" || opt == "count") {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (opt == "match") {
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

void SScanCmd::Do() {
  int64_t next_cursor = 0;
  std::vector<std::string> members;
  s_ = g_pika_server->db()->SScan(key_, cursor_, pattern_, count_, &members, &next_cursor);

  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendContent("*2");
    char buf[32];
    int64_t len = slash::ll2string(buf, sizeof(buf), next_cursor);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);

    res_.AppendArrayLen(members.size());
    for (const auto& member : members) {
      res_.AppendString(member);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void SRemCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSMembers);
    return;
  }
  key_ = argv[1];
  PikaCmdArgsType::iterator iter = argv.begin();
  iter++;
  members_.assign(++iter, argv.end());
  return;
}

void SRemCmd::Do() {
  s_ = g_pika_server->db()->SRem(key_, members_, &deleted_);
  res_.AppendInteger(deleted_);
  KeyNotExistsRem("s", key_);
  return;
}

void SRemCmd::CacheDo() {
  Do();
}

void SRemCmd::PostDo() {
  if (s_.ok() && 0 < deleted_) {
    g_pika_server->Cache()->SRem(key_, members_);
  }
}

void SUnionCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSUnion);
    return;
  }
  PikaCmdArgsType::iterator iter = argv.begin();
  keys_.assign(++iter, argv.end());
  return;
}

void SUnionCmd::Do() {
  std::vector<std::string> members;
  g_pika_server->db()->SUnion(keys_, &members);
  res_.AppendArrayLen(members.size());
  for (const auto& member : members) {
    res_.AppendStringLen(member.size());
    res_.AppendContent(member);
  }
  return;
}

void SUnionstoreCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSUnionstore);
    return;
  }
  dest_key_ = argv[1];
  PikaCmdArgsType::iterator iter = argv.begin();
  iter++;
  keys_.assign(++iter, argv.end());
  return;
}

void SUnionstoreCmd::Do() {
  int32_t count = 0;
  s_ = g_pika_server->db()->SUnionstore(dest_key_, keys_, &count);
  if (s_.ok()) {
    if (count > 0) {
      SlotKeyAdd("s", dest_key_);
    } else {
      KeyNotExistsRem("s", dest_key_);
    }
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void SUnionstoreCmd::CacheDo() {
  Do();
}

void SUnionstoreCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->Del(dest_key_);
  }
}

void SInterCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSInter);
    return;
  }
  PikaCmdArgsType::iterator iter = argv.begin();
  keys_.assign(++iter, argv.end());
  return;
}

void SInterCmd::Do() {
  std::vector<std::string> members;
  g_pika_server->db()->SInter(keys_, &members);
  res_.AppendArrayLen(members.size());
  for (const auto& member : members) {
    res_.AppendStringLen(member.size());
    res_.AppendContent(member);
  }
  return;
}

void SInterstoreCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSInterstore);
    return;
  }
  dest_key_ = argv[1];
  PikaCmdArgsType::iterator iter = argv.begin();
  iter++;
  keys_.assign(++iter, argv.end());
  return;
}

void SInterstoreCmd::Do() {
  int32_t count = 0;
  s_ = g_pika_server->db()->SInterstore(dest_key_, keys_, &count);
  if (s_.ok()) {
    if (count > 0) {
      SlotKeyAdd("s", dest_key_);
    } else {
      KeyNotExistsRem("s", dest_key_);
    }
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void SInterstoreCmd::CacheDo() {
  Do();
}

void SInterstoreCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->Del(dest_key_);
  }
}

void SIsmemberCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSIsmember);
    return;
  }
  key_ = argv[1];
  member_ = argv[2];
  return;
}

void SIsmemberCmd::Do() {
  int32_t is_member = 0;
  s_ = g_pika_server->db()->SIsmember(key_, member_, &is_member);
  if (is_member) {
    res_.AppendContent(":1");
  } else {
    res_.AppendContent(":0");
  }
}

void SIsmemberCmd::PreDo() {
  slash::Status s = g_pika_server->Cache()->SIsmember(key_, member_);
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

void SIsmemberCmd::CacheDo() {
  res_.clear();
  Do();
}

void SIsmemberCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_SET, key_);
  }
}

void SDiffCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSDiff);
    return;
  }
  PikaCmdArgsType::iterator iter = argv.begin();
  keys_.assign(++iter, argv.end());
  return;
}

void SDiffCmd::Do() {
  std::vector<std::string> members;
  g_pika_server->db()->SDiff(keys_, &members);
  res_.AppendArrayLen(members.size());
  for (const auto& member : members) {
    res_.AppendStringLen(member.size());
    res_.AppendContent(member);
  }
  return;
}

void SDiffstoreCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSDiffstore);
    return;
  }
  dest_key_ = argv[1];
  PikaCmdArgsType::iterator iter = argv.begin();
  iter++;
  keys_.assign(++iter, argv.end());
  return;
}

void SDiffstoreCmd::Do() {
  int32_t count = 0;
  s_ = g_pika_server->db()->SDiffstore(dest_key_, keys_, &count);
  if (s_.ok()) {
    if (count > 0) {
      SlotKeyAdd("s", dest_key_);
    } else {
      KeyNotExistsRem("s", dest_key_);
    }
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void SDiffstoreCmd::CacheDo() {
  Do();
}

void SDiffstoreCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->Del(dest_key_);
  }
}

void SMoveCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSMove);
    return;
  }
  src_key_ = argv[1];
  dest_key_ = argv[2];
  member_ = argv[3];
  return;
}

void SMoveCmd::Do() {
  int32_t res = 0;
  s_ = g_pika_server->db()->SMove(src_key_, dest_key_, member_, &res);
  if (s_.ok() || s_.IsNotFound()) {
    if (s_.IsNotFound()){
      res_.AppendInteger(res);
    } else {
      res_.AppendInteger(res);
      SlotKeyAdd("s", dest_key_);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }

  KeyNotExistsRem("s", src_key_);
  return;
}

void SMoveCmd::CacheDo() {
  Do();
}

void SMoveCmd::PostDo() {
  if (s_.ok()) {
    std::vector<std::string> members;
    members.push_back(member_);
    g_pika_server->Cache()->SRem(src_key_, members);    
    // warning: it is not atomic to add dest key member when in cache model
    g_pika_server->Cache()->SAddIfKeyExist(dest_key_, members);
  }
}

void SRandmemberCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSRandmember);
    return;
  }
  key_ = argv[1];
  if (argv.size() > 3) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSRandmember);
    return;
  } else if (argv.size() == 3) {
    if (!slash::string2l(argv[2].data(), argv[2].size(), &count_)) {
      res_.SetRes(CmdRes::kInvalidInt);
    } else {
      reply_arr = true;;
    }
  }
  return;
}

void SRandmemberCmd::Do() {
  std::vector<std::string> members;
  s_ = g_pika_server->db()->SRandmember(key_, count_, &members);
  if (s_.ok() || s_.IsNotFound()) {
    if (!reply_arr && members.size()) {
      res_.AppendStringLen(members[0].size());
      res_.AppendContent(members[0]);
    } else {
      res_.AppendArrayLen(members.size());
      for (const auto& member : members) {
        res_.AppendStringLen(member.size());
        res_.AppendContent(member);
      }
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void SRandmemberCmd::PreDo() {
  std::vector<std::string> members;
  slash::Status s = g_pika_server->Cache()->SRandmember(key_, count_, &members);
  if (s.ok()) {
    if (!reply_arr && members.size()) {
      res_.AppendStringLen(members[0].size());
      res_.AppendContent(members[0]);
    } else {
      res_.AppendArrayLen(members.size());
      for (const auto& member : members) {
        res_.AppendStringLen(member.size());
        res_.AppendContent(member);
      }
    }
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void SRandmemberCmd::CacheDo() {
  res_.clear();
  Do();
}

void SRandmemberCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_SET, key_);
  }
}

