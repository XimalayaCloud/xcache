// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "slash/include/slash_string.h"
#include "pika_zset.h"
#include "pika_server.h"
#include "pika_slot.h"

extern PikaServer *g_pika_server;

void ZAddCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZAdd);
    return;
  }
  size_t argc = argv.size();
  if (argc % 2 == 1) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  key_ = argv[1];
  score_members.clear();
  double score;
  size_t index = 2;
  for (; index < argc; index += 2) {
    if (!slash::string2d(argv[index].data(), argv[index].size(), &score)) {
      res_.SetRes(CmdRes::kInvalidFloat);
      return;
    }
    score_members.push_back({score, argv[index + 1]});
  }
  return;
}

void ZAddCmd::Do() {
  int32_t count = 0;
  s_ = g_pika_server->db()->ZAdd(key_, score_members, &count);
  if (s_.ok()) {
  	SlotKeyAdd("z", key_);
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void ZAddCmd::CacheDo() {
  Do();
}

void ZAddCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->ZAddIfKeyExist(key_, score_members);
  }
}

void ZCardCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZCard);
    return;
  }
  key_ = argv[1];
  return;
}

void ZCardCmd::Do() {
  int32_t card = 0;
  s_ = g_pika_server->db()->ZCard(key_, &card);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(card);
  } else {
    res_.SetRes(CmdRes::kErrOther, "zcard error");
  }
  return;
}

void ZCardCmd::PreDo() {
  unsigned long card = 0;
  slash::Status s = g_pika_server->Cache()->ZCard(key_, &card);
  if (s.ok()) {
    res_.AppendInteger(card);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, "zcard error");
  }
}

void ZCardCmd::CacheDo() {
  res_.clear();
  Do();
}

void ZCardCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_ZSET, key_);
  }
}

void ZScanCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZScan);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &cursor_)) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZScan);
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

void ZScanCmd::Do() {
  int64_t next_cursor = 0;
  std::vector<blackwidow::ScoreMember> score_members;
  s_ = g_pika_server->db()->ZScan(key_, cursor_, pattern_, count_, &score_members, &next_cursor);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendContent("*2");
    char buf[32];
    int64_t len = slash::ll2string(buf, sizeof(buf), next_cursor);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);

    res_.AppendArrayLen(score_members.size() * 2);
    for (const auto& score_member : score_members) {
      res_.AppendString(score_member.member);

      len = slash::d2string(buf, sizeof(buf), score_member.score);
      res_.AppendStringLen(len);
      res_.AppendContent(buf);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void ZIncrbyCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZIncrby);
    return;
  }
  key_ = argv[1];
  if (!slash::string2d(argv[2].data(), argv[2].size(), &by_)) {
    res_.SetRes(CmdRes::kInvalidFloat);
    return;
  }
  member_ = argv[3];
  return;
}

void ZIncrbyCmd::Do() {
  double score = 0;
  s_ = g_pika_server->db()->ZIncrby(key_, member_, by_, &score);
  if (s_.ok()) {
    char buf[32];
    int64_t len = slash::d2string(buf, sizeof(buf), score);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);
	SlotKeyAdd("z", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void ZIncrbyCmd::CacheDo() {
  Do();
}

void ZIncrbyCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->ZIncrbyIfKeyExist(key_, member_, by_);
  }
}

void ZsetRangeParentCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  (void)ptr_info;
  if (argv.size() == 5 && slash::StringToLower(argv[4]) == "withscores") {
    is_ws_ = true;
  } else if (argv.size() != 4) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &start_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (!slash::string2l(argv[3].data(), argv[3].size(), &stop_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  return;
}

void ZRangeCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRange);
    return;
  }
  ZsetRangeParentCmd::DoInitial(argv, NULL);
}

void ZRangeCmd::Do() {
  std::vector<blackwidow::ScoreMember> score_members;
  s_ = g_pika_server->db()->ZRange(key_, start_, stop_, &score_members);
  if (s_.ok() || s_.IsNotFound()) {
    if (is_ws_) {
      char buf[32];
      int64_t len;
      res_.AppendArrayLen(score_members.size() * 2);
      for (const auto& sm : score_members) {
        res_.AppendStringLen(sm.member.size());
        res_.AppendContent(sm.member);
        len = slash::d2string(buf, sizeof(buf), sm.score);
        res_.AppendStringLen(len);
        res_.AppendContent(buf);
      }
    } else {
      res_.AppendArrayLen(score_members.size());
      for (const auto& sm : score_members) {
        res_.AppendStringLen(sm.member.size());
        res_.AppendContent(sm.member);
      }
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void ZRangeCmd::PreDo() {
  std::vector<blackwidow::ScoreMember> score_members;
  slash::Status s = g_pika_server->Cache()->ZRange(key_, start_, stop_, &score_members);
  if (s.ok()) {
    if (is_ws_) {
      char buf[32];
      int64_t len;
      res_.AppendArrayLen(score_members.size() * 2);
      for (const auto& sm : score_members) {
        res_.AppendStringLen(sm.member.size());
        res_.AppendContent(sm.member);
        len = slash::d2string(buf, sizeof(buf), sm.score);
        res_.AppendStringLen(len);
        res_.AppendContent(buf);
      }
    } else {
      res_.AppendArrayLen(score_members.size());
      for (const auto& sm : score_members) {
        res_.AppendStringLen(sm.member.size());
        res_.AppendContent(sm.member);
      }
    }
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void ZRangeCmd::CacheDo() {
  res_.clear();
  Do();
}

void ZRangeCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_ZSET, key_);
  }
}

static void Calcmirror(int64_t &start, int64_t &stop, int32_t t_size) {
	int64_t t_start = stop >= 0 ? stop : t_size + stop;
	int64_t t_stop = start >= 0 ? start : t_size + start;
	t_start = t_size - 1 - t_start;
	t_stop = t_size - 1 - t_stop;
	start = t_start >= 0 ? t_start : t_start - t_size;
	stop = t_stop >= 0 ? t_stop : t_stop - t_size;
}

void ZRevrangeCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRevrange);
    return;
  }
  ZsetRangeParentCmd::DoInitial(argv, NULL);
  int32_t card = 0;
  rocksdb::Status status = g_pika_server->db()->ZCard(key_, &card);
  if (!status.ok() && !status.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, "zcard error");
    return;
  }
  Calcmirror(start_, stop_, card);
}

void ZRevrangeCmd::Do() {
  std::vector<blackwidow::ScoreMember> score_members;
  s_ = g_pika_server->db()->ZRevrange(key_, start_, stop_, &score_members);
  if (s_.ok() || s_.IsNotFound()) {
    if (is_ws_) {
      char buf[32];
      int64_t len;
      res_.AppendArrayLen(score_members.size() * 2);
      for (const auto& sm : score_members) {
        res_.AppendStringLen(sm.member.size());
        res_.AppendContent(sm.member);
        len = slash::d2string(buf, sizeof(buf), sm.score);
        res_.AppendStringLen(len);
        res_.AppendContent(buf);
      }
    } else {
      res_.AppendArrayLen(score_members.size());
      for (const auto& sm : score_members) {
        res_.AppendStringLen(sm.member.size());
        res_.AppendContent(sm.member);
      }
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void ZRevrangeCmd::PreDo() {
  std::vector<blackwidow::ScoreMember> score_members;
  slash::Status s = g_pika_server->Cache()->ZRevrange(key_, start_, stop_, &score_members);
  if (s.ok()) {
    if (is_ws_) {
      char buf[32];
      int64_t len;
      res_.AppendArrayLen(score_members.size() * 2);
      for (const auto& sm : score_members) {
        res_.AppendStringLen(sm.member.size());
        res_.AppendContent(sm.member);
        len = slash::d2string(buf, sizeof(buf), sm.score);
        res_.AppendStringLen(len);
        res_.AppendContent(buf);
      }
    } else {
      res_.AppendArrayLen(score_members.size());
      for (const auto& sm : score_members) {
        res_.AppendStringLen(sm.member.size());
        res_.AppendContent(sm.member);
      }
    }
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  return;
}

void ZRevrangeCmd::CacheDo() {
  res_.clear();
  Do();
}

void ZRevrangeCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_ZSET, key_);
  }
}

int32_t DoScoreStrRange(std::string begin_score, std::string end_score, bool *left_close, bool *right_close, double *min_score, double *max_score) {
  if (begin_score.size() > 0 && begin_score.at(0) == '(') {
    *left_close = false;
    begin_score.erase(begin_score.begin());
  }
  if (begin_score == "-inf") {
    *min_score = blackwidow::ZSET_SCORE_MIN;
  } else if (begin_score == "inf" || begin_score == "+inf") {
    *min_score = blackwidow::ZSET_SCORE_MAX;
  } else if (!slash::string2d(begin_score.data(), begin_score.size(), min_score)) {
    return -1;
  } 
  
  if (end_score.size() > 0 && end_score.at(0) == '(') {
    *right_close = false;
    end_score.erase(end_score.begin());
  }
  if (end_score == "+inf" || end_score == "inf") {
    *max_score = blackwidow::ZSET_SCORE_MAX;
  } else if (end_score == "-inf") {
    *max_score = blackwidow::ZSET_SCORE_MIN;
  } else if (!slash::string2d(end_score.data(), end_score.size(), max_score)) {
    return -1;
  }
  return 0;
}

static void FitLimit(int64_t &count, int64_t &offset, const int64_t size) {
  count = count >= 0 ? count : size;
  offset = (offset >= 0 && offset < size) ? offset : size;
  count = (offset + count < size) ? count : size - offset;
}

void ZsetRangebyscoreParentCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  (void)ptr_info;
  key_ = argv[1];
  min_ = argv[2];
  max_ = argv[3];
  int32_t ret = DoScoreStrRange(argv[2], argv[3], &left_close_, &right_close_, &min_score_, &max_score_);
  if (ret == -1) {
    res_.SetRes(CmdRes::kErrOther, "min or max is not a float");
    return;
  }
  size_t argc = argv.size();
  if (argc < 5) {
    return;
  }
  size_t index = 4;
  while (index < argc) {
    slash::StringToLower(argv[index]);
    if (argv[index] == "withscores") {
      with_scores_ = true;
    } else if (argv[index] == "limit") {
      if (index + 3 > argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      index++;
      if (!slash::string2l(argv[index].data(), argv[index].size(), &offset_)) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }
      index++;
      if (!slash::string2l(argv[index].data(), argv[index].size(), &count_)) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      } 
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
}

void ZRangebyscoreCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRangebyscore);
    return;
  }
  ZsetRangebyscoreParentCmd::DoInitial(argv, NULL);
}

void ZRangebyscoreCmd::Do() {
  if (min_score_ == blackwidow::ZSET_SCORE_MAX || max_score_ == blackwidow::ZSET_SCORE_MIN) {
    res_.AppendContent("*0");
    return;
  }
  std::vector<blackwidow::ScoreMember> score_members;
  s_ = g_pika_server->db()->ZRangebyscore(key_, min_score_, max_score_, left_close_, right_close_, &score_members);
  if (!s_.ok() && !s_.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
    return;
  }
  FitLimit(count_, offset_, score_members.size());
  size_t index = offset_, end = offset_ + count_;
  if (with_scores_) {
    char buf[32];
    int64_t len;
    res_.AppendArrayLen(count_ * 2);
    for (; index < end; index++) {
      res_.AppendStringLen(score_members[index].member.size());
      res_.AppendContent(score_members[index].member);
      len = slash::d2string(buf, sizeof(buf), score_members[index].score);
      res_.AppendStringLen(len);
      res_.AppendContent(buf);
    }
  } else {
    res_.AppendArrayLen(count_);
    for (; index < end; index++) {
      res_.AppendStringLen(score_members[index].member.size());
      res_.AppendContent(score_members[index].member);
    }
  }
  return;
}

void ZRangebyscoreCmd::PreDo() {
  if (min_score_ == blackwidow::ZSET_SCORE_MAX || max_score_ == blackwidow::ZSET_SCORE_MIN) {
    res_.AppendContent("*0");
    return;
  }

  std::vector<blackwidow::ScoreMember> score_members;
  slash::Status s = g_pika_server->Cache()->ZRangebyscore(key_, min_, max_, &score_members);
  if (s.ok()) {
    FitLimit(count_, offset_, score_members.size());
    size_t index = offset_, end = offset_ + count_;
    if (with_scores_) {
      char buf[32];
      int64_t len;
      res_.AppendArrayLen(count_ * 2);
      for (; index < end; index++) {
        res_.AppendStringLen(score_members[index].member.size());
        res_.AppendContent(score_members[index].member);
        len = slash::d2string(buf, sizeof(buf), score_members[index].score);
        res_.AppendStringLen(len);
        res_.AppendContent(buf);
      }
    } else {
      res_.AppendArrayLen(count_);
      for (; index < end; index++) {
        res_.AppendStringLen(score_members[index].member.size());
        res_.AppendContent(score_members[index].member);
      }
    }
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZRangebyscoreCmd::CacheDo() {
  res_.clear();
  Do();
}

void ZRangebyscoreCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_ZSET, key_);
  }
}

void ZRevrangebyscoreCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRevrangebyscore);
    return;
  }
  ZsetRangebyscoreParentCmd::DoInitial(argv, NULL);
  double tmp_score;
  tmp_score = min_score_;
  min_score_ = max_score_;
  max_score_ = tmp_score;

  bool tmp_close;
  tmp_close = left_close_;
  left_close_ = right_close_;
  right_close_ = tmp_close;
}

void ZRevrangebyscoreCmd::Do() {
  if (min_score_ == blackwidow::ZSET_SCORE_MAX || max_score_ == blackwidow::ZSET_SCORE_MIN) {
    res_.AppendContent("*0");
    return;
  }
  std::vector<blackwidow::ScoreMember> score_members;
  s_ = g_pika_server->db()->ZRevrangebyscore(key_, min_score_, max_score_, left_close_, right_close_, &score_members);
  if (!s_.ok() && !s_.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
    return;
  }
  FitLimit(count_, offset_, score_members.size());
  int64_t index = offset_, end = offset_ + count_;
  if (with_scores_) {
    char buf[32];
    int64_t len;
    res_.AppendArrayLen(count_ * 2);
    for (; index < end; index++) {
      res_.AppendStringLen(score_members[index].member.size());
      res_.AppendContent(score_members[index].member);
      len = slash::d2string(buf, sizeof(buf), score_members[index].score);
      res_.AppendStringLen(len);
      res_.AppendContent(buf);
    }
  } else {
    res_.AppendArrayLen(count_);
    for (; index < end; index++) {
      res_.AppendStringLen(score_members[index].member.size());
      res_.AppendContent(score_members[index].member);
    }
  }
  return;
}

void ZRevrangebyscoreCmd::PreDo() {
  if (min_score_ == blackwidow::ZSET_SCORE_MAX || max_score_ == blackwidow::ZSET_SCORE_MIN) {
    res_.AppendContent("*0");
    return;
  }
  std::vector<blackwidow::ScoreMember> score_members;
  slash::Status s = g_pika_server->Cache()->ZRevrangebyscore(key_, min_, max_, &score_members);
  if (s.ok()) {
    FitLimit(count_, offset_, score_members.size());
    int64_t index = offset_, end = offset_ + count_;
    if (with_scores_) {
      char buf[32];
      int64_t len;
      res_.AppendArrayLen(count_ * 2);
      for (; index < end; index++) {
        res_.AppendStringLen(score_members[index].member.size());
        res_.AppendContent(score_members[index].member);
        len = slash::d2string(buf, sizeof(buf), score_members[index].score);
        res_.AppendStringLen(len);
        res_.AppendContent(buf);
      }
    } else {
      res_.AppendArrayLen(count_);
      for (; index < end; index++) {
        res_.AppendStringLen(score_members[index].member.size());
        res_.AppendContent(score_members[index].member);
      }
    }
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZRevrangebyscoreCmd::CacheDo() {
  res_.clear();
  Do();
}

void ZRevrangebyscoreCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_ZSET, key_);
  }
}

void ZCountCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZCount);
    return;
  }
  key_ = argv[1];
  min_ = argv[2];
  max_ = argv[3];
  int32_t ret = DoScoreStrRange(argv[2], argv[3], &left_close_, &right_close_, &min_score_, &max_score_);
  if (ret == -1) {
    res_.SetRes(CmdRes::kErrOther, "min or max is not a float");
    return;
  }
  return;
}

void ZCountCmd::Do() {
  if (min_score_ == blackwidow::ZSET_SCORE_MAX || max_score_ == blackwidow::ZSET_SCORE_MIN) {
    res_.AppendContent("*0");
    return;
  }

  int32_t count = 0;
  s_ = g_pika_server->db()->ZCount(key_, min_score_, max_score_, left_close_, right_close_, &count);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void ZCountCmd::PreDo() {
  if (min_score_ == blackwidow::ZSET_SCORE_MAX || max_score_ == blackwidow::ZSET_SCORE_MIN) {
    res_.AppendContent("*0");
    return;
  }

  unsigned long count = 0;
  slash::Status s = g_pika_server->Cache()->ZCount(key_, min_, max_, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZCountCmd::CacheDo() {
  res_.clear();
  Do();
}

void ZCountCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_ZSET, key_);
  }
}

void ZRemCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRem);
    return;
  }
  key_ = argv[1];
  PikaCmdArgsType::iterator iter = argv.begin() + 2;
  members_.assign(iter, argv.end());
  return;
}

void ZRemCmd::Do() {
  s_ = g_pika_server->db()->ZRem(key_, members_, &deleted_);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(deleted_);
	KeyNotExistsRem("z", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void ZRemCmd::CacheDo() {
  Do();
}

void ZRemCmd::PostDo() {
  if (s_.ok() && 0 < deleted_) {
    g_pika_server->Cache()->ZRem(key_, members_);
  }
}

void ZsetUIstoreParentCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  (void)ptr_info;
  dest_key_ = argv[1];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &num_keys_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (num_keys_ < 1) {
    res_.SetRes(CmdRes::kErrOther, "at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE");
    return;
  }
  int argc = argv.size();
  if (argc < num_keys_ + 3) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  keys_.assign(argv.begin() + 3, argv.begin() + 3 + num_keys_);
  weights_.assign(num_keys_, 1);
  int index = num_keys_ + 3;
  while (index < argc) {
    slash::StringToLower(argv[index]);
    if (argv[index] == "weights") {
      index++;
      if (argc < index + num_keys_) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      double weight;
      int base = index;
      for (; index < base + num_keys_; index++) {
        if (!slash::string2d(argv[index].data(), argv[index].size(), &weight)) {
          res_.SetRes(CmdRes::kErrOther, "weight value is not a float");
          return;
        }
        weights_[index-base] = weight;
      }
    } else if (argv[index] == "aggregate") {
      index++;
      if (argc < index + 1) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      slash::StringToLower(argv[index]);
      if (argv[index] == "sum") {
        aggregate_ = blackwidow::SUM;
      } else if (argv[index] == "min") {
        aggregate_ = blackwidow::MIN;
      } else if (argv[index] == "max") {
        aggregate_ = blackwidow::MAX;
      } else {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      index++;
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
  }
  return;
}

void ZUnionstoreCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZUnionstore);
    return;
  }
  ZsetUIstoreParentCmd::DoInitial(argv, NULL);
}

void ZUnionstoreCmd::Do() {
  int32_t count = 0;
  s_ = g_pika_server->db()->ZUnionstore(dest_key_, keys_, weights_, aggregate_, &count);
  if (s_.ok()) {
    if (count > 0) {
      SlotKeyAdd("z", dest_key_);
    } else {
      KeyNotExistsRem("z", dest_key_);
    }
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void ZUnionstoreCmd::CacheDo() {
  Do();
}

void ZUnionstoreCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->Del(dest_key_);
  }
}

void ZInterstoreCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZInterstore);
    return;
  }
  ZsetUIstoreParentCmd::DoInitial(argv, NULL);
  return;
}

void ZInterstoreCmd::Do() {
  int32_t count = 0;
  s_ = g_pika_server->db()->ZInterstore(dest_key_, keys_, weights_, aggregate_, &count);
  if (s_.ok()) {
    if (count > 0) {
      SlotKeyAdd("z", dest_key_);
    } else {
      KeyNotExistsRem("z", dest_key_);
    }
    res_.AppendInteger(count);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void ZInterstoreCmd::CacheDo() {
  Do();
}

void ZInterstoreCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->Del(dest_key_);
  }
}

void ZsetRankParentCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  (void)ptr_info;
  key_ = argv[1];
  member_ = argv[2];
  return;
}

void ZRankCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRank);
    return;
  }
  ZsetRankParentCmd::DoInitial(argv, NULL);
}

void ZRankCmd::Do() {
  int32_t rank = 0;
  s_ = g_pika_server->db()->ZRank(key_, member_, &rank);
  if (s_.ok()) {
    res_.AppendInteger(rank);
  } else if (s_.IsNotFound()){
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void ZRankCmd::PreDo() {
  long rank = 0;
  slash::Status s = g_pika_server->Cache()->ZRank(key_, member_, &rank);
  if (s.ok()) {
    res_.AppendInteger(rank);
  } else if (s.IsNotFound()){
    res_.SetRes(CmdRes::kCacheMiss);
  } else if (s.IsItemNotExist()) {
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZRankCmd::CacheDo() {
  res_.clear();
  Do();
}

void ZRankCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_ZSET, key_);
  }
}

void ZRevrankCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRevrank);
    return;
  }
  ZsetRankParentCmd::DoInitial(argv, NULL);
}

void ZRevrankCmd::Do() {
  int32_t revrank = 0;
  s_ = g_pika_server->db()->ZRevrank(key_, member_, &revrank);
  if (s_.ok()) {
    res_.AppendInteger(revrank);
  } else if (s_.IsNotFound()){
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void ZRevrankCmd::PreDo() {
  long revrank = 0;
  slash::Status s = g_pika_server->Cache()->ZRevrank(key_, member_, &revrank);
  if (s.ok()) {
    res_.AppendInteger(revrank);
  } else if (s.IsNotFound()){
    res_.SetRes(CmdRes::kCacheMiss);
  } else if (s.IsItemNotExist()) {
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZRevrankCmd::CacheDo() {
  res_.clear();
  Do();
}

void ZRevrankCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_ZSET, key_);
  }
}

void ZScoreCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZScore);
    return;
  }
  key_ = argv[1];
  member_ = argv[2];
}

void ZScoreCmd::Do() {
  double score = 0;
  s_ = g_pika_server->db()->ZScore(key_, member_, &score);
  if (s_.ok()) {
    char buf[32];
    int64_t len = slash::d2string(buf, sizeof(buf), score);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);
  } else if (s_.IsNotFound()) {
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void ZScoreCmd::PreDo() {
  double score = 0;
  slash::Status s = g_pika_server->Cache()->ZScore(key_, member_, &score);
  if (s.ok()) {
    char buf[32];
    int64_t len = slash::d2string(buf, sizeof(buf), score);
    res_.AppendStringLen(len);
    res_.AppendContent(buf);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else if (s.IsItemNotExist()) {
    res_.AppendContent("$-1");
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZScoreCmd::CacheDo() {
  res_.clear();
  Do();
}

void ZScoreCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_ZSET, key_);
  }
}

static int32_t DoMemberRange(const std::string &raw_min_member,
                             const std::string &raw_max_member,
                             bool *left_close,
                             bool *right_close,
                             std::string* min_member,
                             std::string* max_member) {
  if (raw_min_member == "-") {
    *min_member = "-";
  } else if (raw_min_member == "+") {
    *min_member = "+";
  } else {
    if (raw_min_member.size() > 0 && raw_min_member.at(0) == '(') {
      *left_close = false;
    } else if (raw_min_member.size() > 0 && raw_min_member.at(0) == '[') {
      *left_close = true;
    } else {
      return -1;
    }
    min_member->assign(raw_min_member.begin() + 1, raw_min_member.end());
  }

  if (raw_max_member == "+") {
    *max_member = "+";
  } else if (raw_max_member == "-") {
    *max_member = "-";
  } else {
    if (raw_max_member.size() > 0 && raw_max_member.at(0) == '(') {
      *right_close = false;
    } else if (raw_max_member.size() > 0 && raw_max_member.at(0) == '[') {
      *right_close = true;
    } else {
      return -1;
    }
    max_member->assign(raw_max_member.begin() + 1, raw_max_member.end());
  }
  return 0;
}

void ZsetRangebylexParentCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  (void)ptr_info;
  key_ = argv[1];
  min_ = argv[2];
  max_ = argv[3];
  int32_t ret = DoMemberRange(argv[2], argv[3], &left_close_, &right_close_, &min_member_, &max_member_);
  if (ret == -1) {
    res_.SetRes(CmdRes::kErrOther, "min or max not valid string range item");
    return;
  }
  size_t argc = argv.size();
  if (argc == 4) {
    return;
  } else if (argc != 7 || slash::StringToLower(argv[4]) != "limit") {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  if (!slash::string2l(argv[5].data(), argv[5].size(), &offset_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (!slash::string2l(argv[6].data(), argv[6].size(), &count_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void ZRangebylexCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRangebylex);
    return;
  }
  ZsetRangebylexParentCmd::DoInitial(argv, NULL); 
}

void ZRangebylexCmd::Do() {
  if (min_member_ == "+" || max_member_ == "-") {
    res_.AppendContent("*0");
    return;
  }
  std::vector<std::string> members;
  s_ = g_pika_server->db()->ZRangebylex(key_, min_member_, max_member_, left_close_, right_close_, &members);
  if (!s_.ok() && !s_.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
    return;
  }
  FitLimit(count_, offset_, members.size());

  res_.AppendArrayLen(count_);
  size_t index = offset_, end = offset_ + count_;
  for (; index < end; index++) {
    res_.AppendStringLen(members[index].size());
    res_.AppendContent(members[index]);
  }
  return;
}

void ZRangebylexCmd::PreDo() {
  if (min_member_ == "+" || max_member_ == "-") {
    res_.AppendContent("*0");
    return;
  }
  std::vector<std::string> members;
  slash::Status s = g_pika_server->Cache()->ZRangebylex(key_, min_, max_, &members);
  if (s.ok()) {
    FitLimit(count_, offset_, members.size());

    res_.AppendArrayLen(count_);
    size_t index = offset_, end = offset_ + count_;
    for (; index < end; index++) {
      res_.AppendStringLen(members[index].size());
      res_.AppendContent(members[index]);
    }
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZRangebylexCmd::CacheDo() {
  res_.clear();
  Do();
}

void ZRangebylexCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_ZSET, key_);
  }
}

void ZRevrangebylexCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRevrangebylex);
    return;
  }
  ZsetRangebylexParentCmd::DoInitial(argv, NULL); 

  std::string tmp_s;
  tmp_s = min_member_;
  min_member_ = max_member_;
  max_member_ = tmp_s;

  bool tmp_b;
  tmp_b = left_close_;
  left_close_ = right_close_;
  right_close_ = tmp_b;
}

void ZRevrangebylexCmd::Do() {
  if (min_member_ == "+" || max_member_ == "-") {
    res_.AppendContent("*0");
    return;
  }
  std::vector<std::string> members;
  s_ = g_pika_server->db()->ZRangebylex(key_, min_member_, max_member_, left_close_, right_close_, &members);
  if (!s_.ok() && !s_.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
    return;
  }
  FitLimit(count_, offset_, members.size());
  
  res_.AppendArrayLen(count_);
  int64_t index = members.size() - 1 - offset_, end = index - count_;
  for (; index > end; index--) {
    res_.AppendStringLen(members[index].size());
    res_.AppendContent(members[index]);
  }
  return;
}

void ZRevrangebylexCmd::PreDo() {
  if (min_member_ == "+" || max_member_ == "-") {
    res_.AppendContent("*0");
    return;
  }
  std::vector<std::string> members;
  slash::Status s = g_pika_server->Cache()->ZRevrangebylex(key_, min_, max_, &members);
  if (s.ok()) {
    FitLimit(count_, offset_, members.size());
    
    res_.AppendArrayLen(count_);
    int64_t index = members.size() - 1 - offset_, end = index - count_;
    for (; index > end; index--) {
      res_.AppendStringLen(members[index].size());
      res_.AppendContent(members[index]);
    }
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZRevrangebylexCmd::CacheDo() {
  res_.clear();
  Do();
}

void ZRevrangebylexCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_ZSET, key_);
  }
}

void ZLexcountCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZLexcount);
    return;
  }
  key_ = argv[1];
  min_ = argv[2];
  max_ = argv[3];
  int32_t ret = DoMemberRange(argv[2], argv[3], &left_close_, &right_close_, &min_member_, &max_member_);
  if (ret == -1) {
    res_.SetRes(CmdRes::kErrOther, "min or max not valid string range item");
    return;
  }
}

void ZLexcountCmd::Do() {
  if (min_member_ == "+" || max_member_ == "-") {
    res_.AppendContent(":0");
    return;
  }
  int32_t count = 0;
  s_ = g_pika_server->db()->ZLexcount(key_, min_member_, max_member_, left_close_, right_close_, &count);
  if (!s_.ok() && !s_.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
    return;
  }
  res_.AppendInteger(count);
  return;
}

void ZLexcountCmd::PreDo() {
  if (min_member_ == "+" || max_member_ == "-") {
    res_.AppendContent(":0");
    return;
  }
  unsigned long count = 0;
  slash::Status s = g_pika_server->Cache()->ZLexcount(key_, min_, max_, &count);
  if (s.ok()) {
    res_.AppendInteger(count);
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kCacheMiss);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

void ZLexcountCmd::CacheDo() {
  res_.clear();
  Do();
}

void ZLexcountCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_ZSET, key_);
  }
}

void ZRemrangebyrankCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRemrangebyrank);
    return;
  }
  key_ = argv[1];
  min_ = argv[2];
  max_ = argv[3];
  if (!slash::string2l(argv[2].data(), argv[2].size(), &start_rank_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (!slash::string2l(argv[3].data(), argv[3].size(), &stop_rank_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
}

void ZRemrangebyrankCmd::Do() {
  int32_t count = 0;
  s_ = g_pika_server->db()->ZRemrangebyrank(key_, start_rank_, stop_rank_, &count);
  if (s_.ok() || s_.IsNotFound()) {
    res_.AppendInteger(count);
    KeyNotExistsRem("z", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
  return;
}

void ZRemrangebyrankCmd::CacheDo() {
  Do();
}

void ZRemrangebyrankCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->ZRemrangebyrank(key_, min_, max_);
  }
}

void ZRemrangebyscoreCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRemrangebyscore);
    return;
  }
  key_ = argv[1];
  min_ = argv[2];
  max_ = argv[3];
  int32_t ret = DoScoreStrRange(argv[2], argv[3], &left_close_, &right_close_, &min_score_, &max_score_);
  if (ret == -1) {
    res_.SetRes(CmdRes::kErrOther, "min or max is not a float");
    return;
  }
  return;
}

void ZRemrangebyscoreCmd::Do() {
  if (min_score_ == blackwidow::ZSET_SCORE_MAX || max_score_ == blackwidow::ZSET_SCORE_MIN) {
    res_.AppendContent(":0");
    return;
  }
  int32_t count = 0;
  s_ = g_pika_server->db()->ZRemrangebyscore(key_, min_score_, max_score_, left_close_, right_close_, &count);
  if (!s_.ok() && !s_.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
    return;
  }
  KeyNotExistsRem("z", key_);
  res_.AppendInteger(count);
  return;
}

void ZRemrangebyscoreCmd::CacheDo() {
  Do();
}

void ZRemrangebyscoreCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->ZRemrangebyscore(key_, min_, max_);
  }
}

void ZRemrangebylexCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZRemrangebylex);
    return;
  }
  key_ = argv[1];
  min_ = argv[2];
  max_ = argv[3];
  int32_t ret = DoMemberRange(argv[2], argv[3], &left_close_, &right_close_, &min_member_, &max_member_);
  if (ret == -1) {
    res_.SetRes(CmdRes::kErrOther, "min or max not valid string range item");
    return;
  }
  return;
}

void ZRemrangebylexCmd::Do() {
  if (min_member_ == "+" || max_member_ == "-") {
    res_.AppendContent("*0");
    return;
  }
  int32_t count = 0;
  s_ = g_pika_server->db()->ZRemrangebylex(key_, min_member_, max_member_, left_close_, right_close_, &count);
  if (!s_.ok() && !s_.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
    return;
  }
  KeyNotExistsRem("z", key_);
  res_.AppendInteger(count);
  return;
}

void ZRemrangebylexCmd::CacheDo() {
  Do();
}

void ZRemrangebylexCmd::PostDo() {
  if (s_.ok()) {
    g_pika_server->Cache()->ZRemrangebylex(key_, min_, max_);
  }
}

