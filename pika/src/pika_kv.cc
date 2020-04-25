// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "slash/include/slash_string.h"
#include "pika_kv.h"
#include "pika_server.h"
#include "pika_slot.h"
#include "pika_commonfunc.h"

extern PikaServer *g_pika_server;

void SetCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSet);
    return;
  }
  key_ = argv[1];
  value_ = argv[2];
  condition_ = SetCmd::kNONE;
  sec_ = 0;
  has_ttl_ = false;
  size_t index = 3;
  while (index != argv.size()) {
    std::string opt = argv[index];
    if (!strcasecmp(opt.data(), "xx")) {
      condition_ = SetCmd::kXX;
    } else if (!strcasecmp(opt.data(), "nx")) {
      condition_ = SetCmd::kNX;
    } else if (!strcasecmp(opt.data(), "vx")) {
      condition_ = SetCmd::kVX;
      index++;
      if (index == argv.size()) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      } else {
        target_ = argv[index];
      }
    } else if (!strcasecmp(opt.data(), "ex") || !strcasecmp(opt.data(), "px")) {
      condition_ = (condition_ == SetCmd::kNONE) ? SetCmd::kEXORPX : condition_;
      index++;
      if (index == argv.size()) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!slash::string2l(argv[index].data(), argv[index].size(), &sec_)) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      } else if (sec_ <= 0) {
        res_.SetRes(CmdRes::kErrOther, "invalid expire time in set");
        return;
      }

      if (!strcasecmp(opt.data(), "px")) {
        sec_ /= 1000;
      }
      has_ttl_ = true;
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
  return;
}

void SetCmd::Do() {
  int32_t res = 1;
  switch (condition_) {
    case SetCmd::kXX:
      s_ = g_pika_server->db()->Setxx(key_, value_, &res, sec_);
      break;
    case SetCmd::kNX:
      s_ = g_pika_server->db()->Setnx(key_, value_, &res, sec_);
      break;
    case SetCmd::kVX:
      s_ = g_pika_server->db()->Setvx(key_, target_, value_, &success_, sec_);
      break;
    case SetCmd::kEXORPX:
      s_ = g_pika_server->db()->Setex(key_, value_, sec_);
      break;
    default:
      s_ = g_pika_server->db()->Set(key_, value_);
      break;
  }

  if (s_.ok() || s_.IsNotFound()) {
    if (condition_ == SetCmd::kVX) {
      res_.AppendInteger(success_);
    } else {
      if (res == 1) {
        res_.SetRes(CmdRes::kOk);
      } else {
        res_.AppendArrayLen(-1);;
      }
    }

    //when exec set xx and key IsNotFound
    if (condition_ == SetCmd::kXX && s_.IsNotFound()) {
      return;
    }
    SlotKeyAdd("k", key_);
  } else {
    res_.SetRes(CmdRes::kErrOther, s_.ToString());
  }
}

void SetCmd::CacheDo() {
	Do();
}

void SetCmd::PostDo() {
	if (SetCmd::kNX == condition_) {
		return;
	}

	if (s_.ok()) {
		if (has_ttl_) {
			g_pika_server->Cache()->Setxx(key_, value_, sec_);
		} else {
			g_pika_server->Cache()->SetxxWithoutTTL(key_, value_);
		}
	}
}

void GetCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameGet);
		return;
	}
	key_ = argv[1];
	return;
}

void GetCmd::Do() {
	s_ = g_pika_server->db()->Get(key_, &value_);
	if (s_.ok()) {
		res_.AppendStringLen(value_.size());
		res_.AppendContent(value_);
	} else if (s_.IsNotFound()) {
		res_.AppendStringLen(-1);
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
}

void GetCmd::PreDo() {
	slash::Status s = g_pika_server->Cache()->Get(key_, &value_);
	if (s.ok()) {
		res_.AppendStringLen(value_.size());
		res_.AppendContent(value_);
	} else {
		res_.SetRes(CmdRes::kCacheMiss);
	}
}

void GetCmd::CacheDo() {
	res_.clear();
	s_ = g_pika_server->db()->GetWithTTL(key_, &value_, &sec_);
	if (s_.ok()) {
		res_.AppendStringLen(value_.size());
		res_.AppendContent(value_);
	} else if (s_.IsNotFound()) {
		res_.AppendStringLen(-1);
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
}

void GetCmd::PostDo() {
	if (s_.ok()) {
		g_pika_server->Cache()->WriteKvToCache(key_, value_, sec_);
	}
}

void DelCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameDel);
		return;
	}
	std::vector<std::string>::const_iterator iter = argv.begin();
	keys_.assign(++iter, argv.end());
	return;
}

void DelCmd::Do() {
	std::map<blackwidow::DataType, blackwidow::Status> type_status;
	//must del slots info first, because slots info need to get the type of key
	std::vector<std::string>::const_iterator it;
	for (it = keys_.begin(); it != keys_.end(); it++) {
		SlotKeyRem(*it);
	}
	int64_t count = g_pika_server->db()->Del(keys_, &type_status);
	if (count >= 0) {
		res_.AppendInteger(count);
		s_ = rocksdb::Status::OK();
	} else {
		res_.SetRes(CmdRes::kErrOther, "delete error");
		s_ = rocksdb::Status::Corruption("delete error");
	}
	return;
}

void DelCmd::CacheDo() {
	if (1 < keys_.size()) {
		res_.SetRes(CmdRes::kErrOther, "only can delete one key in cache model");
		return;
	}

	Do();
}

void DelCmd::PostDo() {
	if (s_.ok()) {
		g_pika_server->Cache()->Del(keys_[0]);
	}
}


void IncrCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameIncr);
		return;
	}
	key_ = argv[1];
	return;
}

void IncrCmd::Do() {
	s_ = g_pika_server->db()->Incrby(key_, 1, &new_value_);
	if (s_.ok()) {
		res_.AppendContent(":" + std::to_string(new_value_));
		SlotKeyAdd("k", key_);
	} else if (s_.IsCorruption() && s_.ToString() == "Corruption: Value is not a integer") {
		res_.SetRes(CmdRes::kInvalidInt);
	} else if (s_.IsInvalidArgument()) {
		res_.SetRes(CmdRes::kOverFlow);
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
	return;
}

void IncrCmd::CacheDo() {
	Do();
}

void IncrCmd::PostDo() {
	if (s_.ok()) {
		g_pika_server->Cache()->Incrxx(key_);
	}
}

void IncrbyCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameIncrby);
		return;
	}
	key_ = argv[1];
	if (!slash::string2l(argv[2].data(), argv[2].size(), &by_)) {
		res_.SetRes(CmdRes::kInvalidInt, kCmdNameIncrby);
		return;
	}
	return;
}

void IncrbyCmd::Do() {
	s_ = g_pika_server->db()->Incrby(key_, by_, &new_value_);
	if (s_.ok()) {
		res_.AppendContent(":" + std::to_string(new_value_));
		SlotKeyAdd("k", key_);
	} else if (s_.IsCorruption() && s_.ToString() == "Corruption: Value is not a integer") {
		res_.SetRes(CmdRes::kInvalidInt);
	} else if (s_.IsInvalidArgument()) {
		res_.SetRes(CmdRes::kOverFlow);
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
	return;
}

void IncrbyCmd::CacheDo() {
	Do();
}

void IncrbyCmd::PostDo() {
	if (s_.ok()) {
		g_pika_server->Cache()->IncrByxx(key_, by_);
	}
}

void IncrbyfloatCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameIncrbyfloat);
		return;
	}
	key_ = argv[1];
	value_ = argv[2];
	return;
}

void IncrbyfloatCmd::Do() {
	s_ = g_pika_server->db()->Incrbyfloat(key_, value_, &new_value_);
	if (s_.ok()) {
		res_.AppendStringLen(new_value_.size());
		res_.AppendContent(new_value_);
		SlotKeyAdd("k", key_);
	} else if (s_.IsCorruption() && s_.ToString() == "Corruption: Value is not a vaild float"){
		res_.SetRes(CmdRes::kInvalidFloat);
	} else if (s_.IsInvalidArgument()) {
		res_.SetRes(CmdRes::kOverFlow);
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
	return;
}

void IncrbyfloatCmd::CacheDo() {
	Do();
}

void IncrbyfloatCmd::PostDo() {
	if (s_.ok()) {
		long double long_double_by;
		if (blackwidow::StrToLongDouble(value_.data(), value_.size(), &long_double_by) != -1) {
			g_pika_server->Cache()->Incrbyfloatxx(key_, long_double_by);
		}
	}
}

void DecrCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameDecr);
		return;
	}
	key_ = argv[1];
	return;
}

void DecrCmd::Do() {
	s_ = g_pika_server->db()->Decrby(key_, 1, &new_value_);
	if (s_.ok()) {
		SlotKeyAdd("k", key_);
		res_.AppendContent(":" + std::to_string(new_value_));
	} else if (s_.IsCorruption() && s_.ToString() == "Corruption: Value is not a integer") {
		res_.SetRes(CmdRes::kInvalidInt);
	} else if (s_.IsInvalidArgument()) {
		res_.SetRes(CmdRes::kOverFlow);
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
	return;
}

void DecrCmd::CacheDo() {
	Do();
}

void DecrCmd::PostDo() {
	if (s_.ok()) {
		g_pika_server->Cache()->Decrxx(key_);
	}
}

void DecrbyCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameDecrby);
		return;
	}
	key_ = argv[1];
	if (!slash::string2l(argv[2].data(), argv[2].size(), &by_)) {
		res_.SetRes(CmdRes::kInvalidInt);
		return;
	}
	return;
}

void DecrbyCmd::Do() {
	s_ = g_pika_server->db()->Decrby(key_, by_, &new_value_);
	if (s_.ok()) {
		SlotKeyAdd("k", key_);
		res_.AppendContent(":" + std::to_string(new_value_));
	} else if (s_.IsCorruption() && s_.ToString() == "Corruption: Value is not a integer") {
		res_.SetRes(CmdRes::kInvalidInt);
	} else if (s_.IsInvalidArgument()) {
		res_.SetRes(CmdRes::kOverFlow);
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
	return;
}

void DecrbyCmd::CacheDo() {
	Do();
}

void DecrbyCmd::PostDo() {
	if (s_.ok()) {
		g_pika_server->Cache()->DecrByxx(key_, by_);
	}
}

void GetsetCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameGetset);
		return;
	}
	key_ = argv[1];
	new_value_ = argv[2];
	return;
}

void GetsetCmd::Do() {
	std::string old_value;
	s_ = g_pika_server->db()->GetSet(key_, new_value_, &old_value);
	if (s_.ok()) {
		if (old_value.empty()) {
			res_.AppendContent("$-1");
		} else {
			res_.AppendStringLen(old_value.size());
			res_.AppendContent(old_value);
		}
		SlotKeyAdd("k", key_);
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
	return;
}

void GetsetCmd::CacheDo() {
	Do();
}

void GetsetCmd::PostDo() {
	if (s_.ok()) {
		g_pika_server->Cache()->SetxxWithoutTTL(key_, new_value_);
	}
}

void AppendCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameAppend);
		return;
	}
	key_ = argv[1];
	value_ = argv[2];
	return;
}

void AppendCmd::Do() {
	int32_t new_len = 0;
	s_ = g_pika_server->db()->Append(key_, value_, &new_len);
	if (s_.ok() || s_.IsNotFound()) {
		res_.AppendInteger(new_len);
		SlotKeyAdd("k", key_);
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
	return;
}

void AppendCmd::CacheDo() {
	Do();
}

void AppendCmd::PostDo() {
	if (s_.ok()) {
		g_pika_server->Cache()->Appendxx(key_, value_);
	}
}

void MgetCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameMget);
		return;
	}
	keys_ = argv;
	keys_.erase(keys_.begin());
	return;
}

void MgetCmd::Do() {
	std::vector<blackwidow::ValueStatus> vss;
	s_ = g_pika_server->db()->MGet(keys_, &vss);
	if (s_.ok()) {
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

void MgetCmd::PreDo() {
	if (1 < keys_.size()) {
		res_.SetRes(CmdRes::kErrOther, "only can get one key in cache model");
		return;
	}

	slash::Status s = g_pika_server->Cache()->Get(keys_[0], &value_);
	if (s.ok()) {
		res_.AppendArrayLen(1);
		res_.AppendStringLen(value_.size());
		res_.AppendContent(value_);
	} else {
		res_.SetRes(CmdRes::kCacheMiss);
	}
}

void MgetCmd::CacheDo() {
	res_.clear();
	s_ = g_pika_server->db()->GetWithTTL(keys_[0], &value_, &ttl_);
	res_.AppendArrayLen(1);
	if (s_.ok()) {
		res_.AppendStringLen(value_.size());
		res_.AppendContent(value_);
	} else if (s_.IsNotFound()) {
		res_.AppendContent("$-1");
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
}

void MgetCmd::PostDo() {
	if (s_.ok()) {
		g_pika_server->Cache()->WriteKvToCache(keys_[0], value_, ttl_);
	}
}

void KeysCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameKeys);
		return;
	}
	pattern_ = argv[1];
	if (argv.size() == 3) {
		std::string opt = argv[2];
		slash::StringToLower(opt);
		if (opt == "string" || opt == "zset" || opt == "set" || opt == "list" || opt == "hash" || opt == "ehash") {
			type_ = opt;
		} else {
			res_.SetRes(CmdRes::kSyntaxErr);
		}
	} else if (argv.size() > 3) {
		res_.SetRes(CmdRes::kSyntaxErr);
	}
	return;
}

void KeysCmd::Do() {
	std::vector<std::string> keys;
	s_ = g_pika_server->db()->Keys(type_, pattern_, &keys);
	res_.AppendArrayLen(keys.size());
	for (const auto& key : keys) {
		res_.AppendString(key);
	}
	return;
}

void SetnxCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameSetnx);
		return;
	}
	key_ = argv[1];
	value_ = argv[2];
	return;
}

void SetnxCmd::Do() {
	success_ = 0;
	s_ = g_pika_server->db()->Setnx(key_, value_, &success_);
	if (s_.ok()) {
		res_.AppendInteger(success_);
		SlotKeyAdd("k", key_);
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
	return;
}

void SetexCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameSetex);
		return;
	}
	key_ = argv[1];
	if (!slash::string2l(argv[2].data(), argv[2].size(), &sec_)) {
		res_.SetRes(CmdRes::kInvalidInt);
		return;
	}
	value_ = argv[3];
	return;
}

void SetexCmd::Do() {
	s_ = g_pika_server->db()->Setex(key_, value_, sec_);
	if (s_.ok()) {
		res_.SetRes(CmdRes::kOk);
		SlotKeyAdd("k", key_);
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
}

void SetexCmd::CacheDo() {
	Do();
}

void SetexCmd::PostDo() {
	if (s_.ok()) {
		g_pika_server->Cache()->Setxx(key_, value_, sec_);
	}
}

void MsetCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameMset);
		return;
	}
	size_t argc = argv.size();
	if (argc % 2 == 0) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameMset);
		return;
	}
	kvs_.clear();
	for (size_t index = 1; index != argc; index += 2) {
		kvs_.push_back({argv[index], argv[index+1]});
	}
	return;
}

void MsetCmd::Do() {
	s_ = g_pika_server->db()->MSet(kvs_);
	if (s_.ok()) {
		res_.SetRes(CmdRes::kOk);
		for (const auto& kv : kvs_) {
			SlotKeyAdd("k", kv.key);
		}
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
}

void MsetCmd::CacheDo() {
	if (1 < kvs_.size()) {
		res_.SetRes(CmdRes::kErrOther, "only can mset one kv in cache model");
		return;
	}

	Do();
}

void MsetCmd::PostDo() {
	if (s_.ok()) {
		g_pika_server->Cache()->SetxxWithoutTTL(kvs_[0].key, kvs_[0].value);
	}
}

void MsetnxCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameMsetnx);
		return;
	}
	size_t argc = argv.size();
	if (argc % 2 == 0) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameMsetnx);
		return;
	}
	kvs_.clear();
	for (size_t index = 1; index != argc; index += 2) {
		kvs_.push_back({argv[index], argv[index+1]});
	}
	return;
}

void MsetnxCmd::Do() {
	success_ = 0;
	s_ = g_pika_server->db()->MSetnx(kvs_, &success_);
	if (s_.ok()) {
		res_.AppendInteger(success_);
		for (const auto& kv : kvs_) {
			SlotKeyAdd("k", kv.key);
		}
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
}

void GetrangeCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameGetrange);
		return;
	}
	key_ = argv[1];
	if (!slash::string2l(argv[2].data(), argv[2].size(), &start_)) {
		res_.SetRes(CmdRes::kInvalidInt);
		return;
	}
	if (!slash::string2l(argv[3].data(), argv[3].size(), &end_)) {
		res_.SetRes(CmdRes::kInvalidInt);
		return;
	}
	return;
}

void GetrangeCmd::Do() {
	std::string substr;
	s_ = g_pika_server->db()->Getrange(key_, start_, end_, &substr);
	if (s_.ok() || s_.IsNotFound()) {
		res_.AppendStringLen(substr.size());
		res_.AppendContent(substr);
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
}

void GetrangeCmd::PreDo() {
	std::string substr;
	slash::Status s = g_pika_server->Cache()->GetRange(key_, start_, end_, &substr);
	if (s.ok()) {
		res_.AppendStringLen(substr.size());
		res_.AppendContent(substr);
	} else {
		res_.SetRes(CmdRes::kCacheMiss);
	}	
}

void GetrangeCmd::CacheDo() {
	res_.clear();
	std::string substr;
	s_ = g_pika_server->db()->GetrangeWithValue(key_, start_, end_, &substr, &value_, &sec_);
	if (s_.ok()) {
		res_.AppendStringLen(substr.size());
		res_.AppendContent(substr);
	} else if (s_.IsNotFound()) {
		res_.AppendStringLen(substr.size());
		res_.AppendContent(substr);
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
}

void GetrangeCmd::PostDo() {
	if (s_.ok()) {
		g_pika_server->Cache()->WriteKvToCache(key_, value_, sec_);
	}
}

void SetrangeCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameSetrange);
		return;
	}
	key_ = argv[1];
	if (!slash::string2l(argv[2].data(), argv[2].size(), &offset_)) {
		res_.SetRes(CmdRes::kInvalidInt);
		return;
	}
	value_ = argv[3];
	return;
}

void SetrangeCmd::Do() {
	int32_t new_len;
	s_ = g_pika_server->db()->Setrange(key_, offset_, value_, &new_len);
	if (s_.ok()) {
		res_.AppendInteger(new_len);
		SlotKeyAdd("k", key_);
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
	return;
}

void SetrangeCmd::CacheDo() {
	Do();
}

void SetrangeCmd::PostDo() {
	if (s_.ok()) {
		g_pika_server->Cache()->SetRangexx(key_, offset_, value_);
	}
}

void StrlenCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameStrlen);
		return;
	}
	key_ = argv[1];
	return;
}

void StrlenCmd::Do() {
	int32_t len = 0;
	s_ = g_pika_server->db()->Strlen(key_, &len);
	if (s_.ok() || s_.IsNotFound()) {
		res_.AppendInteger(len);
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
	return;
}

void StrlenCmd::PreDo() {
	int32_t len = 0;
	slash::Status s = g_pika_server->Cache()->Strlen(key_, &len);
	if (s.ok()) {
		res_.AppendInteger(len);
	} else {
		res_.SetRes(CmdRes::kCacheMiss);
	}	
}

void StrlenCmd::CacheDo() {
	res_.clear();
	s_ = g_pika_server->db()->GetWithTTL(key_, &value_, &sec_);
	if (s_.ok() || s_.IsNotFound()) {
		res_.AppendInteger(value_.size());
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
}

void StrlenCmd::PostDo() {
	if (s_.ok()) {
		g_pika_server->Cache()->WriteKvToCache(key_, value_, sec_);
	}
}

void ExistsCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameExists);
		return;
	}
	keys_ = argv;
	keys_.erase(keys_.begin());
	return;
}

void ExistsCmd::Do() {
	std::map<blackwidow::DataType, rocksdb::Status> type_status;
	int64_t res = g_pika_server->db()->Exists(keys_, &type_status);
	if (res != -1) {
		res_.AppendInteger(res);
	} else {
		res_.SetRes(CmdRes::kErrOther, "exists internal error");
	}
	return;
}

void ExistsCmd::PreDo() {
	if (1 < keys_.size()) {
		res_.SetRes(CmdRes::kCacheMiss);
		return;
	}

	bool exist = g_pika_server->Cache()->Exists(keys_[0]);
	if (exist) {
		res_.AppendInteger(1);
	} else {
		res_.SetRes(CmdRes::kCacheMiss);
	}
}

void ExistsCmd::CacheDo() {
	res_.clear();
	Do();
}

void ExpireCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameExpire);
		return;
	}
	key_ = argv[1];
	if (!slash::string2l(argv[2].data(), argv[2].size(), &sec_)) {
		res_.SetRes(CmdRes::kInvalidInt);
		return;
	}
	return;
}

void ExpireCmd::Do() {
	std::map<blackwidow::DataType, rocksdb::Status> type_status;
	int64_t res = g_pika_server->db()->Expire(key_, sec_, &type_status);
	if (res != -1) {
		res_.AppendInteger(res);
		s_ = rocksdb::Status::OK();
	} else {
		res_.SetRes(CmdRes::kErrOther, "expire internal error");
		s_ = rocksdb::Status::Corruption("expire internal error");
	}
	return;
}

std::string ExpireCmd::ToBinlog() {
	std::string content;
	content.reserve(RAW_ARGS_LEN);
	RedisAppendLen(content, 3, "*");

	// to expireat cmd
	std::string expireat_cmd("expireat");
	RedisAppendLen(content, expireat_cmd.size(), "$");
	RedisAppendContent(content, expireat_cmd);
	// key
	RedisAppendLen(content, key_.size(), "$");
	RedisAppendContent(content, key_);
	// sec
	char buf[100];
	int64_t expireat = time(nullptr) + sec_;
	slash::ll2string(buf, 100, expireat);
	std::string at(buf);
	RedisAppendLen(content, at.size(), "$");
	RedisAppendContent(content, at);
	return content;
}

void ExpireCmd::CacheDo() {
	Do();
}

void ExpireCmd::PostDo() {
	if (s_.ok()) {
		g_pika_server->Cache()->Expire(key_, sec_);
	}
}

void PexpireCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNamePexpire);
		return;
	}
	key_ = argv[1];
	if (!slash::string2l(argv[2].data(), argv[2].size(), &msec_)) {
		res_.SetRes(CmdRes::kInvalidInt);
		return;
	}
	return;
}

void PexpireCmd::Do() {
	std::map<blackwidow::DataType, rocksdb::Status> type_status;
	int64_t res = g_pika_server->db()->Expire(key_, msec_/1000, &type_status);
	if (res != -1) {
		res_.AppendInteger(res);
		s_ = rocksdb::Status::OK();
	} else {
		res_.SetRes(CmdRes::kErrOther, "expire internal error");
		s_ = rocksdb::Status::Corruption("expire internal error");
	}
	return;
}

std::string PexpireCmd::ToBinlog() {
  std::string content;
  content.reserve(RAW_ARGS_LEN);
  RedisAppendLen(content, 3, "*");

  // to expireat cmd
  std::string expireat_cmd("expireat");
  RedisAppendLen(content, expireat_cmd.size(), "$");
  RedisAppendContent(content, expireat_cmd);
  // key
  RedisAppendLen(content, key_.size(), "$");
  RedisAppendContent(content, key_);
  // sec
  char buf[100];
  int64_t expireat = time(nullptr) + msec_ / 1000;
  slash::ll2string(buf, 100, expireat);
  std::string at(buf);
  RedisAppendLen(content, at.size(), "$");
  RedisAppendContent(content, at);

  return content;
}

void PexpireCmd::CacheDo() {
	Do();
}

void PexpireCmd::PostDo() {
	if (s_.ok()) {
		g_pika_server->Cache()->Expire(key_, msec_/1000);
	}
}

void ExpireatCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameExpireat);
		return;
	}
	key_ = argv[1];
	if (!slash::string2l(argv[2].data(), argv[2].size(), &time_stamp_)) {
		res_.SetRes(CmdRes::kInvalidInt);
		return;
	}
	return;
}

void ExpireatCmd::Do() {
	std::map<blackwidow::DataType, rocksdb::Status> type_status;
	int32_t res = g_pika_server->db()->Expireat(key_, time_stamp_, &type_status);
	if (res != -1) {
		res_.AppendInteger(res);
		s_ = rocksdb::Status::OK();
	} else {
		res_.SetRes(CmdRes::kErrOther, "expireat internal error");
		s_ = rocksdb::Status::Corruption("expireat internal error");
	}
}

void ExpireatCmd::CacheDo() {
	Do();
}

void ExpireatCmd::PostDo() {
	if (s_.ok()) {
		g_pika_server->Cache()->Expireat(key_, time_stamp_);
	}
}

void PexpireatCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNamePexpireat);
		return;
	}
	key_ = argv[1];
	if (!slash::string2l(argv[2].data(), argv[2].size(), &time_stamp_ms_)) {
		res_.SetRes(CmdRes::kInvalidInt);
		return;
	}
	return;
}

void PexpireatCmd::Do() {
	std::map<blackwidow::DataType, rocksdb::Status> type_status;
	int32_t res = g_pika_server->db()->Expireat(key_, time_stamp_ms_/1000, &type_status);
	if (res != -1) {
		res_.AppendInteger(res);
		s_ = rocksdb::Status::OK();
	} else {
		res_.SetRes(CmdRes::kErrOther, "pexpireat internal error");
		s_ = rocksdb::Status::Corruption("pexpireat internal error");
	}
	return;
}

void PexpireatCmd::CacheDo() {
	Do();
}

void PexpireatCmd::PostDo() {
	if (s_.ok()) {
		g_pika_server->Cache()->Expireat(key_, time_stamp_ms_/1000);
	}
}

void TtlCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameTtl);
		return;
	}
	key_ = argv[1];
	return;
}

void TtlCmd::Do() {
	std::map<blackwidow::DataType, int64_t> type_timestamp;
	std::map<blackwidow::DataType, rocksdb::Status> type_status;
	type_timestamp = g_pika_server->db()->TTL(key_, &type_status);
	for (const auto& item : type_timestamp) {
		// mean operation exception errors happen in database
		if (item.second == -3) {
			res_.SetRes(CmdRes::kErrOther, "ttl internal error");
			return;
		} else if (item.second != -2) {
			res_.AppendInteger(item.second);
			return;
		}
	}
	
	// mean this key not exist
	res_.AppendInteger(-2);
	return;
}

void TtlCmd::PreDo() {
	int64_t ttl;
	slash::Status s = g_pika_server->Cache()->TTL(key_, &ttl);
	if (s.ok()) {
		res_.AppendInteger(ttl);
	} else {
		res_.SetRes(CmdRes::kCacheMiss);
	}
}

void TtlCmd::CacheDo() {
	res_.clear();
	Do();
}

void PttlCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNamePttl);
		return;
	}
	key_ = argv[1];
	return;
}

void PttlCmd::Do() {
	std::map<blackwidow::DataType, int64_t> type_timestamp;
	std::map<blackwidow::DataType, rocksdb::Status> type_status;
	type_timestamp = g_pika_server->db()->TTL(key_, &type_status);
	for (const auto& item : type_timestamp) {
		// mean operation exception errors happen in database
		if (item.second == -3) {
			res_.SetRes(CmdRes::kErrOther, "ttl internal error");
			return;
		} else if (item.second != -2) {
			if (item.second == -1) {
				res_.AppendInteger(-1);
			} else {
				res_.AppendInteger(item.second * 1000);
			}
			return;
		}
	}

	// mean this key not exist
	res_.AppendInteger(-2);
	return;
}

void PttlCmd::PreDo() {
	int64_t ttl;
	slash::Status s = g_pika_server->Cache()->TTL(key_, &ttl);
	if (s.ok()) {
		res_.AppendInteger(ttl * 1000);
	} else {
		res_.SetRes(CmdRes::kCacheMiss);
	}
}

void PttlCmd::CacheDo() {
	res_.clear();
	Do();
}

void PersistCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNamePersist);
		return;
	}
	key_ = argv[1];
	return;
}

void PersistCmd::Do() {
	std::map<blackwidow::DataType, rocksdb::Status> type_status;
	int32_t res = g_pika_server->db()->Persist(key_, &type_status);
	if (res != -1) {
		res_.AppendInteger(res);
		s_ = rocksdb::Status::OK();
	} else {
		res_.SetRes(CmdRes::kErrOther, "persist internal error");
		s_ = rocksdb::Status::Corruption("persist internal error");
	}
	return;
}

void PersistCmd::CacheDo() {
	Do();
}

void PersistCmd::PostDo() {
	if (s_.ok()) {
		g_pika_server->Cache()->Persist(key_);
	}
}

void TypeCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameType);
		return;
	}
	key_ = argv[1];
	return;
}

void TypeCmd::Do() {
	std::string res;
	s_ = g_pika_server->db()->Type(key_, &res);
	if (s_.ok()) {
		res_.AppendContent("+" + res);
	} else {
		res_.SetRes(CmdRes::kErrOther, s_.ToString());
	}
	return;
}

void TypeCmd::PreDo() {
	std::string type;
	slash::Status s = g_pika_server->Cache()->Type(key_, &type);
	if (s.ok()) {
		res_.AppendContent("+" + type);
	} else {
		res_.SetRes(CmdRes::kCacheMiss);
	}
}

void TypeCmd::CacheDo() {
	res_.clear();
	Do();
}

void ScanCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameScan);
		return;
	}
	if (!slash::string2l(argv[1].data(), argv[1].size(), &cursor_)) {
		res_.SetRes(CmdRes::kInvalidInt);
		return;
	}
	size_t index = 2, argc = argv.size();

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
			} else if (!slash::string2l(argv[index].data(), argv[index].size(), &count_) || count_ <= 0) {
				res_.SetRes(CmdRes::kInvalidInt);
				return;
			}
		} else {
			res_.SetRes(CmdRes::kSyntaxErr);
			return;
		}
		index++;
	}
	return;
}

void ScanCmd::Do() {
	std::vector<std::string> keys;
	int64_t cursor_ret = g_pika_server->db()->Scan(cursor_, pattern_, count_, &keys);
  
	res_.AppendArrayLen(2);

	char buf[32];
	int len = slash::ll2string(buf, sizeof(buf), cursor_ret);
	res_.AppendStringLen(len);
	res_.AppendContent(buf);

	res_.AppendArrayLen(keys.size());
	std::vector<std::string>::const_iterator iter;
	for (iter = keys.begin(); iter != keys.end(); iter++) {
		res_.AppendStringLen(iter->size());
		res_.AppendContent(*iter);
	}
	return;
}
