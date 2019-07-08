// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_KV_H_
#define PIKA_KV_H_
#include "pika_command.h"
#include "blackwidow/blackwidow.h"


/*
 * kv
 */
class SetCmd : public Cmd {
 public:
  enum SetCondition{kNONE, kNX, kXX, kVX, kEXORPX};
  SetCmd() : sec_(0), condition_(kNONE) {};
  virtual void Do() override;
  virtual void CacheDo();
  virtual void PostDo();

 private:
  std::string key_;
  std::string value_;
  std::string target_;
  int32_t success_;
  int64_t sec_;
  SetCmd::SetCondition condition_;
  bool has_ttl_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info) override;
  virtual void Clear() override {
    sec_ = 0;
    success_ = 0;
    condition_ = kNONE;
    has_ttl_ = false;
  }
};

class GetCmd : public Cmd {
public:
  GetCmd() {};
  virtual void Do();
  virtual void PreDo();
  virtual void CacheDo();
  virtual void PostDo();
private:
  std::string key_;
  std::string value_;
  int64_t sec_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class DelCmd : public Cmd {
public:
  DelCmd() {}
  virtual void Do();
  virtual void CacheDo();
  virtual void PostDo();
private:
  std::vector<std::string> keys_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const  CmdInfo* const ptr_info);
};

class IncrCmd : public Cmd {
public:
  IncrCmd() {}
  virtual void Do();
  virtual void CacheDo();
  virtual void PostDo();
private:
  std::string key_;
  int64_t new_value_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class IncrbyCmd : public Cmd {
public:
  IncrbyCmd() {}
  virtual void Do();
  virtual void CacheDo();
  virtual void PostDo();
private:
  std::string key_;
  int64_t by_, new_value_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class IncrbyfloatCmd : public Cmd {
public:
  IncrbyfloatCmd() {}
  virtual void Do();
  virtual void CacheDo();
  virtual void PostDo();
private:
  std::string key_, value_, new_value_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class DecrCmd : public Cmd {
public:
  DecrCmd() {}
  virtual void Do();
  virtual void CacheDo();
  virtual void PostDo();
private:
  std::string key_;
  int64_t new_value_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class DecrbyCmd : public Cmd {
public:
  DecrbyCmd() {}
  virtual void Do();
  virtual void CacheDo();
  virtual void PostDo();
private:
  std::string key_;
  int64_t by_, new_value_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class GetsetCmd : public Cmd {
public:
  GetsetCmd() {}
  virtual void Do();
  virtual void CacheDo();
  virtual void PostDo();
private:
  std::string key_;
  std::string new_value_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class AppendCmd : public Cmd {
public:
  AppendCmd() {}
  virtual void Do();
  virtual void CacheDo();
  virtual void PostDo();
private:
  std::string key_;
  std::string value_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class MgetCmd : public Cmd {
public:
  MgetCmd() {}
  virtual void Do();
  virtual void PreDo();
  virtual void CacheDo();
  virtual void PostDo();
private:
  std::vector<std::string> keys_;
  std::string value_;
  int64_t ttl_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class KeysCmd : public Cmd {
public:
  KeysCmd() : type_("all") {}
  virtual void Do();
private:
  std::string pattern_;
  std::string type_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
  virtual void Clear() {
    type_ = "all";
  }
};

class SetnxCmd : public Cmd {
public:
  SetnxCmd() {}
  virtual void Do();
private:
  std::string key_;
  std::string value_;
  int32_t success_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class SetexCmd : public Cmd {
public:
  SetexCmd() {}
  virtual void Do();
  virtual void CacheDo();
  virtual void PostDo();
private:
  std::string key_;
  int64_t sec_;
  std::string value_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class MsetCmd : public Cmd {
public:
  MsetCmd() {}
  virtual void Do();
  virtual void CacheDo();
  virtual void PostDo();
 private:
  std::vector<blackwidow::KeyValue> kvs_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class MsetnxCmd : public Cmd {
public:
  MsetnxCmd() {}
  virtual void Do();
 private:
  std::vector<blackwidow::KeyValue> kvs_;
  int32_t success_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class GetrangeCmd : public Cmd {
public:
  GetrangeCmd() {}
  virtual void Do();
  virtual void PreDo();
  virtual void CacheDo();
  virtual void PostDo();
private:
  std::string key_;
  int64_t start_;
  int64_t end_;
  std::string value_;
  int64_t sec_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class SetrangeCmd : public Cmd {
public:
  SetrangeCmd() {}
  virtual void Do();
  virtual void CacheDo();
  virtual void PostDo();
private:
  std::string key_;
  int64_t offset_;
  std::string value_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class StrlenCmd : public Cmd {
public:
  StrlenCmd() {}
  virtual void Do();
  virtual void PreDo();
  virtual void CacheDo();
  virtual void PostDo();
private:
  std::string key_;
  std::string value_;
  int64_t sec_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class ExistsCmd : public Cmd {
public:
  ExistsCmd() {}
  virtual void Do();
  virtual void PreDo();
  virtual void CacheDo();
private:
  std::vector<std::string> keys_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class ExpireCmd : public Cmd {
public:
  ExpireCmd() {}
  virtual void Do();
  virtual void CacheDo();
  virtual void PostDo();
  virtual std::string ToBinlog() override;
private:
  std::string key_;
  int64_t sec_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) override;
};

class PexpireCmd : public Cmd {
public:
  PexpireCmd() {}
  virtual void Do();
  virtual void CacheDo();
  virtual void PostDo();
  virtual std::string ToBinlog() override;
private:
  std::string key_;
  int64_t msec_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) override;
};

class ExpireatCmd : public Cmd {
public:
  ExpireatCmd() {}
  virtual void Do();
  virtual void CacheDo();
  virtual void PostDo();
private:
  std::string key_;
  int64_t time_stamp_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) override;
};

class PexpireatCmd : public Cmd {
public:
  PexpireatCmd() {}
  virtual void Do();
  virtual void CacheDo();
  virtual void PostDo();
private:
  std::string key_;
  int64_t time_stamp_ms_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) override;
};

class TtlCmd : public Cmd {
public:
  TtlCmd() {}
  virtual void Do();
  virtual void PreDo();
  virtual void CacheDo();
private:
  std::string key_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class PttlCmd : public Cmd {
public:
  PttlCmd() {}
  virtual void Do();
  virtual void PreDo();
  virtual void CacheDo();
private:
  std::string key_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class PersistCmd : public Cmd {
public:
  PersistCmd() {}
  virtual void Do();
  virtual void CacheDo();
  virtual void PostDo();
private:
  std::string key_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class TypeCmd : public Cmd {
public:
  TypeCmd() {}
  virtual void Do();
  virtual void PreDo();
  virtual void CacheDo();
private:
  std::string key_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
};

class ScanCmd : public Cmd {
public:
  ScanCmd() : pattern_("*"), count_(10) {}
  virtual void Do();
private:
  int64_t cursor_;
  std::string pattern_;
  int64_t count_;
  virtual void DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info);
  virtual void Clear() {
    pattern_ = "*";
    count_ = 10;
  }
};
#endif
