#include <glog/logging.h>

#include "slash/include/slash_string.h"
#include "pika_ehash.h"
#include "pika_server.h"
#include "pika_slot.h"

extern PikaServer *g_pika_server;


void EhsetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhset);
        return;
    }
    key_ = argv[1];
    field_ = argv[2];
    value_ = argv[3];
    return;
}

void EhsetCmd::Do() {
    int32_t ret = 0;
    s_ = g_pika_server->db()->Ehset(key_, field_, value_, &ret);
    if (s_.ok()) {
        res_.AppendContent(":" + std::to_string(ret));
        SlotKeyAdd("e", key_);
    } else {
        res_.SetRes(CmdRes::kErrOther, s_.ToString());
    }
    return;
}

void EhsetnxCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhsetnx);
        return;
    }
    key_ = argv[1];
    field_ = argv[2];
    value_ = argv[3];
    return;
}

void EhsetnxCmd::Do() {
    int32_t ret = 0;
    s_ = g_pika_server->db()->Ehsetnx(key_, field_, value_, &ret);
    if (s_.ok()) {
        res_.AppendContent(":" + std::to_string(ret));
        SlotKeyAdd("e", key_);
    } else {
        res_.SetRes(CmdRes::kErrOther, s_.ToString());
    }
    return;
}

void EhsetexCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhsetex);
        return;
    }
    key_ = argv[1];
    field_ = argv[2];
    value_ = argv[3];

    if (!slash::string2l(argv[4].data(), argv[4].size(), &sec_)) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }
    return;
}

void EhsetexCmd::Do() {
    s_ = g_pika_server->db()->Ehsetex(key_, field_, value_, sec_);
    if (s_.ok()) {
        SlotKeyAdd("e", key_);
        res_.SetRes(CmdRes::kOk);
    } else {
        res_.SetRes(CmdRes::kErrOther, s_.ToString());
    }
    return;
}

void EhexpireCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhexpire);
        return;
    }
    key_ = argv[1];
    field_ = argv[2];

    if (!slash::string2l(argv[3].data(), argv[3].size(), &sec_)) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }

    if (sec_ <= 0) {
        res_.SetRes(CmdRes::kErrOther, "invalid expire time");
        return;
    }

    return;
}

void EhexpireCmd::Do() {
    int32_t res = g_pika_server->db()->Ehexpire(key_, field_, sec_);
    if (res != -1) {
        res_.AppendInteger(res);
    } else {
        res_.SetRes(CmdRes::kErrOther, "EhexpireCmd internal error");
    }
    return;
}

void EhexpireatCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhexpireat);
        return;
    }
    key_ = argv[1];
    field_ = argv[2];

    if (!slash::string2l(argv[3].data(), argv[3].size(), &time_stamp_)) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }

    if (time_stamp_ <= 0) {
        res_.SetRes(CmdRes::kErrOther, "invalid expire time");
        return;
    }
    
    return;
}

void EhexpireatCmd::Do() {
    int32_t res = g_pika_server->db()->Ehexpireat(key_, field_, time_stamp_);
    if (res != -1) {
        res_.AppendInteger(res);
    } else {
        res_.SetRes(CmdRes::kErrOther, "EhexpireatCmd internal error");
    }
    return;
}


void EhttlCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhttl);
        return;
    }
    key_ = argv[1];
    field_ = argv[2];
    return;
}

void EhttlCmd::Do() {
    int64_t ttl = g_pika_server->db()->Ehttl(key_, field_);
    if (ttl == -3) {
        res_.AppendInteger(-2);
    } else {
        res_.AppendInteger(ttl);
    }
    return;
}

void EhpersistCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhpersist);
        return;
    }
    key_ = argv[1];
    field_ = argv[2];
    return;
}

void EhpersistCmd::Do() {
    int32_t res = g_pika_server->db()->Ehpersist(key_, field_);
    if (res != -1) {
        res_.AppendInteger(res);
    } else {
        res_.SetRes(CmdRes::kErrOther, "EhpersistCmd internal error");
    }
    return;
}

void EhgetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhget);
        return;
    }
    key_ = argv[1];
    field_ = argv[2];
    return;
}

void EhgetCmd::Do() {
    std::string value;
    s_ = g_pika_server->db()->Ehget(key_, field_, &value);
    if (s_.ok()) {
        res_.AppendStringLen(value.size());
        res_.AppendContent(value);
    } else if (s_.IsNotFound()) {
        res_.AppendContent("$-1");
    } else {
        res_.SetRes(CmdRes::kErrOther, s_.ToString());
    }
}

void EhexistsCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhexists);
        return;
    }
    key_ = argv[1];
    field_ = argv[2];
    return;
}

void EhexistsCmd::Do() {
    s_ = g_pika_server->db()->Ehexists(key_, field_);
    if (s_.ok()) {
        res_.AppendContent(":1");
    } else if (s_.IsNotFound()) {
        res_.AppendContent(":0");
    } else {
        res_.SetRes(CmdRes::kErrOther, s_.ToString());
    }
}

void EhdelCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhdel);
        return;
    }
    key_ = argv[1];
    PikaCmdArgsType::iterator iter = argv.begin();
    iter++; 
    iter++;
    fields_.assign(iter, argv.end());
    return;
}

void EhdelCmd::Do() {
    int32_t deleted = 0;
    s_ = g_pika_server->db()->Ehdel(key_, fields_, &deleted);
    if (s_.ok() || s_.IsNotFound()) {
        res_.AppendInteger(deleted);
        KeyNotExistsRem("e", key_);
    } else {
        res_.SetRes(CmdRes::kErrOther, s_.ToString());
    }
    return;
}

void EhlenCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    (void)ptr_info;

    is_force_ = false;
    if (argv.size() == 3 && slash::StringToLower(argv[2]) == "force") {
        is_force_ = true;
    } else if (argv.size() != 2) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
    }

    key_ = argv[1];
    return;
}

void EhlenCmd::Do() {
    int32_t len = 0;

    if (is_force_) {
        s_ = g_pika_server->db()->EhlenForce(key_, &len);
    } else {
        s_ = g_pika_server->db()->Ehlen(key_, &len);
    }
    
    if (s_.ok() || s_.IsNotFound()) {
        res_.AppendInteger(len);
    } else {
        res_.SetRes(CmdRes::kErrOther, "something wrong in Ehlen");
    }
    return;
}

void EhstrlenCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhstrlen);
        return;
    }
    key_ = argv[1];
    field_ = argv[2];
    return;
}

void EhstrlenCmd::Do() {
    int32_t len = 0;
    s_ = g_pika_server->db()->Ehstrlen(key_, field_, &len);
    if (s_.ok() || s_.IsNotFound()) {
        res_.AppendInteger(len);
    } else {
        res_.SetRes(CmdRes::kErrOther, "something wrong in EhstrlenCmd");
    }
    return;
}

void EhincrbyCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhincrby);
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

void EhincrbyCmd::Do() {
    int64_t new_value;
    s_ = g_pika_server->db()->Ehincrby(key_, field_, by_, &new_value);
    if (s_.ok() || s_.IsNotFound()) {
        res_.AppendContent(":" + std::to_string(new_value));
        SlotKeyAdd("e", key_);
    } else if (s_.IsCorruption() && s_.ToString() == "Corruption: hash value is not an integer") {
        res_.SetRes(CmdRes::kInvalidInt);
    } else if (s_.IsInvalidArgument()) {
        res_.SetRes(CmdRes::kOverFlow);
    } else {
        res_.SetRes(CmdRes::kErrOther, s_.ToString());
    }
    return;
}

void EhincrbyfloatCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhincrbyfloat);
        return;
    }
    key_ = argv[1];
    field_ = argv[2];
    by_ = argv[3];
    return;
}

void EhincrbyfloatCmd::Do() {
    std::string new_value;
    s_ = g_pika_server->db()->Ehincrbyfloat(key_, field_, by_, &new_value);
    if (s_.ok()) {
        res_.AppendStringLen(new_value.size());
        res_.AppendContent(new_value);
        SlotKeyAdd("e", key_);
    } else if (s_.IsCorruption() && s_.ToString() == "Corruption: value is not a vaild float") {
        res_.SetRes(CmdRes::kInvalidFloat);
    } else if (s_.IsInvalidArgument()) {
        res_.SetRes(CmdRes::kOverFlow);
    } else {
        res_.SetRes(CmdRes::kErrOther, s_.ToString());
    }
    return;
}

void EhmsetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhmset);
        return;
    }
    key_ = argv[1];
    size_t argc = argv.size();
    if (argc % 2 != 0) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhmset);
        return;
    }
    size_t index = 2;
    fvs_.clear();
    for (; index < argc; index += 2) {
        fvs_.push_back({argv[index], argv[index + 1]});
    }
    return;
}

void EhmsetCmd::Do() {
    s_ = g_pika_server->db()->Ehmset(key_, fvs_);
    if (s_.ok()) {
        res_.SetRes(CmdRes::kOk);
        SlotKeyAdd("e", key_);
    } else {
        res_.SetRes(CmdRes::kErrOther, s_.ToString());
    }
    return;
}

void EhmsetexCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhmsetex);
        return;
    }
    key_ = argv[1];
    size_t argc = argv.size() - 2;
    if (argc % 3 != 0) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhmsetex);
        return;
    }
    size_t index = 2;
    fvts_.clear();
    for (; index < argc; index += 3) {
        int64_t ttl;
        if (!slash::string2l(argv[index + 2].data(), argv[index + 2].size(), &ttl)) {
            res_.SetRes(CmdRes::kInvalidInt);
            return;
        }
        fvts_.push_back({argv[index], argv[index + 1], static_cast<int32_t>(ttl)});
    }
    return;
}

void EhmsetexCmd::Do() {
    s_ = g_pika_server->db()->Ehmsetex(key_, fvts_);
    if (s_.ok()) {
        res_.SetRes(CmdRes::kOk);
        SlotKeyAdd("e", key_);
    } else {
        res_.SetRes(CmdRes::kErrOther, s_.ToString());
    }
    return;
}

void EhmgetCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhmget);
        return;
    }
    key_ = argv[1];
    PikaCmdArgsType::iterator iter = argv.begin();
    iter++;
    iter++;
    fields_.assign(iter, argv.end()); 
    return;
}

void EhmgetCmd::Do() {
    std::vector<blackwidow::ValueStatus> vss;
    s_ = g_pika_server->db()->Ehmget(key_, fields_, &vss);
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

void EhkeysCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhkeys);
        return;
    }
    key_ = argv[1];
    return;
}

void EhkeysCmd::Do() {
    std::vector<std::string> fields;
    s_ = g_pika_server->db()->Ehkeys(key_, &fields);
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

void EhvalsCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhvals);
        return;
    }
    key_ = argv[1];
    return;
}

void EhvalsCmd::Do() {
    std::vector<std::string> values;
    s_ = g_pika_server->db()->Ehvals(key_, &values);
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

void EhgetallCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    (void)ptr_info;

    is_wt_ = false;
    if (argv.size() == 3 && slash::StringToLower(argv[2]) == "withttl") {
        is_wt_ = true;
    } else if (argv.size() != 2) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
    }

    key_ = argv[1];
    return;
}

void EhgetallCmd::Do() {
    std::vector<blackwidow::FieldValueTTL> fvts;
    s_ = g_pika_server->db()->Ehgetall(key_, &fvts);
    if (s_.ok() || s_.IsNotFound()) {
        if (is_wt_) {
            char buf[32];
            int64_t len;
            res_.AppendArrayLen(fvts.size() * 3);
            for (const auto& fv : fvts) {
                res_.AppendStringLen(fv.field.size());
                res_.AppendContent(fv.field);
                res_.AppendStringLen(fv.value.size());
                res_.AppendContent(fv.value);
                len = slash::ll2string(buf, sizeof(buf), fv.ttl);
                res_.AppendStringLen(len);
                res_.AppendContent(buf);
            }
        } else {
            res_.AppendArrayLen(fvts.size() * 2);
            for (const auto& fv : fvts) {
                res_.AppendStringLen(fv.field.size());
                res_.AppendContent(fv.field);
                res_.AppendStringLen(fv.value.size());
                res_.AppendContent(fv.value);
            }
        }
    } else {
        res_.SetRes(CmdRes::kErrOther, s_.ToString());
    }
    return;
}

void EhscanCmd::DoInitial(PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameEhscan);
        return;
    }
    key_ = argv[1];
    if (!slash::string2l(argv[2].data(), argv[2].size(), &cursor_)) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }
    size_t index = 3, argc = argv.size();

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
        } else if (opt == "withttl") {
            is_wt_ = true;
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

void EhscanCmd::Do() {
    int64_t next_cursor = 0;
    std::vector<blackwidow::FieldValueTTL> fvts;
    s_ = g_pika_server->db()->Ehscan(key_, cursor_, pattern_, count_, &fvts, &next_cursor);

    if (s_.ok() || s_.IsNotFound()) {
        res_.AppendContent("*2");
        char buf[32];
        int32_t len = slash::ll2string(buf, sizeof(buf), next_cursor);
        res_.AppendStringLen(len);
        res_.AppendContent(buf);

        if (is_wt_) {
            res_.AppendArrayLen(fvts.size() * 3);
            for (const auto& fv : fvts) {
                res_.AppendStringLen(fv.field.size());
                res_.AppendContent(fv.field);
                res_.AppendStringLen(fv.value.size());
                res_.AppendContent(fv.value);
                len = slash::ll2string(buf, sizeof(buf), fv.ttl);
                res_.AppendStringLen(len);
                res_.AppendContent(buf);
            }
        } else {
            res_.AppendArrayLen(fvts.size() * 2);
            for (const auto& fv : fvts) {
                res_.AppendStringLen(fv.field.size());
                res_.AppendContent(fv.field);
                res_.AppendStringLen(fv.value.size());
                res_.AppendContent(fv.value);
            }
        }
    } else {
        res_.SetRes(CmdRes::kErrOther, s_.ToString());
    }
    return;
}
