#ifndef PIKA_EHASH_H_
#define PIKA_EHASH_H_

#include "pika_command.h"
#include "blackwidow/blackwidow.h"

/*
 * ehash
 */

class EhsetCmd : public Cmd {
public:
    enum SetCondition{kNONE, kNX, kXX, kEX};
    EhsetCmd() : sec_(0), condition_(kNONE) {}
    virtual void Do();
private:
    std::string key_, field_, value_;
    int64_t sec_;
    SetCondition condition_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhsetnxCmd : public Cmd {
public:
    EhsetnxCmd() {}
    virtual void Do();
private:
    std::string key_, field_, value_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhsetexCmd : public Cmd {
public:
    EhsetexCmd() {}
    virtual void Do();
private:
    std::string key_, field_, value_;
    int64_t sec_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhexpireCmd : public Cmd {
public:
    EhexpireCmd() {}
    virtual void Do();
private:
    std::string key_, field_;
    int64_t sec_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhexpireatCmd : public Cmd {
public:
    EhexpireatCmd() {}
    virtual void Do();
private:
    std::string key_, field_;
    int64_t time_stamp_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhttlCmd : public Cmd {
public:
    EhttlCmd() {}
    virtual void Do();
private:
    std::string key_, field_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhpersistCmd : public Cmd {
public:
    EhpersistCmd() {}
    virtual void Do();
private:
    std::string key_, field_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhgetCmd : public Cmd {
public:
    EhgetCmd() {}
    virtual void Do();
private:
    std::string key_, field_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhexistsCmd : public Cmd {
public:
    EhexistsCmd() {}
    virtual void Do();
private:
    std::string key_, field_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhdelCmd : public Cmd {
public:
    EhdelCmd() {}
    virtual void Do();
private:
    std::string key_;
    std::vector<std::string> fields_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhlenCmd : public Cmd {
public:
    EhlenCmd() {}
    virtual void Do();
private:
    std::string key_;
    bool is_force_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhstrlenCmd : public Cmd {
public:
    EhstrlenCmd() {}
    virtual void Do();
private:
    std::string key_, field_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhincrbyCmd : public Cmd {
public:
    enum SetCondition{kNONE, kEX, kNXEX, kXXEX};
    EhincrbyCmd() : condition_(kNONE) {}
    virtual void Do();
private:
    std::string key_, field_;
    int64_t by_;
    int64_t sec_;
    SetCondition condition_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhincrbyfloatCmd : public Cmd {
public:
    enum SetCondition{kNONE, kEX, kNXEX, kXXEX};
    EhincrbyfloatCmd() : condition_(kNONE) {}
    virtual void Do();
private:
    std::string key_, field_, by_;
    int64_t sec_;
    SetCondition condition_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhmsetCmd : public Cmd {
public:
    EhmsetCmd() {}
    virtual void Do();
private:
    std::string key_;
    std::vector<blackwidow::FieldValue> fvs_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhmsetexCmd : public Cmd {
public:
    EhmsetexCmd() {}
    virtual void Do();
private:
    std::string key_;
    std::vector<blackwidow::FieldValueTTL> fvts_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhmgetCmd : public Cmd {
public:
    EhmgetCmd() {}
    virtual void Do();
private:
    std::string key_;
    std::vector<std::string> fields_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhkeysCmd : public Cmd {
public:
    EhkeysCmd() {}
    virtual void Do();
private:
    std::string key_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhvalsCmd : public Cmd {
public:
    EhvalsCmd() {}
    virtual void Do();
private:
    std::string key_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhgetallCmd : public Cmd {
public:
    EhgetallCmd() {}
    virtual void Do();
private:
    std::string key_;
    bool is_wt_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class EhscanCmd : public Cmd {
public:
    EhscanCmd() {}
    virtual void Do();
private:
    std::string key_, pattern_;
    int64_t cursor_, count_;
    bool is_wt_;
    virtual void DoInitial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
    virtual void Clear() {
        pattern_ = "*";
        count_ = 10;
        is_wt_ = false;
    }
};

#endif
