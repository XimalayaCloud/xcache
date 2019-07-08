#ifndef PIKA_SLOT_H_
#define PIKA_SLOT_H_

#include "pika_command.h"
#include "pika_client_conn.h"
#include "strings.h"
#include "pink/include/redis_cli.h"
#include "pink/include/pink_cli.h"
#include "blackwidow/blackwidow.h"

const std::string SlotPrefix = "_internal:4migrate:slot";
const std::string SlotKeyPrefix = "_internal:4migrate:slotkey:";
const std::string SlotTagPrefix = "_internal:4migrate:slottag:";
const size_t MaxKeySendSize = 10 * 1024;

//crc 32
#define HASH_SLOTS_MASK 0x000003ff
#define HASH_SLOTS_SIZE (HASH_SLOTS_MASK + 1)

int SlotNum(const std::string &str);
rocksdb::Status KeyType(const std::string &key, std::string* key_type);
blackwidow::DataType KeyType(const char key_type);

int GetSlotsNum(const std::string &str, uint32_t *pcrc, int *phastag);
std::string GetSlotsSlotKey(int slot);
std::string GetSlotsTagKey(uint32_t crc);

void SlotKeyAdd(const std::string &type, const std::string &key, const bool force = false);

void SlotKeyRemByType(const std::string &type, const std::string &key);
void SlotKeyRem(const std::string &key);
void KeyNotExistsRem(const std::string &type, const std::string &key);
void WriteDelKeyToBinlog(const std::string &key);

class MigrateCli : public pink::RedisCli {
public:
	MigrateCli()    { gettimeofday(&last_interaction_, NULL); }
	virtual ~MigrateCli(){}

	struct timeval last_interaction_;

private:

};

class PikaMigrate{
public:
	PikaMigrate();
	virtual ~PikaMigrate();

	int MigrateKey(const std::string &host, const int port, int db, int timeout, const std::string &key, const char type, std::string &detail);
	void CleanMigrateClient();

	void Lock()         { mutex_.Lock(); }
	int  Trylock()      { return mutex_.Trylock(); }
	void Unlock()       { mutex_.Unlock(); }
	MigrateCli* GetMigrateClient(const std::string &host, const int port, int timeout);

private:

	std::map<std::string, void *> migrate_clients_;
	slash::Mutex mutex_;

	//MigrateCli* GetMigrateClient(const std::string &host, const int port, int timeout);
	void KillMigrateClient(MigrateCli *migrate_cli);
	void KillAllMigrateClient();

	int MigrateSend(MigrateCli *migrate_cli, const std::string &key, const char type, std::string &detail);
	bool MigrateRecv(MigrateCli *migrate_cli, int need_receive, std::string &detail);

	int ParseKey(const std::string &key, const char type, std::string &wbuf_str);
	int ParseKKey(const std::string &key, std::string &wbuf_str);
	int ParseZKey(const std::string &key, std::string &wbuf_str);
	int ParseSKey(const std::string &key, std::string &wbuf_str);
	int ParseHKey(const std::string &key, std::string &wbuf_str);
	int ParseLKey(const std::string &key, std::string &wbuf_str);
	int ParseEHKey(const std::string &key, std::string &wbuf_str);
	bool SetTTL(const std::string &key, std::string &wbuf_str, int64_t ttl);
};

/*
 * Slots
 */

////slotsinfo
class SlotsInfoCmd : public Cmd {
public:
	SlotsInfoCmd() : begin_(0), end_(HASH_SLOTS_SIZE) {}
	virtual void Do();
private:
	virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
	virtual void Clear() {
		begin_ = 0;
		end_ = HASH_SLOTS_SIZE;
	}

	int64_t begin_;
	int64_t end_;
};

////slotshashkey
class SlotsHashKeyCmd : public Cmd {
public:
	SlotsHashKeyCmd() {}
	virtual void Do();
private:
	virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
	std::vector<std::string> keys_;
};


////slotsdel
class SlotsDelCmd : public Cmd {
public:
	SlotsDelCmd() {}
	virtual void Do();
private:
	virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
	std::vector<int64_t> slots_;
};

class SlotsDelOffCmd : public Cmd {
public:
	SlotsDelOffCmd() {}
	virtual void Do();
private:
	virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

////slotsmgrttagslot host port timeout slot
class SlotsMgrtTagSlotCmd : public Cmd {
public:
	SlotsMgrtTagSlotCmd() {}
	virtual void Do();
private:
	virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
	std::string host_;
	int port_;
	int64_t timeout_;
	int64_t slot_;
};

////slotsmgrttagone host port timeout key
class SlotsMgrtTagOneCmd : public Cmd {
public:
	SlotsMgrtTagOneCmd() {}
	virtual void Do();
private:
	virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
	rocksdb::Status KeyTypeCheck();
	std::string host_;
	int port_;
	int64_t timeout_;
	std::string key_;
	char type_;
};

////slotsrestore key ttl(ms) value(rdb)
struct RestoreKey {
	std::string key;
	int64_t ttlms;
	std::string value;
};

class SlotsrestoreCmd : public Cmd {
public:
	SlotsrestoreCmd() {}
	virtual void Do();
private:
	virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);

	std::vector<struct RestoreKey> restore_keys_;
};

class SlotsReloadCmd : public Cmd {
public:
	SlotsReloadCmd() {}
	virtual void Do();
private:
	virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SlotsReloadOffCmd : public Cmd {
public:
	SlotsReloadOffCmd() {}
	virtual void Do();
private:
	virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SlotsScanCmd : public Cmd {
public:
  SlotsScanCmd() : pattern_("*"), count_(10) {}
  virtual void Do();
private:
  std::string key_, pattern_;
  int64_t cursor_, count_;
  virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
  virtual void Clear() {
	pattern_ = "*";
	count_ = 10;
  }
};

class SlotsMgrtTagSlotAsyncCmd : public Cmd {
public:
    SlotsMgrtTagSlotAsyncCmd() {}
    virtual void Do();
private:
    std::string dest_ip_;
    int64_t dest_port_;
    int64_t timeout_ms_;
    int64_t max_bulks_;
    int64_t max_bytes_;
    int64_t slot_num_;
    int64_t keys_num_;

    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SlotsMgrtExecWrapperCmd : public Cmd {
public:
    SlotsMgrtExecWrapperCmd() {}
    virtual void Do();
private:
    std::string key_;
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SlotsMgrtAsyncStatusCmd : public Cmd {
public:
    SlotsMgrtAsyncStatusCmd() {}
    virtual void Do();
private:
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

class SlotsMgrtAsyncCancelCmd : public Cmd {
public:
    SlotsMgrtAsyncCancelCmd() {}
    virtual void Do();
private:
    virtual void DoInitial(PikaCmdArgsType &argvs, const CmdInfo* const ptr_info);
};

#endif
