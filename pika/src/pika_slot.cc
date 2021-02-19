// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "slash/include/slash_string.h"
#include "slash/include/slash_status.h"
#include "pika_conf.h"
#include "pika_slot.h"
#include "pika_server.h"
#include "pika_redis.h"
#include "pika_commonfunc.h"

#define min(a, b)  (((a) > (b)) ? (b) : (a))
#define MAX_MEMBERS_NUM	512

extern PikaServer *g_pika_server;
extern PikaConf *g_pika_conf;


static const char* GetSlotsTag(const std::string &str, int *plen) {
	const char *s = str.data();
	int i, j, n = str.length();
	for (i = 0; i < n && s[i] != '{'; i ++) {}
	if (i == n) {
		return NULL;
	}
	i ++;
	for (j = i; j < n && s[j] != '}'; j ++) {}
	if (j == n) {
		return NULL;
	}
	if (plen != NULL) {
		*plen = j - i;
	}
	return s + i;
}

std::string GetSlotsSlotKey(int slot) {
	return SlotKeyPrefix + std::to_string(slot);
}

//rocksdb namespace function
std::string GetSlotsTagKey(uint32_t crc) {
	return SlotTagPrefix + std::to_string(crc);
}

int SlotNum(const std::string &str) {
    uint32_t crc = PikaCommonFunc::CRC32Update(0, str.data(), (int)str.size());
    return (int)(crc & HASH_SLOTS_MASK);
}

// get the slot number by key
int GetSlotsNum(const std::string &str, uint32_t *pcrc, int *phastag) {
	const char *s = str.data();
	int taglen;
	int hastag = 0;
	const char *tag = GetSlotsTag(str, &taglen);
	if (tag == NULL) {
		tag = s, taglen = str.length();
	} else {
		hastag = 1;
	}
	uint32_t crc = PikaCommonFunc::CRC32CheckSum(tag, taglen);
	if (pcrc != NULL) {
		*pcrc = crc;
	}
	if (phastag != NULL) {
		*phastag = hastag;
	}
	return (int)(crc & HASH_SLOTS_MASK);
}

PikaMigrate::PikaMigrate(){
	migrate_clients_.clear();
}

PikaMigrate::~PikaMigrate() {
	//close and release all client
	//get the mutex lock
	slash::MutexLock lm(&mutex_);
	KillAllMigrateClient();
}

pink::PinkCli* PikaMigrate::GetMigrateClient(const std::string &host, const int port, int timeout){
	std::string ip_port = host + ":" + std::to_string(port);
	pink::PinkCli* migrate_cli;
	slash::Status s;

	std::map<std::string, void *>::iterator migrate_clients_iter = migrate_clients_.find(ip_port);
	if(migrate_clients_iter == migrate_clients_.end()){
		migrate_cli = pink::NewRedisCli();
		s = migrate_cli->Connect(host, port, g_pika_server->host());
		if(!s.ok()){
			LOG(ERROR) << "GetMigrateClient: new  migrate_cli[" << ip_port.c_str() << "] failed";

			delete migrate_cli;
			return NULL;
		}

		LOG(INFO) << "GetMigrateClient: new  migrate_cli[" << ip_port.c_str() << "]";

		std::string userpass = g_pika_conf->userpass();
		if (userpass != "") {
			pink::RedisCmdArgsType argv;
			std::string wbuf_str;
			argv.push_back("auth");
			argv.push_back(userpass);
			pink::SerializeRedisCommand(argv, &wbuf_str);

			s = migrate_cli->Send(&wbuf_str);
			if (!s.ok()) {
				LOG(ERROR) << "GetMigrateClient: new  migrate_cli Send, error: " << s.ToString();
				delete migrate_cli;
				return NULL;
			}

			s = migrate_cli->Recv(&argv);
			if (!s.ok()) {
				LOG(ERROR) << "GetMigrateClient: new  migrate_cli Recv, error: " << s.ToString();
				delete migrate_cli;
				return NULL;
			}

			if (strcasecmp(argv[0].data(), kInnerReplOk.data())) {
				LOG(ERROR) << "GetMigrateClient: new  migrate_cli auth error";
				delete migrate_cli;
				return NULL;
			}
		}

		//add new migrate client to the map
		migrate_clients_[ip_port] = migrate_cli;
	}else{
		//LOG(INFO) << "GetMigrateClient: find  migrate_cli[" << ip_port.c_str() << "]";
		migrate_cli = static_cast<pink::PinkCli*>(migrate_clients_iter->second);
	}

	//set the client connect timeout
	migrate_cli->set_send_timeout(timeout);
	migrate_cli->set_recv_timeout(timeout);

	//modify the client last time
	gettimeofday(&migrate_cli->last_interaction_, NULL);

	return migrate_cli;
}

void PikaMigrate::KillMigrateClient(pink::PinkCli *migrate_cli){
	std::map<std::string, void *>::iterator migrate_clients_iter = migrate_clients_.begin();
	while(migrate_clients_iter != migrate_clients_.end()){
		if( migrate_cli == static_cast<pink::PinkCli*>(migrate_clients_iter->second)){
			LOG(INFO) << "KillMigrateClient: kill  migrate_cli[" << migrate_clients_iter->first.c_str() << "]";

			migrate_cli->Close();
			delete migrate_cli;
			migrate_cli = NULL;

			migrate_clients_.erase(migrate_clients_iter);
			break;
		}

		++migrate_clients_iter;
	}
}

//clean and realse timeout client
void PikaMigrate::CleanMigrateClient(){
	struct timeval now;

	//if the size of migrate_clients_ <= 0, don't need clean
	if(migrate_clients_.size() <= 0){
		return;
	}

	gettimeofday(&now, NULL);
	std::map<std::string, void *>::iterator migrate_clients_iter = migrate_clients_.begin();
	while(migrate_clients_iter != migrate_clients_.end()){
		pink::PinkCli* migrate_cli = static_cast<pink::PinkCli*>(migrate_clients_iter->second);
		//pika_server do DoTimingTask every 10s, so we Try colse the migrate_cli before pika timeout, do it at least 20s in advance
		int timeout = (g_pika_conf->timeout() > 0) ? g_pika_conf->timeout() : 60;
		if( now.tv_sec - migrate_cli->last_interaction_.tv_sec > timeout - 20 ){

			LOG(INFO) << "CleanMigrateClient: clean  migrate_cli[" << migrate_clients_iter->first.c_str() << "]";
			migrate_cli->Close();
			delete migrate_cli;

			migrate_clients_iter = migrate_clients_.erase(migrate_clients_iter);
		}else{
			++migrate_clients_iter;
		}
	}
}

//clean and realse all client
void PikaMigrate::KillAllMigrateClient(){
	std::map<std::string, void *>::iterator migrate_clients_iter = migrate_clients_.begin();
	while(migrate_clients_iter != migrate_clients_.end()){
		pink::PinkCli* migrate_cli = static_cast<pink::PinkCli*>(migrate_clients_iter->second);

		LOG(INFO) << "KillAllMigrateClient: kill  migrate_cli[" << migrate_clients_iter->first.c_str() << "]";

		migrate_cli->Close();
		delete migrate_cli;

		migrate_clients_iter = migrate_clients_.erase(migrate_clients_iter);
	}
}

/* *
 * do migrate a key-value for slotsmgrt/slotsmgrtone commands
 * return value:
 *    -1 - error happens
 *   >=0 - # of success migration (0 or 1)
 * */
int PikaMigrate::MigrateKey(const std::string &host, const int port, int db, int timeout, const std::string &key, const char type, std::string &detail){
	int send_command_num = -1;

	pink::PinkCli* migrate_cli = GetMigrateClient(host, port, timeout);
	if( NULL == migrate_cli ){
		detail = "GetMigrateClient failed";
		return -1;
	}

	send_command_num = MigrateSend(migrate_cli, key, type, detail);
	if (send_command_num <= 0) {
		return send_command_num;
	}

	if( MigrateRecv(migrate_cli, send_command_num, detail) ){
		return send_command_num;
	} 

	return -1;
}

int PikaMigrate::MigrateSend(pink::PinkCli *migrate_cli, const std::string &key, const char type, std::string &detail) {
	std::string wbuf_str;
	slash::Status s;
	int command_num = -1;

	//chech the client is alive
	if(NULL == migrate_cli){
		return -1;
	}

	command_num = ParseKey(key, type, wbuf_str);
	if( command_num < 0 ){
		detail = "ParseKey failed";
		return command_num;
	}

	//dont need seed data, key is not exist
	if (command_num == 0 || wbuf_str.empty()) {
		return 0;
	}

	s = migrate_cli->Send(&wbuf_str);
	if (!s.ok()) {
		LOG(ERROR) << "Connect slots target, Send error: " << s.ToString();
		detail = "Connect slots target, Send error: " + s.ToString();
		KillMigrateClient(migrate_cli);
		return -1;
	}

	return command_num;
}

bool PikaMigrate::MigrateRecv(pink::PinkCli *migrate_cli, int need_receive, std::string &detail) {
	slash::Status s;
	std::string reply;
	int64_t ret;

	if(NULL == migrate_cli || need_receive < 0){
		return false;
	}

	pink::RedisCmdArgsType argv;
	while (need_receive) {
		s = migrate_cli->Recv(&argv);
		if (!s.ok()) {
			LOG(ERROR) << "Connect slots target, Recv error: " << s.ToString();
			detail = "Connect slots target, Recv error: " + s.ToString();
			KillMigrateClient(migrate_cli);
			return false;
		}

		reply = argv[0];
		need_receive--;

		//set   return ok
		//zadd  return number
		//hset  return 0 or 1
		//hmset return ok
		//sadd  return number
		//rpush return length
		if (argv.size() == 1 && 
				( kInnerReplOk == slash::StringToLower(reply) || slash::string2l(reply.data(), reply.size(), &ret)) ) {
			//success

			//continue reiceve response
			if( need_receive > 0 ){
				continue;
			}

			//has got all response
			break;
		}
		
		//failed
		detail = "something wrong with slots migrate, reply: " + reply;
		LOG(ERROR) << "something wrong with slots migrate, reply:" << reply;
		return false;
	}

	return true;
}

//return -1 is error; 0 dont migrate; >0 the number of commond
int PikaMigrate::ParseKey(const std::string &key, const char type, std::string &wbuf_str) {
	int command_num = -1;
	int64_t ttl = 0;
	rocksdb::Status s;

	switch(type){
		case 'k':
			command_num = ParseKKey(key, wbuf_str);
			break;
		case 'h':
			command_num = ParseHKey(key, wbuf_str);
			break;
		case 'l':
			command_num = ParseLKey(key, wbuf_str);
			break;
		case 'z':
			command_num = ParseZKey(key, wbuf_str);
			break;
		case 's':
			command_num = ParseSKey(key, wbuf_str);
			break;
		case 'e':
			command_num = ParseEHKey(key, wbuf_str);
			break;
		default:
			LOG(INFO) << "ParseKey key[" << key << "], the type[" << type << "] is not support.";
			return -1;
			break;
	}

	//error or key is not exist
	if (command_num <= 0) {
		LOG(INFO) << "ParseKey key[" << key << "], parse return " << command_num << ", the key maybe is not exist or expired.";
		return command_num;
	}

	//skip kv, because kv cmd: SET key value ttl
	if (type == 'k') {
		return command_num;
	}

	
	s = g_pika_server->db()->TTLByType(type, key, &ttl);

	if (!s.ok() && !s.IsNotFound()) {
		return -1;
	}
	
	//-1 indicates the key is valid forever
	if(ttl == -1){
		return command_num;
	}

	//key is expired or not exist, dont migrate
	if (ttl == 0 or ttl == -2) {
		wbuf_str.clear();
		return 0;
	}

	// no kv, because kv cmd: SET key value ttl
	if (SetTTL(key, wbuf_str, ttl)) {
		command_num += 1;
	}

	return command_num;
}

bool PikaMigrate::SetTTL(const std::string &key, std::string &wbuf_str, int64_t ttl) {

	//-1 indicates the key is valid forever
	if(ttl == -1){
		return false;
	}

	//if ttl = -2 indicates the key is not exist
	if(ttl < 0){
		LOG(INFO) << "SetTTL key[" << key << "], ttl is " << ttl;
		ttl = 0;
	}

	pink::RedisCmdArgsType argv;
	std::string cmd;

	argv.push_back("EXPIRE");
	argv.push_back(key);
	argv.push_back(std::to_string(ttl));

	pink::SerializeRedisCommand(argv, &cmd);
	wbuf_str.append(cmd);

	return true;
}

//return -1 is error; 0 dont migrate; >0 the number of commond
int PikaMigrate::ParseKKey(const std::string &key, std::string &wbuf_str) {
	pink::RedisCmdArgsType argv;
	std::string cmd;
	std::string value;
	int64_t ttl = 0; 
	rocksdb::Status s;

	s = g_pika_server->db()->Get(key, &value);

	//key is not exist, dont migrate
	if (s.IsNotFound()) {
		return 0;
	}

	if (!s.ok()) {
		return -1;
	}

	argv.push_back("SET");
	argv.push_back(key);
	argv.push_back(value);

	s = g_pika_server->db()->TTLByType('k', key, &ttl);
	// mean operation exception errors happen in database
	if (!s.ok() && !s.IsNotFound()) {
		return -1;
	}
	
	if (ttl > 0) {
		argv.push_back("EX");
		argv.push_back(std::to_string(ttl));
	}

	//ttl = -1 indicates the key is valid forever, dont process

	//key is expired or not exist, dont migrate
	if (ttl == 0 or ttl == -2) {
		wbuf_str.clear();
		return 0;
	}

	pink::SerializeRedisCommand(argv, &cmd);
	wbuf_str.append(cmd);

	return 1;
}

int PikaMigrate::ParseZKey(const std::string &key, std::string &wbuf_str) {
	int command_num = 0;

	int64_t next_cursor = 0;
	std::vector<blackwidow::ScoreMember> score_members;
	do {
		score_members.clear();
		rocksdb::Status s = g_pika_server->db()->ZScan(key, next_cursor, "*", MAX_MEMBERS_NUM, &score_members, &next_cursor);
		if (s.ok()) {
			if (score_members.empty()) {
				break;
			}

			pink::RedisCmdArgsType argv;
			std::string cmd;
			argv.push_back("ZADD");
			argv.push_back(key);

			for (const auto& score_member : score_members) {
				argv.push_back(std::to_string(score_member.score));
				argv.push_back(score_member.member);
			}

			pink::SerializeRedisCommand(argv, &cmd);
			wbuf_str.append(cmd);
			command_num++;
		} else if (s.IsNotFound()) {
			wbuf_str.clear();
			return 0;
		} else {
			wbuf_str.clear();
			return -1;
		}
	} while(next_cursor > 0);

	return command_num;
}

//return -1 is error; 0 dont migrate; >0 the number of commond
int PikaMigrate::ParseHKey(const std::string &key, std::string &wbuf_str) {
	int64_t next_cursor = 0;
	int command_num = 0;
	std::vector<blackwidow::FieldValue> field_values;
	do {
		field_values.clear();
		rocksdb::Status s = g_pika_server->db()->HScan(key, next_cursor, "*", MAX_MEMBERS_NUM, &field_values, &next_cursor);
		if (s.ok()) {
			if (field_values.empty()) {
				break;
			}

			pink::RedisCmdArgsType argv;
			std::string cmd;
			argv.push_back("HMSET");
			argv.push_back(key);

			for (const auto& field_value : field_values) {
				argv.push_back(field_value.field);
				argv.push_back(field_value.value);
			}

			pink::SerializeRedisCommand(argv, &cmd);
			wbuf_str.append(cmd);
			command_num++;
		} else if (s.IsNotFound()) {
			wbuf_str.clear();
			return 0;
		} else {
			wbuf_str.clear();
			return -1;
		}
	} while (next_cursor > 0);

	return command_num;
}

//return -1 is error; 0 dont migrate; >0 the number of commond
int PikaMigrate::ParseSKey(const std::string &key, std::string &wbuf_str) {
	int command_num = 0;
	int64_t next_cursor = 0;
	std::vector<std::string> members;

	do {
		members.clear();
		rocksdb::Status s = g_pika_server->db()->SScan(key, next_cursor, "*", MAX_MEMBERS_NUM, &members, &next_cursor);

		if (s.ok()) {
			if (members.empty()) {
				break;
			}

			pink::RedisCmdArgsType argv;
			std::string cmd;
			argv.push_back("SADD");
			argv.push_back(key);

			for (const auto& member : members) {
				argv.push_back(member);
			}

			pink::SerializeRedisCommand(argv, &cmd);
			wbuf_str.append(cmd);
			command_num++;
		} else if (s.IsNotFound()) {
			wbuf_str.clear();
			return 0;
		} else {
			wbuf_str.clear();
			return -1;
		}
	} while (next_cursor > 0);

	return command_num;
}

//return -1 is error; 0 dont migrate; >0 the number of commond
int PikaMigrate::ParseLKey(const std::string &key, std::string &wbuf_str) {
	int64_t left = 0;
	int64_t len = MAX_MEMBERS_NUM; //original 512
	int command_num = 0;
	std::vector<std::string> values;

	pink::RedisCmdArgsType argv;
	std::string cmd;

	//del old key, before migrate list; prevent redo when failed
	argv.push_back("DEL");
	argv.push_back(key);
	pink::SerializeRedisCommand(argv, &cmd);
	wbuf_str.append(cmd);
	command_num++;

	do {
		values.clear();
		rocksdb::Status s = g_pika_server->db()->LRange(key, left, left + (len - 1), &values);
		if (s.ok()) {
			if (values.empty()) {
				break;
			}

			pink::RedisCmdArgsType argv;
			std::string cmd;

			argv.push_back("RPUSH");
			argv.push_back(key);

			for (const auto& value : values) {
				argv.push_back(value);
			}

			pink::SerializeRedisCommand(argv, &cmd);
			wbuf_str.append(cmd);
			command_num++;

			left += len;	
		} else if (s.IsNotFound()) {
			wbuf_str.clear();
			return 0;
		} else {
			wbuf_str.clear();
			return -1;
		}
	} while (!values.empty());

	if (command_num == 1) {
		wbuf_str.clear();
		command_num = 0;
	}

	return command_num;
}

//return -1 is error; 0 dont migrate; >0 the number of commond
int PikaMigrate::ParseEHKey(const std::string &key, std::string &wbuf_str) {
	int64_t next_cursor = 0;
	int command_num = 0;
	std::vector<blackwidow::FieldValueTTL> fvts;
	do {
		fvts.clear();
		rocksdb::Status s = g_pika_server->db()->Ehscan(key, next_cursor, "*", MAX_MEMBERS_NUM, &fvts, &next_cursor);
		if (s.ok()) {
			if (fvts.empty()) {
				break;
			}

			pink::RedisCmdArgsType argv;
			std::string cmd;
			argv.push_back("EHMSETEX");
			argv.push_back(key);

			for (const auto& field_value : fvts) {
				argv.push_back(field_value.field);
				argv.push_back(field_value.value);
				argv.push_back(std::to_string(field_value.ttl));
			}

			pink::SerializeRedisCommand(argv, &cmd);
			wbuf_str.append(cmd);
			command_num++;
		} else if (s.IsNotFound()) {
			wbuf_str.clear();
			return 0;
		} else {
			wbuf_str.clear();
			return -1;
		}
	} while (next_cursor > 0);

	return command_num;
}

void SlotKeyAdd(const std::string &type, const std::string &key, const bool force){
	if (g_pika_conf->slotmigrate() != true && force != true){
		return;
	}

	rocksdb::Status s;
	int32_t res = -1;
	uint32_t crc;
	int hastag;
	int slot = GetSlotsNum(key, &crc, &hastag);
	std::string slot_key = GetSlotsSlotKey(slot);
	std::vector<std::string> members;
	members.push_back(type+key);
	s = g_pika_server->db()->SAdd(slot_key, members, &res);
	if (!s.ok()) {
		LOG(ERROR) << "sadd key[" << key << "] to slotKey[" << slot_key << "] failed, error: " << s.ToString();
		return;
	}

    //if res == 0, indicate the key is exist; may return, 
    //prevent write slot_key success, but write tag_key failed, so always write tag_key
    if (hastag){
        std::string tag_key = GetSlotsTagKey(crc);
        s = g_pika_server->db()->SAdd(tag_key, members, &res);
        if(!s.ok()){
            LOG(ERROR) << "sadd key[" << key << "] to tagKey[" << tag_key << "] failed, error: " << s.ToString();
            return;
        }
    }
}

rocksdb::Status KeyType(const std::string &key, std::string* type) {
	std::string type_str;
	rocksdb::Status s = g_pika_server->db()->Type(key, &type_str);
	if (!s.ok()) {
		LOG(ERROR) << "Get key[" <<  key << "] type failed, error: " << s.ToString();
		*type = "";
		return s;
	}

	if (type_str=="string"){
		*type = "k";
	}else if (type_str=="hash"){
		*type = "h";
	}else if (type_str=="list"){
		*type = "l";
	}else if (type_str=="set"){
		*type = "s";
	}else if (type_str=="zset"){
		*type = "z";
	}else if (type_str=="ehash"){
		*type = "e";
	}else{
		*type = "";
		return rocksdb::Status::NotFound("");
	}

	return rocksdb::Status::OK();
}

blackwidow::DataType KeyType(const char key_type) {
	switch (key_type) {
		case 'k':
			return blackwidow::DataType::kStrings;
		case 'h':
			return blackwidow::DataType::kHashes;
		case 's':
			return blackwidow::DataType::kSets;
		case 'l':
			return blackwidow::DataType::kLists;
		case 'z':
			return blackwidow::DataType::kZSets;
		case 'e':
			return blackwidow::DataType::kEhashs;
		default:
			return blackwidow::DataType::kNONE_DB;
	}
}

rocksdb::Status KeyIsExists(const std::string &type, const std::string &key) {
	rocksdb::Status s;
	int32_t len = -1;

	if (type == "k"){
		std::string value;
		s = g_pika_server->db()->Get(key, &value);
		return s;
	}else if (type == "h"){
		s = g_pika_server->db()->HLen(key, &len);
	}else if (type == "s"){
		s = g_pika_server->db()->SCard(key, &len);
	}else if (type == "z"){
		s = g_pika_server->db()->ZCard(key, &len);
	}else if (type == "l"){
		uint64_t llen = -1;
		s = g_pika_server->db()->LLen(key, &llen);
		if (!s.ok() && !s.IsNotFound()) {
		  return s;
		}
		len = (int32_t)llen;
	}else if (type == "e"){
		s = g_pika_server->db()->Ehlen(key, &len);
	}else{
		return rocksdb::Status::Corruption("dont support type " + type);
	}

	if (len > 0) {
	  return rocksdb::Status::OK();
	}

	return rocksdb::Status::NotFound();
}

void SlotKeyRemByType(const std::string &type, const std::string &key) {
	uint32_t crc;
	int hastag;
	int slot = GetSlotsNum(key, &crc, &hastag);

	std::string slot_key = GetSlotsSlotKey(slot); 
	int32_t res = 0;

	std::vector<std::string> members;
	members.push_back(type+key);
	rocksdb::Status s = g_pika_server->db()->SRem(slot_key, members, &res);
	if (!s.ok()) {
		LOG(ERROR) << "srem key[" << key << "] from slotKey[" << slot_key << "] failed, error: " << s.ToString();
		return;
	}

	if (hastag) {
		std::string tag_key = GetSlotsTagKey(crc);
		s = g_pika_server->db()->SRem(tag_key, members, &res);
		if (!s.ok()) {
			LOG(ERROR) << "srem key[" << key << "] from tagKey[" << tag_key << "] failed, error: " << s.ToString();
			return;
		}
	}
}

//del key from slotkey
void SlotKeyRem(const std::string& key) {
	if (g_pika_conf->slotmigrate() != true){
		return;
	}
	std::string type;
	rocksdb::Status s = KeyType(key, &type);
	if (!s.ok()){
		//LOG(WARNING) << "slotkeyrem key: " << key <<" from slotKey, error: " << s.ToString();
		return;
	}

	SlotKeyRemByType(type, key);
}

//check key exists
void KeyNotExistsRem(const std::string &type, const std::string &key) {
	if (g_pika_conf->slotmigrate() != true){
		return;
	}

	rocksdb::Status s = KeyIsExists(type, key);
	if (s.IsNotFound()) {
		SlotKeyRemByType(type, key);
	}

	return;
}

slash::Status BinlogPut(const std::string &key, const std::string &raw_args){
	//g_pika_server->logger_->Lock();
	//slash::Status s = g_pika_server->logger_->Put(raw_args);
	//g_pika_server->logger_->Unlock();
    uint32_t crc = PikaCommonFunc::CRC32Update(0, key.data(), (int)key.size());
    int binlog_writer_num = g_pika_conf->binlog_writer_num();
    int thread_index =  (int)(crc % binlog_writer_num);
	slash::Status s = g_pika_server->binlog_write_thread_[thread_index]->WriteBinlog(raw_args, "sync" == g_pika_conf->binlog_writer_method());
	if (!s.ok()) {
		LOG(ERROR) << "Writing binlog failed, maybe no space left on device";
		//g_pika_server->SetBinlogIoError(true);
		for (int i=0; i<binlog_writer_num; i++) {
			if (i != thread_index) {
				g_pika_server->binlog_write_thread_[i]->SetBinlogIoError(true);
			}
		}
		g_pika_conf->SetReadonly(true);
		//return "-ERR Writing binlog failed, maybe no space left on device\r\n";
	}

	return s;
}


//write del key to binlog for slave
void WriteDelKeyToBinlog(const std::string &key){
	std::string raw_args;
	raw_args.clear();

	RedisAppendLen(raw_args, 2, "*");
	RedisAppendLen(raw_args, 3, "$");
	RedisAppendContent(raw_args, "DEL");
	RedisAppendLen(raw_args, key.size(), "$");
	RedisAppendContent(raw_args, key);

	BinlogPut(key, raw_args);
}

/* *
 * do migrate a key-value for slotsmgrt/slotsmgrtone commands
 * return value:
 *    -1 - error happens
 *   >=0 - # of success migration (0 or 1)
 * */
static int SlotsMgrtOne(const std::string &host, const int port, int timeout, const std::string &key, const char type, std::string &detail) {
	int send_command_num = 0;
	rocksdb::Status s;
	std::map<blackwidow::DataType, blackwidow::Status> type_status;

	send_command_num = g_pika_server->pika_migrate_.MigrateKey(host, port, 0, timeout, key, type, detail);

	//the key is migrated to target, delete key and slotsinfo
	if (send_command_num >= 1) {
		std::vector<std::string> keys;
		keys.push_back(key);
		int64_t count = g_pika_server->db()->Del(keys, &type_status);
		if (count > 0) {
			WriteDelKeyToBinlog(key);
		}

		//del slots info
		SlotKeyRemByType(std::string(1, type), key);
		return 1;
	}

	//key is not exist, only del slotsinfo
	if (send_command_num == 0){
		//del slots info
		SlotKeyRemByType(std::string(1, type), key);
		return 0;
	}

	return -1;
}

/* *
 * do migrate mutli key-value(s) for {slotsmgrt/slotsmgrtone}with tag commands
 * return value:
 *    -1 - error happens
 *   >=0 - # of success migration
 * */
static int SlotsMgrtTag(const std::string &host, const int port, int timeout, const std::string &key, const char type, std::string &detail) {
	int count = 0;
	uint32_t crc;
	int hastag;
	
	GetSlotsNum(key, &crc, &hastag);
	if (!hastag) {
		if (type == 0) {
			return 0;
		}
		int ret = SlotsMgrtOne(host, port, timeout, key, type, detail);
		return ret;
	}

	std::string tag_key = GetSlotsTagKey(crc);
	std::vector<std::string> members;

	//get all key that has the same crc
	rocksdb::Status s = g_pika_server->db()->SMembers(tag_key, &members);
	if (!s.ok()) {
		return -1;
	}

	std::vector<std::string>::const_iterator iter = members.begin();
	for (; iter != members.end(); iter++) {
		std::string key = *iter;
		char type = key.at(0);
		key.erase(key.begin());
		int ret = SlotsMgrtOne(host, port, timeout, key, type, detail);

		//the key is migrated to target
		if (ret == 1){
			count++;
			continue;
		}

		if (ret == 0){
			continue;
		}

		return -1;
	}

	return count;
}


/* *
 * slotsinfo [start] [count]
 * */
void SlotsInfoCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
	res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsInfo);
	return;
  }

  if(argv.size() >= 2){
	if (!slash::string2l(argv[1].data(), argv[1].size(), &begin_)) {
	  res_.SetRes(CmdRes::kInvalidInt);
	  return;
	}

	if (begin_ < 0 || begin_ >= end_) {
	  std::string detail = "invalid slot begin = " + argv[1];
	  res_.SetRes(CmdRes::kErrOther, detail);
	  return;
	}
  }

  if(argv.size() >= 3){
	int64_t count = 0;
	if (!slash::string2l(argv[2].data(), argv[2].size(), &count)) {
	  res_.SetRes(CmdRes::kInvalidInt);
	  return;
	}

	if (count < 0) {
	  std::string detail = "invalid slot count = " + argv[2];
	  res_.SetRes(CmdRes::kErrOther, detail);
	  return;
	}

	if (begin_ + count < end_) {
	  end_ = begin_ + count;
	}
  }

  if (argv.size() >= 4) {
	res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsInfo);
	return;
  }
}

void SlotsInfoCmd::Do() {
  int slots_slot[HASH_SLOTS_SIZE] = {0};
  int slots_size[HASH_SLOTS_SIZE] = {0};
  int n = 0;
  int i = 0;
  int32_t len = 0;
  std::string slot_key;

  for (i = begin_; i < end_; i ++) {
	slot_key = GetSlotsSlotKey(i);
	len = 0;
	rocksdb::Status s = g_pika_server->db()->SCard(slot_key, &len);
	if (!s.ok() || len == 0) {
	  continue;
	}

	slots_slot[n] = i;
	slots_size[n] = len;
	n++;
  }

  res_.AppendArrayLen(n);
  for (i = 0; i < n; i ++) {
	res_.AppendArrayLen(2);
	res_.AppendInteger(slots_slot[i]);
	res_.AppendInteger(slots_size[i]);
  }

  return;
}


/* *
 * slotshashkey [key1 key2...]
 * */
void SlotsHashKeyCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
	res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsHashKey);
	return;
  }

  std::vector<std::string>::const_iterator iter = argv.begin();
  keys_.assign(++iter, argv.end());
  return;
}

void SlotsHashKeyCmd::Do() {
  std::vector<std::string>::const_iterator keys_it;

  res_.AppendArrayLen(keys_.size());
  for (keys_it = keys_.begin(); keys_it != keys_.end(); ++keys_it) {
	res_.AppendInteger(GetSlotsNum(*keys_it, NULL, NULL));;
  }

  return;
}


/* *
 * slotsdel slot1 [slot2 ...]
 * */
void SlotsDelCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
	res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsDel);
	return;
  }

  size_t i;
  int64_t slotNo;

  //clear vector
  slots_.clear();

  if (argv.size() == 4 && !strcasecmp(argv[1].data(), "range")) {
  		int64_t start;
  		int64_t end;
  		if (!slash::string2l(argv[2].data(), argv[2].size(), &start)) {
		  res_.SetRes(CmdRes::kInvalidInt);
		  return;
		}

		if (!slash::string2l(argv[3].data(), argv[3].size(), &end)) {
		  res_.SetRes(CmdRes::kInvalidInt);
		  return;
		}

		if (start < 0 || start >= HASH_SLOTS_SIZE) {
		  std::string detail = "invalid start slot number = " + argv[2];
		  res_.SetRes(CmdRes::kErrOther, detail);
		  return;
		}

		if (end < 0 || end >= HASH_SLOTS_SIZE) {
		  std::string detail = "invalid end slot number = " + argv[3];
		  res_.SetRes(CmdRes::kErrOther, detail);
		  return;
		}

		if (start > end) {
			std::string detail = "invalid slot range";
			res_.SetRes(CmdRes::kErrOther, detail);
			return;
		}

		for (int64_t i = start; i <= end; i++) {
			slots_.push_back(i);
		}

		return;

  }

  for(i = 1; i < argv.size(); i++) {
	if (!slash::string2l(argv[i].data(), argv[i].size(), &slotNo)) {
	  res_.SetRes(CmdRes::kInvalidInt);
	  return;
	}

	if (slotNo < 0 || slotNo >= HASH_SLOTS_SIZE) {
	  std::string detail = "invalid slot number = " + argv[i];
	  res_.SetRes(CmdRes::kErrOther, detail);
	  return;
	}

	slots_.push_back(slotNo);
  }
}

void SlotsDelCmd::Do() {
	int n = 0;
	std::string slot_key;
	std::vector<int64_t>::iterator slots_iter;
	rocksdb::Status s;

	n = slots_.size();

	if (n > 0) {
		g_pika_server->SetBgSlotsDelEnable(true);
		g_pika_server->BgSlotsDel(slots_);
	}

	res_.AppendArrayLen(n);
	//return the size of every slot
	for (slots_iter = slots_.begin(); slots_iter != slots_.end(); ++slots_iter){
		slot_key = GetSlotsSlotKey(*slots_iter);
		int32_t len = 0;
		rocksdb::Status s = g_pika_server->db()->SCard(slot_key, &len);

		res_.AppendArrayLen(2);
		res_.AppendInteger(*slots_iter);
		res_.AppendInteger(len);
	}

	return;
}

void SlotsDelOffCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsDelOff);
	}
	return;
}

void SlotsDelOffCmd::Do() {
	g_pika_server->SetBgSlotsDelEnable(false);
	res_.SetRes(CmdRes::kOk);
	return;
}


/* *
 * slotsmgrttagslot host port timeout slot
 * */
void SlotsMgrtTagSlotCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
	res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtTagSlot);
	return;
  }

  host_ = argv[1];

  int64_t port = 0;
  if (!slash::string2l(argv[2].data(), argv[2].size(), &port)) {
	  res_.SetRes(CmdRes::kInvalidInt);
	  return;
  }
  if(port < 0 || port > 65535){
	std::string detail = "invalid port nummber = " + argv[2];
	res_.SetRes(CmdRes::kErrOther, detail);
	return;
  }

  port_ = (int)port;

  if (!slash::string2l(argv[3].data(), argv[3].size(), &timeout_)) {
	res_.SetRes(CmdRes::kInvalidInt);
	return;
  }
  if (timeout_ < 0 ) {
	std::string detail = "invalid timeout nummber = " + argv[3];
	res_.SetRes(CmdRes::kErrOther, detail);
	return;
  }
  if(timeout_ == 0){
	timeout_ = 100;
  }

  if (!slash::string2l(argv[4].data(), argv[4].size(), &slot_)) {
	res_.SetRes(CmdRes::kInvalidInt);
	return;
  }
  if (slot_ < 0 || slot_ >= HASH_SLOTS_SIZE) {
	std::string detail = "invalid slot nummber = " + argv[4];
	res_.SetRes(CmdRes::kErrOther, detail);
	return;
  }
}

void SlotsMgrtTagSlotCmd::Do() {
	if (g_pika_conf->slotmigrate() != true){
		LOG(WARNING) << "Not in slotmigrate mode";
		res_.SetRes(CmdRes::kErrOther, "not set slotmigrate");
		return;
	}

	int32_t len = 0;
	int ret = 0;
	std::string detail;
	std::string slot_key = GetSlotsSlotKey(slot_);

	//first get the count of slot_key, prevent to sscan key very slowly when the key is notfund
	
	rocksdb::Status s = g_pika_server->db()->SCard(slot_key, &len);
	if (len < 0) {
		detail = "Get the len of slot Error";
	}

	//mutex between SlotsMgrtTagSlotCmd、SlotsMgrtTagOneCmd and migrator_thread
	if (len > 0 && g_pika_server->pika_migrate_.Trylock() == 0 ) {
		int64_t next_cursor = 0;
		std::vector<std::string> members;
		rocksdb::Status s = g_pika_server->db()->SScan(slot_key, 0, "*", 1, &members, &next_cursor);

		if (s.ok()) {
			for (const auto& member : members) {
				std::string key = member;
				char type = key.at(0);
				key.erase(key.begin());
				ret = SlotsMgrtTag(host_, port_, timeout_, key, type, detail);
			}
		}

		//unlock
		g_pika_server->pika_migrate_.Unlock();
	}

	//return the slots info
	//len = g_pika_server->db()->SCard(slot_key);

	if (len >= 0 && ret >= 0 ) {
		res_.AppendArrayLen(2);
		res_.AppendInteger(1);    //the number of keys migrated 
		res_.AppendInteger(len);  //the number of keys remained
	} else {
		res_.SetRes(CmdRes::kErrOther, detail);
	}

	return;
}

/* *
 * slotsmgrttagone host port timeout key
 * */
void SlotsMgrtTagOneCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
	res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtTagOne);
	return;
  }

  host_ = argv[1];

  int64_t port = 0;

  if (!slash::string2l(argv[2].data(), argv[2].size(), &port)) {
	  res_.SetRes(CmdRes::kInvalidInt);
	  return;
  }
  if(port < 0 || port > 65535){
	std::string detail = "invalid port nummber = " + argv[2];
	res_.SetRes(CmdRes::kErrOther, detail);
	return;
  }

  port_ = (int)port;

  if (!slash::string2l(argv[3].data(), argv[3].size(), &timeout_)) {
	res_.SetRes(CmdRes::kInvalidInt);
	return;
  }
  if (timeout_ < 0 ) {
	std::string detail = "invalid timeout nummber = " + argv[3];
	res_.SetRes(CmdRes::kErrOther, detail);
	return;
  }
  if(timeout_ == 0){
	timeout_ = 100;
  }

  key_ = argv[4];
}

// check key type
rocksdb::Status SlotsMgrtTagOneCmd::KeyTypeCheck() {
    std::string type_str;
    rocksdb::Status s = g_pika_server->db()->Type(key_, &type_str);
    if (!s.ok()) {
        LOG(ERROR) << "Migrate slot key: " << key_ << ", error: " << s.ToString();
        type_ = 0;
        return s; 
    }
    if (type_str == "string"){
        type_ = 'k';
    }else if (type_str == "hash"){
        type_ = 'h';
    }else if (type_str == "list"){
        type_ = 'l';
    }else if (type_str == "set"){
        type_ = 's';
    }else if (type_str == "zset"){
        type_ = 'z';
    }else if (type_str == "ehash"){
    	type_ = 'e';
    }else{
        LOG(INFO) << "Migrate slot key: " << key_ << " not found";
		type_ = 0;
		return rocksdb::Status::NotFound("");
	}

	return rocksdb::Status::OK();
}

void SlotsMgrtTagOneCmd::Do() {
	if (g_pika_conf->slotmigrate() != true){
		LOG(WARNING) << "Not in slotmigrate mode";
		res_.SetRes(CmdRes::kErrOther, "not set slotmigrate");
		return;
	}

	int64_t ret = 0;
	int32_t len = 0;
	int     hastag = 0;
	uint32_t crc;
	std::string detail;
	rocksdb::Status s;
	std::map<blackwidow::DataType, rocksdb::Status> type_status;

	//check if need migrate key, if the key is not exist, return
	GetSlotsNum(key_, &crc, &hastag);
	if (!hastag) {
		std::vector<std::string> keys;
		keys.push_back(key_);

		//check the key is not exist
		ret = g_pika_server->db()->Exists(keys, &type_status);

		//when the key is not exist, ret = 0
		if ( ret == -1 ){
			res_.SetRes(CmdRes::kErrOther, "exists internal error");
			return;
		}
		
		if( ret == 0 ) {
			res_.AppendInteger(0);
			return;
		}

		//else need to migrate
	}else{
		//key is tag_key, check the number of the tag_key
		std::string tag_key = GetSlotsTagKey(crc);
		s = g_pika_server->db()->SCard(tag_key, &len);
		if (!s.ok() || len == -1) {
			res_.SetRes(CmdRes::kErrOther, "cont get the mumber of tag_key");
			return;
		}

		if (len == 0) {
			res_.AppendInteger(0);
			return;
		}

		//else need to migrate
	}

	//lock batch migrate, dont do slotsmgrttagslot when do slotsmgrttagone 
	//g_pika_server->mgrttagslot_mutex_.Lock();
	//pika_server thread exit(~PikaMigrate) and dispatch thread do CronHandle nead lock()
	g_pika_server->pika_migrate_.Lock();


  //codis can guarantee that the operate of the slots key is processed by migrate command only
  //do record lock for key_
  //g_pika_server->mutex_record_.Lock(key_);

	//check if need migrate key, if the key is not exist, return
	//GetSlotsNum(key_, &crc, &hastag);
	if (!hastag) {
		std::vector<std::string> keys;
		keys.push_back(key_);
		//the key may be deleted by other thread
		std::map<blackwidow::DataType, rocksdb::Status> type_status;
		ret = g_pika_server->db()->Exists(keys, &type_status);

		//when the key is not exist, ret = 0
		if (ret == -1) {
			detail = s.ToString();
		} else if (KeyTypeCheck() != rocksdb::Status::OK()) {
			detail = "cont get the key type.";
			ret = -1;
		} else {
			ret = SlotsMgrtTag(host_, port_, timeout_, key_, type_, detail);
		}
	} else {
		//key maybe not exist, the key is tag key, migrate the same tag key
		ret = SlotsMgrtTag(host_, port_, timeout_, key_, 0, detail);
	}

  //unlock the record lock
  //g_pika_server->mutex_record_.Unlock(key_);

  //unlock
  g_pika_server->pika_migrate_.Unlock();
  //g_pika_server->mgrttagslot_mutex_.Unlock();


  if(ret >= 0){
	res_.AppendInteger(ret);
  }else{
	if(detail.size() == 0){
	  detail = "Unknown Error";
	}
	res_.SetRes(CmdRes::kErrOther, detail);
  }

  return;
}

void SlotsReloadCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsReload);
	}
	return;
}

void SlotsReloadCmd::Do() {
	g_pika_server->Bgslotsreload();
	const PikaServer::BGSlotsReload& info = g_pika_server->bgslots_reload();
	char buf[256];
	snprintf(buf, sizeof(buf), "+%s : %lu", 
		info.s_start_time.c_str(), g_pika_server->GetSlotsreloadingCursor());
	res_.AppendContent(buf);
	return;
}

void SlotsReloadOffCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsReloadOff);
	}
	return;
}

void SlotsReloadOffCmd::Do() {
	g_pika_server->SetSlotsreloading(false);
	res_.SetRes(CmdRes::kOk);
	return;
}

void SlotsScanCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
	if (!ptr_info->CheckArg(argv.size())) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsScan);
		return;
	}
	key_ = SlotKeyPrefix + argv[1];
	if (!slash::string2l(argv[2].data(), argv[2].size(), &cursor_)) {
		res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsScan);
		return;
	}
	size_t argc = argv.size(), index = 3;
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

void SlotsScanCmd::Do() {

	/*int32_t card = -1;
	rocksdb::Status s =  g_pika_server->db()->SCard(key_, &card);
	if (card >= 0 && cursor_ >= card) {
		cursor_ = 0;
	}*/
	std::vector<std::string> members;
	rocksdb::Status s = g_pika_server->db()->SScan(key_, cursor_, pattern_, count_, &members, &cursor_);

	if (members.size() <= 0) {
		cursor_ = 0;
	}
	res_.AppendContent("*2");
  
	char buf[32];
	int64_t len = slash::ll2string(buf, sizeof(buf), cursor_);
	res_.AppendStringLen(len);
	res_.AppendContent(buf);

	res_.AppendArrayLen(members.size());
	std::vector<std::string>::const_iterator iter_member = members.begin();
	for (; iter_member != members.end(); iter_member++) {
		res_.AppendStringLen(iter_member->size());
		res_.AppendContent(*iter_member);
	}
	return;
}

static rocksdb::Status RestoreKV(const std::string &key, const restore_value &dbvalue) {
  if(dbvalue.type != blackwidow::kStrings){
	return rocksdb::Status::Corruption("dbvalue format error");
  }

  rocksdb::Status s;
  s = g_pika_server->db()->Set(key, dbvalue.kvv, 0);
  if (!s.ok()) {
	return s;
  }

  SlotKeyAdd("k", key);

  return s;
}

static rocksdb::Status RestoreList(const std::string &key, const restore_value &dbvalue) {
  if(dbvalue.type != blackwidow::kLists){
		return rocksdb::Status::Corruption("dbvalue format error");
  }

  uint64_t llen = 0;
  rocksdb::Status s;

  //std::vector<std::string>::const_iterator iter = dbvalue.listv.begin();
  s = g_pika_server->db()->RPush(key, dbvalue.listv, &llen);
  if (!s.ok()) {
  	return s;
  }

  SlotKeyAdd("l", key);

  return s;
}

static rocksdb::Status RestoreSet(const std::string &key, const restore_value &dbvalue) {
  if(dbvalue.type != blackwidow::kSets){
	return rocksdb::Status::Corruption("dbvalue format error");
  }

  int32_t res = 0;
  rocksdb::Status s;
	s = g_pika_server->db()->SAdd(key, dbvalue.setv, &res);
	if (!s.ok()) {
	  return s;
	}

  SlotKeyAdd("s", key);

  return s;
}

static rocksdb::Status RestoreZset(const std::string &key, const restore_value &dbvalue) {
  if(dbvalue.type != blackwidow::kZSets){
	return rocksdb::Status::Corruption("dbvalue format error");
  }

  int32_t res = 0;
  rocksdb::Status s;

  
	s = g_pika_server->db()->ZAdd(key, dbvalue.zsetv, &res);;
	if (!s.ok()) {
	  return s;
	}
  
  SlotKeyAdd("z", key);

  return s;
}

static rocksdb::Status RestoreHash(const std::string &key, const restore_value &dbvalue) {
  if(dbvalue.type != blackwidow::kHashes){
	return rocksdb::Status::Corruption("dbvalue format error");
  }

  int32_t ret = 0;
  rocksdb::Status s;

  std::vector<blackwidow::FieldValue>::const_iterator iter = dbvalue.hashv.begin();
  for (; iter != dbvalue.hashv.end(); iter++) {
	s = g_pika_server->db()->HSet(key, iter->field, iter->value, &ret);
	if (!s.ok()) {
	  return s;
	}
  }

  SlotKeyAdd("h", key);

  return s;
}

static void WriteCommandToBinlog(const std::string &key, const int64_t ttlms, const restore_value &dbvalue) {
  std::string raw_args;
  raw_args.reserve(1024*1024);

  switch (dbvalue.type){
	case blackwidow::kStrings :
	  {
		RedisAppendLen(raw_args, 3, "*");
		RedisAppendLen(raw_args, 3, "$");
		RedisAppendContent(raw_args, "set");
		RedisAppendLen(raw_args, key.size(), "$");
		RedisAppendContent(raw_args, key);
		RedisAppendLen(raw_args, dbvalue.kvv.size(), "$");
		RedisAppendContent(raw_args, dbvalue.kvv);
	  }
	  break;
	case blackwidow::kLists :
	  {
		//first del old key, prevent last migrate failed
		RedisAppendLen(raw_args, 2, "*");
		RedisAppendLen(raw_args, 3, "$");
		RedisAppendContent(raw_args, "del");
		RedisAppendLen(raw_args, key.size(), "$");
		RedisAppendContent(raw_args, key);
		BinlogPut(key, raw_args);

		//rpush key member
		raw_args.clear();
		int64_t len = 1 + 1 + dbvalue.listv.size();
		RedisAppendLen(raw_args, len, "*");
		RedisAppendLen(raw_args, 5, "$");
		RedisAppendContent(raw_args, "rpush");
		RedisAppendLen(raw_args, key.size(), "$");
		RedisAppendContent(raw_args, key);
		std::vector<std::string>::const_iterator iter = dbvalue.listv.begin();
		for (; iter != dbvalue.listv.end(); iter++) {
		  RedisAppendLen(raw_args, (*iter).size(), "$");
		  RedisAppendContent(raw_args, *iter);
		}
	  }
	  break;
	case blackwidow::kSets :
	  {
		//sadd key member
		int64_t len = 1 + 1 + dbvalue.setv.size();
		RedisAppendLen(raw_args, len, "*");
		RedisAppendLen(raw_args, 4, "$");
		RedisAppendContent(raw_args, "sadd");
		RedisAppendLen(raw_args, key.size(), "$");
		RedisAppendContent(raw_args, key);
		std::vector<std::string>::const_iterator iter = dbvalue.setv.begin();
		for (; iter != dbvalue.setv.end(); iter++) {
		  RedisAppendLen(raw_args, (*iter).size(), "$");
		  RedisAppendContent(raw_args, *iter);
		}
	  }
	  break;
	case blackwidow::kZSets :
	  {
		//zadd key score member
		int64_t len = 1 + 1 + dbvalue.zsetv.size() * 2;
		RedisAppendLen(raw_args, len, "*");
		RedisAppendLen(raw_args, 4, "$");
		RedisAppendContent(raw_args, "zadd");
		RedisAppendLen(raw_args, key.size(), "$");
		RedisAppendContent(raw_args, key);
		std::vector<blackwidow::ScoreMember>::const_iterator iter = dbvalue.zsetv.begin();
		for (; iter != dbvalue.zsetv.end(); iter++) {
		  RedisAppendLen(raw_args, std::to_string(iter->score).size(), "$");
		  RedisAppendContent(raw_args, std::to_string(iter->score));
		  RedisAppendLen(raw_args, iter->member.size(), "$");
		  RedisAppendContent(raw_args, iter->member);
		}
	  }
	  break;
	case blackwidow::kHashes :
	  {
		//hset key field value
		int64_t len = 1 + 1 + dbvalue.hashv.size() * 2;
		RedisAppendLen(raw_args, len, "*");
		RedisAppendLen(raw_args, 5, "$");
		RedisAppendContent(raw_args, "hmset");
		RedisAppendLen(raw_args, key.size(), "$");
		RedisAppendContent(raw_args, key);
		std::vector<blackwidow::FieldValue>::const_iterator iter = dbvalue.hashv.begin();
		for (; iter != dbvalue.hashv.end(); iter++) {
		  RedisAppendLen(raw_args, iter->field.size(), "$");
		  RedisAppendContent(raw_args, iter->field);
		  RedisAppendLen(raw_args, iter->value.size(), "$");
		  RedisAppendContent(raw_args, iter->value);
		}
	  }
	  break;
	default:
	  raw_args = "";
  }

  //write key command to binlog
  if (raw_args.size() > 0) {
	BinlogPut(key, raw_args);

	//write pexpire command to binlog, ttlms is 0 or >=1, 0 indicates no expire
	if (ttlms > 0){
		//add ttlms command
		raw_args.clear();
		RedisAppendLen(raw_args, 3, "*");
		RedisAppendLen(raw_args, 7, "$");
		RedisAppendContent(raw_args, "pexpire");
		RedisAppendLen(raw_args, key.size(), "$");
		RedisAppendContent(raw_args, key);
		RedisAppendLen(raw_args, std::to_string(ttlms).size(), "$");
		RedisAppendContent(raw_args, std::to_string(ttlms));

	  	BinlogPut(key, raw_args);
	}
  }
  
}

/* *
 * slotsrestore key ttlms value
 * ttlms is 0 or >=1, 0 indicates no expire
 * */
void SlotsrestoreCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size()) || (argv.size() - 1) % 3 != 0) {
	res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsrestore);
	return;
  }

  restore_keys_.clear();

  size_t index = 1;
  int64_t ttlms;

  for (; index < argv.size(); index += 3){
	if (!slash::string2l(argv[index+1].data(), argv[index+1].size(), &ttlms)) {
	  res_.SetRes(CmdRes::kInvalidInt);
	  return;
	}

	if (ttlms < 0){
	  res_.SetRes(CmdRes::kErrOther, "invalid ttl value, ttl must be >= 0");
	  return;
	}

	restore_keys_.push_back({argv[index], ttlms, argv[index+2]});
  }
}

void SlotsrestoreCmd::Do() {
  rocksdb::Status s;
  std::vector<struct RestoreKey>::const_iterator iter = restore_keys_.begin();

  for (; iter != restore_keys_.end(); iter++) {
	//LOG(ERROR) << "SlotsrestoreCmd: key[" << iter->key << "], ttlms[" << iter->ttlms << "]"; //, value[" << iter->value << "]";

	if (verifyDumpPayload((unsigned char *)iter->value.data(), iter->value.size()) != REDIS_OK) {
	  std::string detail = "dump payload version or checksum are wrong";
	  LOG(ERROR) << detail;
	  res_.SetRes(CmdRes::kErrOther, detail);
	  return;
	}

	rio payload;
	int rdbtype;
	restore_value dbvalue;
	rioInitWithBuffer(&payload, iter->value.data(), iter->value.size());

	//check the type of rdb, and parse rdb
	if ((rdbtype = rdbLoadObjectType(&payload)) == -1) {
		std::string detail = "load object type failed";
		LOG(ERROR) << detail;
		res_.SetRes(CmdRes::kErrOther, detail);
		return;
	}
	// not support quicklist, just skip
	if (rdbtype == REDIS_RDB_TYPE_LIST_QUICKLIST) {
		res_.SetRes(CmdRes::kOk);
		return;
	}
	if (rdbLoadObject(rdbtype, &payload, &dbvalue) != REDIS_OK) {
		std::string detail = "bad slotsrestore rdb format";
	  LOG(ERROR) << detail;
	  res_.SetRes(CmdRes::kErrOther, detail);
	  return;
	}

	// if ((rdbtype = rdbLoadObjectType(&payload)) == -1 ||
	// 			rdbLoadObject(rdbtype, &payload, &dbvalue) != REDIS_OK ) {
	//   std::string detail = "bad slotsrestore rdb format";
	//   LOG(ERROR) << detail;
	//   res_.SetRes(CmdRes::kErrOther, detail);
	//   return;
	// }

	//lock record, here dont lock because of codis has one migrate connect
	//g_pika_server->mutex_record_.Lock(iter->key);

	//LOG(ERROR) << "slotsrestore(): dbvalue.type = " << dbvalue.type;
	switch (dbvalue.type){
	  case blackwidow::kStrings :
		s = RestoreKV(iter->key, dbvalue);
		break;
	  case blackwidow::kLists :
		{
		  //del old key, prevent last migrate failed
		  int64_t count = 0;
		  std::vector<std::string> keys;
		  std::map<blackwidow::DataType, blackwidow::Status> type_status;
		  keys.push_back(iter->key);
		  count = g_pika_server->db()->Del(keys, &type_status);
		  if (count < 0) {
			res_.SetRes(CmdRes::kErrOther, "delete error");
			return;
		  }

		  s = RestoreList(iter->key, dbvalue);
		}
		break;
	  case blackwidow::kSets :
		s = RestoreSet(iter->key, dbvalue);
		break;
	  case blackwidow::kZSets :
		s = RestoreZset(iter->key, dbvalue);
		break;
	  case blackwidow::kHashes :
		s = RestoreHash(iter->key, dbvalue);
		break;
	  default:
		std::string detail = "error db type";
		res_.SetRes(CmdRes::kErrOther, detail);
		return;
	}

	//unlock record
	//g_pika_server->mutex_record_.Unlock(argv_[1])
	if (!s.ok()) {
	  res_.SetRes(CmdRes::kErrOther, s.ToString());
	  return;
	}

	//set ttl, ttlms is 0 or >=1, 0 indicates no expire
	if (iter->ttlms > 0){
	  std::map<blackwidow::DataType, rocksdb::Status> type_status;
	  int32_t ret = g_pika_server->db()->Expire(iter->key, iter->ttlms/1000, &type_status);
	  if (ret == -1) {
		std::string detail = "expire exec failed";
		res_.SetRes(CmdRes::kErrOther, detail);
		return;
	  }

	}

	//write binlog
	//del key and add key, or send restore command
	WriteCommandToBinlog(iter->key, iter->ttlms, dbvalue);
  }

  res_.SetRes(CmdRes::kOk);
  return;
}

void SlotsMgrtTagSlotAsyncCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info)
{
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtTagSlotAsync);
        return;
    }
    PikaCmdArgsType::const_iterator it = argv.begin() + 1; //Remember the first args is the opt name
    dest_ip_ = *it++;
    slash::StringToLower(dest_ip_);

    std::string str_dest_port = *it++;
    if (!slash::string2l(str_dest_port.data(), str_dest_port.size(), &dest_port_) || dest_port_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }

    if ((dest_ip_ == "127.0.0.1" || dest_ip_ == g_pika_server->host()) && dest_port_ == g_pika_server->port()) {
        res_.SetRes(CmdRes::kErrOther, "destination address error");
        return;
    }

    std::string str_timeout_ms = *it++;
    if (!slash::string2l(str_timeout_ms.data(), str_timeout_ms.size(), &timeout_ms_) || timeout_ms_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }

    std::string str_max_bulks = *it++;
    if (!slash::string2l(str_max_bulks.data(), str_max_bulks.size(), &max_bulks_) || max_bulks_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }

    std::string str_max_bytes_ = *it++;
    if (!slash::string2l(str_max_bytes_.data(), str_max_bytes_.size(), &max_bytes_) || max_bytes_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }

    std::string str_slot_num = *it++;
    if (!slash::string2l(str_slot_num.data(), str_slot_num.size(), &slot_num_) || slot_num_ < 0 || slot_num_ >= HASH_SLOTS_SIZE) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }

    std::string str_keys_num = *it++;
    if (!slash::string2l(str_keys_num.data(), str_keys_num.size(), &keys_num_) || keys_num_ < 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }
    return;
}

void SlotsMgrtTagSlotAsyncCmd::Do()
{
    // check whether open slotmigrate 
    if (!g_pika_conf->slotmigrate()) {
        res_.SetRes(CmdRes::kErrOther, "please open slotmigrate and reload slot");
        return;
    }

    g_pika_server->ReqMigrateBatch(dest_ip_, dest_port_, timeout_ms_, slot_num_, keys_num_);
    
    std::string slotKey = SlotKeyPrefix + std::to_string(slot_num_);
    int32_t len = 0;
    g_pika_server->db()->SCard(slotKey, &len);
    if (0 <= len) {
        res_.AppendArrayLen(2);
        res_.AppendInteger(0);    //the number of keys migrated
        res_.AppendInteger(len);  //the number of keys remained
    }
    else {
        res_.SetRes(CmdRes::kErrOther, "Get the len of slot Error");
    }

    return;
}

void SlotsMgrtExecWrapperCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info)
{
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtExecWrapper);
        return;
    }

    PikaCmdArgsType::const_iterator it = argv.begin() + 1;
    key_ = *it++;
    slash::StringToLower(key_);
    return;
}

void SlotsMgrtExecWrapperCmd::Do()
{
    res_.AppendArrayLen(2);
    int ret = g_pika_server->ReqMigrateOne(key_);
    switch (ret) {
    case 0:
        res_.AppendInteger(0);
        res_.AppendInteger(0);
        return;
    case 1:
        res_.AppendInteger(1);
        res_.AppendInteger(1);
        return;
    default:
        res_.AppendInteger(-1);
        res_.AppendInteger(-1);
        return;
    }
    return;
}

void SlotsMgrtAsyncStatusCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info)
{
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtAsyncStatus);
        return;
    }
    return;
}

void SlotsMgrtAsyncStatusCmd::Do()
{
    std::string status;
    std::string ip;
    int64_t port, slot, moved, remained;
    bool migrating;
    g_pika_server->GetMigrateStatus(&ip, &port, &slot, &migrating, &moved, &remained);
    std::string mstatus = migrating ? "yes" : "no";
    res_.AppendArrayLen(5);
    status = "dest server: " + ip + ":" + std::to_string(port);
    res_.AppendStringLen(status.size());
    res_.AppendContent(status);
    status = "slot number: " + std::to_string(slot);
    res_.AppendStringLen(status.size());
    res_.AppendContent(status);
    status = "migrating  : " + mstatus;
    res_.AppendStringLen(status.size());
    res_.AppendContent(status);
    status = "moved keys : " + std::to_string(moved);
    res_.AppendStringLen(status.size());
    res_.AppendContent(status);
    status = "remain keys: " + std::to_string(remained);
    res_.AppendStringLen(status.size());
    res_.AppendContent(status);

    return;
}

void SlotsMgrtAsyncCancelCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info)
{
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtAsyncCancel);
        return;
    }
    return;
}

void SlotsMgrtAsyncCancelCmd::Do()
{
    g_pika_server->CancelMigrate();
    res_.SetRes(CmdRes::kOk);
    return;
}
