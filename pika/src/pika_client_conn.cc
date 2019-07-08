// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <sstream>
#include <vector>
#include <algorithm>

#include <glog/logging.h>
#include <time.h>

#include "slash/include/slash_coding.h"
#include "slash/include/slash_recordlock.h"
#include "pika_server.h"
#include "pika_conf.h"
#include "pika_client_conn.h"
#include "pika_dispatch_thread.h"
#include "pika_define.h"
#include "pika_commonfunc.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;
static const int RAW_ARGS_LEN = 1024 * 1024; 

static std::string ConstructPubSubResp(
                                const std::string& cmd,
                                const std::vector<std::pair<std::string, int>>& result) {
	std::stringstream resp;
	if (result.size() == 0) {
		resp << "*3\r\n" << "$" << cmd.length() << "\r\n" << cmd << "\r\n" <<
	                    "$" << -1           << "\r\n" << ":" << 0      << "\r\n";
	}
	for (auto it = result.begin(); it != result.end(); it++) {
		resp << "*3\r\n" << "$" << cmd.length()       << "\r\n" << cmd       << "\r\n" <<
	                    "$" << it->first.length() << "\r\n" << it->first << "\r\n" <<
	                    ":" << it->second         << "\r\n";
	}
	return resp.str();
}

PikaClientConn::PikaClientConn(int fd, std::string ip_port,
															 pink::ServerThread* server_thread,
															 void* worker_specific_data)
			: RedisConn(fd, ip_port, server_thread),
				server_thread_(server_thread),
				cmds_table_(reinterpret_cast<CmdTable*>(worker_specific_data)),
				is_pubsub_(false) {
	auth_stat_.Init();
}

std::string PikaClientConn::RestoreArgs() {
	std::string res;
	res.reserve(RAW_ARGS_LEN);
	RedisAppendLen(res, argv_.size(), "*");
	PikaCmdArgsType::const_iterator it = argv_.begin();
	for ( ; it != argv_.end(); ++it) {
		RedisAppendLen(res, (*it).size(), "$");
		RedisAppendContent(res, *it);
	}
	return res;
}

std::string PikaClientConn::DoCmd(const std::string& opt) {
	// Get command info
	const CmdInfo* const cinfo_ptr = GetCmdInfo(opt);
	Cmd* c_ptr = GetCmdFromTable(opt, *cmds_table_);
	if (!cinfo_ptr || !c_ptr) {
			return "-Err unknown or unsupported command \'" + opt + "\'\r\n";
	}

	// Check authed
	if (!auth_stat_.IsAuthed(cinfo_ptr)) {
		return "-ERR NOAUTH Authentication required.\r\n";
	}
	
	uint64_t start_us = 0;
	if (g_pika_conf->slowlog_slower_than() >= 0) {
		start_us = slash::NowMicros();
	}

	// For now, only shutdown need check local
	if (cinfo_ptr->is_local()) {
		if (ip_port().find("127.0.0.1") == std::string::npos
				&& ip_port().find(g_pika_server->host()) == std::string::npos) {
			LOG(WARNING) << "\'shutdown\' should be localhost";
			return "-ERR \'shutdown\' should be localhost\r\n";
		}
	}

	std::string monitor_message;
	bool is_monitoring = g_pika_server->HasMonitorClients();
	if (is_monitoring) {
		monitor_message = std::to_string(1.0*slash::NowMicros()/1000000) +
			" [" + this->ip_port() + "]";
		for (PikaCmdArgsType::iterator iter = argv_.begin(); iter != argv_.end(); iter++) {
			monitor_message += " " + slash::ToRead(*iter);
		}
		g_pika_server->AddMonitorMessage(monitor_message);
	}

	// Initial
	c_ptr->Initial(argv_, cinfo_ptr);
	if (!c_ptr->res().ok()) {
		return c_ptr->res().message();
	}

	// PubSub connection
	if (this->IsPubSub()) {
		if (opt != kCmdNameSubscribe &&
			opt != kCmdNameUnSubscribe &&
			opt != kCmdNamePing &&
			opt != kCmdNamePSubscribe &&
			opt != kCmdNamePUnSubscribe) {
			return "-ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context\r\n";
		}
	}

	if (opt == kCmdNameMonitor) {
		pink::PinkConn* conn = server_thread_->MoveConnOut(fd());
		assert(conn == this);
		g_pika_server->AddMonitorClient(static_cast<PikaClientConn*>(conn));
		g_pika_server->AddMonitorMessage("OK");
		return ""; // Monitor thread will return "OK"
	}

	// PubSub
	if (opt == kCmdNameSubscribe) {                                   // Subscribe
		pink::PinkConn* conn = this;
		if (!this->IsPubSub()) {
			conn = server_thread_->MoveConnOut(fd());
		}
		std::vector<std::string > channels;
		for (size_t i = 1; i < argv_.size(); i++) {
			channels.push_back(slash::StringToLower(argv_[i]));
		}
		std::vector<std::pair<std::string, int>> result;
		g_pika_server->Subscribe(conn, channels, false, &result);
		this->SetIsPubSub(true);
		return ConstructPubSubResp(kCmdNameSubscribe, result);
	} else if (opt == kCmdNameUnSubscribe) {                          // UnSubscribe
		std::vector<std::string > channels;
		for (size_t i = 1; i < argv_.size(); i++) {
			channels.push_back(slash::StringToLower(argv_[i]));
		}
		std::vector<std::pair<std::string, int>> result;
		int subscribed = g_pika_server->UnSubscribe(this, channels, false, &result);
		if (subscribed == 0 && this->IsPubSub()) {
			/*
			* if the number of client subscribed is zero,
			* the client will exit the Pub/Sub state
			*/
			server_thread_->HandleNewConn(fd(), ip_port());
			this->SetIsPubSub(false);
		}
		return ConstructPubSubResp(kCmdNameUnSubscribe, result);
	} else if (opt == kCmdNamePSubscribe) {                           // PSubscribe
		pink::PinkConn* conn = this;
		if (!this->IsPubSub()) {
			conn = server_thread_->MoveConnOut(fd());
		}
		std::vector<std::string > channels;
		for (size_t i = 1; i < argv_.size(); i++) {
			channels.push_back(slash::StringToLower(argv_[i]));
		}
		std::vector<std::pair<std::string, int>> result;
		g_pika_server->Subscribe(conn, channels, true, &result);
		this->SetIsPubSub(true);
		return ConstructPubSubResp(kCmdNamePSubscribe, result);
	} else if (opt == kCmdNamePUnSubscribe) {                          // PUnSubscribe
		std::vector<std::string > channels;
		for (size_t i = 1; i < argv_.size(); i++) {
			channels.push_back(slash::StringToLower(argv_[i]));
		}
		std::vector<std::pair<std::string, int>> result;
		int subscribed = g_pika_server->UnSubscribe(this, channels, true, &result);
		if (subscribed == 0 && this->IsPubSub()) {
			/*
			* if the number of client subscribed is zero,
			* the client will exit the Pub/Sub state
			*/
			server_thread_->HandleNewConn(fd(), ip_port());
			this->SetIsPubSub(false);
		}
		return ConstructPubSubResp(kCmdNamePUnSubscribe, result);
	}

	std::string raw_args;
	if (cinfo_ptr->is_write()) {
		if (g_pika_server->BinlogIoError()) {
			return "-ERR Writing binlog failed, maybe no space left on device\r\n";
		}
		if (g_pika_conf->readonly()) {
			return "-ERR Server in read-only\r\n";
		}
		raw_args = RestoreArgs();
		if (argv_.size() >= 2) {
			g_pika_server->LockMgr()->TryLock(argv_[1]);
		}
	}

	// Add read lock for no suspend command
	if (!cinfo_ptr->is_suspend()) {
		g_pika_server->RWLockReader();
	}

	/*
	* 操作缓存条件：
	* 1. 该命令需要操作缓存
	* 2. 缓存模式不为NONE
	* 3. 缓存状态为OK
	* 4. 当前不是slave状态
	*/
	if (cinfo_ptr->need_cache_do() 
		&& PIKA_CACHE_NONE != g_pika_conf->cache_model()
		&& PIKA_CACHE_STATUS_OK == g_pika_server->Cache()->CacheStatus()
		&& !g_pika_server->is_slave()) {

		// 是否需要读缓存
		if (cinfo_ptr->need_read_cache()) {
			// LOG(INFO) << "PikaClientConn::DoCmd " << argv_[0] << " PreDo";
			c_ptr->PreDo();
		}

		// 当前是读命令，并且缓存未命中，则需要继续访问rocksdb
		if (cinfo_ptr->is_read() && c_ptr->res().CacheMiss()) {
			// 访问Rocksdb时，需要对key进行加锁，保证操作rocksdb和cache是原子的
			slash::ScopeRecordLock l(g_pika_server->LockMgr(), argv_[1]);
			// LOG(INFO) << "PikaClientConn::DoCmd " << argv_[0] << " read CacheDo";
			c_ptr->CacheDo();

			// 访问Rocksdb成功并且该命令需要更新缓存
			if (c_ptr->CmdStatus().ok() && cinfo_ptr->need_write_cache()) {
				// LOG(INFO) << "PikaClientConn::DoCmd " << argv_[0] << " read PostDo";
				c_ptr->PostDo();
			}	
		}  else if (cinfo_ptr->is_write()) {
			// 当前是写命令时，目前还不支持异步写入，所有写命令都要去操作rocksdb
			// LOG(INFO) << "PikaClientConn::DoCmd " << argv_[0] << " write CacheDo";
			c_ptr->CacheDo();

			// 访问Rocksdb成功并且该命令需要更新缓存
			if (c_ptr->CmdStatus().ok() && cinfo_ptr->need_write_cache()) {
				// LOG(INFO) << "PikaClientConn::DoCmd " << argv_[0] << " write PostDo";
				c_ptr->PostDo();
			}
		}
	} else {
		c_ptr->Do();
	}
	
	if (cinfo_ptr->is_write()) {
		if (c_ptr->res().ok()) {
			//g_pika_server->logger_->Lock();
			//slash::Status s = g_pika_server->logger_->Put(raw_args);
			//g_pika_server->logger_->Unlock();
			std::string key = argv_.size() >= 2 ? argv_[1] : argv_[0];
			uint32_t crc = PikaCommonFunc::CRC32Update(0, key.data(), (int)key.size());
			int binlog_writer_num = g_pika_conf->binlog_writer_num();
    		int thread_index =  (int)(crc % binlog_writer_num);
			slash::Status s = g_pika_server->binlog_write_thread_[thread_index]->WriteBinlog(raw_args, "sync" == g_pika_conf->binlog_writer_method());
			if (!s.ok()) {
				LOG(WARNING) << "Writing binlog failed, maybe no space left on device";
				//g_pika_server->SetBinlogIoError(true);
				for (int i=0; i<binlog_writer_num; i++) {
					if (i != thread_index) {
						g_pika_server->binlog_write_thread_[i]->SetBinlogIoError(true);
					}
				}

				if (!cinfo_ptr->is_suspend()) {
					g_pika_server->RWUnlock();
				}
				if (argv_.size() >= 2) {
					g_pika_server->LockMgr()->UnLock(argv_[1]);
				}
				
				return "-ERR Writing binlog failed, maybe no space left on device\r\n";
			}
		}
	}

	if (!cinfo_ptr->is_suspend()) {
		g_pika_server->RWUnlock();
	}

	if (cinfo_ptr->is_write()) {
		if (argv_.size() >= 2) {
			g_pika_server->LockMgr()->UnLock(argv_[1]);
		}
	}

	if (g_pika_conf->slowlog_slower_than() >= 0) {
		int64_t duration = slash::NowMicros() - start_us;
		if (duration > g_pika_conf->slowlog_slower_than()) {
			int32_t start_time = start_us / 1000000;
			std::string slow_log;
			for (unsigned int i = 0; i < argv_.size(); i++) {
				slow_log.append(" ");
				slow_log.append(slash::ToRead(argv_[i]));
				if (slow_log.size() >= 1000) {
					slow_log.resize(1000);
					slow_log.append("...\"");
					break;
				}
			}
			static uint64_t id = 0;
			LOG(ERROR) << "id:" << ++id << ", ip_port: "<< ip_port() << ", command:" << slow_log << ", start_time(s): " << start_time << ", duration(us): " << duration;
			g_pika_server->SlowlogPushEntry(argv_, start_time, duration);
		}
	}

	if (opt == kCmdNameAuth) {
		if(!auth_stat_.ChecknUpdate(c_ptr->res().raw_message())) {
//      LOG(WARNING) << "(" << ip_port() << ")Wrong Password";
		}
	}
	return c_ptr->res().message();
}

int PikaClientConn::DealMessage() {
	g_pika_server->PlusThreadQuerynum();
	
	if (argv_.empty()) return -2;
	std::string opt = argv_[0];
	slash::StringToLower(opt);
	std::string res = DoCmd(opt);
	
	if ((wbuf_size_ - wbuf_len_ < res.size())) {
		//LOG(WARNING) << "do expandwbuf, ip_port_ : " << ip_port() << ", fd_ : " << fd();
		//LOG(WARNING) << "do expandwbuf, wbuf_size_ : " << wbuf_size_ << ", wbuf_len_ : " << wbuf_len_ << ", res.size() : " << res.size();
		if (!ExpandWbufTo(wbuf_len_ + res.size())) {
			LOG(WARNING) << "ExpandWbufTo " << wbuf_len_ + res.size() << " Failed";
			memcpy(wbuf_, "-ERR expand writer buffer failed\r\n", 34);
			wbuf_len_ = 34;
			set_is_reply(true);
			return 0;
		}
	}
	memcpy(wbuf_ + wbuf_len_, res.data(), res.size());
	wbuf_len_ += res.size();
	set_is_reply(true);
	return 0;
}

// Initial permission status
void PikaClientConn::AuthStat::Init() {
	// Check auth required
	stat_ = g_pika_conf->userpass() == "" ?
		kLimitAuthed : kNoAuthed;
	if (stat_ == kLimitAuthed 
			&& g_pika_conf->requirepass() == "") {
		stat_ = kAdminAuthed;
	}
}

// Check permission for current command
bool PikaClientConn::AuthStat::IsAuthed(const CmdInfo* const cinfo_ptr) {
	std::string opt = cinfo_ptr->name();
	if (opt == kCmdNameAuth) {
		return true;
	}
	const std::vector<std::string>& blacklist = g_pika_conf->vuser_blacklist();
	switch (stat_) {
		case kNoAuthed:
			return false;
		case kAdminAuthed:
			break;
		case kLimitAuthed:
			if (cinfo_ptr->is_admin_require() 
					|| find(blacklist.begin(), blacklist.end(), opt) != blacklist.end()) {
			return false;
			}
			break;
		default:
			LOG(WARNING) << "Invalid auth stat : " << static_cast<unsigned>(stat_);
			return false;
	}
	return true;
}

// Update permission status
bool PikaClientConn::AuthStat::ChecknUpdate(const std::string& message) {
	// Situations to change auth status
	if (message == "USER") {
		stat_ = kLimitAuthed;
	} else if (message == "ROOT"){
		stat_ = kAdminAuthed;
	} else {
		return false;
	}
	return true;
}
