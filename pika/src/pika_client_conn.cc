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
#include "pika_cmd_table_manager.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;
extern PikaCmdTableManager* g_pika_cmd_table_manager;

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
							   void* worker_specific_data,
							   pink::PinkEpoll* pink_epoll,
							   const pink::HandleType& handle_type)
			: RedisConn(fd, ip_port, server_thread, pink_epoll, handle_type),
			  server_thread_(server_thread),
			  cmds_table_(reinterpret_cast<CmdTable*>(worker_specific_data)),
			  is_pubsub_(false) {
	auth_stat_.Init();
}

std::string PikaClientConn::RestoreArgs(const PikaCmdArgsType& argv) {
	std::string res;
	res.reserve(RAW_ARGS_LEN);
	RedisAppendLen(res, argv.size(), "*");
	PikaCmdArgsType::const_iterator it = argv.begin();
	for ( ; it != argv.end(); ++it) {
		RedisAppendLen(res, (*it).size(), "$");
		RedisAppendContent(res, *it);
	}
	return res;
}

std::string PikaClientConn::DoCmd(const PikaCmdArgsType& argv,
								  const std::string& opt,
								  uint64_t recv_cmd_time_us) {
	// Get command info
	const CmdInfo* const cinfo_ptr = GetCmdInfo(opt);
	Cmd* c_ptr = g_pika_cmd_table_manager->GetCmd(opt);
	if (!cinfo_ptr || !c_ptr) {
		return "-Err unknown or unsupported command \'" + opt + "\'\r\n";
	}

	// Check authed
	if (!auth_stat_.IsAuthed(cinfo_ptr)) {
		return "-ERR NOAUTH Authentication required.\r\n";
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
		for (PikaCmdArgsType::const_iterator iter = argv.begin(); iter != argv.end(); iter++) {
			monitor_message += " " + slash::ToRead(*iter);
		}
		g_pika_server->AddMonitorMessage(monitor_message);
	}

	// Initial
	c_ptr->Initial(argv, cinfo_ptr);
	if (!c_ptr->res().ok()) {
		g_pika_server->GetCmdStats()->IncrOpStatsByCmd(cinfo_ptr->name(), slash::NowMicros() - recv_cmd_time_us, true);
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
		std::shared_ptr<PinkConn> conn= server_thread_->MoveConnOut(fd());
		assert(conn.get() == this);
		g_pika_server->AddMonitorClient(std::dynamic_pointer_cast<PikaClientConn>(conn));
		g_pika_server->AddMonitorMessage("OK");
		return ""; // Monitor thread will return "OK"
	}

	// PubSub
	if (opt == kCmdNamePSubscribe || opt == kCmdNameSubscribe) {    // PSubscribe or Subscribe
		std::shared_ptr<PinkConn> conn = std::dynamic_pointer_cast<PikaClientConn>(shared_from_this());
		if (!this->IsPubSub()) {
			conn = server_thread_->MoveConnOut(fd());
		}
		std::vector<std::string > channels;
		for (size_t i = 1; i < argv.size(); i++) {
			channels.push_back(argv[i]);
		}
		std::vector<std::pair<std::string, int>> result;
		this->SetIsPubSub(true);
		this->SetHandleType(pink::HandleType::kSynchronous);
		g_pika_server->Subscribe(conn, channels, opt == kCmdNamePSubscribe, &result);
		return ConstructPubSubResp(opt, result);
	} else if (opt == kCmdNamePUnSubscribe || opt == kCmdNameUnSubscribe) {  // PUnSubscribe or UnSubscribe
		std::vector<std::string > channels;
		for (size_t i = 1; i < argv.size(); i++) {
			channels.push_back(argv[i]);
		}
		std::vector<std::pair<std::string, int>> result;
		std::shared_ptr<PinkConn> conn = std::dynamic_pointer_cast<PikaClientConn>(shared_from_this());
		int subscribed = g_pika_server->UnSubscribe(conn, channels, opt == kCmdNamePUnSubscribe, &result);
		if (subscribed == 0 && this->IsPubSub()) {
			/*
			* if the number of client subscribed is zero,
			* the client will exit the Pub/Sub state
			*/
			server_thread_->HandleNewConn(fd(), ip_port());
			this->SetIsPubSub(false);
		}
		return ConstructPubSubResp(opt, result);
	}

	if (cinfo_ptr->is_write()) {
		if (g_pika_server->BinlogIoError()) {
			g_pika_server->GetCmdStats()->IncrOpStatsByCmd(cinfo_ptr->name(), slash::NowMicros() - recv_cmd_time_us, true);
			return "-ERR Writing binlog failed, maybe no space left on device\r\n";
		}
		if (g_pika_conf->readonly()) {
			g_pika_server->GetCmdStats()->IncrOpStatsByCmd(cinfo_ptr->name(), slash::NowMicros() - recv_cmd_time_us, true);
			return "-ERR Server in read-only\r\n";
		}
		if (argv.size() >= 2) {
			g_pika_server->LockMgr()->TryLock(argv[1]);
		}
	}

	// Add read lock for no suspend command
	if (!cinfo_ptr->is_suspend()) {
		g_pika_server->RWLockReader();
	}

	uint64_t before_do_time_us = slash::NowMicros();
	uint64_t after_cache_time_us = 0;
	uint64_t after_rocksdb_time_us = 0;

	uint64_t queue_time = before_do_time_us - recv_cmd_time_us; // 命令排队时间
	uint64_t cache_time = 0;  	// 读redisdb时间
	uint64_t rocksdb_time = 0;  // 读rocksdb时间
	uint64_t binlog_time = 0;	// 写binlog时间

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
			// LOG(INFO) << "PikaClientConn::DoCmd " << argv[0] << " PreDo";
			c_ptr->PreDo();
			after_cache_time_us = slash::NowMicros();
			cache_time = after_cache_time_us - before_do_time_us;
		}

		// 当前是读命令，并且缓存未命中，则需要继续访问rocksdb
		if (cinfo_ptr->is_read() && c_ptr->res().CacheMiss()) {
			// 访问Rocksdb时，需要对key进行加锁，保证操作rocksdb和cache是原子的
			slash::ScopeRecordLock l(g_pika_server->LockMgr(), argv[1]);
			// LOG(INFO) << "PikaClientConn::DoCmd " << argv[0] << " read CacheDo";
			c_ptr->CacheDo();
			after_rocksdb_time_us = slash::NowMicros();
			rocksdb_time = after_rocksdb_time_us - after_cache_time_us;

			// 访问Rocksdb成功并且该命令需要更新缓存
			if (c_ptr->CmdStatus().ok() && cinfo_ptr->need_write_cache()) {
				// LOG(INFO) << "PikaClientConn::DoCmd " << argv[0] << " read PostDo";
				c_ptr->PostDo();
			}	
		}  else if (cinfo_ptr->is_write()) {
			// 当前是写命令时，目前还不支持异步写入，所有写命令都要去操作rocksdb
			// LOG(INFO) << "PikaClientConn::DoCmd " << argv[0] << " write CacheDo";
			c_ptr->CacheDo();
			after_rocksdb_time_us = slash::NowMicros();
			rocksdb_time = after_rocksdb_time_us - before_do_time_us;

			// 访问Rocksdb成功并且该命令需要更新缓存
			if (c_ptr->CmdStatus().ok() && cinfo_ptr->need_write_cache()) {
				// LOG(INFO) << "PikaClientConn::DoCmd " << argv[0] << " write PostDo";
				c_ptr->PostDo();
			}
		}
	} else {
		c_ptr->Do();
		after_rocksdb_time_us = slash::NowMicros();
		rocksdb_time = after_rocksdb_time_us - before_do_time_us;
	}
	
	if (g_pika_conf->write_binlog() && cinfo_ptr->is_write()) {
		if (c_ptr->res().ok()) {
			std::string raw_args;
			if (cinfo_ptr->name() == kCmdNameExpire || cinfo_ptr->name() == kCmdNamePexpire) {
				raw_args = c_ptr->ToBinlog();
			} else {
				raw_args = RestoreArgs(argv);
			}

			//g_pika_server->logger_->Lock();
			//slash::Status s = g_pika_server->logger_->Put(raw_args);
			//g_pika_server->logger_->Unlock();
			std::string key = argv.size() >= 2 ? argv[1] : argv[0];
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
				if (argv.size() >= 2) {
					g_pika_server->LockMgr()->UnLock(argv[1]);
				}
				
				g_pika_server->GetCmdStats()->IncrOpStatsByCmd(cinfo_ptr->name(), slash::NowMicros() - recv_cmd_time_us, true);
				return "-ERR Writing binlog failed, maybe no space left on device\r\n";
			}
		}
		binlog_time = slash::NowMicros() - after_rocksdb_time_us;
	}

	if (!cinfo_ptr->is_suspend()) {
		g_pika_server->RWUnlock();
	}

	if (cinfo_ptr->is_write()) {
		if (argv.size() >= 2) {
			g_pika_server->LockMgr()->UnLock(argv[1]);
		}
	}

	if (g_pika_conf->slowlog_slower_than() >= 0) {
		int64_t total_time = queue_time + cache_time + rocksdb_time + binlog_time;
		g_pika_server->GetCmdStats()->IncrOpStatsByCmd(cinfo_ptr->name(), total_time, !(c_ptr->res().ok()));
		if (total_time > g_pika_conf->slowlog_slower_than()) {
			int32_t start_time = recv_cmd_time_us / 1000000;
			std::string slow_log;
			for (unsigned int i = 0; i < argv.size(); i++) {
				slow_log.append(" ");
				slow_log.append(slash::ToRead(argv[i]));
				if (slow_log.size() >= 1000) {
					slow_log.resize(1000);
					slow_log.append("...\"");
					break;
				}
			}
			static uint64_t id = 0;
			LOG(ERROR) << "id:" << ++id 
					   << ", ip_port: "<< ip_port() 
					   << ", command: " << slow_log 
					   << ", start_time(s): " << start_time 
					   << ", [ " << queue_time << " " << cache_time << " " << rocksdb_time << " " << binlog_time << " ]"
					   << ", total_time: " << total_time;
			g_pika_server->SlowlogPushEntry(argv, start_time, total_time);
		}
	}

	if (opt == kCmdNameAuth) {
		if(!auth_stat_.ChecknUpdate(c_ptr->res().raw_message())) {
//      LOG(WARNING) << "(" << ip_port() << ")Wrong Password";
		}
	}
	return c_ptr->res().message();
}

void PikaClientConn::SyncProcessRedisCmd(const pink::RedisCmdArgsType& argv, std::string* response) {
	bool success = true;
	if (ExecRedisCmd(argv, response, slash::NowMicros()) != 0) {
		success = false;
	}

	if (!response->empty()) {
		set_is_reply(true);
		NotifyEpoll(success);
	}
}

void PikaClientConn::AsynProcessRedisCmds(const std::vector<pink::RedisCmdArgsType>& argvs, std::string* response) {
	BgTaskArg* arg = new BgTaskArg();
	arg->redis_cmds = argvs;
	arg->response = response;
	arg->pcc = std::dynamic_pointer_cast<PikaClientConn>(shared_from_this());
	arg->recv_cmd_time_us = slash::NowMicros();

	/* 
	* pipeline时，通过第一个命令判断这批命令是快命令还是慢命令，因为proxy端
	* 做了命令的快慢分离，同一个连接上，要么都是快命令，要么都是慢命令。
	*/
	std::string opt = argvs[0][0];
	slash::StringToLower(opt);
	int priority = g_pika_conf->is_slow_cmd(opt) ? THREADPOOL_SLOW : THREADPOOL_FAST;
	g_pika_server->Schedule(&DoBackgroundTask, arg, priority);
}

void PikaClientConn::BatchExecRedisCmd(const std::vector<pink::RedisCmdArgsType>& argvs,
									   std::string* response,
									   uint64_t recv_cmd_time_us) {
	bool success = true;
	for (const auto& argv : argvs) {
		if (ExecRedisCmd(argv, response, recv_cmd_time_us) != 0) {
			success = false;
			break;
		}
	}
	if (!response->empty()) {
		set_is_reply(true);
		NotifyEpoll(success);
	}
}

int PikaClientConn::ExecRedisCmd(const pink::RedisCmdArgsType& argv,
                				 std::string* response,
                				 uint64_t recv_cmd_time_us) {
	g_pika_server->PlusThreadQuerynum();
	
	if (argv.empty()) return -2;
	std::string opt = argv[0];
	slash::StringToLower(opt);

	if (response->empty()) {
		// Avoid memory copy
		*response = std::move(DoCmd(argv, opt, recv_cmd_time_us));
	} else {
		// Maybe pipeline
		response->append(DoCmd(argv, opt, recv_cmd_time_us));
	}
	return 0;
}

int PikaClientConn::DealMessage(const PikaCmdArgsType& argv, std::string* response) {
	// g_pika_server->PlusThreadQuerynum();
	
	// if (argv.empty()) return -2;
	// std::string opt = argv[0];
	// slash::StringToLower(opt);

	// if (response->empty()) {
	// 	// Avoid memory copy
	// 	*response = std::move(DoCmd(argv, opt));
	// } else {
	// 	// Maybe pipeline
	// 	response->append(DoCmd(argv, opt));
	// }
	return 0;
}

void PikaClientConn::DoBackgroundTask(void* arg) {
	BgTaskArg* bg_arg = reinterpret_cast<BgTaskArg*>(arg);
	bg_arg->pcc->BatchExecRedisCmd(bg_arg->redis_cmds, bg_arg->response, bg_arg->recv_cmd_time_us);
	delete bg_arg;
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
