// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <fstream>
#include <glog/logging.h>
#include <assert.h>
#include <sys/types.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <string.h>
#include <arpa/inet.h>
#include <sstream>
#include <iostream>
#include <iterator>
#include <ctime>

#include "slash/include/env.h"
#include "slash/include/rsync.h"
#include "slash/include/slash_string.h"
#include "pink/include/bg_thread.h"
#include "pika_server.h"
#include "pika_conf.h"
#include "pika_slot.h"
#include "pika_dispatch_thread.h"

#define BASE_CRON_TIME_US       100000
#define BASE_CRON_TIME_MS       100
static long long cron_loops = 0 ;
#define run_with_period(ms) if (((ms) < (BASE_CRON_TIME_MS)) || !(cron_loops %  ((ms) / (BASE_CRON_TIME_MS))))

extern PikaServer *g_pika_server;
extern PikaConf *g_pika_conf;

PikaServer::PikaServer() :
    ping_thread_(NULL),
    db_size_(0),
    memtable_usage_(0),
    table_reader_usage_(0),
    cache_usage_(0),
    last_info_data_time_(0),
    log_size_(0),
    last_info_log_time_(0),
    exit_(false),
    binlog_io_error_(false),
    have_scheduled_crontask_(false),
    last_check_compact_time_({0, 0}),
    disable_auto_compactions_is_change_(false),
    sid_(0),
    master_ip_(""),
    master_connection_(0),
    repl_down_since_(0),
    master_port_(0),
    repl_state_(PIKA_REPL_NO_CONNECT),
    role_(PIKA_ROLE_SINGLE),
    force_full_sync_(false),
    bgsave_engine_(NULL),
    purging_(false),
    binlogbg_exit_(false),
    binlogbg_cond_(&binlogbg_mutex_),
    binlogbg_serial_(0) {

    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&rwlock_, &attr);

    //Init server ip host
    if (!ServerInit()) {
        LOG(FATAL) << "ServerInit iotcl error";
    }

    //Create blackwidow handle
    blackwidow::BlackwidowOptions bw_option;
    RocksdbOptionInit(&bw_option);

    std::string db_path = g_pika_conf->db_path();
    LOG(INFO) << "Prepare Blackwidow DB...";
    db_ = std::shared_ptr<blackwidow::BlackWidow>(new blackwidow::BlackWidow());
    rocksdb::Status s = db_->Open(bw_option, db_path);
    assert(db_);
    assert(s.ok());
    LOG(INFO) << "DB Success";

    // Create thread
    worker_num_ = std::min(g_pika_conf->thread_num(),
                                                 PIKA_MAX_WORKER_THREAD_NUM);

    std::set<std::string> ips;
    if (g_pika_conf->network_interface().empty()) {
        ips.insert("0.0.0.0");
    } else {
        ips.insert("127.0.0.1");
        ips.insert(host_);
    }
    // We estimate the queue size
    int worker_queue_limit = g_pika_conf->maxclients() / worker_num_ + 100;
    LOG(INFO) << "Worker queue limit is " << worker_queue_limit;
    pika_dispatch_thread_ = new PikaDispatchThread(ips, port_, worker_num_, 3000, worker_queue_limit);
    pika_binlog_receiver_thread_ = new PikaBinlogReceiverThread(ips, port_ + 1000, 1000);
    pika_heartbeat_thread_ = new PikaHeartbeatThread(ips, port_ + 2000, 1000);
    pika_trysync_thread_ = new PikaTrysyncThread();
    pika_pubsub_thread_ = new pink::PubSubThread();
    pika_thread_pools_[THREADPOOL_FAST] = new pink::ThreadPool(g_pika_conf->fast_thread_pool_size(), 100000, "pika:fast");
    pika_thread_pools_[THREADPOOL_SLOW] = new pink::ThreadPool(g_pika_conf->slow_thread_pool_size(), 100000, "pika:slow");
    monitor_thread_ = new PikaMonitorThread();
    binlog_write_thread_ = new PikaBinlogWriterThread*[g_pika_conf->binlog_writer_num()];
    for (int i = 0; i < g_pika_conf->binlog_writer_num(); i++) {
        binlog_write_thread_[i] = new PikaBinlogWriterThread(g_pika_conf->binlog_writer_queue_size());
    }
    //binlog_write_thread_ = new PikaBinlogWriterThread(g_pika_conf->binlog_writer_queue_size());
    slowlog_ = new PikaSlowlog();
    pika_migrate_thread_ = new PikaMigrateThread();
    pika_zset_auto_del_thread_ = new PikaZsetAutoDelThread();

    // create lock_mgr for record lock to use
    lock_mgr_ = new slash::LockMgr(1000, 0, std::make_shared<slash::MutexFactoryImpl>());
    
    // Create cache
    dory::CacheConfig cache_cfg;
    CacheConfigInit(cache_cfg);

    cache_ = new PikaCache();
    Status ret = cache_->Init(g_pika_conf->cache_num(), &cache_cfg);
    assert(cache_);
    assert(ret.ok());
    LOG(INFO) << "Cache Success";

    for (int j = 0; j < g_pika_conf->sync_thread_num(); j++) {
        binlogbg_workers_.push_back(new BinlogBGWorker(g_pika_conf->sync_buffer_size()));
    }

    pthread_rwlock_init(&state_protector_, NULL);
    logger_ = new Binlog(g_pika_conf->log_path(), g_pika_conf->binlog_file_size());
}

PikaServer::~PikaServer() {
    delete bgsave_engine_;

    // DispatchThread will use queue of worker thread,
    // so we need to delete dispatch before worker.
    delete pika_dispatch_thread_;
    delete pika_thread_pools_[THREADPOOL_FAST];
    delete pika_thread_pools_[THREADPOOL_SLOW];

    {
    slash::MutexLock l(&slave_mutex_);
    std::vector<SlaveItem>::iterator iter = slaves_.begin();
    while (iter != slaves_.end()) {
        if (iter->sender != NULL) {
            delete static_cast<PikaBinlogSenderThread*>(iter->sender);
        }
        iter =  slaves_.erase(iter);
        LOG(INFO) << "Delete slave success";
    }
    }

    delete pika_trysync_thread_;
    delete ping_thread_;
    delete pika_binlog_receiver_thread_;
    delete pika_pubsub_thread_;

    binlogbg_exit_ = true;
    std::vector<BinlogBGWorker*>::iterator binlogbg_iter = binlogbg_workers_.begin();
    while (binlogbg_iter != binlogbg_workers_.end()) {
        binlogbg_cond_.SignalAll();
        delete (*binlogbg_iter);
        binlogbg_iter++;
    }
    delete pika_heartbeat_thread_;
    delete monitor_thread_;
    delete slowlog_;
    delete pika_migrate_thread_;
    delete pika_zset_auto_del_thread_;
    delete cache_;

    StopKeyScan();
    key_scan_thread_.StopThread();

    bgsave_thread_.StopThread();

    for (int i = 0; i < g_pika_conf->binlog_writer_num(); i++) {
        delete binlog_write_thread_[i];
    }
    delete[] binlog_write_thread_;
    delete logger_;
    db_.reset();
    pthread_rwlock_destroy(&state_protector_);
    pthread_rwlock_destroy(&rwlock_);

    LOG(INFO) << "PikaServer " << pthread_self() << " exit!!!";
}

bool PikaServer::ServerInit() {
    std::string network_interface = g_pika_conf->network_interface();

    if (network_interface == "") {

        std::ifstream routeFile("/proc/net/route", std::ios_base::in);
        if (!routeFile.good())
        {
            return false;
        }

        std::string line;
        std::vector<std::string> tokens;
        while(std::getline(routeFile, line))
        {
            std::istringstream stream(line);
            std::copy(std::istream_iterator<std::string>(stream),
                    std::istream_iterator<std::string>(),
                    std::back_inserter<std::vector<std::string> >(tokens));

            // the default interface is the one having the second
            // field, Destination, set to "00000000"
            if ((tokens.size() >= 2) && (tokens[1] == std::string("00000000")))
            {
                network_interface = tokens[0];
                break;
            }

            tokens.clear();
        }
        routeFile.close();
    }
    LOG(INFO) << "Using Networker Interface: " << network_interface;

    struct ifaddrs * ifAddrStruct = NULL;
    struct ifaddrs * ifa = NULL;
    void * tmpAddrPtr = NULL;

    if (getifaddrs(&ifAddrStruct) == -1) {
        LOG(FATAL) << "getifaddrs failed: " << strerror(errno);
    }

    for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL) {
            continue;
        }
        if (ifa ->ifa_addr->sa_family==AF_INET) { // Check it is
            // a valid IPv4 address
            tmpAddrPtr = &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
            char addressBuffer[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
            if (std::string(ifa->ifa_name) == network_interface) {
                host_ = addressBuffer;
                break;
            }
        } else if (ifa->ifa_addr->sa_family==AF_INET6) { // Check it is
            // a valid IPv6 address
            tmpAddrPtr = &((struct sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
            char addressBuffer[INET6_ADDRSTRLEN];
            inet_ntop(AF_INET6, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN);
            if (std::string(ifa->ifa_name) == network_interface) {
                host_ = addressBuffer;
                break;
            }
        }
    }

    if (ifAddrStruct != NULL) {
        freeifaddrs(ifAddrStruct);
    }
    if (ifa == NULL) {
        LOG(FATAL) << "error network interface: " << network_interface << ", please check!";
    }

    port_ = g_pika_conf->port();
    LOG(INFO) << "host: " << host_ << " port: " << port_;

    // optimize systme min_free_kbytes to reclaim cached memory fast
    if (g_pika_conf->optimize_min_free_kbytes()) {
        slash::SetSysMinFreeKbytesRatio(0.03);
    }
    
    return true;
}

void PikaServer::Schedule(pink::TaskFunc func, void* arg, int priority) {
    if (priority == THREADPOOL_FAST || priority == THREADPOOL_SLOW) {
        pika_thread_pools_[priority]->Schedule(func, arg);
    }
}

void PikaServer::RocksdbOptionInit(blackwidow::BlackwidowOptions* bw_option) {
    bw_option->options.create_if_missing = true;
    bw_option->options.keep_log_file_num = 10;
    bw_option->options.max_manifest_file_size = 64 * 1024 * 1024;
    bw_option->options.max_log_file_size = 512 * 1024 * 1024;

    bw_option->options.write_buffer_size = g_pika_conf->write_buffer_size();
    bw_option->options.target_file_size_base = g_pika_conf->target_file_size_base();
    bw_option->options.max_background_flushes = g_pika_conf->max_background_flushes();
    bw_option->options.max_background_compactions = g_pika_conf->max_background_compactions();
    bw_option->options.max_open_files = g_pika_conf->max_cache_files();
    bw_option->options.max_bytes_for_level_multiplier = g_pika_conf->max_bytes_for_level_multiplier();
    bw_option->options.max_write_buffer_number = g_pika_conf->max_write_buffer_number();
    bw_option->options.max_bytes_for_level_base = g_pika_conf->max_bytes_for_level_base();
    bw_option->options.disable_auto_compactions = g_pika_conf->disable_auto_compactions();
    bw_option->options.level0_file_num_compaction_trigger = g_pika_conf->level0_file_num_compaction_trigger();
    bw_option->options.level0_slowdown_writes_trigger = g_pika_conf->level0_slowdown_writes_trigger();
    bw_option->options.level0_stop_writes_trigger = g_pika_conf->level0_stop_writes_trigger();
    bw_option->options.max_subcompactions = g_pika_conf->max_subcompactions();
    bw_option->options.optimize_filters_for_hits = g_pika_conf->optimize_filters_for_hits();
    bw_option->options.level_compaction_dynamic_level_bytes = g_pika_conf->level_compaction_dynamic_level_bytes();
    bw_option->options.use_direct_reads = g_pika_conf->use_direct_reads();
    bw_option->options.use_direct_io_for_flush_and_compaction = g_pika_conf->use_direct_io_for_flush_and_compaction();

    if (g_pika_conf->compression() == "none") {
        bw_option->options.compression = rocksdb::CompressionType::kNoCompression;
    } else if (g_pika_conf->compression() == "snappy") {
        bw_option->options.compression = rocksdb::CompressionType::kSnappyCompression;
    } else if (g_pika_conf->compression() == "zlib") {
        bw_option->options.compression = rocksdb::CompressionType::kZlibCompression;
    }

    bw_option->table_options.block_size = g_pika_conf->block_size();
    bw_option->table_options.cache_index_and_filter_blocks = g_pika_conf->cache_index_and_filter_blocks();
    bw_option->block_cache_size = g_pika_conf->block_cache();
    bw_option->share_block_cache = g_pika_conf->share_block_cache();

    if (bw_option->block_cache_size == 0) {
        bw_option->table_options.no_block_cache = true;
    } else if (bw_option->share_block_cache) {
        bw_option->table_options.block_cache =
            rocksdb::NewLRUCache(bw_option->block_cache_size);
    }

    // all db use one rate limiter
    bw_option->rate_limiter.reset(rocksdb::NewGenericRateLimiter(g_pika_conf->rate_bytes_per_sec()));

    bw_option->min_blob_size = g_pika_conf->min_blob_size();
    bw_option->disable_wal = g_pika_conf->disable_wal();

    bw_option->max_gc_batch_size = g_pika_conf->max_gc_batch_size();
    bw_option->blob_file_discardable_ratio = static_cast<float>(g_pika_conf->blob_file_discardable_ratio()) / 100;
    bw_option->gc_sample_cycle = g_pika_conf->gc_sample_cycle();
    bw_option->max_gc_queue_size = g_pika_conf->max_gc_queue_size();
}

void PikaServer::CacheConfigInit(dory::CacheConfig &cache_cfg) {
    cache_cfg.maxmemory = g_pika_conf->cache_maxmemory();
    cache_cfg.maxmemory_policy = g_pika_conf->cache_maxmemory_policy();
    cache_cfg.maxmemory_samples = g_pika_conf->cache_maxmemory_samples();
    cache_cfg.lfu_decay_time = g_pika_conf->cache_lfu_decay_time();
}

void PikaServer::Start() {
    int ret = 0;
    for (int pool_id = 0; pool_id < THREADPOOL_NUM; ++pool_id) {
        ret = pika_thread_pools_[pool_id]->start_thread_pool();
        if (ret != pink::kSuccess) {
            delete logger_;
            db_.reset();
            LOG(FATAL) << "Start ThreadPool Error: " << ret << (ret == pink::kCreateThreadError ? ": create thread error " : ": other error");
        }
    }
    ret = pika_dispatch_thread_->StartThread();
    if (ret != pink::kSuccess) {
        delete logger_;
        db_.reset();
        LOG(FATAL) << "Start Dispatch Error: " << ret << (ret == pink::kBindError ? ": bind port conflict" : ": other error");
    }
    ret = pika_binlog_receiver_thread_->StartThread();
    if (ret != pink::kSuccess) {
        delete logger_;
        db_.reset();
        LOG(FATAL) << "Start BinlogReceiver Error: " << ret << (ret == pink::kBindError ? ": bind port conflict" : ": other error");
    }
    ret = pika_heartbeat_thread_->StartThread();
    if (ret != pink::kSuccess) {
        delete logger_;
        db_.reset();
        LOG(FATAL) << "Start Heartbeat Error: " << ret << (ret == pink::kBindError ? ": bind port conflict" : ": other error");
    }
    ret = pika_trysync_thread_->StartThread();
    if (ret != pink::kSuccess) {
        delete logger_;
        db_.reset();
        LOG(FATAL) << "Start Trysync Error: " << ret << (ret == pink::kBindError ? ": bind port conflict" : ": other error");
    }
    ret = pika_pubsub_thread_->StartThread();
    if (ret != pink::kSuccess) {
        delete logger_;
        db_.reset();
        LOG(FATAL) << "Start Pubsub Error: " << ret << (ret == pink::kBindError ? ": bind port conflict" : ": other error");
    }
    ret = pika_zset_auto_del_thread_->StartThread();
    if (ret != pink::kSuccess) {
        delete logger_;
        db_.reset();
        LOG(FATAL) << "Start ZsetAutoDel Error: " << ret << (ret == pink::kBindError ? ": bind port conflict" : ": other error");
    }

    for (int i = 0; i < g_pika_conf->binlog_writer_num(); i++) {
        ret = binlog_write_thread_[i]->StartThread();
        if (ret != pink::kSuccess) {
            delete logger_;
            db_.reset();
            LOG(FATAL) << "Start BinlogWriter Error: " << ret << (ret == pink::kBindError ? ": bind port conflict" : ": other error");
        }
    }

    time(&start_time_s_);

    //SetMaster("127.0.0.1", 9221);
    std::string slaveof = g_pika_conf->slaveof();
    if (!slaveof.empty()) {
        int32_t sep = slaveof.find(":");
        std::string master_ip = slaveof.substr(0, sep);
        int32_t master_port = std::stoi(slaveof.substr(sep+1));
        if ((master_ip == "127.0.0.1" || master_ip == host_) && master_port == port_) {
            LOG(FATAL) << "you will slaveof yourself as the config file, please check";
        } else {
            SetMaster(master_ip, master_port);
        }
    }

    LOG(INFO) << "Pika Server going to start";
    while (!exit_) {
 
        // cache cron task, 100ms
        cache_->ProcessCronTask();

        // cache info cron task, 1s
        run_with_period(1000) {
            UpdateCacheInfo();
        }

        // pika cron task, 10s
        run_with_period(10000) {
            DoTimingTask();
        }
		
		// pika tp info
        run_with_period(1000) {
            RefreshCmdStats();
        }

		// fresh infodata cron task,default 60s
        int fresh_info_interval = 1000 * g_pika_conf->fresh_info_interval();
        run_with_period(fresh_info_interval) {
            DoFreshInfoTimingTask();
        }

        // clear system cached memory,default 60s
        int check_free_mem_interval = 1000 * g_pika_conf->check_free_mem_interval();
        run_with_period(check_free_mem_interval) {
            DoClearSysCachedMemory();
        }

        // auto del zset member
        run_with_period(1000) {
            DoAutoDelZsetMember();
        }

		++cron_loops;
        // sleep 100 ms
        usleep(BASE_CRON_TIME_US);
    }
    LOG(INFO) << "Goodbye...";
}

void PikaServer::DeleteSlave(const std::string& ip, int64_t port) {
    std::string ip_port = slash::IpPortString(ip, port);
    int slave_num = 0;
    {
    slash::MutexLock l(&slave_mutex_);
    std::vector<SlaveItem>::iterator iter = slaves_.begin();
    while (iter != slaves_.end()) {
        if (iter->ip_port == ip_port) {
            break;
        }
        iter++;
    }
    if (iter == slaves_.end()) {
        return;
    }
    if (iter->sender != NULL) {
        delete static_cast<PikaBinlogSenderThread*>(iter->sender);
    }
    slaves_.erase(iter);
    slave_num = slaves_.size();
    }

    slash::RWLock l(&state_protector_, true);
    if (slave_num == 0) {
        role_ &= ~PIKA_ROLE_MASTER;
    }
}

void PikaServer::DeleteSlave(int fd) {
    int slave_num = 0;
    {
    slash::MutexLock l(&slave_mutex_);
    std::vector<SlaveItem>::iterator iter = slaves_.begin();

    while (iter != slaves_.end()) {
        if (iter->hb_fd == fd) {
            if (iter->sender != NULL) {
                delete static_cast<PikaBinlogSenderThread*>(iter->sender);
            }
            slaves_.erase(iter);
            LOG(INFO) << "Delete slave success";
            break;
        }
        iter++;
    }
    slave_num = slaves_.size();
    }
    slash::RWLock l(&state_protector_, true);
    if (slave_num == 0) {
        role_ &= ~PIKA_ROLE_MASTER;
    }
}

/*
 * Change a new db locate in new_path
 * return true when change success
 * db remain the old one if return false
 */
bool PikaServer::ChangeDb(const std::string& new_path) {

    blackwidow::BlackwidowOptions bw_option;
    RocksdbOptionInit(&bw_option);

    std::string db_path = g_pika_conf->db_path();
    std::string tmp_path(db_path);
    if (tmp_path.back() == '/') {
        tmp_path.resize(tmp_path.size() - 1);
    }
    tmp_path += "_bak";
    slash::DeleteDirIfExist(tmp_path);

    RWLock l(&rwlock_, true);
    LOG(INFO) << "Prepare change db from: " << tmp_path;
    db_.reset();
    if (0 != slash::RenameFile(db_path.c_str(), tmp_path)) {
        LOG(WARNING) << "Failed to rename db path when change db, error: " << strerror(errno);
        return false;
    }

    if (0 != slash::RenameFile(new_path.c_str(), db_path.c_str())) {
        LOG(WARNING) << "Failed to rename new db path when change db, error: " << strerror(errno);
        return false;
    }
    db_.reset(new blackwidow::BlackWidow());
    rocksdb::Status s = db_->Open(bw_option, db_path);
    assert(db_);
    assert(s.ok());
    slash::DeleteDirIfExist(tmp_path);
    LOG(INFO) << "Change db success";

    return true;
}

void PikaServer::MayUpdateSlavesMap(int64_t sid, int32_t hb_fd) {
    slash::MutexLock l(&slave_mutex_);
    std::vector<SlaveItem>::iterator iter = slaves_.begin();
    LOG(INFO) << "MayUpdateSlavesMap, sid: " << sid << " hb_fd: " << hb_fd;
    while (iter != slaves_.end()) {
        if (iter->sid == sid) {
            iter->hb_fd = hb_fd;
            iter->stage = SLAVE_ITEM_STAGE_TWO;
            LOG(INFO) << "New Master-Slave connection established successfully, Slave host: " << iter->ip_port;
            break;
        }
        iter++;
    }
}

// Try add Slave, return slave sid if success,
// return -1 when slave already exist
int64_t PikaServer::TryAddSlave(const std::string& ip, int64_t port) {
    std::string ip_port = slash::IpPortString(ip, port);

    slash::MutexLock l(&slave_mutex_);
    std::vector<SlaveItem>::iterator iter = slaves_.begin();
    while (iter != slaves_.end()) {
        if (iter->ip_port == ip_port) {
            return -1;
        }
        iter++;
    }

    // Not exist, so add new
    LOG(INFO) << "Add new slave, " << ip << ":" << port;
    SlaveItem s;
    s.sid = GenSid();
    s.ip_port = ip_port;
    s.port = port;
    s.hb_fd = -1;
    s.stage = SLAVE_ITEM_STAGE_ONE;
    gettimeofday(&s.create_time, NULL);
    s.sender = NULL;
    slaves_.push_back(s);
    return s.sid;
}

// Set binlog sender of SlaveItem
bool PikaServer::SetSlaveSender(const std::string& ip, int64_t port,
        PikaBinlogSenderThread* s){
    std::string ip_port = slash::IpPortString(ip, port);

    slash::MutexLock l(&slave_mutex_);
    std::vector<SlaveItem>::iterator iter = slaves_.begin();
    while (iter != slaves_.end()) {
        if (iter->ip_port == ip_port) {
            break;
        }
        iter++;
    }
    if (iter == slaves_.end()) {
        // Not exist
        return false;
    }

    iter->sender = s;
    iter->sender_tid = s->thread_id();
    LOG(INFO) << "SetSlaveSender ok, tid is " << iter->sender_tid
        << " hd_fd: " << iter->hb_fd << " stage: " << iter->stage;
    return true;
}

int32_t PikaServer::GetSlaveListString(std::string& slave_list_str) {
    size_t index = 0;
    std::string slave_ip_port;
    std::stringstream tmp_stream;

    slash::MutexLock l(&slave_mutex_);
    std::vector<SlaveItem>::iterator iter = slaves_.begin();
    for (; iter != slaves_.end(); ++iter) {
        if ((*iter).sender == NULL) {
            // Binlog Sender has not yet created
            continue;
        }
        slave_ip_port =(*iter).ip_port;
        tmp_stream << "slave" << index++
            << ":ip=" << slave_ip_port.substr(0, slave_ip_port.find(":"))
            << ",port=" << slave_ip_port.substr(slave_ip_port.find(":")+1)
            << ",state=" << ((*iter).stage == SLAVE_ITEM_STAGE_TWO ? "online" : "offline")
            << "\r\n";
    }
    slave_list_str.assign(tmp_stream.str());
    return index;
}

void PikaServer::BecomeMaster() {
    slash::RWLock l(&state_protector_, true);
    role_ |= PIKA_ROLE_MASTER;
}

bool PikaServer::SetMaster(std::string& master_ip, int master_port) {
    if (master_ip == "127.0.0.1") {
        master_ip = host_;
    }
    slash::RWLock l(&state_protector_, true);
    if ((role_ ^ PIKA_ROLE_SLAVE) && repl_state_ == PIKA_REPL_NO_CONNECT) {
        master_ip_ = master_ip;
        master_port_ = master_port;
        role_ |= PIKA_ROLE_SLAVE;
        repl_state_ = PIKA_REPL_CONNECT;
        LOG(INFO) << "open read-only mode";
        g_pika_conf->SetReadonly(true);
        return true;
    } else if (master_ip_ == master_ip && master_port_ == master_port){ //add this for codis command
            return true;
    }       
    return false;
}

bool PikaServer::WaitingDBSync() {
    slash::RWLock l(&state_protector_, false);
    DLOG(INFO) << "repl_state: " << repl_state_ << " role: " << role_ << " master_connection: " << master_connection_;
    if (repl_state_ == PIKA_REPL_WAIT_DBSYNC) {
        return true;
    }
    return false;
}

void PikaServer::NeedWaitDBSync() {
    slash::RWLock l(&state_protector_, true);
    repl_state_ = PIKA_REPL_WAIT_DBSYNC;
}

void PikaServer::WaitDBSyncFinish() {
    slash::RWLock l(&state_protector_, true);
    if (repl_state_ == PIKA_REPL_WAIT_DBSYNC) {
        repl_state_ = PIKA_REPL_CONNECT;
    }
}

void PikaServer::KillBinlogSenderConn() {
    pika_binlog_receiver_thread_->KillBinlogSender();
}

void
PikaServer::SlowlogTrim(void)
{
    return slowlog_->Trim();
}

void
PikaServer::SlowlogReset(void)
{
    return slowlog_->Reset();
}

uint32_t
PikaServer::SlowlogLen(void)
{
    return slowlog_->Len();
}

void
PikaServer::SlowlogObtain(int64_t number, std::vector<SlowlogEntry>* slowlogs)
{
    return slowlog_->GetInfo(number, slowlogs);
}

void
PikaServer::SlowlogPushEntry(const PikaCmdArgsType& argv, int32_t time, int64_t duration)
{
    return slowlog_->Push(argv, time, duration);
}

bool PikaServer::ShouldConnectMaster() {
    slash::RWLock l(&state_protector_, false);
    DLOG(INFO) << "repl_state: " << repl_state_ << " role: " << role_ << " master_connection: " << master_connection_;
    if (repl_state_ == PIKA_REPL_CONNECT) {
        return true;
    }
    return false;
}

void PikaServer::ConnectMasterDone() {
    slash::RWLock l(&state_protector_, true);
    if (repl_state_ == PIKA_REPL_CONNECT) {
        repl_state_ = PIKA_REPL_CONNECTING;
    }
}

bool PikaServer::ShouldStartPingMaster() {
    slash::RWLock l(&state_protector_, false);
    DLOG(INFO) << "ShouldStartPingMaster: master_connection " << master_connection_ << " repl_state " << repl_state_;
    if (repl_state_ == PIKA_REPL_CONNECTING && master_connection_ < 2) {
        return true;
    }
    return false;
}

void PikaServer::MinusMasterConnection() {
    slash::RWLock l(&state_protector_, true);
    if (master_connection_ > 0) {
        if ((--master_connection_) <= 0) {
            // two connection with master has been deleted
            if (role_ & PIKA_ROLE_SLAVE) {
                repl_state_ = PIKA_REPL_CONNECT; // not change by slaveof no one, so set repl_state = PIKA_REPL_CONNECT, continue to connect master
                time(&repl_down_since_);
            } else {
                repl_state_ = PIKA_REPL_NO_CONNECT; // change by slaveof no one, so set repl_state = PIKA_REPL_NO_CONNECT, reset to SINGLE state
            }
            master_connection_ = 0;
        }
    }
}

void PikaServer::PlusMasterConnection() {
    slash::RWLock l(&state_protector_, true);
    if (master_connection_ < 2) {
        if ((++master_connection_) >= 2) {
            // two connection with master has been established
            repl_state_ = PIKA_REPL_CONNECTED;
            master_connection_ = 2;
            LOG(INFO) << "Master-Slave connection established successfully";
        }
    }
}

bool PikaServer::ShouldAccessConnAsMaster(const std::string& ip) {
    if (ip != master_ip_) {
        return false;
    }

    //add this for pika-2.1.0, binlogsender will connet slave when reveive trysync commnd in pika-2.1.0
    int tmp_repl_state = 0;
    {
        slash::RWLock l(&state_protector_, false);
        tmp_repl_state = repl_state_;
    }
    if (tmp_repl_state == PIKA_REPL_CONNECT) {
        LOG(WARNING) << "ShouldAccessConnAsMaster, repl_state_: " << repl_state_ << " ip: " << ip << " master_ip: " << master_ip_ << " sleep 2s.";
        sleep(2);
    }

    {
        slash::RWLock l(&state_protector_, false);
        DLOG(INFO) << "ShouldAccessConnAsMaster, repl_state_: " << repl_state_ << " ip: " << ip << " master_ip: " << master_ip_;
    //  if (repl_state_ != PIKA_REPL_NO_CONNECT && repl_state_ != PIKA_REPL_WAIT_DBSYNC && ip == master_ip_) {
        if ((repl_state_ == PIKA_REPL_CONNECTING || repl_state_ == PIKA_REPL_CONNECTED) &&
                ip == master_ip_) {
            return true;
        }
        return false;
    }
}

void PikaServer::SyncError() {

    {
    slash::RWLock l(&state_protector_, true);
    repl_state_ = PIKA_REPL_ERROR;
    }
    if (ping_thread_ != NULL) {
        int err = ping_thread_->StopThread();
        if (err != 0) {
            std::string msg = "can't join thread " + std::string(strerror(err));
            LOG(WARNING) << msg;
        }
        delete ping_thread_;
        ping_thread_ = NULL;
    }
    LOG(WARNING) << "Sync error, set repl_state to PIKA_REPL_ERROR";
}

void PikaServer::RemoveMaster() {

    {
    slash::RWLock l(&state_protector_, true);
    repl_state_ = PIKA_REPL_NO_CONNECT;
    role_ &= ~PIKA_ROLE_SLAVE;
    master_ip_ = "";
    master_port_ = -1;
    }

    {
        slash::MutexLock l(&slave_mutex_);
        if (ping_thread_ != NULL) {
            int err = ping_thread_->StopThread();
            if (err != 0) {
                std::string msg = "can't join thread " + std::string(strerror(err));
                LOG(WARNING) << msg;
            }
            delete ping_thread_;
            ping_thread_ = NULL;
        }
    }
    {
    slash::RWLock l(&state_protector_, true);
    master_connection_ = 0;
    }
    LOG(INFO) << "close read-only mode";
    g_pika_conf->SetReadonly(false);
}

void PikaServer::TryDBSync(const std::string& ip, int port, int32_t top) {
    std::string bg_path;
    uint32_t bg_filenum = 0;
    {
        slash::MutexLock l(&bgsave_protector_);
        bg_path = bgsave_info_.path;
        bg_filenum = bgsave_info_.filenum;
    }

    if (0 != slash::IsDir(bg_path) ||                               //Bgsaving dir exist
            !slash::FileExists(NewFileName(logger_->filename, bg_filenum)) ||  //filenum can be found in binglog
            top - bg_filenum > kDBSyncMaxGap) {      //The file is not too old
        // Need Bgsave first
        Bgsave();
    }
    DBSync(ip, port);
}

void PikaServer::DBSync(const std::string& ip, int port) {
    // Only one DBSync task for every ip_port
    std::string ip_port = slash::IpPortString(ip, port);
    {
        slash::MutexLock ldb(&db_sync_protector_);
        if (db_sync_slaves_.find(ip_port) != db_sync_slaves_.end()) {
            return;
        }
        db_sync_slaves_.insert(ip_port);
    }
    // Reuse the bgsave_thread_
    // Since we expect Bgsave and DBSync execute serially
    bgsave_thread_.StartThread();
    DBSyncArg *arg = new DBSyncArg(this, ip, port);
    bgsave_thread_.Schedule(&DoDBSync, static_cast<void*>(arg));
}

void PikaServer::DoDBSync(void* arg) {
    DBSyncArg *ppurge = static_cast<DBSyncArg*>(arg);
    PikaServer* ps = ppurge->p;

    ps->DBSyncSendFile(ppurge->ip, ppurge->port);

    delete (PurgeArg*)arg;
}

void PikaServer::DBSyncSendFile(const std::string& ip, int port) {
    std::string bg_path;
    {
        slash::MutexLock l(&bgsave_protector_);
        bg_path = bgsave_info_.path;
    }
    // Get all files need to send
    std::vector<std::string> descendant;
    int ret = 0;
    LOG(INFO) << "Start Send files in " << bg_path << " to " << ip;
    ret = slash::GetChildren(bg_path, descendant);
    if (ret != 0) {
        LOG(WARNING) << "Get child directory when try to do sync failed, error: " << strerror(ret);
        return;
    }

    // Iterate to send files
    ret = 0;
    std::string local_path, target_path;
    std::string module = kDBSyncModule + "_" + slash::IpPortString(host_, port_);
    std::vector<std::string>::iterator it = descendant.begin();
    slash::RsyncRemote remote(ip, port, module, g_pika_conf->db_sync_speed() * 1024);
    for (; it != descendant.end(); ++it) {
        local_path = bg_path + "/" + *it;
        target_path = *it;

        if (target_path == kBgsaveInfoFile) {
            continue;
        }
        
        if (slash::IsDir(local_path) == 0 && local_path.back() != '/') {
            local_path.push_back('/');
            target_path.push_back('/');
        }

        // We need specify the speed limit for every single file
        ret = slash::RsyncSendFile(local_path, target_path, remote);

        if (0 != ret) {
            LOG(WARNING) << "rsync send file failed! From: " << *it
                << ", To: " << target_path
                << ", At: " << ip << ":" << port
                << ", Error: " << ret;
            break;
        }
    }

    // Clear target path
    slash::RsyncSendClearTarget(bg_path + "/strings", "strings", remote);
    slash::RsyncSendClearTarget(bg_path + "/hashes", "hashes", remote);
    slash::RsyncSendClearTarget(bg_path + "/lists", "lists", remote);
    slash::RsyncSendClearTarget(bg_path + "/sets", "sets", remote);
    slash::RsyncSendClearTarget(bg_path + "/zsets", "zsets", remote);
    slash::RsyncSendClearTarget(bg_path + "/ehashes", "ehashes", remote);

    // Send info file at last
    if (0 == ret) {
        if (0 != (ret = slash::RsyncSendFile(bg_path + "/" + kBgsaveInfoFile, kBgsaveInfoFile, remote))) {
            LOG(WARNING) << "send info file failed";
        }
    }

    // remove slave
    std::string ip_port = slash::IpPortString(ip, port);
    {
        slash::MutexLock ldb(&db_sync_protector_);
        db_sync_slaves_.erase(ip_port);
    }
    if (0 == ret) {
        LOG(INFO) << "rsync send files success";
    }
}

/*
 * BinlogSender
 */
Status PikaServer::AddBinlogSender(const std::string& ip, int64_t port,
        uint32_t filenum, uint64_t con_offset) {
    // Sanity check
    if (con_offset > logger_->file_size()) {
        return Status::InvalidArgument("AddBinlogSender invalid binlog offset");
    }
    uint32_t cur_filenum = 0;
    uint64_t cur_offset = 0;
    logger_->GetProducerStatus(&cur_filenum, &cur_offset);
    if (filenum != UINT32_MAX &&
            (cur_filenum < filenum || (cur_filenum == filenum && cur_offset < con_offset))) {
        return Status::InvalidArgument("AddBinlogSender invalid binlog offset");
    }

    if (filenum == UINT32_MAX) {
        LOG(INFO) << "Maybe force full sync";
    }

    // Create and set sender
    slash::SequentialFile *readfile;
    std::string confile = NewFileName(logger_->filename, filenum);
    if (!slash::FileExists(confile)) {
        // Not found binlog specified by filenum
        TryDBSync(ip, port + 3000, cur_filenum);
        return Status::Incomplete("Bgsaving and DBSync first");
    }
    if (!slash::NewSequentialFile(confile, &readfile).ok()) {
        return Status::IOError("AddBinlogSender new sequtialfile");
    }

    PikaBinlogSenderThread* sender = new PikaBinlogSenderThread(ip,
            port + 1000, readfile, filenum, con_offset);

    if (sender->trim() == 0 // Error binlog
            && SetSlaveSender(ip, port, sender)) { // SlaveItem not exist
        sender->StartThread();
        return Status::OK();
    } else {
        delete sender;
        LOG(WARNING) << "AddBinlogSender failed";
        return Status::NotFound("AddBinlogSender bad sender");
    }
}

// Prepare engine, need bgsave_protector protect
bool PikaServer::InitBgsaveEnv() {
    {
        slash::MutexLock l(&bgsave_protector_);
        // Prepare for bgsave dir
        bgsave_info_.start_time = time(NULL);
        char s_time[32];
        int len = strftime(s_time, sizeof(s_time), "%Y%m%d%H%M%S", localtime(&bgsave_info_.start_time));
        bgsave_info_.s_start_time.assign(s_time, len);
        std::string bgsave_path(g_pika_conf->bgsave_path());
        bgsave_info_.path = bgsave_path + g_pika_conf->bgsave_prefix() + std::string(s_time, 8);
        if (!slash::DeleteDirIfExist(bgsave_info_.path)) {
            LOG(WARNING) << "remove exist bgsave dir failed";
            return false;
        }
        slash::CreatePath(bgsave_info_.path, 0755);
        // Prepare for failed dir
        if (!slash::DeleteDirIfExist(bgsave_info_.path + "_FAILED")) {
            LOG(WARNING) << "remove exist fail bgsave dir failed :";
            return false;
        }
    }
    return true;
}

// Prepare bgsave env, need bgsave_protector protect
bool PikaServer::InitBgsaveEngine() {
    delete bgsave_engine_;
    rocksdb::Status s = blackwidow::BackupEngine::Open(db().get(), &bgsave_engine_);
    if (!s.ok()) {
        LOG(WARNING) << "open backup engine failed " << s.ToString();
        return false;
    }

    {
        RWLock l(&rwlock_, true);
        {
            time_t start_time = time(NULL);
            if (g_pika_conf->binlog_writer_method() != "sync") {
                while (!IsBinlogWriterIdle()) {
                    usleep(10000);
                    if (time(NULL) - start_time >= 3) {
                        LOG(WARNING) << "timeout to wait binlog_writer_thread idle";
                        return false;
                    }
                } 
            }
            
            slash::MutexLock l(&bgsave_protector_);
            logger_->GetProducerStatus(&bgsave_info_.filenum, &bgsave_info_.offset);
        }
        s = bgsave_engine_->SetBackupContent();
        if (!s.ok()){
            LOG(WARNING) << "set backup content failed " << s.ToString();
            return false;
        }
    }
    return true;
}

bool PikaServer::IsBinlogWriterIdle() {
    for (int i=0; i<g_pika_conf->binlog_writer_num(); i++) {
        if (!binlog_write_thread_[i]->IsBinlogWriterIdle()) {
            return false;
        }
    }
    return true;
}

bool PikaServer::RunBgsaveEngine() {
  // Prepare for Bgsaving
  if (!InitBgsaveEnv() || !InitBgsaveEngine()) {
    ClearBgsave();
    return false;
  }
  LOG(INFO) << "after prepare bgsave";
  
  BGSaveInfo info = bgsave_info();
  LOG(INFO) << "   bgsave_info: path=" << info.path
    << ",  filenum=" << info.filenum
    << ", offset=" << info.offset;

  // Backup to tmp dir
  rocksdb::Status s = bgsave_engine_->CreateNewBackup(info.path);
  LOG(INFO) << "Create new backup finished.";

    if (!s.ok()) {
        LOG(WARNING) << "backup failed :" << s.ToString();
        return false;
    }
    return true;
}

void PikaServer::Bgsave() {
    // Only one thread can go through
    {
        slash::MutexLock l(&bgsave_protector_);
        if (bgsave_info_.bgsaving) {
            return;
        }
        bgsave_info_.bgsaving = true;
    }

  // Start new thread if needed
  bgsave_thread_.StartThread();
  bgsave_thread_.Schedule(&DoBgsave, static_cast<void*>(this));
}

void PikaServer::DoBgsave(void* arg) {
  PikaServer* p = static_cast<PikaServer*>(arg);

  // Do bgsave
  bool ok = p->RunBgsaveEngine();

  // Some output
  BGSaveInfo info = p->bgsave_info();
  std::ofstream out;
  out.open(info.path + "/" + kBgsaveInfoFile, std::ios::in | std::ios::trunc);
  if (out.is_open()) {
    out << (time(NULL) - info.start_time) << "s\n"
      << p->host() << "\n"
      << p->port() << "\n"
      << info.filenum << "\n"
      << info.offset << "\n";
    out.close();
  }
  if (!ok) {
    std::string fail_path = info.path + "_FAILED";
    slash::RenameFile(info.path.c_str(), fail_path.c_str());
  }
  p->FinishBgsave();
}

bool PikaServer::Bgsaveoff() {
    {
        slash::MutexLock l(&bgsave_protector_);
        if (!bgsave_info_.bgsaving) {
            return false;
        }
    }
    if (bgsave_engine_ != NULL) {
        bgsave_engine_->StopBackup();
    }
    return true;
}


//delete slots info by slot number
rocksdb::Status PikaServer::SlotsDelSlot(int slot){
    int32_t total = 0;
    int64_t count;
    int64_t ret;
    std::string type;
    std::string key;
    rocksdb::Status s;
    int64_t next_cursor = 0;
    std::vector<std::string> members;

    std::string slot_key = GetSlotsSlotKey(slot);
    s = db_->SCard(slot_key, &total);
    if (total <= 0){
        slash::MutexLock l(&bgsave_protector_);
        bgslots_del_.deleting = false;

        //如果slot_key不存在表示该slot当中key的数量为0，也返回ok
        if (s.IsNotFound() || s.ok()) {
            return rocksdb::Status::OK();
        }

        return s;
    }

    {
        slash::MutexLock l(&bgsave_protector_);
        bgslots_del_.slot_no = slot;
        bgslots_del_.total = total;
        bgslots_del_.deleting = true;
        bgslots_del_.current = 0;

        count = bgslots_del_.count > 0 ? bgslots_del_.count : 1;
    }

    do {
        members.clear();
        s = g_pika_server->db()->SScan(slot_key, next_cursor, "*", count, &members, &next_cursor);
        if (s.ok() && members.size() > 0) {
            for (const auto& member : members) {
                key = member;
                if (key.size() > 0) {
                    type = key.at(0);
                    key.erase(key.begin());
                } else {
                    LOG(ERROR) << "have a null member in " << slot_key;
                    continue;
                }   
                
                ret = db_->DelByType(key, KeyType(type.at(0)));
                if (ret < 0) {
                    LOG(ERROR) << "del key: " << key << " error";
                    s = rocksdb::Status::Corruption("del key error");
                    break;
                }

                SlotKeyRemByType(type, key);
                WriteDelKeyToBinlog(key);
            }

            {
                slash::MutexLock l(&bgsave_protector_);
                bgslots_del_.current += members.size();
            }

            if (!GetBgSlotsDelEnable()){
                break;
            }
        } else {
            break;
        }
    } while (next_cursor != 0 && s.ok());

    {
        slash::MutexLock l(&bgsave_protector_);
        bgslots_del_.deleting = false;
        //bgslots_del_.Clear();
    }

    if (s.IsNotFound() || s.ok()) {
        return rocksdb::Status::OK();
    }
    return s;
}

void PikaServer::DoBgSlotsDel(void* arg){
    BGSlotsDelArg *pSlotsDelArg = static_cast<BGSlotsDelArg*>(arg);
    PikaServer *p = pSlotsDelArg->p;

    //del slot
    std::vector<int64_t>::iterator slots_iter = pSlotsDelArg->slots.begin();
    for (; slots_iter != pSlotsDelArg->slots.end() && p->GetBgSlotsDelEnable(); ++slots_iter){
        rocksdb::Status s = p->SlotsDelSlot(*slots_iter);
        if (!s.ok()) {
            LOG(ERROR) << "del slot[" << *slots_iter << "] failed.";
            break;
        }

        LOG(INFO) << "del slot[" << *slots_iter << "] finished.";
    }

    delete (BGSlotsDelArg*)arg;
}

void PikaServer::BgSlotsDel(const std::vector<int64_t> &slots) {
    {
        slash::MutexLock l(&bgsave_protector_);
        bgslots_del_.start_time = time(NULL);
        char s_time[32];
        int len = strftime(s_time, sizeof(s_time), "%Y%m%d%H%M%S", localtime(&bgslots_del_.start_time));
        bgslots_del_.s_start_time.assign(s_time, len);
    }

    bgsave_thread_.StartThread();
    BGSlotsDelArg *arg = new BGSlotsDelArg(this, slots);
    bgsave_thread_.Schedule(&DoBgSlotsDel, static_cast<void*>(arg));
}

void PikaServer::Bgslotsreload() {
    // Only one thread can go through
    {
        slash::MutexLock l(&bgsave_protector_);
        if (bgslots_reload_.reloading || bgsave_info_.bgsaving) {
            return;
        }
        bgslots_reload_.reloading = true;
    }

    bgslots_reload_.start_time = time(NULL);
    char s_time[32];
    int len = strftime(s_time, sizeof(s_time), "%Y%m%d%H%M%S", localtime(&bgslots_reload_.start_time));
    bgslots_reload_.s_start_time.assign(s_time, len);
    bgslots_reload_.cursor = 0;
    bgslots_reload_.pattern = "*";
    bgslots_reload_.count = 100;

    LOG(INFO) << "Start slot reloading";

    // Start new thread if needed
    bgsave_thread_.StartThread();
    bgsave_thread_.Schedule(&DoBgslotsreload, static_cast<void*>(this));
}

void PikaServer::DoBgslotsreload(void* arg) {
    PikaServer* p = static_cast<PikaServer*>(arg);
    BGSlotsReload reload = p->bgslots_reload();

    // Do slotsreload
    rocksdb::Status s;
    std::vector<std::string> keys;
    int64_t cursor_ret = -1;
    while(cursor_ret != 0 && p->GetSlotsreloading()){
        cursor_ret = p->db()->Scan(reload.cursor, reload.pattern, reload.count, &keys);

        std::vector<std::string>::const_iterator iter;
        for (iter = keys.begin(); iter != keys.end(); iter++){
            std::string key_type;

            s = KeyType(*iter, &key_type);
            if (s.ok()){
                //if key is slotkey, can't add to SlotKey
                if (key_type == "s" && (*iter).compare(0, SlotPrefix.length(), SlotPrefix) == 0){
                    continue;
                }

                SlotKeyAdd(key_type, *iter, true);
            }
        }

        reload.cursor = cursor_ret;
        p->SetSlotsreloadingCursor(cursor_ret);
        keys.clear();
    }
    p->SetSlotsreloading(false);
    
    if (cursor_ret == 0) {
        LOG(INFO) << "Finish slot reloading";
    } else{
        LOG(INFO) << "Stop slot reloading";
    }
}

bool PikaServer::PurgeLogs(uint32_t to, bool manual, bool force) {
    // Only one thread can go through
    bool expect = false;
    if (!purging_.compare_exchange_strong(expect, true)) {
        LOG(WARNING) << "purge process already exist";
        return false;
    }
    PurgeArg *arg = new PurgeArg();
    arg->p = this;
    arg->to = to;
    arg->manual = manual;
    arg->force = force;
    // Start new thread if needed
    purge_thread_.StartThread();
    purge_thread_.Schedule(&DoPurgeLogs, static_cast<void*>(arg));
    return true;
}

void PikaServer::DoPurgeLogs(void* arg) {
    PurgeArg *ppurge = static_cast<PurgeArg*>(arg);
    PikaServer* ps = ppurge->p;

    ps->PurgeFiles(ppurge->to, ppurge->manual, ppurge->force);

    ps->ClearPurge();
    delete (PurgeArg*)arg;
}

bool PikaServer::GetPurgeWindow(uint32_t &max) {
    uint64_t tmp;
    logger_->GetProducerStatus(&max, &tmp);
    slash::MutexLock l(&slave_mutex_);
    std::vector<SlaveItem>::iterator it;
    for (it = slaves_.begin(); it != slaves_.end(); ++it) {
        if ((*it).sender == NULL) {
            // One Binlog Sender has not yet created, no purge
            return false;
        }
        PikaBinlogSenderThread *pb = static_cast<PikaBinlogSenderThread*>((*it).sender);
        uint32_t filenum = pb->filenum();
        max = filenum < max ? filenum : max;
    }
    // remain some more
    if (max >= 10) {
        max -= 10;
        return true;
    }
    return false;
}

bool PikaServer::CouldPurge(uint32_t index) {
    uint32_t pro_num;
    uint64_t tmp;
    logger_->GetProducerStatus(&pro_num, &tmp);

    index += 10; //remain some more
    if (index > pro_num) {
        return false;
    }
    slash::MutexLock l(&slave_mutex_);
    std::vector<SlaveItem>::iterator it;
    for (it = slaves_.begin(); it != slaves_.end(); ++it) {
        if ((*it).sender == NULL) {
            // One Binlog Sender has not yet created, no purge
            return false;
        }
        PikaBinlogSenderThread *pb = static_cast<PikaBinlogSenderThread*>((*it).sender);
        uint32_t filenum = pb->filenum();
        if (index > filenum) {
            return false;
        }
    }
    return true;
}

bool PikaServer::PurgeFiles(uint32_t to, bool manual, bool force)
{
    std::map<uint32_t, std::string> binlogs;
    if (!GetBinlogFiles(binlogs)) {
        LOG(WARNING) << "Could not get binlog files!";
        return false;
    }

    int delete_num = 0;
    struct stat file_stat;
    int remain_expire_num = binlogs.size() - g_pika_conf->expire_logs_nums();
    std::map<uint32_t, std::string>::iterator it;
    for (it = binlogs.begin(); it != binlogs.end(); ++it) {
        if ((manual && it->first <= to) ||           // Argument bound
                remain_expire_num > 0 ||                 // Expire num trigger
                (binlogs.size() > 10 /* at lease remain 10 files */
                && stat(((g_pika_conf->log_path() + it->second)).c_str(), &file_stat) == 0 &&
                file_stat.st_mtime < time(NULL) - g_pika_conf->expire_logs_days()*24*3600)) // Expire time trigger
        {
            // We check this every time to avoid lock when we do file deletion
            if (!CouldPurge(it->first) && !force) {
                LOG(WARNING) << "Could not purge "<< (it->first) << ", since it is already be used";
                return false;
            }

            // Do delete
            slash::Status s = slash::DeleteFile(g_pika_conf->log_path() + it->second);
            if (s.ok()) {
                ++delete_num;
                --remain_expire_num;
            } else {
                LOG(WARNING) << "Purge log file : " << (it->second) <<  " failed! error:" << s.ToString();
            }
        } else {
            // Break when face the first one not satisfied
            // Since the binlogs is order by the file index
            break;
        }
    }
    if (delete_num) {
        LOG(INFO) << "Success purge "<< delete_num;
    }

    return true;
}

bool PikaServer::GetBinlogFiles(std::map<uint32_t, std::string>& binlogs) {
    std::vector<std::string> children;
    int ret = slash::GetChildren(g_pika_conf->log_path(), children);
    if (ret != 0){
        LOG(WARNING) << "Get all files in log path failed! error:" << ret;
        return false;
    }

    int64_t index = 0;
    std::string sindex;
    std::vector<std::string>::iterator it;
    for (it = children.begin(); it != children.end(); ++it) {
        if ((*it).compare(0, kBinlogPrefixLen, kBinlogPrefix) != 0) {
            continue;
        }
        sindex = (*it).substr(kBinlogPrefixLen);
        if (slash::string2l(sindex.c_str(), sindex.size(), &index) == 1) {
            binlogs.insert(std::pair<uint32_t, std::string>(static_cast<uint32_t>(index), *it));
        }
    }
    return true;
}

void PikaServer::AutoCompactRange() {
    struct statfs disk_info;
    int ret = statfs(g_pika_conf->db_path().c_str(), &disk_info);
    if (ret == -1) {
        LOG(WARNING) << "statfs error: " << strerror(errno);
        return;
    }

    uint64_t total_size = disk_info.f_bsize * disk_info.f_blocks;
    uint64_t free_size = disk_info.f_bsize * disk_info.f_bfree;
//      LOG(INFO) << "free_size: " << free_size << " disk_size: " << total_size <<
//        " cal: " << ((double)free_size / total_size) * 100;
    std::string ci = g_pika_conf->compact_interval();
    std::string cc = g_pika_conf->compact_cron();

    if ("" != cc) {
        std::string::size_type colon = cc.find("-");
        std::string::size_type underline = cc.find("/");
        int start = std::atoi(cc.substr(0, colon).c_str());
        int end = std::atoi(cc.substr(colon+1, underline).c_str());
        std::time_t t = std::time(nullptr);
        std::tm* t_m = std::localtime(&t);

        bool in_window = false;
        if (start < end && (t_m->tm_hour >= start && t_m->tm_hour < end)) {
            in_window = true;
        } else if (start > end && ((t_m->tm_hour >= start && t_m->tm_hour < 24) ||
                    (t_m->tm_hour >= 0 && t_m->tm_hour < end))) {
            in_window = true;
        } 

        if (in_window) {
            if (!disable_auto_compactions_is_change_ && g_pika_conf->disable_auto_compactions()) {
                disable_auto_compactions_is_change_ = true;
                rocksdb::Status s = db_->ResetOption("disable_auto_compactions", "false");
                if (!s.ok()) {
                    LOG(WARNING) << "faild to open auto_compactions at " << cc << ": " << s.ToString().c_str();
                }
            }
        } else {
            if (disable_auto_compactions_is_change_) {
                disable_auto_compactions_is_change_ = false;
                std::string value = g_pika_conf->disable_auto_compactions() ? "true" : "false";
                rocksdb::Status s = db_->ResetOption("disable_auto_compactions", value);
                if (!s.ok()) {
                    LOG(WARNING) << "falid to reset disable_auto_compactions: " << s.ToString().c_str();
                }
            }
        }
    }

    if ("" == ci && "" == cc) {
        return;
    }

    if ("" == ci) {
        ci = "24/0";
    } 

    if ("" == cc) {
        cc = "00-23/0";
    }

    if ("" != ci) {
        std::string::size_type slash = ci.find("/");
        int interval = std::atoi(ci.substr(0, slash).c_str());
        int usage = std::atoi(ci.substr(slash+1).c_str());
        struct timeval now;
        gettimeofday(&now, NULL);
            
        if (now.tv_sec - last_check_compact_time_.tv_sec < interval * 3600) {
            return;
        }

        if (((double)free_size / total_size) * 100 < usage) {
            return;
        }
    }

    if ("" != cc) {
        std::string::size_type colon = cc.find("-");
        std::string::size_type underline = cc.find("/");
        int start = std::atoi(cc.substr(0, colon).c_str());
        int end = std::atoi(cc.substr(colon+1, underline).c_str());
        int usage = std::atoi(cc.substr(underline+1).c_str());
        std::time_t t = std::time(nullptr);
        std::tm* t_m = std::localtime(&t);

        bool in_window = false;
        if (start < end && (t_m->tm_hour >= start && t_m->tm_hour < end)) {
            in_window = true;
        } else if (start > end && ((t_m->tm_hour >= start && t_m->tm_hour < 24) ||
                    (t_m->tm_hour >= 0 && t_m->tm_hour < end))) {
            in_window = true;
        } 

        if (in_window) {
            if (((double)free_size / total_size) * 100 >= usage) {
                gettimeofday(&last_check_compact_time_, NULL);
                rocksdb::Status s = db_->Compact(blackwidow::kAll);
                if (s.ok()) {
                    LOG(INFO) << "schedule compactRange, freesize: " << free_size/1048576 << "MB, disksize: " << total_size/1048576 << "MB";
                } else {
                    LOG(INFO) << "schedule compactRange Failed, freesize: " << free_size/1048576 << "MB, disksize: " << total_size/1048576
                        << "MB, error: " << s.ToString();
                }
            }
        }
    }
}

void PikaServer::AutoPurge() {
    if (!PurgeLogs(0, false, false)) {
        DLOG(WARNING) << "Auto purge failed";
        return;
    }
}

void PikaServer::AutoDeleteExpiredDump() {
    std::string db_sync_prefix = g_pika_conf->bgsave_prefix();
    std::string db_sync_path = g_pika_conf->bgsave_path();
    int expiry_days = g_pika_conf->expire_dump_days();
    std::vector<std::string> dump_dir;

    // Never expire
    if (expiry_days <= 0) {
        return;
    }

    // Dump is not exist
    if (!slash::FileExists(db_sync_path)) {
        return;
    }

    // Directory traversal
    if (slash::GetChildren(db_sync_path, dump_dir) != 0) {
        return;
    }

    // Handle dump directory
    for (size_t i = 0; i < dump_dir.size(); i++) {
        if (dump_dir[i].substr(0, db_sync_prefix.size()) != db_sync_prefix || dump_dir[i].size() != (db_sync_prefix.size() + 8)) {
            continue;
        }

        std::string str_date = dump_dir[i].substr(db_sync_prefix.size(), (dump_dir[i].size() - db_sync_prefix.size()));
        char *end = NULL;
        std::strtol(str_date.c_str(), &end, 10);
        if (*end != 0) {
            continue;
        }

        // Parse filename
        int dump_year = std::atoi(str_date.substr(0, 4).c_str());
        int dump_month = std::atoi(str_date.substr(4, 2).c_str());
        int dump_day = std::atoi(str_date.substr(6, 2).c_str());

        time_t t = time(NULL);
        struct tm *now = localtime(&t);
        int now_year = now->tm_year + 1900;
        int now_month = now->tm_mon + 1;
        int now_day = now->tm_mday;

        struct tm dump_time, now_time;

        dump_time.tm_year = dump_year;
        dump_time.tm_mon = dump_month;
        dump_time.tm_mday = dump_day;
        dump_time.tm_hour = 0;
        dump_time.tm_min = 0;
        dump_time.tm_sec = 0;

        now_time.tm_year = now_year;
        now_time.tm_mon = now_month;
        now_time.tm_mday = now_day;
        now_time.tm_hour = 0;
        now_time.tm_min = 0;
        now_time.tm_sec = 0;

        long dump_timestamp = mktime(&dump_time);
        long now_timestamp = mktime(&now_time);
        // How many days, 1 day = 86400s
        int interval_days = (now_timestamp - dump_timestamp) / 86400;

        if (interval_days >= expiry_days) {
            std::string dump_file = db_sync_path + dump_dir[i];
            if (CountSyncSlaves() == 0) {
                LOG(INFO) << "Not syncing, delete dump file: " << dump_file;
                slash::DeleteDirIfExist(dump_file);
            } else if (bgsave_info().path != dump_file) {
                LOG(INFO) << "Syncing, delete expired dump file: " << dump_file;
                slash::DeleteDirIfExist(dump_file);
            } else {
                LOG(INFO) << "Syncing, can not delete " << dump_file << " dump file";
            }
        }
    }
}

bool PikaServer::FlushAll() {
    {
        slash::MutexLock l(&bgsave_protector_);
        if (bgsave_info_.bgsaving) {
            return false;
        }
    }
    {
        slash::MutexLock l(&key_scan_protector_);
        if (key_scan_info_.key_scaning_) {
            return false;
        }
    }
    
    LOG(INFO) << "Delete old db...";
    db_.reset();
    
    std::string dbpath = g_pika_conf->db_path();
    if (dbpath[dbpath.length() - 1] == '/') {
        dbpath.erase(dbpath.length() - 1);
    }
    int pos = dbpath.find_last_of('/');
    dbpath = dbpath.substr(0, pos);
    dbpath.append("/deleting");
    slash::RenameFile(g_pika_conf->db_path(), dbpath.c_str());

    //Create blackwidow handle
    blackwidow::BlackwidowOptions bw_option;
    RocksdbOptionInit(&bw_option);

    LOG(INFO) << "Prepare open new db...";
    db_ = std::shared_ptr<blackwidow::BlackWidow>(new blackwidow::BlackWidow());
    rocksdb::Status s = db_->Open(bw_option, g_pika_conf->db_path());
    assert(db_);
    assert(s.ok());
    LOG(INFO) << "open new db success";
    PurgeDir(dbpath);
    return true;
}

void PikaServer::PurgeDir(std::string& path) {
    std::string *dir_path = new std::string(path);
    // Start new thread if needed
    purge_thread_.StartThread();
    purge_thread_.Schedule(&DoPurgeDir, static_cast<void*>(dir_path));
}

void PikaServer::DoPurgeDir(void* arg) {
    std::string path = *(static_cast<std::string*>(arg));
    LOG(INFO) << "Delete dir: " << path << " start";
    slash::DeleteDir(path);
    LOG(INFO) << "Delete dir: " << path << " done";
    delete static_cast<std::string*>(arg);
}

// PubSub

// Publish
int PikaServer::Publish(const std::string& channel, const std::string& msg) {
  int receivers = pika_pubsub_thread_->Publish(channel, msg);
  return receivers;
}

// Subscribe/PSubscribe
void PikaServer::Subscribe(std::shared_ptr<pink::PinkConn> conn,
                           const std::vector<std::string>& channels,
                           bool pattern,
                           std::vector<std::pair<std::string, int>>* result) {
  pika_pubsub_thread_->Subscribe(conn, channels, pattern, result);
}

// UnSubscribe/PUnSubscribe
int PikaServer::UnSubscribe(std::shared_ptr<pink::PinkConn> conn,
                            const std::vector<std::string>& channels,
                            bool pattern,
                            std::vector<std::pair<std::string, int>>* result) {
  int subscribed = pika_pubsub_thread_->UnSubscribe(conn, channels, pattern, result);
  return subscribed;
}

/*
 * PubSub
 */
void PikaServer::PubSubChannels(const std::string& pattern,
                      std::vector<std::string >* result) {
  pika_pubsub_thread_->PubSubChannels(pattern, result);
}

void PikaServer::PubSubNumSub(const std::vector<std::string>& channels,
                    std::vector<std::pair<std::string, int>>* result) {
  pika_pubsub_thread_->PubSubNumSub(channels, result);
}
int PikaServer::PubSubNumPat() {
  return pika_pubsub_thread_->PubSubNumPat();
}

void PikaServer::AddMonitorClient(std::shared_ptr<PikaClientConn> client_ptr) {
    monitor_thread_->AddMonitorClient(client_ptr);
}

void PikaServer::AddMonitorMessage(const std::string& monitor_message) {
    monitor_thread_->AddMonitorMessage(monitor_message);
}

bool PikaServer::HasMonitorClients() {
    return monitor_thread_->HasMonitorClients();
}

void PikaServer::ReqMigrateBatch(const std::string &ip,
                                 int64_t port,
                                 int64_t time_out,
                                 int64_t slot,
                                 int64_t keys_num)
{
    return pika_migrate_thread_->ReqMigrateBatch(ip, port, time_out, slot, keys_num);
}

int PikaServer::ReqMigrateOne(const std::string &key)
{
    return pika_migrate_thread_->ReqMigrateOne(key);
}

void PikaServer::GetMigrateStatus(std::string *ip,
                                  int64_t *port,
                                  int64_t *slot,
                                  bool *migrating,
                                  int64_t *moved,
                                  int64_t *remained)
{
    return pika_migrate_thread_->GetMigrateStatus(ip, port, slot, migrating, moved, remained);
}

void PikaServer::CancelMigrate(void)
{
    return pika_migrate_thread_->CancelMigrate();
}

void PikaServer::DispatchBinlogBG(const std::string &key,
        PikaCmdArgsType* argv, const std::string& raw_args,
        uint64_t cur_serial, bool readonly) {
    size_t index = str_hash(key) % binlogbg_workers_.size();
    binlogbg_workers_[index]->Schedule(argv, raw_args, cur_serial, readonly);
}

bool PikaServer::WaitTillBinlogBGSerial(uint64_t my_serial) {
    binlogbg_mutex_.Lock();
    //DLOG(INFO) << "Binlog serial wait: " << my_serial << ", current: " << binlogbg_serial_;
    while (binlogbg_serial_ != my_serial && !binlogbg_exit_) {
        binlogbg_cond_.Wait();
    }
    binlogbg_mutex_.Unlock();
    return (binlogbg_serial_ == my_serial);
}

void PikaServer::SignalNextBinlogBGSerial() {
    binlogbg_mutex_.Lock();
    //DLOG(INFO) << "Binlog serial signal: " << binlogbg_serial_;
    ++binlogbg_serial_;
    binlogbg_cond_.SignalAll();
    binlogbg_mutex_.Unlock();
}

void PikaServer::RunKeyScan() {
    std::vector<uint64_t> new_key_nums_v;

    rocksdb::Status s = db_->GetKeyNum(&new_key_nums_v);

    slash::MutexLock lm(&key_scan_protector_);
    if (s.ok()) {
        key_scan_info_.key_nums_v = new_key_nums_v;
    }
    key_scan_info_.key_scaning_ = false;
}

void PikaServer::DoKeyScan(void *arg) {
    PikaServer *p = reinterpret_cast<PikaServer*>(arg);
    p->RunKeyScan();
}

void PikaServer::StopKeyScan() {
    slash::MutexLock l(&key_scan_protector_);
    if (key_scan_info_.key_scaning_) {
        db_->StopScanKeyNum();
        key_scan_info_.key_scaning_ = false;
    }
}

void PikaServer::KeyScan() {
    key_scan_protector_.Lock();
    if (key_scan_info_.key_scaning_) {
        key_scan_protector_.Unlock();
        return;
    }
    key_scan_info_.key_scaning_ = true;
    key_scan_protector_.Unlock();

    key_scan_thread_.StartThread();
    InitKeyScan();
    key_scan_thread_.Schedule(&DoKeyScan, reinterpret_cast<void*>(this));
}

void PikaServer::InitKeyScan() {
    key_scan_info_.start_time = time(NULL);
    char s_time[32];
    int len = strftime(s_time, sizeof(s_time), "%Y-%m-%d %H:%M:%S", localtime(&key_scan_info_.start_time));
    key_scan_info_.s_start_time.assign(s_time, len);
}

void PikaServer::ClientKillAll() {
    pika_dispatch_thread_->ClientKillAll();
    monitor_thread_->ThreadClientKill();
}

int PikaServer::ClientKill(const std::string &ip_port) {
    if (pika_dispatch_thread_->ClientKill(ip_port) ||
            monitor_thread_->ThreadClientKill(ip_port)) {
        return 1;
    }
    return 0;
}

int64_t PikaServer::ClientList(std::vector<ClientInfo> *clients) {
    int64_t clients_num = 0;
    clients_num += pika_dispatch_thread_->ThreadClientList(clients);
    clients_num += monitor_thread_->ThreadClientList(clients);
    return clients_num;
}

void PikaServer::RWLockWriter() {
    pthread_rwlock_wrlock(&rwlock_);
}

void PikaServer::RWLockReader() {
    pthread_rwlock_rdlock(&rwlock_);
}

void PikaServer::RWUnlock() {
    pthread_rwlock_unlock(&rwlock_);
}

void PikaServer::PlusThreadQuerynum() {
    statistic_data_.thread_querynum++;
}

uint64_t PikaServer::ServerQueryNum() {
    slash::ReadLock l(&statistic_data_.statistic_lock);
    return statistic_data_.thread_querynum;
}

void PikaServer::ResetStat() {
    statistic_data_.accumulative_connections.store(0);
    slash::WriteLock l(&statistic_data_.statistic_lock);
    statistic_data_.thread_querynum = 0;
    statistic_data_.last_thread_querynum = 0;
}

uint64_t PikaServer::ServerCurrentQps() {
    slash::ReadLock l(&statistic_data_.statistic_lock);
    return statistic_data_.last_sec_thread_querynum;
}

void PikaServer::ResetLastSecQuerynum() {
 slash::WriteLock l(&statistic_data_.statistic_lock);
 uint64_t cur_time_us = slash::NowMicros();
 statistic_data_.last_sec_thread_querynum = (
            (statistic_data_.thread_querynum - statistic_data_.last_thread_querynum)
            * 1000000 / (cur_time_us - statistic_data_.last_time_us + 1));
 statistic_data_.last_thread_querynum.store(statistic_data_.thread_querynum.load());
 statistic_data_.last_time_us = cur_time_us;
}

void PikaServer::ResetCacheAsync(uint32_t cache_num, dory::CacheConfig *cache_cfg)
{
    if (PIKA_CACHE_STATUS_OK == cache_->CacheStatus()
        || PIKA_CACHE_STATUS_NONE == cache_->CacheStatus()) {

        common_bg_thread_.StartThread();
        BGCacheTaskArg *arg = new BGCacheTaskArg();
        arg->p = this;
        arg->cache_num = cache_num;
        if (NULL == cache_cfg) {
            arg->task_type = CACHE_BGTASK_RESET_NUM;
        } else {
            arg->task_type = CACHE_BGTASK_RESET_CFG;
            arg->cache_cfg = *cache_cfg;
        }
        common_bg_thread_.Schedule(&DoCacheBGTask, static_cast<void*>(arg));
    } else {
        LOG(WARNING) << "can not reset cache in status: " << cache_->CacheStatus();
    }
}

void PikaServer::RefreshCmdStats(){
    cmd_stats_.RefreshCmdStats();
}

void PikaServer::UpdateCacheInfo(void)
{
    if (PIKA_CACHE_STATUS_OK != cache_->CacheStatus()) {
        return;
    }

    // get cache info from redis cache
    PikaCache::CacheInfo cache_info;
    cache_->Info(cache_info);

    slash::WriteLock l(&cache_info_rwlock_);
    cache_info_.status = cache_info.status;
    cache_info_.cache_num = cache_info.cache_num;
    cache_info_.keys_num = cache_info.keys_num;
    cache_info_.used_memory = cache_info.used_memory;
    cache_info_.waitting_load_keys_num = cache_info.waitting_load_keys_num;
    cache_usage_ = cache_info.used_memory;

    uint64_t all_cmds = cache_info.hits + cache_info.misses;
    cache_info_.hitratio_all = (0 >= all_cmds) ? 0.0 : (cache_info.hits * 100.0) / all_cmds;

    uint64_t cur_time_us = slash::NowMicros();
    uint64_t delta_time = cur_time_us - cache_info_.last_time_us + 1;
    uint64_t delta_hits = cache_info.hits - cache_info_.hits;
    cache_info_.hits_per_sec = delta_hits * 1000000 / delta_time;

    uint64_t delta_all_cmds = all_cmds - (cache_info_.hits + cache_info_.misses);
    cache_info_.read_cmd_per_sec = delta_all_cmds * 1000000 / delta_time;

    cache_info_.hitratio_per_sec = (0 >= delta_all_cmds) ? 0.0 : (delta_hits * 100.0) / delta_all_cmds;

    uint64_t delta_load_keys = cache_info.async_load_keys_num - cache_info_.last_load_keys_num;
    cache_info_.load_keys_per_sec = delta_load_keys * 1000000 / delta_time;

    cache_info_.hits = cache_info.hits;
    cache_info_.misses = cache_info.misses;
    cache_info_.last_time_us = cur_time_us;
    cache_info_.last_load_keys_num = cache_info.async_load_keys_num;
}

void PikaServer::GetCacheInfo(DisplayCacheInfo &cache_info)
{
    slash::ReadLock l(&cache_info_rwlock_);
    cache_info = cache_info_;
}

void PikaServer::ResetDisplayCacheInfo(int status)
{
    slash::WriteLock l(&cache_info_rwlock_);
    cache_info_.status = status;
    cache_info_.cache_num = 0;
    cache_info_.keys_num = 0;
    cache_info_.used_memory = 0;
    cache_info_.hits = 0;
    cache_info_.misses = 0;
    cache_info_.hits_per_sec = 0;
    cache_info_.read_cmd_per_sec = 0;
    cache_info_.hitratio_per_sec = 0.0;
    cache_info_.hitratio_all = 0.0;
    cache_info_.load_keys_per_sec = 0;
    cache_info_.waitting_load_keys_num = 0;
    cache_usage_ = 0;
}

void
PikaServer::ClearCacheDbSync(void)
{
    if (PIKA_CACHE_STATUS_OK != cache_->CacheStatus()) {
        LOG(WARNING) << "can not clear cache in status: " << cache_->CacheStatus();
        return;
    }
    
    LOG(INFO) << "clear cache start...";
    cache_->SetCacheStatus(PIKA_CACHE_STATUS_CLEAR);
    ResetDisplayCacheInfo(PIKA_CACHE_STATUS_CLEAR);
    cache_->FlushDb();
    LOG(INFO) << "clear cache finish";
    cache_->SetCacheStatus(PIKA_CACHE_STATUS_OK);
}

void
PikaServer::ClearCacheDbAsync(void)
{
    if (PIKA_CACHE_STATUS_OK != cache_->CacheStatus()) {
        LOG(WARNING) << "can not clear cache in status: " << cache_->CacheStatus();
        return;
    }

    common_bg_thread_.StartThread();
    BGCacheTaskArg *arg = new BGCacheTaskArg();
    arg->p = this;
    arg->task_type = CACHE_BGTASK_CLEAR;
    common_bg_thread_.Schedule(&DoCacheBGTask, static_cast<void*>(arg));
}

void
PikaServer::ClearHitRatio(void)
{
    cache_->ClearHitRatio();
}

void
PikaServer::ResetCacheConfig(void)
{
    dory::CacheConfig cache_cfg;
    cache_cfg.maxmemory = g_pika_conf->cache_maxmemory();
    cache_cfg.maxmemory_policy = g_pika_conf->cache_maxmemory_policy();
    cache_cfg.maxmemory_samples = g_pika_conf->cache_maxmemory_samples();
    cache_cfg.lfu_decay_time = g_pika_conf->cache_lfu_decay_time();
    cache_->ResetConfig(&cache_cfg);
}

int
PikaServer::CacheStatus(void)
{
    return cache_->CacheStatus();
}

void PikaServer::DoCacheBGTask(void* arg)
{
    BGCacheTaskArg *pCacheTaskArg = static_cast<BGCacheTaskArg*>(arg);
    PikaServer* p = pCacheTaskArg->p;

    switch (pCacheTaskArg->task_type) {
        case CACHE_BGTASK_CLEAR:
            LOG(INFO) << "clear cache start...";
            p->Cache()->SetCacheStatus(PIKA_CACHE_STATUS_CLEAR);
            p->ResetDisplayCacheInfo(PIKA_CACHE_STATUS_CLEAR);
            p->Cache()->FlushDb();
            LOG(INFO) << "clear cache finish";
            break;
        case CACHE_BGTASK_RESET_NUM:
            LOG(INFO) << "reset cache num start...";
            p->Cache()->SetCacheStatus(PIKA_CACHE_STATUS_RESET);
            p->ResetDisplayCacheInfo(PIKA_CACHE_STATUS_RESET);
            p->Cache()->Reset(pCacheTaskArg->cache_num);
            LOG(INFO) << "reset cache num finish";
            break;
        case CACHE_BGTASK_RESET_CFG:
            LOG(INFO) << "reset cache config start...";
            p->Cache()->SetCacheStatus(PIKA_CACHE_STATUS_RESET);
            p->ResetDisplayCacheInfo(PIKA_CACHE_STATUS_RESET);
            p->Cache()->Reset(pCacheTaskArg->cache_num, &pCacheTaskArg->cache_cfg);
            LOG(INFO) << "reset cache config finish";
            break;
        default:
            LOG(WARNING) << "invalid cache task type: " << pCacheTaskArg->task_type;
            break;
    }
    p->Cache()->SetCacheStatus(PIKA_CACHE_STATUS_OK);

    delete (BGCacheTaskArg*)arg;
}

void PikaServer::DoTimingTask() {
    // Maybe schedule compactrange
    AutoCompactRange();
    // Purge log
    AutoPurge();
    // Delete expired dump
    AutoDeleteExpiredDump();

    // Check rsync deamon
    if (((role_ & PIKA_ROLE_SLAVE) ^ PIKA_ROLE_SLAVE) || // Not a slave
        repl_state_ == PIKA_REPL_NO_CONNECT ||
        repl_state_ == PIKA_REPL_CONNECTED ||
        repl_state_ == PIKA_REPL_ERROR) {
        slash::StopRsync(g_pika_conf->db_sync_path());
    }

    //clean migrate clients, and the migrate_clients_ don't need clean immediately
    if(g_pika_server->pika_migrate_.Trylock() == 0) {
        g_pika_server->pika_migrate_.CleanMigrateClient();
        g_pika_server->pika_migrate_.Unlock();
    }
}

void PikaServer::DoFreshInfoTimingTask() {
	//fresh data_info
    g_pika_server->RWLockReader();
	g_pika_server->db_size_ = slash::Du(g_pika_conf->db_path());
	g_pika_server->db()->GetUsage(blackwidow::USAGE_TYPE_ROCKSDB_MEMTABLE, &g_pika_server->memtable_usage_);
	g_pika_server->db()->GetUsage(blackwidow::USAGE_TYPE_ROCKSDB_TABLE_READER, &g_pika_server->table_reader_usage_);
	//fresh log_info
	g_pika_server->log_size_ = slash::Du(g_pika_conf->log_path());
    g_pika_server->RWUnlock();
}

void PikaServer::DoClearSysCachedMemory() {
    if (0 == g_pika_conf->min_system_free_mem()) {
        return;
    }

    unsigned long free_mem = 0;
    int ret = slash::SystemFreeMemory(&free_mem);
    if (ret != 0) {
        LOG(ERROR) << "get system free memroy failed, errno:" << ret;
        return;
    }

    if (static_cast<int64_t>(free_mem) < g_pika_conf->min_system_free_mem()) {
        slash::ClearSystemCachedMemory();
    }
}

void PikaServer::DoAutoDelZsetMember() {
    if (is_slave() || 0 == g_pika_conf->zset_auto_del_threshold()) {
        return;
    }

    int zset_auto_del_interval = g_pika_conf->zset_auto_del_interval();
    std::string zset_auto_del_cron = g_pika_conf->zset_auto_del_cron();

    if (0 == zset_auto_del_interval && "" == zset_auto_del_cron) {
        return;
    }

    if (0 == zset_auto_del_interval) {
        zset_auto_del_interval = 24;
    } 

    if ("" == zset_auto_del_cron) {
        zset_auto_del_cron = "00-23";
    }

    // check interval
    if (0 != zset_auto_del_interval) {
        struct timeval now;
        gettimeofday(&now, NULL);
        int64_t last_check_time = pika_zset_auto_del_thread_->LastFinishCheckAllZsetTime();
        if (now.tv_sec - last_check_time < zset_auto_del_interval * 3600) {
            return;
        }  
    }

    // check cron
    if ("" != zset_auto_del_cron) {
        std::string::size_type colon = zset_auto_del_cron.find("-");
        int start = std::atoi(zset_auto_del_cron.substr(0, colon).c_str());
        int end = std::atoi(zset_auto_del_cron.substr(colon+1).c_str());
        std::time_t t = std::time(nullptr);
        std::tm* t_m = std::localtime(&t);

        bool in_window = false;
        if (start < end && (t_m->tm_hour >= start && t_m->tm_hour < end)) {
            in_window = true;
        } else if (start > end && ((t_m->tm_hour >= start && t_m->tm_hour < 24) ||
                    (t_m->tm_hour >= 0 && t_m->tm_hour < end))) {
            in_window = true;
        } 

        if (in_window) {
            pika_zset_auto_del_thread_->RequestCronTask();
        }
    }
}

Status PikaServer::ZsetAutoDel(int64_t cursor, double speed_factor) {
    if (is_slave()) {
        return Status::NotSupported("slave not support this command");
    }

    if (0 == g_pika_conf->zset_auto_del_threshold()) {
        return Status::NotSupported("zset_auto_del_threshold is 0, means not use zset length limit");
    }

    pika_zset_auto_del_thread_->RequestManualTask(cursor, speed_factor);
    return Status::OK();
}

Status PikaServer::ZsetAutoDelOff() {
    if (is_slave()) {
        return Status::NotSupported("slave not support this command");
    }

    pika_zset_auto_del_thread_->StopManualTask();
    return Status::OK();
}

void PikaServer::GetZsetInfo(ZsetInfo &info) {
    pika_zset_auto_del_thread_->GetZsetInfo(info);
}
