// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <fstream>
#include <iostream>

#include "sys/stat.h"
#include "pika_conf.h"
#include "glog/logging.h"


PikaConf::PikaConf(const std::string& path)
    : slash::BaseConf(path)
    , conf_path_(path)
{
    pthread_rwlock_init(&rwlock_, NULL);
}

int PikaConf::Load()
{
    int ret = LoadConf();
    if (ret != 0) {
        return ret;
    }

    // Mutable Section
    std::string loglevel;
    GetConfStr("loglevel", &loglevel);
    slash::StringToLower(loglevel);
    if (loglevel == "info") {
        log_level_ = 0;
    } else if (loglevel == "error") {
        log_level_ = 1;
    } else {
        log_level_ = 0;
        fprintf(stderr, "Invalid loglevel value in conf file, only INFO or ERROR\n");
        exit(-1);
    }

    int timeout = 60;
    GetConfInt("timeout", &timeout);
    timeout_ = (timeout < 0) ? 60 : timeout;

    int fresh_info_interval = 60;
    GetConfInt("fresh-info_interval", &fresh_info_interval);
    fresh_info_interval_ = (fresh_info_interval <= 0) ? 60 : fresh_info_interval;

    GetConfStr("requirepass", &requirepass_);
    GetConfStr("masterauth", &masterauth_);
    GetConfStr("userpass", &userpass_);

    int maxclients = 20000; 
    GetConfInt("maxclients", &maxclients);
    maxclients_ = (maxclients <= 0) ? 20000 : maxclients;

    int root_connection_num = 2;
    GetConfInt("root-connection-num", &root_connection_num);
    root_connection_num_ = (root_connection_num < 0) ? 2 : root_connection_num;

    int slowlog_log_slower_than = 100000;
    GetConfInt("slowlog-log-slower-than", &slowlog_log_slower_than);
    slowlog_log_slower_than_ = (slowlog_log_slower_than < 0) ? 100000 : slowlog_log_slower_than;

    int slowlog_max_len = 12800;
    GetConfInt("slowlog-max-len", &slowlog_max_len);
    slowlog_max_len_ = (100 > slowlog_max_len || 10000000 < slowlog_max_len) ? 12800 : slowlog_max_len;

    std::string user_blacklist;
    GetConfStr("userblacklist", &user_blacklist);
    SetUserBlackList(std::string(user_blacklist));

    GetConfStr("dump-path", &bgsave_path_);
    if (bgsave_path_[bgsave_path_.length() - 1] != '/') {
        bgsave_path_ += "/";
    }

    int expire_dump_days = 0;
    GetConfInt("dump-expire", &expire_dump_days);
    expire_dump_days_ = (expire_dump_days < 0) ? 0 : expire_dump_days;

    GetConfStr("dump-prefix", &bgsave_prefix_);

    int expire_logs_nums =10 ;
    GetConfInt("expire-logs-nums", &expire_logs_nums);
    expire_logs_nums_ = (expire_logs_nums <= 10) ? 10 : expire_logs_nums;

    int expire_logs_days = 1;
    GetConfInt("expire-logs-days", &expire_logs_days);
    expire_logs_days_ = (expire_logs_days <= 0) ? 1 : expire_logs_days;

    int binlog_writer_queue_size =100 ;
    GetConfInt("binlog-writer-queue-size", &binlog_writer_queue_size);
    if (binlog_writer_queue_size <= 0 || binlog_writer_queue_size > 10000) {
        binlog_writer_queue_size_ = 100;
    } else {
        binlog_writer_queue_size_ = binlog_writer_queue_size;
    }

    GetConfStr("binlog-writer-method", &binlog_writer_method_);
    slash::StringToLower(binlog_writer_method_);
    if (binlog_writer_method_ != "sync" && binlog_writer_method_ != "async") {
        binlog_writer_method_ = "sync";
    }

    int binlog_writer_num = 1 ;
    GetConfInt("binlog-writer-num", &binlog_writer_num);
    if (binlog_writer_num <= 0 || binlog_writer_num > 24) {
        binlog_writer_num_ = 1;
    } else {
        binlog_writer_num_ = binlog_writer_num;
    }

    GetConfStr("compression", &compression_);

    bool readonly = 0 ;
    GetConfBool("slave-read-only", &readonly);
    readonly_ = readonly;

    int slave_priority = 100;
    GetConfInt("expire-logs-days", &slave_priority);
    slave_priority_ = slave_priority;

    // Immutable Sections
    int port = 9221 ;
    GetConfInt("port", &port);
    port_ = port;

    GetConfStr("log-path", &log_path_);

    GetConfStr("db-path", &db_path_);
    if (log_path_[log_path_.length() - 1] != '/') {
        log_path_ += "/";
    }

    int max_log_size = 1800;
    GetConfInt("max-log-size", &max_log_size);
    max_log_size_ = (max_log_size <= 0) ? 1800 : max_log_size;

    int thread_num = 1;
    GetConfInt("thread-num", &thread_num);
    if (thread_num <= 0) {
        thread_num_ = 12;
    } else if (thread_num > 24) {
        thread_num_ = 24;
    } else {
        thread_num_ = thread_num;
    }

    int sync_thread_num = 6;
    GetConfInt("sync-thread-num", &sync_thread_num);
    if (sync_thread_num <= 0) {
        sync_thread_num_ = 3;
    } else if (sync_thread_num > 24) {
        sync_thread_num_ = 24;
    } else {
        sync_thread_num_ = sync_thread_num;
    }

    int sync_buffer_size = 10;
    GetConfInt("sync-buffer-size", &sync_buffer_size);
    if (sync_buffer_size <= 0) {
        sync_buffer_size_ = 5;
    } else if (sync_buffer_size > 100) {
        sync_buffer_size_ = 100;
    } else {
        sync_buffer_size_ = sync_buffer_size;
    }

    compact_cron_ = "";
    GetConfStr("compact-cron", &compact_cron_);
    if (compact_cron_ != "") {
        std::string::size_type len = compact_cron_.length();
        std::string::size_type colon = compact_cron_.find("-");
        std::string::size_type underline = compact_cron_.find("/");
        if (colon == std::string::npos || underline == std::string::npos ||
            colon >= underline || colon + 1 >= len ||
            colon + 1 == underline || underline + 1 >= len) {
            compact_cron_ = "";
        } else {
            int start = std::atoi(compact_cron_.substr(0, colon).c_str());
            int end = std::atoi(compact_cron_.substr(colon+1, underline).c_str());
            int usage = std::atoi(compact_cron_.substr(underline+1).c_str());
            if (start < 0 || start > 23 || end < 0 || end > 23 || usage < 0 || usage > 100) {
                compact_cron_ = "";
            }
        }
    }

    compact_interval_ = "";
    GetConfStr("compact-interval", &compact_interval_);
    if (compact_interval_ != "") {
        std::string::size_type len = compact_interval_.length();
        std::string::size_type slash = compact_interval_.find("/");
        if (slash == std::string::npos || slash + 1 >= len) {
            compact_interval_ = "";
        } else {
            int interval = std::atoi(compact_interval_.substr(0, slash).c_str());
            int usage = std::atoi(compact_interval_.substr(slash+1).c_str());
            if (interval <= 0 || usage < 0 || usage > 100) {
                compact_interval_ = "";
            }
        }
    }

    // write_buffer_size
    int64_t write_buffer_size = 268435456;
    GetConfInt64("write-buffer-size", &write_buffer_size);
    write_buffer_size_ = (write_buffer_size <= 0) ? 268435456 : write_buffer_size; // 256M

    // max_write_buffer_number
    int max_write_buffer_number = 2;
    GetConfInt("max-write-buffer-number", &max_write_buffer_number);
    max_write_buffer_number_ = (max_write_buffer_number < 2) ? 2 : max_write_buffer_number;

    // target_file_size_base
    int target_file_size_base = 1048576;
    GetConfInt("target-file-size-base", &target_file_size_base);
    target_file_size_base_ = (target_file_size_base <= 0) ? 1048576 : target_file_size_base; // 10M

    // max_bytes_for_level_base
    int max_bytes_for_level_base = 268435456;
    GetConfInt("max-bytes-for-level-base", &max_bytes_for_level_base);
    max_bytes_for_level_base_ = (max_bytes_for_level_base <= 0) ? 268435456 : max_bytes_for_level_base; // 256M

    int level0_file_num_compaction_trigger = 4;
    GetConfInt("level0-file-num-compaction-trigger", &level0_file_num_compaction_trigger);
    level0_file_num_compaction_trigger_ = (level0_file_num_compaction_trigger <= 0) ? 4 : level0_file_num_compaction_trigger;

    int level0_slowdown_writes_trigger = 20;
    GetConfInt("level0-slowdown-writes-trigger", &level0_slowdown_writes_trigger);
    level0_slowdown_writes_trigger_ = (level0_slowdown_writes_trigger <= 0) ? 20 : level0_slowdown_writes_trigger;

    int level0_stop_writes_trigger = 32;
    GetConfInt("level0-stop-writes-trigger", &level0_stop_writes_trigger);
    level0_stop_writes_trigger_ = (level0_stop_writes_trigger <= 0) ? 32 : level0_stop_writes_trigger;

    int max_background_flushes = 2;
    GetConfInt("max-background-flushes", &max_background_flushes);
    if (max_background_flushes <= 0) {
        max_background_flushes_ = 1;
    } else if (max_background_flushes >= 4) {
        max_background_flushes_ = 4;
    } else {
        max_background_flushes_ = max_background_flushes;
    }

    int max_background_compactions = 4;
    GetConfInt("max-background-compactions", &max_background_compactions);
    if (max_background_compactions <= 0) {
        max_background_compactions_ = 2;
    } else if (max_background_compactions >= 8) {
        max_background_compactions_ = 8;
    } else {
        max_background_compactions_ = max_background_compactions;
    }

    int max_cache_files = 5000;
    GetConfInt("max-cache-files", &max_cache_files);
    max_cache_files_ = (max_cache_files < -1) ? 5000 : max_cache_files;

    int max_bytes_for_level_multiplier = 5;
    GetConfInt("max-bytes-for-level-multiplier", &max_bytes_for_level_multiplier);
    max_bytes_for_level_multiplier_ = (max_bytes_for_level_multiplier < 10) ? 5 : max_bytes_for_level_multiplier;

    std::string disable_auto_compactions = "no";
    GetConfStr("disable-auto-compactions", &disable_auto_compactions);
    disable_auto_compactions_ = (disable_auto_compactions == "yes" || disable_auto_compactions == "1") ? true : false;
  
    int block_size = 4 * 1024;
    GetConfInt("block-size", &block_size);
    block_size_ = (block_size <= 0) ? 4 * 1024 : block_size;

    int block_cache = 8 * 1024 * 1024;
    GetConfInt("block-cache", &block_cache);
    block_cache_ = (block_cache < 0) ? 8 * 1024 * 1024 : block_cache;

    std::string sbc = "no";
    GetConfStr("share-block-cache", &sbc);
    share_block_cache_ = (sbc == "yes") ? true : false;

    std::string ciafb = "no";
    GetConfStr("cache-index-and-filter-blocks", &ciafb);
    cache_index_and_filter_blocks_ = (ciafb == "yes") ? true : false;

    std::string offh = "no" ;
    GetConfStr("optimize-filters-for-hits", &offh);
    optimize_filters_for_hits_ = (offh == "yes") ? true : false;

    std::string lcdlb = "no";
    GetConfStr("level-compaction-dynamic-level-bytes", &lcdlb);
    level_compaction_dynamic_level_bytes_ = (lcdlb == "yes") ? true : false;

    int max_subcompactions = 3;
    GetConfInt("max-subcompactions", &max_subcompactions);
    max_subcompactions_ = (max_subcompactions <= 0) ? 3 : max_subcompactions;

    // daemonize
    std::string dmz = "no";
    GetConfStr("daemonize", &dmz);
    daemonize_ =  (dmz == "yes") ? true : false;

    int binlog_file_size;
    GetConfInt("binlog-file-size", &binlog_file_size);
    if (binlog_file_size < 1024 || static_cast<int64_t>(binlog_file_size) > (1024LL * 1024 * 1024)) {
        binlog_file_size_ = 100 * 1024 * 1024;    // 100M
    } else {
        binlog_file_size_ = binlog_file_size;
    }

    GetConfStr("pidfile", &pidfile_);

    // slot migrate
    std::string smgrt = "no";
    GetConfStr("slotmigrate", &smgrt);
    slotmigrate_ =  (smgrt == "yes") ? true : false;

    // slot migrate thread num
    int slotmigrate_thread_num = 8;
    GetConfInt("slotmigrate-thread-num", &slotmigrate_thread_num);
    slotmigrate_thread_num_ = (1 > slotmigrate_thread_num || 24 < slotmigrate_thread_num) ? 8 : slotmigrate_thread_num;

    // thread migrate keys num
    int thread_migrate_keys_num = 64;
    GetConfInt("thread-migrate-keys-num", &thread_migrate_keys_num);
    thread_migrate_keys_num_ = (8 > thread_migrate_keys_num || 128 < thread_migrate_keys_num) ? 64 : thread_migrate_keys_num;

    // db sync
    GetConfStr("db-sync-path", &db_sync_path_);
    if (db_sync_path_[db_sync_path_.length() - 1] != '/') {
        db_sync_path_ += "/";
    }

    int db_sync_speed = -1;
    GetConfInt("db-sync-speed", &db_sync_speed);
    db_sync_speed_ = (db_sync_speed < 0 || db_sync_speed > 125) ? 125 : db_sync_speed;

    // network interface
    network_interface_ = "";
    GetConfStr("network-interface", &network_interface_);

    // slaveof
    slaveof_ = "";
    GetConfStr("slaveof", &slaveof_);

    int cache_num =16 ;
    GetConfInt("cache-num", &cache_num);
    cache_num_ = (0 >= cache_num || 48 < cache_num) ? 16 : cache_num;

    int cache_model = 0;
    GetConfInt("cache-model", &cache_model);
    cache_model_ = (PIKA_CACHE_NONE > cache_model || PIKA_CACHE_READ < cache_model) ? PIKA_CACHE_NONE : cache_model;

    int64_t cache_maxmemory = 10737418240 ;
    GetConfInt64("cache-maxmemory", &cache_maxmemory);
    cache_maxmemory_ = (PIKA_CACHE_SIZE_MIN > cache_maxmemory) ? PIKA_CACHE_SIZE_DEFAULT : cache_maxmemory;

    int cache_maxmemory_policy = 1;
    GetConfInt("cache-maxmemory-policy", &cache_maxmemory_policy);
    cache_maxmemory_policy_ = (0 > cache_maxmemory_policy || 7 < cache_maxmemory_policy) ? 1 : cache_maxmemory_policy;

    int cache_maxmemory_samples =5 ;
    GetConfInt("cache-maxmemory-samples", &cache_maxmemory_samples);
    cache_maxmemory_samples_ = (1 > cache_maxmemory_samples) ? 5 : cache_maxmemory_samples;

    int cache_lfu_decay_time = 1;
    GetConfInt("cache-lfu-decay-time", &cache_lfu_decay_time);
    cache_lfu_decay_time_ = (0 > cache_lfu_decay_time) ? 1 : cache_lfu_decay_time;

    int64_t min_blob_size = 65536;
    GetConfInt64("min-blob-size", &min_blob_size);
    min_blob_size_ = (256 > min_blob_size) ? 256 : min_blob_size;

    return ret;
}

int PikaConf::ConfigRewrite() {
    int ret = 0;
    slash::MutexLock l(&config_mutex_);
    SetConfInt("port", port_);
    SetConfInt("thread-num", thread_num_);
    SetConfInt("sync-thread-num", sync_thread_num_);
    SetConfInt("sync-buffer-size", sync_buffer_size_);
    SetConfStr("log-path", log_path_);
    SetConfStr("loglevel", log_level_ ? "ERROR" : "INFO");
    SetConfInt("max-log-size", max_log_size_);
    SetConfStr("db-path", db_path_);
    SetConfStr("db-sync-path", db_sync_path_);
    SetConfInt("db-sync-speed", db_sync_speed_);
    SetConfInt64("write-buffer-size", write_buffer_size_);
    SetConfInt("max-write-buffer-number", max_write_buffer_number_);
    SetConfInt("timeout", timeout_);
    SetConfInt("fresh-info-interval", fresh_info_interval_);
    SetConfStr("requirepass", requirepass_);
    SetConfStr("masterauth", masterauth_);
    SetConfStr("userpass", userpass_);
    SetConfStr("userblacklist", suser_blacklist());
    SetConfStr("dump-prefix", bgsave_prefix_);
    SetConfStr("daemonize", daemonize_ ? "yes" : "no");
    SetConfStr("slotmigrate", slotmigrate_ ? "yes" : "no");
    SetConfStr("dump-path", bgsave_path_);
    SetConfInt("dump-expire", expire_dump_days_);
    SetConfStr("pidfile", pidfile_);
    SetConfInt("maxclients", maxclients_);
    SetConfInt("target-file-size-base", target_file_size_base_);
    SetConfInt("max-bytes-for-level-base", max_bytes_for_level_base_);
    SetConfInt("expire-logs-days", expire_logs_days_);
    SetConfInt("expire-logs-nums", expire_logs_nums_);
    SetConfInt("binlog-writer-queue-size", binlog_writer_queue_size_);
    SetConfStr("binlog-writer-method", binlog_writer_method_);
    SetConfInt("binlog-writer-num", binlog_writer_num_);
    SetConfInt("root-connection-num", root_connection_num_);
    SetConfInt("slowlog-log-slower-than", slowlog_log_slower_than_);
    SetConfInt("slowlog-max-len", slowlog_max_len_);
    SetConfBool("slave-read-only", readonly_);
    SetConfStr("compact-cron", compact_cron_);
    SetConfStr("compact-interval", compact_interval_);
    SetConfStr("network-interface", network_interface_);
    SetConfStr("slaveof", slaveof_);
    SetConfInt("slave-priority", slave_priority_);

    SetConfInt("binlog-file-size", binlog_file_size_);
    SetConfStr("compression", compression_);
    SetConfInt("max-background-flushes", max_background_flushes_);
    SetConfInt("max-background-compactions", max_background_compactions_);
    SetConfInt("max-cache-files", max_cache_files_);
    SetConfInt("max-bytes-for-level-multiplier", max_bytes_for_level_multiplier_);
    SetConfStr("disable-auto-compactions", disable_auto_compactions_ ? "yes" : "no");
    SetConfInt("level0-file-num-compaction-trigger", level0_file_num_compaction_trigger_);
    SetConfInt("level0-slowdown-writes-trigger", level0_slowdown_writes_trigger_);
    SetConfInt("level0-stop-writes-trigger", level0_stop_writes_trigger_);
    SetConfInt("block-size", block_size_);
    SetConfInt("block-cache", block_cache_);
    SetConfStr("share-block-cache", share_block_cache_ ? "yes" : "no");
    SetConfStr("cache-index-and-filter-blocks", cache_index_and_filter_blocks_ ? "yes" : "no");
    SetConfStr("optimize-filters-for-hits", optimize_filters_for_hits_ ? "yes" : "no");
    SetConfStr("level-compaction-dynamic-level-bytes", level_compaction_dynamic_level_bytes_ ? "yes" : "no");
    SetConfInt("max-subcompactions", max_subcompactions_);
    SetConfInt("cache-model", cache_model_);

    ret = WriteBack();
    return ret;
}
