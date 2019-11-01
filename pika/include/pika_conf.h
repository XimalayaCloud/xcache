// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CONF_H_
#define PIKA_CONF_H_
#include <pthread.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <atomic>

#include "slash/include/base_conf.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/slash_string.h"
#include "slash/include/xdebug.h"
#include "pika_define.h"

typedef slash::RWLock RWLock;

class PikaConf : public slash::BaseConf {
public:
    PikaConf(const std::string& path);
    ~PikaConf() { pthread_rwlock_destroy(&rwlock_); }

    // Getter
    int port()                      { return port_; }
    std::string slaveof()           { RWLock l(&rwlock_, false); return slaveof_;}
    int slave_priority()            { return slave_priority_;}
    int thread_num()                { return thread_num_; }
    int sync_thread_num()           { return sync_thread_num_; }
    int sync_buffer_size()          { return sync_buffer_size_; }
    std::string log_path()          { RWLock l(&rwlock_, false); return log_path_; }
    int log_level()                 { return log_level_; }
    int max_log_size()              { return max_log_size_; }
    std::string db_path()           { RWLock l(&rwlock_, false); return db_path_; }
    std::string db_sync_path()      { RWLock l(&rwlock_, false); return db_sync_path_; }
    int db_sync_speed()             { return db_sync_speed_; }
    std::string compact_cron()      { RWLock l(&rwlock_, false); return compact_cron_; }
    std::string compact_interval()  { RWLock l(&rwlock_, false); return compact_interval_; }
    int64_t write_buffer_size()     { return write_buffer_size_; }
    int max_write_buffer_number()   { return max_write_buffer_number_; }
    int timeout()                   { return timeout_; }
    int fresh_info_interval()       { return fresh_info_interval_; }

    std::string requirepass()       { RWLock l(&rwlock_, false); return requirepass_; }
    std::string masterauth()        { RWLock l(&rwlock_, false); return masterauth_; }
    bool slotmigrate()              { return slotmigrate_; }
    int slotmigrate_thread_num()    { return slotmigrate_thread_num_; }
    int thread_migrate_keys_num()   { return thread_migrate_keys_num_; }
    std::string bgsave_path()       { RWLock l(&rwlock_, false); return bgsave_path_; }
    int expire_dump_days()          { return expire_dump_days_; }
    std::string bgsave_prefix()     { RWLock l(&rwlock_, false); return bgsave_prefix_; }
    std::string userpass()          { RWLock l(&rwlock_, false); return userpass_; }
    const std::string suser_blacklist() { RWLock l(&rwlock_, false); return slash::StringConcat(user_blacklist_, COMMA); }
    const std::vector<std::string>& vuser_blacklist() { RWLock l(&rwlock_, false); return user_blacklist_; }
    std::string compression()       { RWLock l(&rwlock_, false); return compression_; }
    int target_file_size_base()     { return target_file_size_base_; }
    int max_bytes_for_level_base()  { return max_bytes_for_level_base_; }
    int max_background_flushes()    { return max_background_flushes_; }
    int max_background_compactions(){ return max_background_compactions_; }
    int max_cache_files()           { return max_cache_files_; }
    int max_bytes_for_level_multiplier() { return max_bytes_for_level_multiplier_; }
    int disable_auto_compactions()  { return disable_auto_compactions_; }
    int block_size()                { return block_size_; }
    int64_t block_cache()           { return block_cache_; }
    bool share_block_cache()        { return share_block_cache_; }
    bool cache_index_and_filter_blocks() { return cache_index_and_filter_blocks_; }
    bool optimize_filters_for_hits(){ return optimize_filters_for_hits_; }
    bool level_compaction_dynamic_level_bytes() { return level_compaction_dynamic_level_bytes_; }
    int max_subcompactions()        { return max_subcompactions_; }
    int expire_logs_nums()          { return expire_logs_nums_; }
    int expire_logs_days()          { return expire_logs_days_; }
    int binlog_writer_queue_size()  { return binlog_writer_queue_size_; }
    std::string binlog_writer_method() { RWLock l(&rwlock_, false); return binlog_writer_method_; }
    int binlog_writer_num()         { return binlog_writer_num_; }
    std::string conf_path()         { RWLock l(&rwlock_, false); return conf_path_; }
    bool readonly()                 { return readonly_; }
    int maxclients()                { return maxclients_; }
    int root_connection_num()       { return root_connection_num_; }
    int slowlog_slower_than()       { return slowlog_log_slower_than_; }
    int slowlog_max_len()           { return slowlog_max_len_;}
    std::string network_interface() { RWLock l(&rwlock_, false); return network_interface_; }
    int level0_file_num_compaction_trigger() { return level0_file_num_compaction_trigger_; }
    int level0_slowdown_writes_trigger()     { return level0_slowdown_writes_trigger_; }
    int level0_stop_writes_trigger()         { return level0_stop_writes_trigger_; }
    int cache_num()                 { return cache_num_; }
    int cache_model()               { return cache_model_; }
    int64_t cache_maxmemory()       { return cache_maxmemory_; }
    int cache_maxmemory_policy()    { return cache_maxmemory_policy_; }
    int cache_maxmemory_samples()   { return cache_maxmemory_samples_; }
    int cache_lfu_decay_time()      { return cache_lfu_decay_time_; }

    // Immutable config items, we don't use lock.
    bool daemonize()                { return daemonize_; }
    std::string pidfile()           { return pidfile_; }
    int binlog_file_size()          { return binlog_file_size_; }
    int64_t min_blob_size()         { return min_blob_size_; }
    int64_t rate_bytes_per_sec()    { return rate_bytes_per_sec_; }
    bool disable_wal()              { return disable_wal_; }
    bool use_direct_reads()         { return use_direct_reads_; }
    bool use_direct_io_for_flush_and_compaction() { return use_direct_io_for_flush_and_compaction_; }
    int64_t min_system_free_mem()   { return min_system_free_mem_; }
    int64_t max_gc_batch_size()     { return max_gc_batch_size_; }
    int blob_file_discardable_ratio() { return blob_file_discardable_ratio_; }
    int64_t gc_sample_cycle()       { return gc_sample_cycle_; }
    int max_gc_queue_size()         { return max_gc_queue_size_; }

    // Setter
    void SetPort(const int value)           { port_ = value; }
    void SetThreadNum(const int value)      { thread_num_ = value; }
    void SetLogLevel(const int value)       { log_level_ = value; }
    void SetMaxLogSize(const int value)     { max_log_size_ = value; }
    void SetTimeout(const int value)        { timeout_ = value; }
    void SetFreshInfoInterval(const int value)   	 { fresh_info_interval_ = value; }
    void SetSlaveof(const std::string value){ RWLock l(&rwlock_, true); slaveof_ = value; }
    void SetSlavePriority(const int value)  { slave_priority_ = value; }
    void SetBgsavePath(const std::string &value) {
        RWLock l(&rwlock_, true);
        bgsave_path_ = value;
        if (value[value.length() - 1] != '/') {
            bgsave_path_ += "/";
        }
    }
    void SetExpireDumpDays(const int value)         { expire_dump_days_ = value; }
    void SetBgsavePrefix(const std::string &value)  { RWLock l(&rwlock_, true); bgsave_prefix_ = value; }
    void SetRequirePass(const std::string &value)   { RWLock l(&rwlock_, true); requirepass_ = value; }
    void SetMasterAuth(const std::string &value)    { RWLock l(&rwlock_, true); masterauth_ = value; }
    void SetSlotMigrate(const bool value)           { slotmigrate_ =  value; }
    void SetSlotMigrateThreadNum(const int value)   { slotmigrate_thread_num_ = value; }
    void SetThreadMigrateKeysNum(const int value)   { thread_migrate_keys_num_ = value; }
    void SetUserPass(const std::string &value)      { RWLock l(&rwlock_, true); userpass_ = value; }
    void SetUserBlackList(const std::string &value) {
        RWLock l(&rwlock_, true);
        std::string lower_value = value;
        slash::StringToLower(lower_value);
        slash::StringSplit(lower_value, COMMA, user_blacklist_);
    }
    void SetReadonly(const bool value)              { readonly_ = value; }
    void SetExpireLogsNums(const int value)         { expire_logs_nums_ = value; }
    void SetExpireLogsDays(const int value)         { expire_logs_days_ = value; }
    void SetBinlogWriterQueueSize(const int value)  { binlog_writer_queue_size_ = value; }
    void SetMaxConnection(const int value)          { maxclients_ = value; }
    void SetRootConnectionNum(const int value)      { root_connection_num_ = value; }
    void SetSlowlogSlowerThan(const int value)      { slowlog_log_slower_than_ = value; }
    void SetSlowlogMaxLen(const int value)          { slowlog_max_len_ = value; }
    void SetDbSyncSpeed(const int value)            { db_sync_speed_ = value; }
    void SetCompactCron(const std::string &value)   { RWLock l(&rwlock_, true); compact_cron_ = value; }
    void SetCompactInterval(const std::string &value) { RWLock l(&rwlock_, true); compact_interval_ = value; }
    void SetTargetFileSizeBase(const int value)     { target_file_size_base_ = value; }
    void SetMaxBytesForLevelBase(const int value)   { max_bytes_for_level_base_ = value; }
    void SetWriteBufferSize(const int64_t value)    { write_buffer_size_ = value; }
    void SetMaxWriteBufferNumber(const int value)   { max_write_buffer_number_ = value; }
    void SetDisableAutoCompactions(const bool value){ disable_auto_compactions_ = value; }
    void SetLevel0FileNumCompactionTrigger(const int value) { level0_file_num_compaction_trigger_ = value; }
    void SetLevel0SlowdownWritesTrigger(const int value) { level0_slowdown_writes_trigger_ = value; }
    void SetLevel0StopWritesTrigger(const int value){ level0_stop_writes_trigger_ = value; }
    void SetCacheNum(const int value)               { cache_num_ = value; }
    void SetCacheModel(const int value)             { cache_model_ = value; }
    void SetCacheMaxmemory(const int64_t value)     { cache_maxmemory_ = value; }
    void SetCacheMaxmemoryPolicy(const int value)   { cache_maxmemory_policy_ = value; }
    void SetCacheMaxmemorySamples(const int value)  { cache_maxmemory_samples_ = value; }
    void SetCacheLFUDecayTime(const int value)      { cache_lfu_decay_time_ = value; }
    void SetRateBytesPerSec(const int64_t value)    { rate_bytes_per_sec_ = value; }
    void SetDisableWAL(const bool value)            { disable_wal_ = value; }
    void SetMinSystemFreeMem(const int64_t value)   { min_system_free_mem_ = value; }
    void SetMaxGCBatchSize(const int64_t value)     { max_gc_batch_size_ = value; }
    void SetBlobFileDiscardableRatio(const int value) { blob_file_discardable_ratio_ = value; }
    void SetGCSampleCycle(const int64_t value)      { gc_sample_cycle_ = value; }
    void SetMaxGCQueueSize(const int value)         { max_gc_queue_size_ = value; }

    int Load();
    int ConfigRewrite();

private:
    std::atomic<int> port_;
    std::string slaveof_;
    std::atomic<int> slave_priority_;
    std::atomic<int> thread_num_;
    std::atomic<int> sync_thread_num_;
    std::atomic<int> sync_buffer_size_;
    std::string log_path_;
    std::string db_path_;
    std::string db_sync_path_;
    std::atomic<int> expire_dump_days_;
    std::atomic<int> db_sync_speed_;
    std::string compact_cron_;
    std::string compact_interval_;
    std::atomic<int64_t> write_buffer_size_;
    std::atomic<int> max_write_buffer_number_;
    std::atomic<int> log_level_;
    std::atomic<int> max_log_size_;
    std::atomic<bool> daemonize_;
    std::atomic<bool> slotmigrate_;
    std::atomic<int> slotmigrate_thread_num_;
    std::atomic<int> thread_migrate_keys_num_;
    std::atomic<int> timeout_;
    std::atomic<int> fresh_info_interval_;
    std::string requirepass_;
    std::string masterauth_;
    std::string userpass_;
    std::vector<std::string> user_blacklist_;
    std::string bgsave_path_;
    std::string bgsave_prefix_;
    std::string pidfile_;
    std::atomic<int> cache_num_;
    std::atomic<int> cache_model_;
    std::atomic<int64_t> cache_maxmemory_;
    std::atomic<int> cache_maxmemory_policy_;
    std::atomic<int> cache_maxmemory_samples_;
    std::atomic<int> cache_lfu_decay_time_;

    std::string compression_;
    std::atomic<int> maxclients_;
    std::atomic<int> root_connection_num_;
    std::atomic<int> slowlog_log_slower_than_;
    std::atomic<int> slowlog_max_len_;
    std::atomic<int> expire_logs_days_;
    std::atomic<int> expire_logs_nums_;
    std::atomic<int> binlog_writer_queue_size_;
    std::string binlog_writer_method_;
    std::atomic<int> binlog_writer_num_;
    std::atomic<bool> readonly_;
    std::string conf_path_;
    std::atomic<int> max_background_flushes_;
    std::atomic<int> max_background_compactions_;
    std::atomic<int> max_cache_files_;
    std::atomic<int> max_bytes_for_level_multiplier_;
    std::string network_interface_;
    std::atomic<bool> disable_auto_compactions_;
    std::atomic<int> block_size_;
    std::atomic<int64_t> block_cache_;
    std::atomic<bool> share_block_cache_;
    std::atomic<bool> cache_index_and_filter_blocks_;
    std::atomic<bool> optimize_filters_for_hits_;
    std::atomic<bool> level_compaction_dynamic_level_bytes_;
    std::atomic<int> max_subcompactions_;

    // Critical configure items
    std::atomic<int> target_file_size_base_;
    std::atomic<int> max_bytes_for_level_base_;
    std::atomic<int> binlog_file_size_;
    std::atomic<int> level0_file_num_compaction_trigger_;
    std::atomic<int> level0_slowdown_writes_trigger_;
    std::atomic<int> level0_stop_writes_trigger_;

    std::atomic<int64_t> min_blob_size_;
    std::atomic<int64_t> rate_bytes_per_sec_;
    std::atomic<bool> disable_wal_;
    std::atomic<bool> use_direct_reads_;
    std::atomic<bool> use_direct_io_for_flush_and_compaction_;
    std::atomic<int64_t> min_system_free_mem_;
    std::atomic<int64_t> max_gc_batch_size_;
    std::atomic<int> blob_file_discardable_ratio_;
    std::atomic<int64_t> gc_sample_cycle_;
    std::atomic<int> max_gc_queue_size_;

    pthread_rwlock_t rwlock_;
    slash::Mutex config_mutex_;
};

#endif
