#include <glog/logging.h>

#include "slash/include/slash_recordlock.h"

#include "pika_cache_load_thread.h"
#include "pika_server.h"

#define CACHE_LOAD_QUEUE_MAX_SIZE   2048
#define CACHE_VALUE_ITEM_MAX_SIZE   2048
#define CACHE_LOAD_NUM_ONE_TIME     256

extern PikaServer *g_pika_server;


PikaCacheLoadThread::PikaCacheLoadThread()
    : should_exit_(false)
    , loadkeys_cond_(&loadkeys_mutex_)
    , async_load_keys_num_(0)
    , waitting_load_keys_num_(0)
{
    set_thread_name("PikaCacheLoadThread");
}

PikaCacheLoadThread::~PikaCacheLoadThread()
{
    {
        slash::MutexLock l(&loadkeys_mutex_);
        should_exit_ = true;
        loadkeys_cond_.Signal();
    }

    StopThread();
}

void
PikaCacheLoadThread::Push(const char key_type, std::string &key)
{
    slash::MutexLock lq(&loadkeys_mutex_);
    slash::MutexLock lm(&loadkeys_map_mutex_);

    if (CACHE_LOAD_QUEUE_MAX_SIZE < loadkeys_queue_.size()) {
        // 5s打印一次日志
        static uint64_t last_log_time_us = 0;
        if (slash::NowMicros() - last_log_time_us > 5000000) {
          LOG(WARNING) << "PikaCacheLoadThread::Push waiting...";
          last_log_time_us = slash::NowMicros();
        }
        return;
    }

    if (loadkeys_map_.find(key) == loadkeys_map_.end()) {
        std::pair<const char, std::string> kpair = std::make_pair(key_type, key);
        loadkeys_queue_.push_back(kpair);
        loadkeys_map_[key] = std::string("");
        loadkeys_cond_.Signal();
    }
}

bool
PikaCacheLoadThread::LoadKv(std::string &key)
{
    std::string value;
    int64_t ttl;
    rocksdb::Status s = g_pika_server->db()->GetWithTTL(key, &value, &ttl);
    if (!s.ok()) {
        LOG(WARNING) << "load kv failed, key=" << key;
        return false;
    }

    g_pika_server->Cache()->WriteKvToCache(key, value, ttl);
    return true;
}

bool
PikaCacheLoadThread::LoadHash(std::string &key)
{
    int32_t len = 0;
    g_pika_server->db()->HLen(key, &len);
    if (0 >= len || CACHE_VALUE_ITEM_MAX_SIZE < len) {
        LOG(WARNING) << "can not load key, because item size:" << len << " beyond max item size:" << CACHE_VALUE_ITEM_MAX_SIZE;
        return false;
    }

    std::vector<blackwidow::FieldValue> fvs;
    int64_t ttl;
    rocksdb::Status s = g_pika_server->db()->HGetallWithTTL(key, &fvs, &ttl);
    if (!s.ok()) {
        LOG(WARNING) << "load hash failed, key=" << key;
        return false;
    }

    g_pika_server->Cache()->WriteHashToCache(key, fvs, ttl);
    return true;
}

bool
PikaCacheLoadThread::LoadList(std::string &key)
{
    uint64_t len = 0;
    g_pika_server->db()->LLen(key, &len);
    if (0 >= len || CACHE_VALUE_ITEM_MAX_SIZE < len) {
        LOG(WARNING) << "can not load key, because item size:" << len << " beyond max item size:" << CACHE_VALUE_ITEM_MAX_SIZE;
        return false;
    }

    std::vector<std::string> values;
    int64_t ttl;
    rocksdb::Status s = g_pika_server->db()->LRangeWithTTL(key, 0, -1, &values, &ttl);
    if (!s.ok()) {
        LOG(WARNING) << "load list failed, key=" << key;
        return false;
    }

    g_pika_server->Cache()->WriteListToCache(key, values, ttl);
    return true;
}

bool
PikaCacheLoadThread::LoadSet(std::string &key)
{
    int32_t len = 0;
    g_pika_server->db()->SCard(key, &len);
    if (0 >= len || CACHE_VALUE_ITEM_MAX_SIZE < len) {
        LOG(WARNING) << "can not load key, because item size:" << len << " beyond max item size:" << CACHE_VALUE_ITEM_MAX_SIZE;
        return false;
    }

    std::vector<std::string> values;
    int64_t ttl;
    rocksdb::Status s = g_pika_server->db()->SMembersWithTTL(key, &values, &ttl);
    if (!s.ok()) {
        LOG(WARNING) << "load set failed, key=" << key;
        return false;
    }

    g_pika_server->Cache()->WriteSetToCache(key, values, ttl);
    return true;
}

bool
PikaCacheLoadThread::LoadZset(std::string &key)
{
    int32_t len = 0;
    g_pika_server->db()->ZCard(key, &len);
    if (0 >= len || CACHE_VALUE_ITEM_MAX_SIZE < len) {
        LOG(WARNING) << "can not load key, because item size:" << len << " beyond max item size:" << CACHE_VALUE_ITEM_MAX_SIZE;
        return false;
    }

    std::vector<blackwidow::ScoreMember> score_members;
    int64_t ttl;
    rocksdb::Status s = g_pika_server->db()->ZRangeWithTTL(key, 0, -1, &score_members, &ttl);
    if (!s.ok()) {
        LOG(WARNING) << "load zset failed, key=" << key;
        return false;
    }

    g_pika_server->Cache()->WriteZSetToCache(key, score_members, ttl);
    return true;
}

bool
PikaCacheLoadThread::LoadKey(const char key_type, std::string &key)
{
    // 加载缓存时，保证操作rocksdb和cache是原子的
    slash::ScopeRecordLock l(g_pika_server->LockMgr(), key);
    switch (key_type) {
        case 'k':
            return LoadKv(key);
        case 'h':
            return LoadHash(key);
        case 'l':
            return LoadList(key);
        case 's':
            return LoadSet(key);
        case 'z':
            return LoadZset(key);
        default:
            LOG(WARNING) << "PikaCacheLoadThread::LoadKey invalid key type : " << key_type;
            return false;
    }
}

void*
PikaCacheLoadThread::ThreadMain()
{
    LOG(INFO) << "PikaCacheLoadThread::ThreadMain Start";

    while (!should_exit_) {

        std::deque<std::pair<const char, std::string>> load_keys;
        {
            slash::MutexLock l(&loadkeys_mutex_);
            waitting_load_keys_num_ = loadkeys_queue_.size();
            while (!should_exit_ && 0 >= loadkeys_queue_.size()) {
                loadkeys_cond_.Wait();
            }

            if (should_exit_) {
                return NULL;
            }

            for (int i = 0 ; i <  CACHE_LOAD_NUM_ONE_TIME; ++i) {
                if (!loadkeys_queue_.empty()) {
                    load_keys.push_back(loadkeys_queue_.front());
                    loadkeys_queue_.pop_front();
                }
            }
        }

        for (auto iter = load_keys.begin(); iter != load_keys.end(); ++iter) {
            if (LoadKey(iter->first, iter->second)) {
                ++async_load_keys_num_;
            } else {
                LOG(WARNING) << "PikaCacheLoadThread::ThreadMain LoadKey: " << iter->second << " failed !!!";
            }

            slash::MutexLock lm(&loadkeys_map_mutex_);
            loadkeys_map_.erase(iter->second);
        }
    }

    return NULL;
}