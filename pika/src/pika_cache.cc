#include <ctime>
#include <glog/logging.h>

#include "pika_cache.h"
#include "pika_commonfunc.h"
#include "pika_define.h"
#include "pika_cache_load_thread.h"


PikaCache::PikaCache()
    : cache_status_(PIKA_CACHE_STATUS_NONE)
    , cache_num_(0)
    , cache_load_thread_(NULL)
{
    pthread_rwlock_init(&rwlock_, NULL);

    cache_load_thread_ = new PikaCacheLoadThread();
    cache_load_thread_->StartThread();
}

PikaCache::~PikaCache()
{
    delete cache_load_thread_;

    {
        slash::RWLock l(&rwlock_, true);
        DestroyWithoutLock();
    }
    pthread_rwlock_destroy(&rwlock_);
}

Status
PikaCache::Init(uint32_t cache_num, dory::CacheConfig *cache_cfg)
{
    slash::RWLock l(&rwlock_, true);

    if (NULL == cache_cfg) {
        return Status::Corruption("invalid arguments !!!");
    }
    return InitWithoutLock(cache_num, cache_cfg);
}

Status
PikaCache::Reset(uint32_t cache_num, dory::CacheConfig *cache_cfg)
{
    slash::RWLock l(&rwlock_, true);

    DestroyWithoutLock();
    return InitWithoutLock(cache_num, cache_cfg);
}

void
PikaCache::ResetConfig(dory::CacheConfig *cache_cfg)
{
    slash::RWLock l(&rwlock_, false);
    dory::RedisCache::SetConfig(cache_cfg);
}

void
PikaCache::Destroy(void)
{
    slash::RWLock l(&rwlock_, true);
    DestroyWithoutLock();
}

void
PikaCache::ProcessCronTask(void)
{
    slash::RWLock l(&rwlock_, false);
    for (uint32_t i = 0; i < caches_.size(); ++i) {
        slash::MutexLock lm(cache_mutexs_[i]);
        caches_[i]->ActiveExpireCycle();
    }
}

void
PikaCache::SetCacheStatus(int status)
{
    cache_status_ = status;
}

int
PikaCache::CacheStatus(void)
{
    return cache_status_;
}

/*-----------------------------------------------------------------------------
 * Normal Commands
 *----------------------------------------------------------------------------*/
void
PikaCache::Info(CacheInfo &info)
{
    info.clear();
    slash::RWLock l(&rwlock_, false);
    info.status = cache_status_;
    info.cache_num = cache_num_;
    info.used_memory = dory::RedisCache::GetUsedMemory();
    info.async_load_keys_num = cache_load_thread_->AsyncLoadKeysNum();
    info.waitting_load_keys_num = cache_load_thread_->WaittingLoadKeysNum();
    dory::RedisCache::GetHitAndMissNum(&info.hits, &info.misses);
    for (uint32_t i = 0; i < caches_.size(); ++i) {
        slash::MutexLock lm(cache_mutexs_[i]);
        info.keys_num += caches_[i]->DbSize();
    }
}

bool
PikaCache::Exists(std::string &key)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->Exists(key);
}

void
PikaCache::FlushDb(void)
{
    slash::RWLock l(&rwlock_, false);
    for (uint32_t i = 0; i < caches_.size(); ++i) {
        slash::MutexLock lm(cache_mutexs_[i]);
        caches_[i]->FlushDb();
    }
}

 double
 PikaCache::HitRatio(void)
 {
    slash::RWLock l(&rwlock_, false);

    long long hits = 0;
    long long misses = 0;
    dory::RedisCache::GetHitAndMissNum(&hits, &misses);
    long long all_cmds = hits + misses;
    if (0 >= all_cmds) {
        return 0;
    }

    return hits / (all_cmds * 1.0);
 }

void
PikaCache::ClearHitRatio(void)
{
    slash::RWLock l(&rwlock_, false);
    dory::RedisCache::ResetHitAndMissNum();
}

Status
PikaCache::Del(std::string &key)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->Del(key);
}

Status
PikaCache::Expire(std::string &key, int64_t ttl)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->Expire(key, ttl);
}

Status
PikaCache::Expireat(std::string &key, int64_t ttl)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->Expireat(key, ttl);
}

Status
PikaCache::TTL(std::string &key, int64_t *ttl)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->TTL(key, ttl);
}

Status
PikaCache::Persist(std::string &key)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->Persist(key);
}

Status
PikaCache::Type(std::string &key, std::string *value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->Type(key, value);
}

Status
PikaCache::RandomKey(std::string *key)
{
    slash::RWLock l(&rwlock_, false);

    Status s;
    srand((unsigned)time(NULL));
    int cache_index = rand() % caches_.size();
    for (unsigned int i = 0; i < caches_.size(); ++i) {
        cache_index = (cache_index + i) % caches_.size();

        slash::MutexLock lm(cache_mutexs_[cache_index]);
        s = caches_[cache_index]->RandomKey(key);
        if (s.ok()) {
            break;
        }
    }
    return s;
}

/*-----------------------------------------------------------------------------
 * String Commands
 *----------------------------------------------------------------------------*/
Status
PikaCache::Set(std::string &key, std::string &value, int64_t ttl)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->Set(key, value, ttl);
}

Status
PikaCache::Setnx(std::string &key, std::string &value, int64_t ttl)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->Setnx(key, value, ttl);
}

Status
PikaCache::SetnxWithoutTTL(std::string &key, std::string &value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->SetnxWithoutTTL(key, value);
}

Status
PikaCache::Setxx(std::string &key, std::string &value, int64_t ttl)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->Setxx(key, value, ttl);
}

Status
PikaCache::SetxxWithoutTTL(std::string &key, std::string &value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->SetxxWithoutTTL(key, value);
}

Status
PikaCache::Get(std::string &key, std::string *value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->Get(key, value);
}

Status
PikaCache::Incrxx(std::string &key)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (caches_[cache_index]->Exists(key)) {
        return caches_[cache_index]->Incr(key);
    }
    return Status::NotFound("key not exist");
}

Status
PikaCache::Decrxx(std::string &key)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (caches_[cache_index]->Exists(key)) {
        return caches_[cache_index]->Decr(key);
    }
    return Status::NotFound("key not exist");
}

Status
PikaCache::IncrByxx(std::string &key, long long incr)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (caches_[cache_index]->Exists(key)) {
        return caches_[cache_index]->IncrBy(key, incr);
    }
    return Status::NotFound("key not exist");
}

Status
PikaCache::DecrByxx(std::string &key, long long incr)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (caches_[cache_index]->Exists(key)) {
        return caches_[cache_index]->DecrBy(key, incr);
    }
    return Status::NotFound("key not exist");
}

Status
PikaCache::Incrbyfloatxx(std::string &key, long double incr)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (caches_[cache_index]->Exists(key)) {
        return caches_[cache_index]->Incrbyfloat(key, incr);
    }
    return Status::NotFound("key not exist");
}

Status
PikaCache::Appendxx(std::string &key, std::string &value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (caches_[cache_index]->Exists(key)) {
        return caches_[cache_index]->Append(key, value);
    }
    return Status::NotFound("key not exist");
}

Status
PikaCache::GetRange(std::string &key, int64_t start, int64_t end, std::string *value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->GetRange(key, start, end, value);
}

Status
PikaCache::SetRangexx(std::string &key, int64_t start, std::string &value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (caches_[cache_index]->Exists(key)) {
        return caches_[cache_index]->SetRange(key, start, value);
    }
    return Status::NotFound("key not exist");
}

Status
PikaCache::Strlen(std::string &key, int32_t *len)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->Strlen(key, len); 
}

/*-----------------------------------------------------------------------------
 * Hash Commands
 *----------------------------------------------------------------------------*/
Status
PikaCache::HDel(std::string& key, std::vector<std::string> &fields)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->HDel(key, fields);
}

Status
PikaCache::HSet(std::string &key, std::string &field, std::string &value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->HSet(key, field, value); 
}

Status
PikaCache::HSetIfKeyExist(std::string &key, std::string &field, std::string &value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (caches_[cache_index]->Exists(key)) {
        return caches_[cache_index]->HSet(key, field, value);
    }
    return Status::NotFound("key not exist"); 
}

Status
PikaCache::HSetIfKeyExistAndFieldNotExist(std::string &key, std::string &field, std::string &value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (caches_[cache_index]->Exists(key)) {
        return caches_[cache_index]->HSetnx(key, field, value);
    }
    return Status::NotFound("key not exist"); 
}

Status
PikaCache::HMSet(std::string &key, std::vector<blackwidow::FieldValue> &fvs)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->HMSet(key, fvs); 
}

Status
PikaCache::HMSetnx(std::string &key, std::vector<blackwidow::FieldValue> &fvs, int64_t ttl)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (!caches_[cache_index]->Exists(key)) {
        caches_[cache_index]->HMSet(key, fvs);
        caches_[cache_index]->Expire(key, ttl);
        return Status::OK();
    } else {
        return Status::NotFound("key exist");
    }
}

Status
PikaCache::HMSetnxWithoutTTL(std::string &key, std::vector<blackwidow::FieldValue> &fvs)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (!caches_[cache_index]->Exists(key)) {
        caches_[cache_index]->HMSet(key, fvs);
        return Status::OK();
    } else {
        return Status::NotFound("key exist");
    }
}

Status
PikaCache::HMSetxx(std::string &key, std::vector<blackwidow::FieldValue> &fvs)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (caches_[cache_index]->Exists(key)) {
        return caches_[cache_index]->HMSet(key, fvs);
    } else {
        return Status::NotFound("key not exist");
    }   
}

Status
PikaCache::HGet(std::string &key, std::string &field, std::string *value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->HGet(key, field, value);
}

Status
PikaCache::HMGet(std::string &key,
                 std::vector<std::string> &fields,
                 std::vector<blackwidow::ValueStatus> *vss)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->HMGet(key, fields, vss);
}

Status
PikaCache::HGetall(std::string &key, std::vector<blackwidow::FieldValue> *fvs)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->HGetall(key, fvs);
}

Status
PikaCache::HKeys(std::string &key, std::vector<std::string> *fields)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->HKeys(key, fields);
}

Status
PikaCache::HVals(std::string &key, std::vector<std::string> *values)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->HVals(key, values);
}

Status
PikaCache::HExists(std::string &key, std::string &field)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->HExists(key, field);
}

Status
PikaCache::HIncrbyxx(std::string &key, std::string &field, int64_t value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (caches_[cache_index]->Exists(key)) {
        return caches_[cache_index]->HIncrby(key, field, value);
    }
    return Status::NotFound("key not exist"); 
}

Status
PikaCache::HIncrbyfloatxx(std::string &key, std::string &field, long double value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (caches_[cache_index]->Exists(key)) {
        return caches_[cache_index]->HIncrbyfloat(key, field, value);
    }
    return Status::NotFound("key not exist"); 
}

Status
PikaCache::HLen(std::string &key, unsigned long *len)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->HLen(key, len);
}

Status
PikaCache::HStrlen(std::string &key, std::string &field, unsigned long *len)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->HStrlen(key, field, len);
}

/*-----------------------------------------------------------------------------
 * List Commands
 *----------------------------------------------------------------------------*/
Status
PikaCache::LIndex(std::string &key, long index, std::string *element)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->LIndex(key, index, element);
}

Status
PikaCache::LInsert(std::string &key,
                   blackwidow::BeforeOrAfter &before_or_after,
                   std::string &pivot,
                   std::string &value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->LInsert(key, before_or_after, pivot, value);
}

Status
PikaCache::LLen(std::string &key, unsigned long *len)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->LLen(key, len);
}

Status
PikaCache::LPop(std::string &key, std::string *element)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->LPop(key, element);
}

Status
PikaCache::LPush(std::string &key, std::vector<std::string> &values)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->LPush(key, values);
}

Status
PikaCache::LPushx(std::string &key, std::vector<std::string> &values)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->LPushx(key, values);
}

Status
PikaCache::LRange(std::string &key, long start, long stop, std::vector<std::string> *values)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->LRange(key, start, stop, values);
}

Status
PikaCache::LRem(std::string &key, long count, std::string &value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->LRem(key, count, value);
}

Status
PikaCache::LSet(std::string &key, long index, std::string &value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->LSet(key, index, value);
}

Status
PikaCache::LTrim(std::string &key, long start, long stop)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->LTrim(key, start, stop);
}

Status
PikaCache::RPop(std::string &key, std::string *element)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->RPop(key, element);
}

Status
PikaCache::RPush(std::string &key, std::vector<std::string> &values)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->RPush(key, values);
}

Status
PikaCache::RPushx(std::string &key, std::vector<std::string> &values)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->RPushx(key, values);
}

Status
PikaCache::RPushnx(std::string &key, std::vector<std::string> &values, int64_t ttl)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (!caches_[cache_index]->Exists(key)) {
        caches_[cache_index]->RPush(key, values);
        caches_[cache_index]->Expire(key, ttl);
        return Status::OK();
    } else {
        return Status::NotFound("key exist");
    }
}

Status
PikaCache::RPushnxWithoutTTL(std::string &key, std::vector<std::string> &values)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (!caches_[cache_index]->Exists(key)) {
        caches_[cache_index]->RPush(key, values);
        return Status::OK();
    } else {
        return Status::NotFound("key exist");
    }
}

/*-----------------------------------------------------------------------------
 * Set Commands
 *----------------------------------------------------------------------------*/
Status
PikaCache::SAdd(std::string &key, std::vector<std::string> &members)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->SAdd(key, members);
}

Status
PikaCache::SAddIfKeyExist(std::string &key, std::vector<std::string> &members)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (caches_[cache_index]->Exists(key)) {
        return caches_[cache_index]->SAdd(key, members);
    }
    return Status::NotFound("key not exist"); 
}

Status
PikaCache::SAddnx(std::string &key, std::vector<std::string> &members, int64_t ttl)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (!caches_[cache_index]->Exists(key)) {
        caches_[cache_index]->SAdd(key, members);
        caches_[cache_index]->Expire(key, ttl);
        return Status::OK();
    } else {
        return Status::NotFound("key exist");
    }
}

Status
PikaCache::SAddnxWithoutTTL(std::string &key, std::vector<std::string> &members)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (!caches_[cache_index]->Exists(key)) {
        caches_[cache_index]->SAdd(key, members);
        return Status::OK();
    } else {
        return Status::NotFound("key exist");
    }
}

Status
PikaCache::SCard(std::string &key, unsigned long *len)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->SCard(key, len);
}

Status
PikaCache::SIsmember(std::string &key, std::string &member)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->SIsmember(key, member);
}

Status
PikaCache::SMembers(std::string &key, std::vector<std::string> *members)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->SMembers(key, members);
}

Status
PikaCache::SRem(std::string &key, std::vector<std::string> &members)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->SRem(key, members);
}

Status
PikaCache::SRandmember(std::string &key, long count, std::vector<std::string> *members)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->SRandmember(key, count, members);
}

/*-----------------------------------------------------------------------------
 * ZSet Commands
 *----------------------------------------------------------------------------*/
Status
PikaCache::ZAdd(std::string &key, std::vector<blackwidow::ScoreMember> &score_members)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->ZAdd(key, score_members);
}

Status
PikaCache::ZAddIfKeyExist(std::string &key, std::vector<blackwidow::ScoreMember> &score_members)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (caches_[cache_index]->Exists(key)) {
        return caches_[cache_index]->ZAdd(key, score_members);
    }
    return Status::NotFound("key not exist"); 
}

Status
PikaCache::ZAddnx(std::string &key, std::vector<blackwidow::ScoreMember> &score_members, int64_t ttl)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (!caches_[cache_index]->Exists(key)) {
        caches_[cache_index]->ZAdd(key, score_members);
        caches_[cache_index]->Expire(key, ttl);
        return Status::OK();
    } else {
        return Status::NotFound("key exist");
    }
}

Status
PikaCache::ZAddnxWithoutTTL(std::string &key, std::vector<blackwidow::ScoreMember> &score_members)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (!caches_[cache_index]->Exists(key)) {
        caches_[cache_index]->ZAdd(key, score_members);
        return Status::OK();
    } else {
        return Status::NotFound("key exist");
    }
}

Status
PikaCache::ZCard(std::string &key, unsigned long *len)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->ZCard(key, len);
}

Status
PikaCache::ZCount(std::string &key, std::string &min, std::string &max, unsigned long *len)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->ZCount(key, min, max, len);
}

Status
PikaCache::ZIncrby(std::string &key, std::string &member, double increment)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->ZIncrby(key, member, increment);
}

Status
PikaCache::ZIncrbyIfKeyExist(std::string &key, std::string &member, double increment)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (caches_[cache_index]->Exists(key)) {
        return caches_[cache_index]->ZIncrby(key, member, increment);
    }
    return Status::NotFound("key not exist");
}

Status
PikaCache::ZRange(std::string &key,
                  long start, long stop,
                  std::vector<blackwidow::ScoreMember> *score_members)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->ZRange(key, start, stop, score_members);
}

Status
PikaCache::ZRangebyscore(std::string &key,
                         std::string &min, std::string &max,
                         std::vector<blackwidow::ScoreMember> *score_members)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->ZRangebyscore(key, min, max, score_members);
}

Status
PikaCache::ZRank(std::string &key, std::string &member, long *rank)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->ZRank(key, member, rank);
}

Status
PikaCache::ZRem(std::string &key, std::vector<std::string> &members)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->ZRem(key, members);
}

Status
PikaCache::ZRemrangebyrank(std::string &key, std::string &min, std::string &max)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->ZRemrangebyrank(key, min, max);
}

Status
PikaCache::ZRemrangebyscore(std::string &key, std::string &min, std::string &max)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->ZRemrangebyscore(key, min, max);
}

Status
PikaCache::ZRevrange(std::string &key,
                     long start, long stop,
                     std::vector<blackwidow::ScoreMember> *score_members)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->ZRevrange(key, start, stop, score_members);
}

Status
PikaCache::ZRevrangebyscore(std::string &key,
                            std::string &min, std::string &max,
                            std::vector<blackwidow::ScoreMember> *score_members)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->ZRevrangebyscore(key, min, max, score_members);
}

Status
PikaCache::ZRevrangebylex(std::string &key,
                          std::string &min, std::string &max,
                          std::vector<std::string> *members)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->ZRevrangebylex(key, min, max, members);
}

Status
PikaCache::ZRevrank(std::string &key, std::string &member, long *rank)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->ZRevrank(key, member, rank);
}

Status
PikaCache::ZScore(std::string &key, std::string &member, double *score)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->ZScore(key, member, score);
}

Status
PikaCache::ZRangebylex(std::string &key,
                       std::string &min, std::string &max,
                       std::vector<std::string> *members)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->ZRangebylex(key, min, max, members);
}

Status
PikaCache::ZLexcount(std::string &key, std::string &min, std::string &max, unsigned long *len)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->ZLexcount(key, min, max, len);
}

Status
PikaCache::ZRemrangebylex(std::string &key, std::string &min, std::string &max)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->ZRemrangebylex(key, min, max);
}


/*-----------------------------------------------------------------------------
 * Bit Commands
 *----------------------------------------------------------------------------*/
Status
PikaCache::SetBit(std::string &key, size_t offset, long value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->SetBit(key, offset, value);
}

Status
PikaCache::SetBitIfKeyExist(std::string &key, size_t offset, long value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    if (caches_[cache_index]->Exists(key)) {
        return caches_[cache_index]->SetBit(key, offset, value);
    }
    return Status::NotFound("key not exist"); 
}

Status
PikaCache::GetBit(std::string &key, size_t offset, long *value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->GetBit(key, offset, value);
}

Status
PikaCache::BitCount(std::string &key, long start, long end, long *value, bool have_offset)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->BitCount(key, start, end, value, have_offset);
}

Status
PikaCache::BitPos(std::string &key, long bit, long *value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->BitPos(key, bit, value);
}

Status
PikaCache::BitPos(std::string &key, long bit, long start, long *value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->BitPos(key, bit, start, value);
}

Status
PikaCache::BitPos(std::string &key, long bit, long start, long end, long *value)
{
    slash::RWLock l(&rwlock_, false);

    int cache_index = CacheIndex(key);
    slash::MutexLock lm(cache_mutexs_[cache_index]);
    return caches_[cache_index]->BitPos(key, bit, start, end, value);
}

Status
PikaCache::InitWithoutLock(uint32_t cache_num, dory::CacheConfig *cache_cfg)
{
    cache_status_ = PIKA_CACHE_STATUS_INIT;

    cache_num_ = cache_num;
    if (NULL != cache_cfg) {
        dory::RedisCache::SetConfig(cache_cfg);
    }

    for (uint32_t i = 0; i < cache_num; ++i) {
        dory::RedisCache *cache = new dory::RedisCache();
        Status s = cache->Open();
        if (!s.ok()) {
            LOG(ERROR) << "PikaCache::InitWithoutLock Open cache failed";
            DestroyWithoutLock();
            cache_status_ = PIKA_CACHE_STATUS_NONE;
            return Status::Corruption("create redis cache failed");
        }
        caches_.push_back(cache);
        cache_mutexs_.push_back(new slash::Mutex());
    }
    cache_status_ = PIKA_CACHE_STATUS_OK;

    return Status::OK();
}

void
PikaCache::DestroyWithoutLock(void)
{
    cache_status_ = PIKA_CACHE_STATUS_DESTROY;

    for (auto iter = caches_.begin(); iter != caches_.end(); ++iter) {
        delete *iter;
    }
    caches_.clear();

    for (auto iter = cache_mutexs_.begin(); iter != cache_mutexs_.end(); ++iter) {
        delete *iter;
    }
    cache_mutexs_.clear();
}

int
PikaCache::CacheIndex(const std::string &key)
{
    uint32_t crc = PikaCommonFunc::CRC32Update(0, key.data(), (int)key.size());
    return (int)(crc % caches_.size());
}

Status
PikaCache::WriteKvToCache(std::string &key, std::string &value, int64_t ttl)
{
    if (0 >= ttl) {
        if (PIKA_TTL_NONE == ttl) {
            return SetnxWithoutTTL(key, value);
        } else {
            return Del(key);
        }
    } else {
        return Setnx(key, value, ttl);
    }
    return Status::OK();
}

Status
PikaCache::WriteHashToCache(std::string &key, std::vector<blackwidow::FieldValue> &fvs, int64_t ttl)
{
    if (0 >= ttl) {
        if (PIKA_TTL_NONE == ttl) {
            return HMSetnxWithoutTTL(key, fvs);
        } else {
            return Del(key);
        }
    } else {
        return HMSetnx(key, fvs, ttl);
    }
    return Status::OK();
}

Status
PikaCache::WriteListToCache(std::string &key, std::vector<std::string> &values, int64_t ttl)
{
    if (0 >= ttl) {
        if (PIKA_TTL_NONE == ttl) {
            return RPushnxWithoutTTL(key, values);
        } else {
            return Del(key);
        }
    } else {
        return RPushnx(key, values, ttl);
    }
    return Status::OK();
}

Status
PikaCache::WriteSetToCache(std::string &key, std::vector<std::string> &members, int64_t ttl)
{
    if (0 >= ttl) {
        if (PIKA_TTL_NONE == ttl) {
            return SAddnxWithoutTTL(key, members);
        } else {
            return Del(key);
        }
    } else {
        return SAddnx(key, members, ttl);
    }
    return Status::OK();
}

Status
PikaCache::WriteZSetToCache(std::string &key, std::vector<blackwidow::ScoreMember> &score_members, int64_t ttl)
{
    if (0 >= ttl) {
        if (PIKA_TTL_NONE == ttl) {
            return ZAddnxWithoutTTL(key, score_members);
        } else {
            return Del(key);
        }
    } else {
        return ZAddnx(key, score_members, ttl);
    }
    return Status::OK();
}

void
PikaCache::PushKeyToAsyncLoadQueue(const char key_type, std::string &key)
{
    cache_load_thread_->Push(key_type, key);
}

