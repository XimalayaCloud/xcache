#ifndef __REDIS_DEF_H__
#define __REDIS_DEF_H__

#include "../../include/pika_define.h"

namespace dory {

/* Redis maxmemory strategies */
#define CACHE_VOLATILE_LRU      0
#define CACHE_ALLKEYS_LRU       1
#define CACHE_VOLATILE_LFU      2
#define CACHE_ALLKEYS_LFU       3
#define CACHE_VOLATILE_RANDOM   4
#define CACHE_ALLKEYS_RANDOM    5
#define CACHE_VOLATILE_TTL      6
#define CACHE_NO_EVICTION       7

#define CACHE_DEFAULT_MAXMEMORY (10 * 1024 * 1024 * 1024LL)     // 10G
#define CACHE_DEFAULT_MAXMEMORY_SAMPLES 5
#define CACHE_DEFAULT_LFU_DECAY_TIME 1

struct CacheConfig {
    unsigned long long maxmemory;       /* Can used max memory */
    int maxmemory_policy;               /* Policy for key eviction */
    int maxmemory_samples;              /* Pricision of random sampling */
    int lfu_decay_time;                 /* LFU counter decay factor. */
    int cache_start_pos;
    int cache_items_per_key;

    CacheConfig() 
    	: maxmemory(CACHE_DEFAULT_MAXMEMORY)
    	, maxmemory_policy(CACHE_NO_EVICTION)
    	, maxmemory_samples(CACHE_DEFAULT_MAXMEMORY_SAMPLES)
        , lfu_decay_time(CACHE_DEFAULT_LFU_DECAY_TIME)
        , cache_start_pos(CACHE_START_FROM_BEGIN)
        , cache_items_per_key(DEFAULT_CACHE_ITEMS_PER_KEY)
    {

    }

    CacheConfig& operator=(const CacheConfig& obj) {
    	maxmemory = obj.maxmemory;
    	maxmemory_policy = obj.maxmemory_policy;
    	maxmemory_samples = obj.maxmemory_samples;
        lfu_decay_time = obj.lfu_decay_time;
        cache_start_pos = obj.cache_start_pos;
        cache_items_per_key = obj.cache_items_per_key;
        return *this;
    }
};

} // namespace dory

#endif
