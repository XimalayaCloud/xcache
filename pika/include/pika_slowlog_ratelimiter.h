#ifndef PIKA_SLOWLOG_RATELIMITER_H_
#define PIKA_SLOWLOG_RATELIMITER_H_

#include "slash/include/slash_mutex.h"

class PikaSlowLogRateLimiter
{
public:
    PikaSlowLogRateLimiter(int64_t capacity,   // 令牌桶容量
                           int64_t every);     // 每秒填充令牌数
    ~PikaSlowLogRateLimiter();

    bool RequestToken();
    void SetSlowlogTokenCapacity(const int64_t value);
    void SetSlowlogTokenFillEvery(const int64_t value);

private:
    void CheckRefill();

    int64_t capacity_;
    int64_t every_;
    int64_t tokens_;
    uint64_t last_refill_us_;
    slash::Mutex mutex_;
};

#endif
