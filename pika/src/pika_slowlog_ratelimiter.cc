#include <glog/logging.h>

#include "slash/include/env.h"

#include "pika_slowlog_ratelimiter.h"



PikaSlowLogRateLimiter::PikaSlowLogRateLimiter(int64_t capacity, int64_t every)
    : capacity_(capacity)
    , every_(every)
    , tokens_(capacity)
{
    last_refill_us_ = slash::NowMicros();
}

PikaSlowLogRateLimiter::~PikaSlowLogRateLimiter()
{

}

bool
PikaSlowLogRateLimiter::RequestToken() {
    slash::MutexLock ml(&mutex_);
    if (0 == capacity_) {
        return true;
    }

    CheckRefill();

    if (tokens_ > 0) {
        --tokens_;
        return true;
    }

    return false;
}

void
PikaSlowLogRateLimiter::SetSlowlogTokenCapacity(const int64_t value) {
    slash::MutexLock ml(&mutex_);
    capacity_ = value;
}

void
PikaSlowLogRateLimiter::SetSlowlogTokenFillEvery(const int64_t value) {
    slash::MutexLock ml(&mutex_);
    every_ = value;
}

void
PikaSlowLogRateLimiter::CheckRefill() {
    int64_t delta_us = slash::NowMicros() - last_refill_us_;
    int64_t fill_tokens = every_ * delta_us / (1000 * 1000);
    // LOG(ERROR) << "PikaSlowLogRateLimiter::CheckRefill every:" << every_
    //                                                    << " capacity:" << capacity_
    //                                                    << " tokens:" << tokens_
    //                                                    << " delta_us:" << delta_us
    //                                                    << " fill_tokens:" << fill_tokens;
    if (0 < fill_tokens) {
        tokens_ = (tokens_ + fill_tokens > capacity_) ? capacity_ : tokens_ + fill_tokens;
        last_refill_us_ = slash::NowMicros();
    }
}

