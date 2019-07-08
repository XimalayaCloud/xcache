#ifndef PIKA_SLOWLOG_H_
#define PIKA_SLOWLOG_H_

#include <list>

#include "slash/include/slash_mutex.h"

#include "pika_command.h"


struct SlowlogEntry {
    int64_t id;
    int64_t start_time;
    int64_t duration;
    PikaCmdArgsType argv;
};

class PikaSlowlog
{
public:
    PikaSlowlog();
    ~PikaSlowlog();

    void Push(const PikaCmdArgsType& argv, int32_t time, int64_t duration);
    void GetInfo(uint32_t number, std::vector<SlowlogEntry> *slowlogs);
    void Trim(void);
    void Reset(void);
    uint32_t Len(void);
    
private:
    PikaSlowlog(const PikaSlowlog&);
    PikaSlowlog& operator=(const PikaSlowlog&);

private:
    std::atomic<uint64_t> entry_id_;
    std::list<SlowlogEntry> slowlog_list_;
    slash::Mutex slowlog_mutex_;
};

#endif

/* EOF */