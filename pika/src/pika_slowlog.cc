#include "pika_slowlog.h"
#include "pika_conf.h"

#define SLOWLOG_ENTRY_MAX_ARGC      32
#define SLOWLOG_ENTRY_MAX_STRING    128

extern PikaConf *g_pika_conf;


PikaSlowlog::PikaSlowlog()
    : entry_id_(0)
{

}

PikaSlowlog::~PikaSlowlog()
{
    slash::MutexLock lm(&slowlog_mutex_);
    slowlog_list_.clear();
}

void
PikaSlowlog::Push(const PikaCmdArgsType& argv, int32_t time, int64_t duration)
{
    SlowlogEntry entry;
    uint32_t slargc = (argv.size() < SLOWLOG_ENTRY_MAX_ARGC) ? argv.size() : SLOWLOG_ENTRY_MAX_ARGC;

    for (uint32_t idx = 0; idx < slargc; ++idx) {
        if (slargc != argv.size() && idx == slargc - 1) {
            char buffer[32];
            sprintf(buffer, "... (%lu more arguments)", argv.size() - slargc + 1);
            entry.argv.push_back(std::string(buffer));
        } else {
            if (argv[idx].size() > SLOWLOG_ENTRY_MAX_STRING) {
                char buffer[32];
                sprintf(buffer, "... (%lu more bytes)", argv[idx].size() - SLOWLOG_ENTRY_MAX_STRING);
                std::string suffix(buffer);
                std::string brief = argv[idx].substr(0, SLOWLOG_ENTRY_MAX_STRING);
                entry.argv.push_back(brief + suffix);
            } else {
                entry.argv.push_back(argv[idx]);
            }
        }
    }

    if (0 == slowlog_mutex_.Trylock()) {
        entry.id = entry_id_++;
        entry.start_time = time;
        entry.duration = duration;
        slowlog_list_.push_front(entry);
        uint32_t slowlog_max_len = static_cast<uint32_t>(g_pika_conf->slowlog_max_len());
        while (slowlog_list_.size() > slowlog_max_len) {
            slowlog_list_.pop_back();
        }
        slowlog_mutex_.Unlock();
    }
}

void
PikaSlowlog::GetInfo(uint32_t number, std::vector<SlowlogEntry> *slowlogs)
{
    slash::MutexLock lm(&slowlog_mutex_);

    slowlogs->clear();
    std::list<SlowlogEntry>::const_iterator iter = slowlog_list_.begin();
    while (number-- && iter != slowlog_list_.end()) {
        slowlogs->push_back(*iter);
        iter++;
    }
}

void
PikaSlowlog::Trim(void)
{
    slash::MutexLock lm(&slowlog_mutex_);
    
    uint32_t slowlog_max_len = static_cast<uint32_t>(g_pika_conf->slowlog_max_len());
    while (slowlog_list_.size() > slowlog_max_len) {
        slowlog_list_.pop_back();
    }
}

void
PikaSlowlog::Reset(void)
{
    slash::MutexLock lm(&slowlog_mutex_);

    slowlog_list_.clear();
}

uint32_t
PikaSlowlog::Len(void)
{
    slash::MutexLock lm(&slowlog_mutex_);

    return slowlog_list_.size();
}

/* EOF */