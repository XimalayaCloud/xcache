#ifndef PIKA_MIGRATE_THREAD_H_
#define PIKA_MIGRATE_THREAD_H_

#include <string>
#include <atomic>
#include <vector>
#include <deque>

#include "pink/include/pink_thread.h"
#include "pink/include/pink_cli.h"
#include "slash/include/slash_mutex.h"
#include "pika_client_conn.h"
#include "blackwidow/blackwidow.h"


class PikaMigrateThread;
class PikaParseSendThread : public pink::Thread
{
public:
    PikaParseSendThread(PikaMigrateThread *migrate_thread);
    ~PikaParseSendThread();

    bool Init(const std::string &ip, int64_t port, int64_t timeout_ms, int64_t mgrtkeys_num);
    void ExitThread(void);

private:
    int MigrateOneKey(const char key_type, const std::string key);
    void DelKeysAndWriteBinlog(std::deque<std::pair<const char, std::string>> &send_keys);
    bool CheckMigrateRecv(int64_t need_receive_num);
    virtual void* ThreadMain();

private:
    std::string dest_ip_;
    int64_t dest_port_;
    int64_t timeout_ms_;
    int32_t mgrtkeys_num_;
    std::atomic<bool> should_exit_;
    PikaMigrateThread *migrate_thread_;
    pink::PinkCli *cli_;
    slash::Mutex working_mutex_;
};


class PikaMigrateThread : public pink::Thread
{
public:
    PikaMigrateThread();
    ~PikaMigrateThread();

    void ReqMigrateBatch(const std::string &ip,
                         int64_t port,
                         int64_t time_out,
                         int64_t slot,
                         int64_t keys_num);

    int ReqMigrateOne(const std::string &key);

    void GetMigrateStatus(std::string *ip,
                          int64_t *port,
                          int64_t *slot,
                          bool *migrating,
                          int64_t *moved,
                          int64_t *remained);

    void CancelMigrate(void);

    void IncWorkingThreadNum(void);
    void DecWorkingThreadNum(void);
    void TaskFailed(void);
    void AddResponseNum(int32_t response_num);

private:
    void ResetThread(void);
    void DestroyThread(bool is_self_exit);
    void NotifyRequestMigrate(void);
    bool IsMigrating(std::pair<const char, std::string> &kpair);
    void ReadSlotKeys(const std::string &slotKey, int64_t need_read_num, int64_t &real_read_num, int32_t *finish);
    bool CreateParseSendThreads(int32_t dispatch_num);
    void DestroyParseSendThreads(void);
    virtual void* ThreadMain();

private:
    std::string dest_ip_;
    int64_t dest_port_;
    int64_t timeout_ms_;
    int64_t slot_num_;
    int64_t keys_num_;
    std::atomic<bool> is_migrating_;
    std::atomic<bool> should_exit_;
    std::atomic<bool> is_task_success_;
    std::atomic<int32_t> send_num_;
    std::atomic<int32_t> response_num_;
    std::atomic<int64_t> moved_num_;

    bool request_migrate_;
    slash::CondVar request_migrate_cond_;
    slash::Mutex request_migrate_mutex_;

    int32_t workers_num_;
    std::vector<PikaParseSendThread*> workers_;

    std::atomic<int32_t> working_thread_num_;
    slash::CondVar workers_cond_;
    slash::Mutex workers_mutex_;

    std::deque<std::pair<const char, std::string>> mgrtone_queue_;
    slash::Mutex mgrtone_queue_mutex_;

    int64_t cursor_;
    std::deque<std::pair<const char, std::string>> mgrtkeys_queue_;
    slash::CondVar mgrtkeys_cond_;
    slash::Mutex mgrtkeys_queue_mutex_;

    std::map<std::pair<const char, std::string>, std::string> mgrtkeys_map_;
    slash::Mutex mgrtkeys_map_mutex_;

    slash::Mutex migrator_mutex_;

    friend class PikaParseSendThread;
};

#endif

/* EOF */