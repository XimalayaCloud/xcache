#include <glog/logging.h>

#include "pika_migrate_thread.h"
#include "pika_commonfunc.h"
#include "pika_server.h"
#include "pika_conf.h"
#include "pika_slot.h"
#include "pika_define.h"


#define min(a, b)  (((a) > (b)) ? (b) : (a))

const int32_t MAX_MEMBERS_NUM = 512;
const std::string INVALID_STR = "NL";

extern PikaServer *g_pika_server;
extern PikaConf *g_pika_conf;


// delete key from cache, slot and db
static int KeyDelete(char key_type, std::string key)
{
    int32_t res = 0;
    std::string slotKey = GetSlotsSlotKey(SlotNum(key));

    // delete from cache
    if (PIKA_CACHE_NONE != g_pika_conf->cache_model()
        && PIKA_CACHE_STATUS_OK == g_pika_server->CacheStatus()) {
        g_pika_server->Cache()->Del(key);
    }

    // delete key from slot
    std::vector<std::string> members;
    members.push_back(key_type + key);
    rocksdb::Status s = g_pika_server->db()->SRem(slotKey, members, &res);
    if (!s.ok()) {
        if (s.IsNotFound()) {
            LOG(INFO) << "Del key Srem key " << key << " not found";
            return 0;
        }
        else {
            LOG(WARNING) << "Del key Srem key: " << key << " from slotKey, error: " << strerror(errno);
            return -1;
        }
    }

    // delete key from db
    members.clear();
    members.push_back(key);
    std::map<blackwidow::DataType, blackwidow::Status> type_status;
    int64_t del_nums = g_pika_server->db()->Del(members, &type_status);
    if (0 > del_nums) {
        LOG(WARNING) << "Del key: " << key << " at slot " << SlotNum(key) << " error";
        return -1;
    }

    return 1;
}

// do migrate key to dest pika server
static int DoMigrate(pink::PinkCli *cli, std::string send_str)
{
    slash::Status s;
    s = cli->Send(&send_str);
    if (!s.ok()) {
        LOG(WARNING) << "Slot Migrate Send error: " << strerror(errno);
        return -1;
    }
    return 1;
}

// migrate key ttl
static int MigrateKeyTTl(pink::PinkCli *cli,
                         const std::string key,
                         blackwidow::DataType data_type)
{
    pink::RedisCmdArgsType argv;
    std::string send_str;
    std::map<blackwidow::DataType, int64_t> type_timestamp;
    std::map<blackwidow::DataType, rocksdb::Status> type_status;
    type_timestamp = g_pika_server->db()->TTL(key, &type_status);
    if (PIKA_TTL_ZERO == type_timestamp[data_type]
        || PIKA_TTL_STALE == type_timestamp[data_type]) {
        argv.push_back("del");
        argv.push_back(key);
        pink::SerializeRedisCommand(argv, &send_str);
    } else if (0 < type_timestamp[data_type]) {
        argv.push_back("expire");
        argv.push_back(key);
        argv.push_back(std::to_string(type_timestamp[data_type]));
        pink::SerializeRedisCommand(argv, &send_str);
    } else {
        // no expire
        return 0;
    }

    if (0 > DoMigrate(cli, send_str)){
        return -1;
    }

    return 1;
}

static int MigrateKv(pink::PinkCli *cli, const std::string key)
{
    std::string value;
    rocksdb::Status s = g_pika_server->db()->Get(key, &value);
    if (!s.ok()) {
        if (s.IsNotFound()) {
            LOG(WARNING) << "Get kv key: "<< key <<" not found ";
            return 0;
        }
        else {
            LOG(WARNING) << "Get kv key: "<< key <<" error: " << strerror(errno);
            return -1;
        }
    } 

    pink::RedisCmdArgsType argv;
    std::string send_str;
    argv.push_back("SET");
    argv.push_back(key);
    argv.push_back(value);
    pink::SerializeRedisCommand(argv, &send_str);

    int send_num = 0;
    if (0 > DoMigrate(cli, send_str)) {
        return -1;
    }
    else {
        ++send_num;
    }

    int r;
    if (0 > (r = MigrateKeyTTl(cli, key, blackwidow::kStrings))){
        return -1;
    }
    else {
        send_num += r;
    }

    return send_num;
}

static int MigrateHash(pink::PinkCli *cli, const std::string key)
{
    int send_num = 0;
    int64_t cursor = 0;
    std::vector<blackwidow::FieldValue> field_values;
    rocksdb::Status s;

    do {
        s = g_pika_server->db()->HScan(key, cursor, "*", MAX_MEMBERS_NUM, &field_values, &cursor);
        if (s.ok() && field_values.size() > 0) {
            pink::RedisCmdArgsType argv;
            std::string send_str;
            argv.push_back("HMSET");
            argv.push_back(key);
            for (const auto& field_value : field_values) {
                argv.push_back(field_value.field);
                argv.push_back(field_value.value);
            }
            pink::SerializeRedisCommand(argv, &send_str);
            if (0 > DoMigrate(cli, send_str)) {
                return -1;
            }
            else {
                ++send_num;
            }
        }
    } while (cursor != 0 && s.ok());

    if (0 < send_num) {
        int r;
        if (0 > (r = MigrateKeyTTl(cli, key, blackwidow::kHashes))){
            return -1;
        }
        else {
            send_num += r;
        }
    }

    return send_num;
}

static int MigrateList(pink::PinkCli *cli, const std::string key)
{    
    // del old key, before migrate list; prevent redo when failed
    int send_num = 0;
    pink::RedisCmdArgsType argv;
    std::string send_str;
    argv.push_back("DEL");
    argv.push_back(key);
    pink::SerializeRedisCommand(argv, &send_str);
    if (0 > DoMigrate(cli, send_str)) {
        return -1;
    }
    else {
        ++send_num;
    }

    std::vector<std::string> values;
    rocksdb::Status s = g_pika_server->db()->LRange(key, 0, -1, &values);
    if (s.ok()) {
        auto iter = values.begin();
        while (iter != values.end()) {
            pink::RedisCmdArgsType argv;
            std::string send_str;
            argv.push_back("RPUSH");
            argv.push_back(key);

            for (int i = 0; iter != values.end() && i < MAX_MEMBERS_NUM; ++iter, ++i) {
                argv.push_back(*iter);
            }

            pink::SerializeRedisCommand(argv, &send_str);
            if (0 > DoMigrate(cli, send_str)) {
                return -1;
            }
            else {
                ++send_num;
            }
        }
    }

    // has send del key command
    if (1 < send_num) {
        int r;
        if (0 > (r = MigrateKeyTTl(cli, key, blackwidow::kLists))){
            return -1;
        }
        else {
            send_num += r;
        }
    }

    return send_num;
}

static int MigrateSet(pink::PinkCli *cli, const std::string key)
{
    int send_num = 0;
    int64_t cursor = 0;
    std::vector<std::string> members;
    rocksdb::Status s;

    do {
        s = g_pika_server->db()->SScan(key, cursor, "*", MAX_MEMBERS_NUM, &members, &cursor);
        if (s.ok() && members.size() > 0) {
            pink::RedisCmdArgsType argv;
            std::string send_str;
            argv.push_back("SADD");
            argv.push_back(key);

            for (const auto& member : members) {
                argv.push_back(member);
            }
            pink::SerializeRedisCommand(argv, &send_str);
            if (0 > DoMigrate(cli, send_str)) {
                return -1;
            }
            else {
                ++send_num;
            }
        }
    } while (cursor != 0 && s.ok());

    if (0 < send_num) {
        int r;
        if (0 > (r = MigrateKeyTTl(cli, key, blackwidow::kSets))){
            return -1;
        }
        else {
            send_num += r;
        }
    }

    return send_num;
}

static int MigrateZset(pink::PinkCli *cli, const std::string key)
{
    int send_num = 0;
    int64_t cursor = 0;
    std::vector<blackwidow::ScoreMember> score_members;
    rocksdb::Status s;

    do {
        s = g_pika_server->db()->ZScan(key, cursor, "*", MAX_MEMBERS_NUM, &score_members, &cursor);
        if (s.ok() && score_members.size() > 0) {
            pink::RedisCmdArgsType argv;
            std::string send_str;
            argv.push_back("ZADD");
            argv.push_back(key);

            for (const auto& score_member : score_members) {
                argv.push_back(std::to_string(score_member.score));
                argv.push_back(score_member.member);
            }
            pink::SerializeRedisCommand(argv, &send_str);
            if (0 > DoMigrate(cli, send_str)) {
                return -1;
            }
            else {
                ++send_num;
            }
        }
    } while (cursor != 0 && s.ok());

    if (0 < send_num) {
        int r;
        if (0 > (r = MigrateKeyTTl(cli, key, blackwidow::kZSets))){
            return -1;
        }
        else {
            send_num += r;
        }
    }

    return send_num;
}

static int MigrateEhash(pink::PinkCli *cli, const std::string key)
{
    int send_num = 0;
    int64_t cursor = 0;
    rocksdb::Status s;
    std::vector<blackwidow::FieldValueTTL> fvts;

    do {
        s = g_pika_server->db()->Ehscan(key, cursor, "*", MAX_MEMBERS_NUM, &fvts, &cursor);
        if (s.ok() && fvts.size() > 0) {
            pink::RedisCmdArgsType argv;
            std::string send_str;
            argv.push_back("EHMSETEX");
            argv.push_back(key);
            for (const auto& field_value : fvts) {
                argv.push_back(field_value.field);
                argv.push_back(field_value.value);
                argv.push_back(std::to_string(field_value.ttl));
            }
            pink::SerializeRedisCommand(argv, &send_str);
            if (0 > DoMigrate(cli, send_str)) {
                return -1;
            }
            else {
                ++send_num;
            }
        }
    } while (cursor != 0 && s.ok());

    if (0 < send_num) {
        int r;
        if (0 > (r = MigrateKeyTTl(cli, key, blackwidow::kEhashs))){
            return -1;
        }
        else {
            send_num += r;
        }
    }

    return send_num;
}

PikaParseSendThread::PikaParseSendThread(PikaMigrateThread *migrate_thread)
    : dest_ip_("none")
    , dest_port_(-1)
    , timeout_ms_(3000)
    , mgrtkeys_num_(64)
    , should_exit_(false)
    , migrate_thread_(migrate_thread)
    , cli_(NULL)
{

}

PikaParseSendThread::~PikaParseSendThread()
{
    if (is_running()) {
        should_exit_ = true;
        StopThread();
    }

    if (cli_) {
        delete cli_;
        cli_ = NULL;
    }
}

bool
PikaParseSendThread::Init(const std::string &ip, int64_t port, int64_t timeout_ms, int64_t mgrtkeys_num)
{
    dest_ip_ = ip;
    dest_port_ = port;
    timeout_ms_ = timeout_ms;
    mgrtkeys_num_ = mgrtkeys_num;

    cli_ = pink::NewRedisCli();
    cli_->set_connect_timeout(timeout_ms_);
    cli_->set_send_timeout(timeout_ms_);
    cli_->set_recv_timeout(timeout_ms_);
    slash::Status result = cli_->Connect(dest_ip_, dest_port_, g_pika_server->host());
    if (!result.ok()) {
        LOG(INFO) << "PikaParseSendThread::Init failed. Connect server(" << dest_ip_ << ":" << dest_port_ << ") " << result.ToString();
        return false;
    }

    // do auth
    if (!PikaCommonFunc::DoAuth(cli_, g_pika_conf->requirepass())) {
        LOG(WARNING) << "PikaParseSendThread::Init do auth failed !!";
        cli_->Close();
        return false;
    }

    return true;
}

void
PikaParseSendThread::ExitThread(void)
{
    should_exit_ = true;
}

int
PikaParseSendThread::MigrateOneKey(const char key_type, const std::string key)
{
    int send_num;
    switch (key_type) {
        case 'k':
            if (0 > (send_num = MigrateKv(cli_, key))) {
                return -1;
            }
            break;
        case 'h':
            if (0 > (send_num = MigrateHash(cli_, key))) {
                return -1;
            }
            break;
        case 'l':
            if (0 > (send_num = MigrateList(cli_, key))) {
                return -1;
            }
            break;
        case 's':
            if (0 > (send_num = MigrateSet(cli_, key))) {
                return -1;
            }
            break;
        case 'z':
            if (0 > (send_num = MigrateZset(cli_, key))) {
                return -1;
            }
            break;
        case 'e':
            if (0 > (send_num = MigrateEhash(cli_, key))) {
                return -1;
            }
            break;
        default:
            return -1;
            break;
    }
    return send_num;
}

void
PikaParseSendThread::DelKeysAndWriteBinlog(std::deque<std::pair<const char, std::string>> &send_keys)
{
    for (auto iter = send_keys.begin(); iter != send_keys.end(); ++iter) {
        KeyDelete(iter->first, iter->second);
        WriteDelKeyToBinlog(iter->second);
    }
}

bool
PikaParseSendThread::CheckMigrateRecv(int64_t need_receive_num)
{
    pink::RedisCmdArgsType argv;
    for (int64_t i = 0; i < need_receive_num; ++i) {
        slash::Status s;
        s = cli_->Recv(&argv);
        if (!s.ok()) {
            LOG(ERROR) << "PikaParseSendThread::CheckMigrateRecv Recv error: " << s.ToString();
            return false;
        }

        //set   return ok
        //zadd  return number
        //hset  return 0 or 1
        //hmset return ok
        //sadd  return number
        //rpush return length
        std::string reply = argv[0];
        int64_t ret;
        if (1 == argv.size()
            && (kInnerReplOk == slash::StringToLower(reply) || slash::string2l(reply.data(), reply.size(), &ret))) {
            continue;
        }
        else {
            LOG(ERROR) << "PikaParseSendThread::CheckMigrateRecv reply error: " << reply;
            return false;
        }
    }

    return true;
}

void*
PikaParseSendThread::ThreadMain()
{
    while (!should_exit_) {

        std::deque<std::pair<const char, std::string>> send_keys;
        {
            slash::MutexLock lm(&migrate_thread_->mgrtkeys_queue_mutex_);
            while (!should_exit_ && 0 >= migrate_thread_->mgrtkeys_queue_.size()) {
                migrate_thread_->mgrtkeys_cond_.Wait();
            }

            if (should_exit_) {
                //LOG(INFO) << "PikaParseSendThread::ThreadMain :"<< pthread_self() << " exit !!!";
                return NULL;
            }

            migrate_thread_->IncWorkingThreadNum();
            for (int32_t i = 0; i < mgrtkeys_num_; ++i) {
                if (migrate_thread_->mgrtkeys_queue_.empty()) {
                    break;
                }
                send_keys.push_back(migrate_thread_->mgrtkeys_queue_.front());
                migrate_thread_->mgrtkeys_queue_.pop_front();
            }
        }

        int64_t send_num = 0;
        int64_t need_receive_num = 0;
        int32_t migrate_keys_num = 0;
        for (auto iter = send_keys.begin(); iter != send_keys.end(); ++iter) {

            if (0 > (send_num = MigrateOneKey(iter->first, iter->second))) {
                LOG(WARNING) << "PikaParseSendThread::ThreadMain MigrateOneKey: " << iter->second <<" failed !!!";
                migrate_thread_->TaskFailed();
                migrate_thread_->DecWorkingThreadNum();
                return NULL;
            }
            else {
                need_receive_num += send_num;
                ++migrate_keys_num;
            }
        }

        // check response
        if (!CheckMigrateRecv(need_receive_num)) {
            LOG(INFO) << "PikaMigrateThread::ThreadMain CheckMigrateRecv failed !!!";
            migrate_thread_->TaskFailed();
            migrate_thread_->DecWorkingThreadNum();
            return NULL;
        }
        else {
            DelKeysAndWriteBinlog(send_keys);
        }

        migrate_thread_->AddResponseNum(migrate_keys_num);
        migrate_thread_->DecWorkingThreadNum();
    }

    return NULL;
}


PikaMigrateThread::PikaMigrateThread()
    : pink::Thread()
    , dest_ip_("none")
    , dest_port_(-1)
    , timeout_ms_(3000)
    , slot_num_(-1)
    , keys_num_(-1)
    , is_migrating_(false)
    , should_exit_(false)
    , is_task_success_(true)
    , send_num_(0)
    , response_num_(0)
    , moved_num_(0)
    , request_migrate_(false)
    , request_migrate_cond_(&request_migrate_mutex_)
    , workers_num_(8)
    , working_thread_num_(0)
    , workers_cond_(&workers_mutex_)
    , cursor_(0)
    , mgrtkeys_cond_(&mgrtkeys_queue_mutex_)
{

}

PikaMigrateThread::~PikaMigrateThread()
{
    LOG(INFO) << "PikaMigrateThread::~PikaMigrateThread";

    if (is_running()) {
        should_exit_ = true;
        NotifyRequestMigrate();
        workers_cond_.Signal();
        StopThread();
    }
}

void
PikaMigrateThread::ReqMigrateBatch(const std::string &ip,
                                   int64_t port,
                                   int64_t time_out,
                                   int64_t slot,
                                   int64_t keys_num)
{
    if (0 == migrator_mutex_.Trylock()) {
        if (is_migrating_) {
            if (dest_ip_ != ip || dest_port_ != port || slot_num_ != slot) {
                LOG(INFO) << "PikaMigrateThread::ReqMigrate current: " << dest_ip_ << ":" << dest_port_ << " slot[" << slot_num_ << "]"
                                << "request: " << ip << ":" << port << " slot[" << slot << "]";
                migrator_mutex_.Unlock();
                return;
            }

            timeout_ms_ = time_out;
            keys_num_ = keys_num;
            NotifyRequestMigrate();
            migrator_mutex_.Unlock();
            return;
        }
        else {
            dest_ip_ = ip;
            dest_port_ = port;
            timeout_ms_ = time_out;
            slot_num_ = slot;
            keys_num_ = keys_num;
            should_exit_ = false;

            ResetThread();
            int ret = StartThread();
            if (0 != ret) {
                LOG(ERROR) << "PikaMigrateThread::ReqMigrateBatch StartThread failed. " << " ret=" << ret;
                is_migrating_ = false;
                StopThread();
            }
            else {
                LOG(INFO) << "PikaMigrateThread::ReqMigrateBatch slot: " << slot;
                is_migrating_ = true;
                NotifyRequestMigrate();
            }
            migrator_mutex_.Unlock();
            return;
        }
    }
    return;
}

int
PikaMigrateThread::ReqMigrateOne(const std::string &key)
{
    slash::MutexLock lm(&migrator_mutex_);

    int slot_num = SlotNum(key);
    std::string type_str;
    char key_type;
    rocksdb::Status s = g_pika_server->db()->Type(key, &type_str);
    if (!s.ok()) {
        if (s.IsNotFound()) {
            LOG(INFO) << "PikaMigrateThread::ReqMigrateOne key: "<< key <<" not found";
            return 0;
        } else {
            LOG(WARNING) << "PikaMigrateThread::ReqMigrateOne key: "<< key <<" error: " << strerror(errno);
            return -1;
        }
    }

    if (type_str=="string") {
        key_type = 'k';
    }
    else if (type_str=="hash"){
        key_type = 'h';
    }
    else if (type_str=="list") {
        key_type = 'l';
    }
    else if (type_str=="set") {
        key_type = 's';
    }
    else if (type_str=="zset") {
        key_type = 'z';
    }
    else if (type_str=="none") {
        return 0;
    }
    else {
        LOG(WARNING) << "PikaMigrateThread::ReqMigrateOne key: "<< key <<" type: " << type_str << " is  illegal";
        return -1;
    }

    if (slot_num != slot_num_) {
        LOG(WARNING) << "PikaMigrateThread::ReqMigrateOne Slot : " << slot_num << " is not the migrating slot:" << slot_num_;
        return -1;
    }

    // if the migrate thread exit, start it
    if (!is_migrating_) {
        ResetThread();
        int ret = StartThread();
        if (0 != ret) {
            LOG(ERROR) << "PikaMigrateThread::ReqMigrateOne StartThread failed. " << " ret=" << ret;
            is_migrating_ = false;
            StopThread();
        }
        else {
            LOG(INFO) << "PikaMigrateThread::ReqMigrateOne StartThread";
            is_migrating_ = true;
            usleep(100);
        }
    }
    else {
        // check the key is migrating
        std::pair<const char, std::string> kpair = std::make_pair(key_type, key);
        if (IsMigrating(kpair)) {
            LOG(INFO) << "PikaMigrateThread::ReqMigrateOne key: "<< key <<" is migrating ! ";
            return 1;
        }
        else {
            slash::MutexLock lo(&mgrtone_queue_mutex_);
            mgrtone_queue_.push_back(kpair);
            NotifyRequestMigrate();
        }
    }

    return 1;
}

void
PikaMigrateThread::GetMigrateStatus(std::string *ip,
                                    int64_t *port,
                                    int64_t *slot,
                                    bool *migrating,
                                    int64_t *moved,
                                    int64_t *remained)
{
    slash::MutexLock lm(&migrator_mutex_);
    *ip = dest_ip_;
    *port = dest_port_;
    *slot = slot_num_;
    *migrating = is_migrating_;
    *moved = moved_num_;
    slash::MutexLock lq(&mgrtkeys_queue_mutex_);
    int64_t migrating_keys_num = mgrtkeys_queue_.size();
    std::string slotKey = GetSlotsSlotKey(slot_num_); // SlotKeyPrefix + std::to_string(slot_num_);
    int32_t slot_size = 0;
    rocksdb::Status s =  g_pika_server->db()->SCard(slotKey, &slot_size);
    if (s.ok()) {
        *remained = slot_size + migrating_keys_num;
    } else {
         *remained = migrating_keys_num;
    }
}

void
PikaMigrateThread::CancelMigrate(void)
{
    LOG(INFO) << "PikaMigrateThread::CancelMigrate";

    if (is_running()) {
        should_exit_ = true;
        NotifyRequestMigrate();
        workers_cond_.Signal();
        StopThread();
    }
}

void
PikaMigrateThread::IncWorkingThreadNum(void)
{
    ++working_thread_num_;
}

void
PikaMigrateThread::DecWorkingThreadNum(void)
{
    slash::MutexLock lw(&workers_mutex_);
    --working_thread_num_;
    workers_cond_.Signal();
}

void
PikaMigrateThread::TaskFailed()
{
    LOG(ERROR) << "PikaMigrateThread::TaskFailed !!!";
    is_task_success_ = false;
}

void
PikaMigrateThread::AddResponseNum(int32_t response_num)
{
    response_num_ += response_num;
}

void
PikaMigrateThread::ResetThread(void)
{
    if (0 != thread_id()) {
        JoinThread();
    }
}

void
PikaMigrateThread::DestroyThread(bool is_self_exit)
{
    slash::MutexLock lm(&migrator_mutex_);
    LOG(INFO) << "PikaMigrateThread::DestroyThread";

    // Destroy work threads
    DestroyParseSendThreads();

    if (is_self_exit) {
        set_is_running(false);
    }
    
    {
        slash::MutexLock lq(&mgrtkeys_queue_mutex_);
        slash::MutexLock lm(&mgrtkeys_map_mutex_);
        std::deque<std::pair<const char, std::string>>().swap(mgrtkeys_queue_);
        std::map<std::pair<const char, std::string>, std::string>().swap(mgrtkeys_map_);
    }

    cursor_ = 0;
    is_migrating_ = false;
    is_task_success_ = true;
    moved_num_ = 0;
}

void
PikaMigrateThread::NotifyRequestMigrate(void)
{
    slash::MutexLock lr(&request_migrate_mutex_);
    request_migrate_ = true;
    request_migrate_cond_.Signal();
}

bool
PikaMigrateThread::IsMigrating(std::pair<const char, std::string> &kpair)
{
    slash::MutexLock lo(&mgrtone_queue_mutex_);
    slash::MutexLock lm(&mgrtkeys_map_mutex_);

    for (auto iter = mgrtone_queue_.begin(); iter != mgrtone_queue_.end(); ++iter) {
        if (iter->first == kpair.first && iter->second == kpair.second) {
            return true;
        }
    }

    auto iter = mgrtkeys_map_.find(kpair);
    if (iter != mgrtkeys_map_.end()) {
        return true;
    }

    return false;
}

void
PikaMigrateThread::ReadSlotKeys(const std::string &slotKey, int64_t need_read_num, int64_t &real_read_num, int32_t *finish)
{
    real_read_num = 0;
    std::string key;
    char key_type;
    int32_t is_member = 0;
    std::vector<std::string> members;

    rocksdb::Status s = g_pika_server->db()->SScan(slotKey, cursor_, "*", need_read_num, &members, &cursor_);
    if (s.ok() && 0 < members.size()) {
        for (const auto& member : members) {
            g_pika_server->db()->SIsmember(slotKey, member, &is_member);
            if (is_member) {
                key = member;
                key_type = key.at(0);
                key.erase(key.begin());
                std::pair<const char, std::string> kpair = std::make_pair(key_type, key);
                if (mgrtkeys_map_.find(kpair) == mgrtkeys_map_.end()) {
                    mgrtkeys_queue_.push_back(kpair);
                    mgrtkeys_map_[kpair] = INVALID_STR;
                    ++real_read_num;
                }
            }
            else {
                LOG(INFO) << "PikaMigrateThread::ReadSlotKeys key " << member << " not found in" << slotKey;
            }
        }
    }

    *finish = (0 == cursor_) ? 1 : 0;
}

bool
PikaMigrateThread::CreateParseSendThreads(int32_t dispatch_num)
{
    workers_num_ = g_pika_conf->slotmigrate_thread_num();
    for (int32_t i = 0; i < workers_num_; ++i) {
        PikaParseSendThread *worker = new PikaParseSendThread(this);
        if (!worker->Init(dest_ip_, dest_port_, timeout_ms_, dispatch_num)) {
            delete worker;
            DestroyParseSendThreads();
            return false;
        }
        else {
            int ret = worker->StartThread();
            if (0 != ret) {
                LOG(INFO) << "PikaMigrateThread::CreateParseSendThreads start work thread failed ret=" << ret;
                delete worker;
                DestroyParseSendThreads();
                return false;
            }
            else {
                workers_.push_back(worker);
            }
        }
    }
    return true;
}

void
PikaMigrateThread::DestroyParseSendThreads(void)
{
    if (!workers_.empty()) {
        for (auto iter = workers_.begin(); iter != workers_.end(); ++iter) {
            (*iter)->ExitThread();
        }

        {
            slash::MutexLock lm(&mgrtkeys_queue_mutex_);
            mgrtkeys_cond_.SignalAll();
        }

        for (auto iter = workers_.begin(); iter != workers_.end(); ++iter) {
            delete *iter;
        }
        workers_.clear();  
    }
}

void*
PikaMigrateThread::ThreadMain()
{
    LOG(INFO) << "PikaMigrateThread::ThreadMain Start";
    
    // Create parse_send_threads
    int32_t dispatch_num = g_pika_conf->thread_migrate_keys_num();
    if (!CreateParseSendThreads(dispatch_num)) {
        LOG(INFO) << "PikaMigrateThread::ThreadMain CreateParseSendThreads failed !!!";
        DestroyThread(true);
        return NULL;
    }

    std::string slotKey = GetSlotsSlotKey(slot_num_);
    int32_t slot_size = 0;
    g_pika_server->db()->SCard(slotKey, &slot_size);

    while (!should_exit_) {

        // Waiting migrate task
        {
            slash::MutexLock lr(&request_migrate_mutex_);
            while (!request_migrate_) {
                request_migrate_cond_.Wait();
            }
            request_migrate_ = false;

            if (should_exit_) {
                LOG(INFO) << "PikaMigrateThread::ThreadMain :"<< pthread_self() << " exit1 !!!";
                DestroyThread(false);
                return NULL;
            }
        }

        // read keys form slot and push to mgrtkeys_queue_
        int64_t round_remained_keys = keys_num_;
        int64_t real_read_num = 0;
        int32_t is_finish = 0;
        send_num_ = 0;
        response_num_ = 0;
        do {
            slash::MutexLock lq(&mgrtkeys_queue_mutex_);
            slash::MutexLock lo(&mgrtone_queue_mutex_);
            slash::MutexLock lm(&mgrtkeys_map_mutex_);

            // first check whether need migrate one key
            if (!mgrtone_queue_.empty()) {
                while (!mgrtone_queue_.empty()) {
                    mgrtkeys_queue_.push_front(mgrtone_queue_.front());
                    mgrtkeys_map_[mgrtone_queue_.front()] = INVALID_STR;
                    mgrtone_queue_.pop_front();
                    ++send_num_;
                }
            }
            else {
                int64_t need_read_num = (0 < round_remained_keys - dispatch_num) ? dispatch_num : round_remained_keys;
                ReadSlotKeys(slotKey, need_read_num, real_read_num, &is_finish);
                round_remained_keys -= need_read_num;
                send_num_ += real_read_num;
            }
            mgrtkeys_cond_.SignalAll();

        } while (0 < round_remained_keys && !is_finish);

        // wait all ParseSenderThread finish
        {
            slash::MutexLock lw(&workers_mutex_);
            while (!should_exit_ && is_task_success_ && send_num_ != response_num_) {
                workers_cond_.Wait();
            }
        }
        // LOG(INFO) << "PikaMigrateThread::ThreadMain send_num:" << send_num_ << " response_num:" << response_num_;

        if (should_exit_) {
            LOG(INFO) << "PikaMigrateThread::ThreadMain :"<< pthread_self() << " exit2 !!!";
            DestroyThread(false);
            return NULL;
        }

        // check one round migrate task success
        if (!is_task_success_) {
            LOG(ERROR) << "PikaMigrateThread::ThreadMain one round migrate task failed !!!";
            DestroyThread(true);
            return NULL;
        }
        else {
            moved_num_ += response_num_;

            slash::MutexLock lm(&mgrtkeys_map_mutex_);
            std::map<std::pair<const char, std::string>, std::string>().swap(mgrtkeys_map_);
        }

        // check slot migrate finish
        int32_t slot_remained_keys = 0;
        g_pika_server->db()->SCard(slotKey, &slot_remained_keys);
        if (0 == slot_remained_keys) {
            LOG(INFO) << "PikaMigrateThread::ThreadMain slot_size:" << slot_size << " moved_num:" << moved_num_;
            if (slot_size != moved_num_) {
                LOG(ERROR) << "PikaMigrateThread::ThreadMain moved_num != slot_size !!!";
            }
            DestroyThread(true);
            return NULL;
        }
    }

    return NULL;
}

/* EOF */
