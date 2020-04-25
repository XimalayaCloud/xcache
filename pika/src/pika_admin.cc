// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include <sys/time.h>
#include <iomanip>

#include "slash/include/slash_string.h"
#include "slash/include/rsync.h"
#include "pika_conf.h"
#include "pika_admin.h"
#include "pika_server.h"
#include "pika_slot.h"
#include "pika_version.h"
#include "build_version.h"
#include "pika_define.h"
#include "pika_commonfunc.h"

#include <sys/utsname.h>
#ifdef TCMALLOC_EXTENSION
#include <gperftools/malloc_extension.h>
#endif

extern PikaServer *g_pika_server;
extern PikaConf *g_pika_conf;

void SlaveofCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlaveof);
        return;
    }
    PikaCmdArgsType::const_iterator it = argv.begin() + 1; //Remember the first args is the opt name

    master_ip_ = *it++;

    is_noone_ = false;
    if (!strcasecmp(master_ip_.data(), "no") && !strcasecmp(it->data(), "one")) {
        if (argv.end() - it == 1) {
            is_noone_ = true;
        } else {
            res_.SetRes(CmdRes::kWrongNum, kCmdNameSlaveof);
        }
        return;
    }

    std::string str_master_port = *it++;
    if (!slash::string2l(str_master_port.data(), str_master_port.size(), &master_port_) || master_port_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }

    if ((master_ip_ == "127.0.0.1" || master_ip_ == g_pika_server->host()) && master_port_ == g_pika_server->port()) {
        res_.SetRes(CmdRes::kErrOther, "you fucked up");
        return;
    }

    have_offset_ = false;
    int cur_size = argv.end() - it;
    if (cur_size == 0) {

    } else if (cur_size == 1) {
        std::string command = *it++;
        if (command != "force") {
            res_.SetRes(CmdRes::kSyntaxErr);
            return;
        }
        g_pika_server->SetForceFullSync(true);
    } else if (cur_size == 2) {
        have_offset_ = true;
        std::string str_filenum = *it++;
        if (!slash::string2l(str_filenum.data(), str_filenum.size(), &filenum_) || filenum_ < 0) {
            res_.SetRes(CmdRes::kInvalidInt);
            return;
        }
        std::string str_pro_offset = *it++;
        if (!slash::string2l(str_pro_offset.data(), str_pro_offset.size(), &pro_offset_) || pro_offset_ < 0) {
            res_.SetRes(CmdRes::kInvalidInt);
            return;
        }
    } else {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlaveof);
    }
}

void SlaveofCmd::Do() {
    // Check if we are already connected to the specified master
    if ((master_ip_ == "127.0.0.1" || g_pika_server->master_ip() == master_ip_) &&
        g_pika_server->master_port() == master_port_) {
        res_.SetRes(CmdRes::kOk);
        return;
    }

    // Stop rsync
    LOG(INFO) << "start slaveof, stop rsync first";
    slash::StopRsync(g_pika_conf->db_sync_path());
    g_pika_server->RemoveMaster();

    if (is_noone_) {
        g_pika_conf->SetSlaveof("");
        if (g_pika_conf->disable_auto_compactions()) {
            g_pika_conf->SetDisableAutoCompactions(false);
            g_pika_server->db()->ResetOption("disable_auto_compactions", "false");
        }
        g_pika_server->SetForceFullSync(false);
        g_pika_conf->ConfigRewrite();
        res_.SetRes(CmdRes::kOk);
        return;
    } else {
        g_pika_conf->SetSlaveof(master_ip_ + ":" + std::to_string(master_port_));
        g_pika_conf->ConfigRewrite();
    }

    if (have_offset_) {
        // Before we send the trysync command, we need purge current logs older than the sync point
        if (filenum_ > 0) {
            g_pika_server->PurgeLogs(filenum_ - 1, true, true);
        }
        g_pika_server->logger_->SetProducerStatus(filenum_, pro_offset_);
    }
    bool sm_ret = g_pika_server->SetMaster(master_ip_, master_port_);
    if (sm_ret) {
        res_.SetRes(CmdRes::kOk);

        // clear cache when in slave model
        LOG(INFO) << "clear cache when change master to slave";
        g_pika_server->ClearCacheDbAsync();
    } else {
        res_.SetRes(CmdRes::kErrOther, "Server is not in correct state for slaveof");
    }
}

void TrysyncCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameTrysync);
        return;
    }
    PikaCmdArgsType::const_iterator it = argv.begin() + 1; //Remember the first args is the opt name
    slave_ip_ = *it++;

    std::string str_slave_port = *it++;
    if (!slash::string2l(str_slave_port.data(), str_slave_port.size(), &slave_port_) || slave_port_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }

    std::string str_filenum = *it++;
    if (!slash::string2l(str_filenum.data(), str_filenum.size(), &filenum_) || filenum_ < 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }

    std::string str_pro_offset = *it++;
    if (!slash::string2l(str_pro_offset.data(), str_pro_offset.size(), &pro_offset_) || pro_offset_ < 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
    }
}

void TrysyncCmd::Do() {
    LOG(INFO) << "Trysync, Slave ip: " << slave_ip_ << " Slave port:" << slave_port_
        << " filenum: " << filenum_ << " pro_offset: " << pro_offset_;
    int64_t sid = g_pika_server->TryAddSlave(slave_ip_, slave_port_);
    if (sid >= 0) {
        Status status = g_pika_server->AddBinlogSender(slave_ip_, slave_port_,
                filenum_, pro_offset_);
        if (status.ok()) {
            res_.AppendInteger(sid);
            LOG(INFO) << "Send Sid to Slave: " << sid;
            g_pika_server->BecomeMaster();
            return;
        }
        // Create Sender failed, delete the slave
        g_pika_server->DeleteSlave(slave_ip_, slave_port_);

        if (status.IsIncomplete()) {
            res_.AppendString(kInnerReplWait);
        } else {
            LOG(WARNING) << "slave offset is larger than mine, slave ip: " << slave_ip_
                << "slave port:" << slave_port_
                << " filenum: " << filenum_ << " pro_offset_: " << pro_offset_;
            res_.SetRes(CmdRes::kErrOther, "InvalidOffset");
        }
    } else {
        LOG(WARNING) << "slave already exist, slave ip: " << slave_ip_
            << "slave port: " << slave_port_;
        res_.SetRes(CmdRes::kErrOther, "AlreadyExist");
    }
}

void AuthCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameAuth);
        return;
    }
    pwd_ = argv[1];
}

void AuthCmd::Do() {
    std::string root_password(g_pika_conf->requirepass());
    std::string user_password(g_pika_conf->userpass());
    if (user_password.empty() && root_password.empty()) {
        res_.SetRes(CmdRes::kErrOther, "Client sent AUTH, but no password is set");
        return;
    }

    if (pwd_ == user_password) {
        res_.SetRes(CmdRes::kOk, "USER");
    }
    if (pwd_ == root_password) {
        res_.SetRes(CmdRes::kOk, "ROOT");
    }
    if (res_.none()) {
        res_.SetRes(CmdRes::kInvalidPwd);
    }
}

void BgsaveCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameBgsave);
        return;
    }
}
void BgsaveCmd::Do() {
    g_pika_server->Bgsave();
    const PikaServer::BGSaveInfo& info = g_pika_server->bgsave_info();
    char buf[256];
    snprintf(buf, sizeof(buf), "+%s : %u: %lu",
            info.s_start_time.c_str(), info.filenum, info.offset);
    res_.AppendContent(buf);
}

void BgsaveoffCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameBgsaveoff);
        return;
    }
}
void BgsaveoffCmd::Do() {
    CmdRes::CmdRet ret;
    if (g_pika_server->Bgsaveoff()) {
     ret = CmdRes::kOk;
    } else {
     ret = CmdRes::kNoneBgsave;
    }
    res_.SetRes(ret);
}

void CompactCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameCompact);
        return;
    }
}

void CompactCmd::Do() {
    rocksdb::Status s;
    s = g_pika_server->db()->Compact(blackwidow::kAll);
    if (s.ok()) {
        res_.SetRes(CmdRes::kOk);
    } else {
        res_.SetRes(CmdRes::kErrOther, s.ToString());
    }
}

void PurgelogstoCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNamePurgelogsto);
        return;
    }
    std::string filename = argv[1];
    slash::StringToLower(filename);
    if (filename.size() <= kBinlogPrefixLen ||
            kBinlogPrefix != filename.substr(0, kBinlogPrefixLen)) {
        res_.SetRes(CmdRes::kInvalidParameter);
        return;
    }
    std::string str_num = filename.substr(kBinlogPrefixLen);
    int64_t num = 0;
    if (!slash::string2l(str_num.data(), str_num.size(), &num) || num < 0) {
        res_.SetRes(CmdRes::kInvalidParameter);
        return;
    }
    num_ = num;
}
void PurgelogstoCmd::Do() {
    if (g_pika_server->PurgeLogs(num_, true, false)) {
        res_.SetRes(CmdRes::kOk);
    } else {
        res_.SetRes(CmdRes::kPurgeExist);
    }
}

void PingCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNamePing);
        return;
    }
}
void PingCmd::Do() {
    res_.SetRes(CmdRes::kPong);
}

void SelectCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSelect);
        return;
    }

    int64_t db_id;
    if (!slash::string2l(argv[1].data(), argv[1].size(), &db_id) ||
            db_id < 0 || db_id > 65535) {
        res_.SetRes(CmdRes::kInvalidIndex);
    }
}
void SelectCmd::Do() {
    res_.SetRes(CmdRes::kOk);
}

void FlushallCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameFlushall);
        return;
    }
}

void FlushallCmd::Do() {
    g_pika_server->RWLockWriter();
    if (g_pika_server->FlushAll()) {
        res_.SetRes(CmdRes::kOk);
    } else {
        res_.SetRes(CmdRes::kErrOther, "There are some bgthread using db now, can not flushall");
    }
    g_pika_server->RWUnlock();
}

void FlushallCmd::CacheDo() {
    Do();
}

void FlushallCmd::PostDo() {
    // clear cache
    if (PIKA_CACHE_NONE != g_pika_conf->cache_model()) {
        g_pika_server->ClearCacheDbAsync();
    }
}

void ReadonlyCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameReadonly);
        return;
    }
    std::string opt = argv[1];
    slash::StringToLower(opt);
    if (opt == "on" || opt == "1") {
        is_open_ = true;
    } else if (opt == "off" || opt == "0") {
        is_open_ = false;
    } else {
        res_.SetRes(CmdRes::kSyntaxErr, kCmdNameReadonly);
        return;
    }
}
void ReadonlyCmd::Do() {
    g_pika_server->RWLockWriter();
    if (is_open_) {
        g_pika_conf->SetReadonly(true);
    } else {
        g_pika_conf->SetReadonly(false);
    }
    res_.SetRes(CmdRes::kOk);
    g_pika_server->RWUnlock();
}

void ClientCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameClient);
        return;
    }
    if (!strcasecmp(argv[1].data(), "list") && argv.size() == 2) {
        //nothing
    } else if (!strcasecmp(argv[1].data(), "kill") && argv.size() == 3) {
        ip_port_ = argv[2];
    } else {
        res_.SetRes(CmdRes::kErrOther, "Syntax error, try CLIENT (LIST | KILL ip:port)");
    return;
    }
    operation_ = argv[1];
    slash::StringToLower(operation_);
    return;
}

void ClientCmd::Do() {
    if (operation_ == "list") {
        struct timeval now;
        gettimeofday(&now, NULL);
        std::vector<ClientInfo> clients;
        g_pika_server->ClientList(&clients);
        std::vector<ClientInfo>::iterator iter= clients.begin();
        std::string reply = "";
        char buf[128];
        while (iter != clients.end()) {
            snprintf(buf, sizeof(buf), "addr=%s fd=%d idle=%ld\n", iter->ip_port.c_str(), iter->fd, iter->last_interaction == 0 ? 0 : now.tv_sec - iter->last_interaction);
            reply.append(buf);
            iter++;
        }
        res_.AppendString(reply);
    } else if (!strcasecmp(operation_.data(), "kill") && !strcasecmp(ip_port_.data(), "all")) {
        g_pika_server->ClientKillAll();
        res_.SetRes(CmdRes::kOk);
    } else if (g_pika_server->ClientKill(ip_port_) == 1) {
        res_.SetRes(CmdRes::kOk);
    } else {
        res_.SetRes(CmdRes::kErrOther, "No such client");
    }
    return;
}

void ShutdownCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameShutdown);
        return;
    }
}
// no return
void ShutdownCmd::Do() {
    DLOG(WARNING) << "handle \'shutdown\'";
    g_pika_server->Exit();
    res_.SetRes(CmdRes::kNone);
}

const std::string InfoCmd::kAllSection = "all";
const std::string InfoCmd::kServerSection = "server";
const std::string InfoCmd::kClientsSection = "clients";
const std::string InfoCmd::kStatsSection = "stats";
const std::string InfoCmd::kReplicationSection = "replication";
const std::string InfoCmd::kKeyspaceSection = "keyspace";
const std::string InfoCmd::kLogSection = "log";
const std::string InfoCmd::kDataSection = "data";
const std::string InfoCmd::kCache = "cache";
const std::string InfoCmd::kZset = "zset";
const std::string InfoCmd::kDelay = "delay";

void InfoCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    (void)ptr_info;
    size_t argc = argv.size();
    if (argc > 3) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
    }
    if (argc == 1) {
        info_section_ = kInfoAll;
        return;
    } //then the agc is 2 or 3

    if (!strcasecmp(argv[1].data(), kAllSection.data())) {
        info_section_ = kInfoAll;
    } else if (!strcasecmp(argv[1].data(), kServerSection.data())) {
        info_section_ = kInfoServer;
    } else if (!strcasecmp(argv[1].data(), kClientsSection.data())) {
        info_section_ = kInfoClients;
    } else if (!strcasecmp(argv[1].data(), kStatsSection.data())) {
        info_section_ = kInfoStats;
    } else if (!strcasecmp(argv[1].data(), kReplicationSection.data())) {
        info_section_ = kInfoReplication;
    } else if (!strcasecmp(argv[1].data(), kKeyspaceSection.data())) {
        info_section_ = kInfoKeyspace;
        if (argc == 2) {
            return;
        }
        if (argv[2] == "1") { //info keyspace [ 0 | 1 | off ]
            rescan_ = true;
        } else if (argv[2] == "off") {
            off_ = true;
        } else if (argv[2] != "0") {
            res_.SetRes(CmdRes::kSyntaxErr);
        }
        return;
    } else if (!strcasecmp(argv[1].data(), kLogSection.data())) {
        info_section_ = kInfoLog;
    } else if (!strcasecmp(argv[1].data(), kDataSection.data())) {
        info_section_ = kInfoData;
    } else if (!strcasecmp(argv[1].data(), kCache.data())) {
        info_section_ = kInfoCache;
    } else if (!strcasecmp(argv[1].data(), kZset.data())) {
        info_section_ = kInfoZset;
    } else if (!strcasecmp(argv[1].data(), kDelay.data())) {
        info_section_ = kInfoDelay;
        if (argc == 2) {
            return;
        }
        std::string interval = argv[2];
        interval_ = std::atoi(interval.c_str());
        return;
    } else {
        info_section_ = kInfoErr;
    }
    if (argc != 2) {
        res_.SetRes(CmdRes::kSyntaxErr);
    }
}

void InfoCmd::Do() {
    std::string info;
    switch (info_section_) {
        case kInfoAll:
            InfoServer(info);
            info.append("\r\n");
            InfoData(info);
            info.append("\r\n");
            InfoLog(info);
            info.append("\r\n");
            InfoClients(info);
            info.append("\r\n");
            InfoStats(info);
            info.append("\r\n");
            InfoReplication(info);
            info.append("\r\n");
            InfoKeyspace(info);
            info.append("\r\n");
            InfoCache(info);
            info.append("\r\n");
            InfoZset(info);
            break;
        case kInfoServer:
            InfoServer(info);
            break;
        case kInfoClients:
            InfoClients(info);
            break;
        case kInfoStats:
            InfoStats(info);
            break;
        case kInfoReplication:
            InfoReplication(info);
            break;
        case kInfoKeyspace:
            InfoKeyspace(info);
            // off_ should return +OK
            if (off_) {
                res_.SetRes(CmdRes::kOk);
            }
            break;
        case kInfoLog:
            InfoLog(info);
            break;
        case kInfoData:
            InfoData(info);
            break;
        case kInfoCache:
            InfoCache(info);
            break;
        case kInfoZset:
            InfoZset(info);
            break;
		case kInfoDelay:
            InfoDelay(info);
            break;
        default:
            //kInfoErr is nothing
            break;
    }


    res_.AppendStringLen(info.size());
    res_.AppendContent(info);
    return;
}

void InfoCmd::InfoServer(std::string &info) {
    static struct utsname host_info;
    static bool host_info_valid = false;
    if (!host_info_valid) {
        uname(&host_info);
        host_info_valid = true;
    }

    time_t current_time_s = time(NULL);
    std::stringstream tmp_stream;
    char version[32];
    snprintf(version, sizeof(version), "%d.%d.%d-%d.%d", PIKA_MAJOR,
            PIKA_MINOR, PIKA_PATCH, PIKA_XMLY_MAJOR, PIKA_XMLY_MINOR);
    tmp_stream << "# Server\r\n";
    tmp_stream << "pika_version:" << version << "\r\n";
    tmp_stream << pika_build_git_sha << "\r\n";
    tmp_stream << "pika_build_compile_date: " <<
        pika_build_compile_date << "\r\n";
    tmp_stream << "os:" << host_info.sysname << " " << host_info.release << " " << host_info.machine << "\r\n";
    tmp_stream << "arch_bits:" << (reinterpret_cast<char*>(&host_info.machine) + strlen(host_info.machine) - 2) << "\r\n";
    tmp_stream << "process_id:" << getpid() << "\r\n";
    tmp_stream << "tcp_port:" << g_pika_conf->port() << "\r\n";
    tmp_stream << "thread_num:" << g_pika_conf->thread_num() << "\r\n";
    tmp_stream << "sync_thread_num:" << g_pika_conf->sync_thread_num() << "\r\n";
    tmp_stream << "uptime_in_seconds:" << (current_time_s - g_pika_server->start_time_s()) << "\r\n";
    tmp_stream << "uptime_in_days:" << (current_time_s / (24*3600) - g_pika_server->start_time_s() / (24*3600) + 1) << "\r\n";
    tmp_stream << "config_file:" << g_pika_conf->conf_path() << "\r\n";

    info.append(tmp_stream.str());
}

void InfoCmd::InfoClients(std::string &info) {
    std::stringstream tmp_stream;
    tmp_stream << "# Clients\r\n";
    tmp_stream << "connected_clients:" << g_pika_server->ClientList() << "\r\n";

    info.append(tmp_stream.str());
}

void InfoCmd::InfoStats(std::string &info) {
    std::stringstream tmp_stream;
    tmp_stream << "# Stats\r\n";

    tmp_stream << "total_connections_received:" << g_pika_server->accumulative_connections() << "\r\n";
    tmp_stream << "instantaneous_ops_per_sec:" << g_pika_server->ServerCurrentQps() << "\r\n";
    tmp_stream << "total_commands_processed:" << g_pika_server->ServerQueryNum() << "\r\n";
    PikaServer::BGSaveInfo bgsave_info = g_pika_server->bgsave_info();
    bool is_bgsaving = g_pika_server->bgsaving();
    time_t current_time_s = time(NULL);
    tmp_stream << "is_bgsaving:" << (is_bgsaving ? "Yes, " : "No, ") << bgsave_info.s_start_time << ", "
                                                                << (is_bgsaving ? (current_time_s - bgsave_info.start_time) : 0) << "\r\n";
    PikaServer::BGSlotsReload bgslotsreload_info = g_pika_server->bgslots_reload();
    bool is_reloading = g_pika_server->GetSlotsreloading();
    tmp_stream << "is_slots_reloading:" << (is_reloading ? "Yes, " : "No, ") << bgslotsreload_info.s_start_time << ", "
                                                                << (is_reloading ? (current_time_s - bgslotsreload_info.start_time) : 0) << "\r\n";
    PikaServer::BGSlotsDel bg_slots_del_info = g_pika_server->bgslots_del();
    bool is_slots_deleting = bg_slots_del_info.deleting;
    tmp_stream << "is_slots_deleting:" << (is_slots_deleting ? "Yes, " : "No, ") << bg_slots_del_info.s_start_time << ", "
                                << (is_slots_deleting ? (current_time_s - bg_slots_del_info.start_time) : 0) << ", slotno["
                                << bg_slots_del_info.slot_no << "], total: " << bg_slots_del_info.total << ", del: " << bg_slots_del_info.current << "\r\n";
    PikaServer::KeyScanInfo key_scan_info = g_pika_server->key_scan_info();
    bool is_scaning = g_pika_server->key_scaning();
    tmp_stream << "is_scaning_keyspace:" << (is_scaning ? ("Yes, " + key_scan_info.s_start_time) + "," : "No");
    if (is_scaning) {
        tmp_stream << current_time_s - key_scan_info.start_time;
    }
    tmp_stream << "\r\n";
    tmp_stream << "is_compact:" << g_pika_server->db()->GetCurrentTaskType() << "\r\n";
    tmp_stream << "compact_cron:" << g_pika_conf->compact_cron() << "\r\n";
    tmp_stream << "compact_interval:" << g_pika_conf->compact_interval() << "\r\n";

    info.append(tmp_stream.str());
}

void InfoCmd::InfoReplication(std::string &info) {
    int host_role = g_pika_server->role();
    std::stringstream tmp_stream;
    tmp_stream << "# Replication(";
    switch (host_role) {
        case PIKA_ROLE_SINGLE :
        case PIKA_ROLE_MASTER : tmp_stream << "MASTER)\r\nrole:master\r\n"; break;
        case PIKA_ROLE_SLAVE : tmp_stream << "SLAVE)\r\nrole:slave\r\n"; break;
        case PIKA_ROLE_MASTER | PIKA_ROLE_SLAVE : tmp_stream << "MASTER/SLAVE)\r\nrole:slave\r\n"; break;
        default: info.append("ERR: server role is error\r\n"); return;
    }

    std::string slaves_list_str;
    //int32_t slaves_num = g_pika_server->GetSlaveListString(slaves_list_str);
    switch (host_role) {
        case PIKA_ROLE_SLAVE :
            tmp_stream << "master_host:" << g_pika_server->master_ip() << "\r\n";
            tmp_stream << "master_port:" << g_pika_server->master_port() << "\r\n";
            tmp_stream << "master_link_status:" << (g_pika_server->repl_state() == PIKA_REPL_CONNECTED ? "up" : "down") << "\r\n";
            if(g_pika_server->repl_state() != PIKA_REPL_CONNECTED){
                time_t now = time(NULL);
                tmp_stream << "master_link_down_since_seconds:" << now - g_pika_server->repl_down_since() << "\r\n";
            }
            tmp_stream << "slave_priority:" << g_pika_conf->slave_priority() << "\r\n";
            tmp_stream << "slave_read_only:" << g_pika_conf->readonly() << "\r\n";
            tmp_stream << "repl_state: " << (g_pika_server->repl_state()) << "\r\n";
            break;
        case PIKA_ROLE_MASTER | PIKA_ROLE_SLAVE :
            tmp_stream << "master_host:" << g_pika_server->master_ip() << "\r\n";
            tmp_stream << "master_port:" << g_pika_server->master_port() << "\r\n";
            tmp_stream << "master_link_status:" << (g_pika_server->repl_state() == PIKA_REPL_CONNECTED ? "up" : "down") << "\r\n";
            if(g_pika_server->repl_state() != PIKA_REPL_CONNECTED){
                time_t now = time(NULL);
                tmp_stream << "master_link_down_since_seconds:" << now - g_pika_server->repl_down_since() << "\r\n";
            }
            tmp_stream << "slave_read_only:" << g_pika_conf->readonly() << "\r\n";
            tmp_stream << "repl_state: " << (g_pika_server->repl_state()) << "\r\n";
        case PIKA_ROLE_SINGLE :
        case PIKA_ROLE_MASTER :
            tmp_stream << "connected_slaves:" << g_pika_server->GetSlaveListString(slaves_list_str) << "\r\n" << slaves_list_str;
    }

    info.append(tmp_stream.str());
}

void InfoCmd::InfoKeyspace(std::string &info) {
    if (off_) {
        g_pika_server->StopKeyScan();
        off_ = false;
        return;
    }

    PikaServer::KeyScanInfo key_scan_info = g_pika_server->key_scan_info();
    std::vector<uint64_t> &key_nums_v = key_scan_info.key_nums_v;
    if (key_scan_info.key_nums_v.size() != 6) {
        info.append("info keyspace error\r\n");
        return;
    }
    std::stringstream tmp_stream;
    tmp_stream << "# Keyspace\r\n";
    tmp_stream << "db0:keys=" << (key_nums_v[0] + key_nums_v[1] + key_nums_v[2] + key_nums_v[3] + key_nums_v[4] + key_nums_v[5]) << "\r\n";
    tmp_stream << "# Time:" << key_scan_info.s_start_time << "\r\n";
    tmp_stream << "kv keys:" << key_nums_v[0] << "\r\n";
    tmp_stream << "hash keys:" << key_nums_v[1] << "\r\n";
    tmp_stream << "list keys:" << key_nums_v[2] << "\r\n";
    tmp_stream << "zset keys:" << key_nums_v[3] << "\r\n";
    tmp_stream << "set keys:" << key_nums_v[4] << "\r\n";
    tmp_stream << "ehash keys:" << key_nums_v[5] << "\r\n";
    info.append(tmp_stream.str());

    if (rescan_) {
        g_pika_server->KeyScan();
    }
    return;
}

void InfoCmd::InfoLog(std::string &info) {

    std::stringstream  tmp_stream;
    tmp_stream << "# Log" << "\r\n";
    uint32_t purge_max;
    int64_t log_size = g_pika_server->log_size_;

    tmp_stream << "log_size:" << log_size << "\r\n";
    tmp_stream << "log_size_human:" << (log_size >> 20) << "M\r\n";
    tmp_stream << "safety_purge:" << (g_pika_server->GetPurgeWindow(purge_max) ?
            kBinlogPrefix + std::to_string(static_cast<int32_t>(purge_max)) : "none") << "\r\n";
    tmp_stream << "expire_logs_days:" << g_pika_conf->expire_logs_days() << "\r\n";
    tmp_stream << "expire_logs_nums:" << g_pika_conf->expire_logs_nums() << "\r\n";
    uint32_t filenum;
    uint64_t offset;
    g_pika_server->logger_->GetProducerStatus(&filenum, &offset);
    tmp_stream << "binlog_offset:" << filenum << " " << offset << "\r\n";

    info.append(tmp_stream.str());
    return;
}

void InfoCmd::InfoData(std::string &info) {

    std::stringstream tmp_stream;
    int64_t db_size = g_pika_server->db_size_;
    // rocksdb related memory usage
    uint64_t memtable_usage = g_pika_server->memtable_usage_;
    uint64_t table_reader_usage = g_pika_server->table_reader_usage_;
    uint64_t cache_usage = g_pika_server->cache_usage_;

    tmp_stream << "# Data" << "\r\n";
    tmp_stream << "db_size:" << db_size << "\r\n";
    tmp_stream << "db_size_human:" << (db_size >> 20) << "M\r\n";
    tmp_stream << "compression:" << g_pika_conf->compression() << "\r\n";

    tmp_stream << "used_memory:" << (memtable_usage + table_reader_usage + cache_usage) << "\r\n";
    tmp_stream << "used_memory_human:" << ((memtable_usage + table_reader_usage + cache_usage) >> 20) << "M\r\n";
    tmp_stream << "db_memtable_usage:" << memtable_usage << "\r\n";
    tmp_stream << "db_tablereader_usage:" << table_reader_usage << "\r\n";
    tmp_stream << "cache_usage:" << cache_usage << "\r\n";

    info.append(tmp_stream.str());
    return;
}

void InfoCmd::InfoCache(std::string &info)
{
    std::stringstream tmp_stream;
    tmp_stream << "# Cache" << "\r\n";
    if (PIKA_CACHE_NONE == g_pika_conf->cache_model()) {
        tmp_stream << "cache_status:Disable" << "\r\n";
    } else {
        PikaServer::DisplayCacheInfo cache_info;
        g_pika_server->GetCacheInfo(cache_info);
        tmp_stream << "cache_status:" << CacheStatusToString(cache_info.status) << "\r\n";
        tmp_stream << "cache_db_num:" << cache_info.cache_num << "\r\n";
        tmp_stream << "cache_keys:" << cache_info.keys_num << "\r\n";
        tmp_stream << "cache_memory:" << cache_info.used_memory << "\r\n";
        tmp_stream << "cache_memory_human:" << (cache_info.used_memory >> 20) << "M\r\n";
        tmp_stream << "hits:" << cache_info.hits << "\r\n";
        tmp_stream << "all_cmds:" << cache_info.hits + cache_info.misses << "\r\n";
        tmp_stream << "hits_per_sec:" << cache_info.hits_per_sec << "\r\n";
        tmp_stream << "read_cmd_per_sec:" << cache_info.read_cmd_per_sec << "\r\n";
        tmp_stream << "hitratio_per_sec:" << std::setprecision(4) << cache_info.hitratio_per_sec << "%" <<"\r\n";
        tmp_stream << "hitratio_all:" << std::setprecision(4) << cache_info.hitratio_all << "%" <<"\r\n";
        tmp_stream << "load_keys_per_sec:" << cache_info.load_keys_per_sec << "\r\n";
        tmp_stream << "waitting_load_keys_num:" << cache_info.waitting_load_keys_num << "\r\n";  
    }

    info.append(tmp_stream.str());
}

void InfoCmd::InfoZset(std::string &info) {
    if (g_pika_server->is_slave()
        || 0 == g_pika_conf->zset_auto_del_threshold()) {
        return;
    }

    std::stringstream tmp_stream;
    tmp_stream << "# Zset auto delete" << "\r\n";
    ZsetInfo zset_info;
    g_pika_server->GetZsetInfo(zset_info);
    tmp_stream << "last_finish_time:" << PikaCommonFunc::TimestampToDate(zset_info.last_finish_time) << "\r\n";
    tmp_stream << "last_spend_time:" << zset_info.last_spend_time << "\r\n";
    tmp_stream << "last_all_keys_num:" << zset_info.last_all_keys_num << "\r\n";
    tmp_stream << "last_del_keys_num:" << zset_info.last_del_keys_num << "\r\n";
    tmp_stream << "last_compact_zset_db:" << (zset_info.last_compact_zset_db ? "Yes" : "No") << "\r\n";
    tmp_stream << "current_task_type:" << TaskTypeToString(zset_info.current_task_type) << "\r\n";
    if (zset_info.current_task_type != ZSET_NO_TASK) {
        tmp_stream << "current_task_start_time:" << PikaCommonFunc::TimestampToDate(zset_info.current_task_start_time) << "\r\n";
        tmp_stream << "current_task_spend_time:" << zset_info.current_task_spend_time << "\r\n";
    } else {
        tmp_stream << "current_task_start_time:" << 0 << "\r\n";
        tmp_stream << "current_task_spend_time:" << 0 << "\r\n";  
    }
    tmp_stream << "current_cursor:" << zset_info.current_cursor << "\r\n";
    
    info.append(tmp_stream.str());
}

std::string InfoCmd::CacheStatusToString(int status)
{
    switch (status) {
        case PIKA_CACHE_STATUS_NONE:
            return std::string("None");
        case PIKA_CACHE_STATUS_OK:
            return std::string("Ok");
        case PIKA_CACHE_STATUS_INIT:
            return std::string("Init");
        case PIKA_CACHE_STATUS_RESET:
            return std::string("Reset");
        case PIKA_CACHE_STATUS_DESTROY:
            return std::string("Destroy");
        case PIKA_CACHE_STATUS_CLEAR:
            return std::string("Clear");
        default:
            return std::string("Unknown");
    }
}

std::string InfoCmd::TaskTypeToString(int task_type) {
    switch (task_type) {
        case ZSET_CRON_TASK:
            return std::string("CRON_TASK");
        case ZSET_MANUAL_TASK:
            return std::string("MANUAL_TASK");
        default:
            return std::string("NO_TASK");
    }
}

void InfoCmd::InfoDelay(std::string &info)
{
    std::stringstream tmp_stream;
    if (interval_ < 0) {
        tmp_stream << "invalid argument for 'info delay'" << "\r\n";
        return; 
    } else if (interval_ >= 0) {
        tmp_stream << g_pika_server->GetCmdStats()->GetCmdStatsByInterval(interval_) << "\r\n";
    }
    
    info.append(tmp_stream.str());
}

void ConfigCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameConfig);
        return;
    }
    size_t argc = argv.size();
    if (!strcasecmp(argv[1].data(), "get")) {
        if (argc != 3) {
            res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG get");
            return;
        }
    } else if (!strcasecmp(argv[1].data(), "set")) {
        if (argc == 3 && argv[2] != "*") {
            res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG set");
            return;
        } else if (argc != 4 && argc != 3) {
            res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG set");
            return;
        }
    } else if (!strcasecmp(argv[1].data(), "rewrite")) {
        if (argc != 2) {
            res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG rewrite");
            return;
        }
    } else if (!strcasecmp(argv[1].data(), "resetstat")) {
        if (argc != 2) {
            res_.SetRes(CmdRes::kErrOther, "Wrong number of arguments for CONFIG resetstat");
            return;
        }
    } else {
        res_.SetRes(CmdRes::kErrOther, "CONFIG subcommand must be one of GET, SET, RESETSTAT, REWRITE");
        return;
    }
    config_args_v_.assign(argv.begin()+1, argv.end());
    return;
}

void ConfigCmd::Do() {
    std::string config_ret;
    if (!strcasecmp(config_args_v_[0].data(), "get")) {
        ConfigGet(config_ret);
    } else if (!strcasecmp(config_args_v_[0].data(), "set")) {
        ConfigSet(config_ret);
    } else if (!strcasecmp(config_args_v_[0].data(), "rewrite")) {
        ConfigRewrite(config_ret);
    } else if (!strcasecmp(config_args_v_[0].data(), "resetstat")) {
        ConfigResetstat(config_ret);
    }
    res_.AppendStringRaw(config_ret);
    return;
}

static void EncodeString(std::string *dst, const std::string &value) {
    dst->append("$");
    dst->append(std::to_string(value.size()));
    dst->append("\r\n");
    dst->append(value.data(), value.size());
    dst->append("\r\n");
}

static void EncodeInt32(std::string *dst, const int32_t v) {
    std::string vstr = std::to_string(v);
    dst->append("$");
    dst->append(std::to_string(vstr.length()));
    dst->append("\r\n");
    dst->append(vstr);
    dst->append("\r\n");
}

static void EncodeInt64(std::string *dst, const int64_t v) {
    std::string vstr = std::to_string(v);
    dst->append("$");
    dst->append(std::to_string(vstr.length()));
    dst->append("\r\n");
    dst->append(vstr);
    dst->append("\r\n");
}

static void EncodeDouble(std::string *dst, const double v) {
    std::string vstr = std::to_string(v);
    dst->append("$");
    dst->append(std::to_string(vstr.length()));
    dst->append("\r\n");
    dst->append(vstr);
    dst->append("\r\n");
}

void ConfigCmd::ConfigGet(std::string &ret) {
    size_t elements = 0;
    std::string config_body;
    std::string pattern = config_args_v_[1];

    if (slash::stringmatch(pattern.data(), "port", 1)) {
        elements += 2;
        EncodeString(&config_body, "port");
        EncodeInt32(&config_body, g_pika_conf->port());
    }

    if (slash::stringmatch(pattern.data(), "thread-num", 1)) {
        elements += 2;
        EncodeString(&config_body, "thread-num");
        EncodeInt32(&config_body, g_pika_conf->thread_num());
    }

    if (slash::stringmatch(pattern.data(), "sync-thread-num", 1)) {
        elements += 2;
        EncodeString(&config_body, "sync-thread-num");
        EncodeInt32(&config_body, g_pika_conf->sync_thread_num());
    }

    if (slash::stringmatch(pattern.data(), "sync-buffer-size", 1)) {
        elements += 2;
        EncodeString(&config_body, "sync-buffer-size");
        EncodeInt32(&config_body, g_pika_conf->sync_buffer_size());
    }

    if (slash::stringmatch(pattern.data(), "log-path", 1)) {
        elements += 2;
        EncodeString(&config_body, "log-path");
        EncodeString(&config_body, g_pika_conf->log_path());
    }

    if (slash::stringmatch(pattern.data(), "loglevel", 1)) {
        elements += 2;
        EncodeString(&config_body, "loglevel");
        EncodeInt32(&config_body, g_pika_conf->log_level());
    }

    if (slash::stringmatch(pattern.data(), "max-log-size", 1)) {
        elements += 2;
        EncodeString(&config_body, "max-log-size");
        EncodeInt32(&config_body, g_pika_conf->max_log_size());
    }

    if (slash::stringmatch(pattern.data(), "db-path", 1)) {
        elements += 2;
        EncodeString(&config_body, "db-path");
        EncodeString(&config_body, g_pika_conf->db_path());
    }

    if (slash::stringmatch(pattern.data(), "db-sync-path", 1)) {
        elements += 2;
        EncodeString(&config_body, "db-sync-path");
        EncodeString(&config_body, g_pika_conf->db_sync_path());
    }

    if (slash::stringmatch(pattern.data(), "db-sync-speed", 1)) {
        elements += 2;
        EncodeString(&config_body, "db-sync-speed");
        EncodeInt32(&config_body, g_pika_conf->db_sync_speed());
    }

    if (slash::stringmatch(pattern.data(), "compact-cron", 1)) {
        elements += 2;
        EncodeString(&config_body, "compact-cron");
        EncodeString(&config_body, g_pika_conf->compact_cron());
    }

    if (slash::stringmatch(pattern.data(), "compact-interval", 1)) {
        elements += 2;
        EncodeString(&config_body, "compact-interval");
        EncodeString(&config_body, g_pika_conf->compact_interval());
    }

    if (slash::stringmatch(pattern.data(), "maxmemory", 1)) {
        elements += 2;
        EncodeString(&config_body, "maxmemory");
        EncodeInt64(&config_body, g_pika_server->db_size_);
    }

    if (slash::stringmatch(pattern.data(), "write-buffer-size", 1)) {
        elements += 2;
        EncodeString(&config_body, "write-buffer-size");
        EncodeInt64(&config_body, g_pika_conf->write_buffer_size());
    }

    if (slash::stringmatch(pattern.data(), "max-write-buffer-number", 1)) {
        elements += 2;
        EncodeString(&config_body, "max-write-buffer-number");
        EncodeInt32(&config_body, g_pika_conf->max_write_buffer_number());
    }

    if (slash::stringmatch(pattern.data(), "timeout", 1)) {
        elements += 2;
        EncodeString(&config_body, "timeout");
        EncodeInt32(&config_body, g_pika_conf->timeout());
    }

    if (slash::stringmatch(pattern.data(), "fresh-info-interval", 1)) {
        elements += 2;
        EncodeString(&config_body, "fresh-info-interval");
        EncodeInt32(&config_body, g_pika_conf->fresh_info_interval());
    }

    if (slash::stringmatch(pattern.data(), "requirepass", 1)) {
        elements += 2;
        EncodeString(&config_body, "requirepass");
        EncodeString(&config_body, g_pika_conf->requirepass());
    }

    if (slash::stringmatch(pattern.data(), "masterauth", 1)) {
        elements += 2;
        EncodeString(&config_body, "masterauth");
        EncodeString(&config_body, g_pika_conf->masterauth());
    }

    if (slash::stringmatch(pattern.data(), "userpass", 1)) {
        elements += 2;
        EncodeString(&config_body, "userpass");
        EncodeString(&config_body, g_pika_conf->userpass());
    }

    if (slash::stringmatch(pattern.data(), "userblacklist", 1)) {
        elements += 2;
        EncodeString(&config_body, "userblacklist");
        EncodeString(&config_body, g_pika_conf->suser_blacklist());
    }

    if (slash::stringmatch(pattern.data(), "dump-prefix", 1)) {
        elements += 2;
        EncodeString(&config_body, "dump-prefix");
        EncodeString(&config_body, g_pika_conf->bgsave_prefix());
    }

    if (slash::stringmatch(pattern.data(), "daemonize", 1)) {
        elements += 2;
        EncodeString(&config_body, "daemonize");
        EncodeString(&config_body, g_pika_conf->daemonize() ? "yes" : "no");
    }

    if (slash::stringmatch(pattern.data(), "slotmigrate", 1)) {
        elements += 2;
        EncodeString(&config_body, "slotmigrate");
        EncodeString(&config_body, g_pika_conf->slotmigrate() ? "yes" : "no");
    }

    if (slash::stringmatch(pattern.data(), "slotmigrate-thread-num", 1)) {
        elements += 2;
        EncodeString(&config_body, "slotmigrate-thread-num");
        EncodeInt32(&config_body, g_pika_conf->slotmigrate_thread_num());
    }

    if (slash::stringmatch(pattern.data(), "thread-migrate-keys-num", 1)) {
        elements += 2;
        EncodeString(&config_body, "thread-migrate-keys-num");
        EncodeInt32(&config_body, g_pika_conf->thread_migrate_keys_num());
    }

   if (slash::stringmatch(pattern.data(), "dump-path", 1)) {
        elements += 2;
        EncodeString(&config_body, "dump-path");
        EncodeString(&config_body, g_pika_conf->bgsave_path());
    }

    if (slash::stringmatch(pattern.data(), "dump-expire", 1)) {
        elements += 2;
        EncodeString(&config_body, "dump-expire");
        EncodeInt32(&config_body, g_pika_conf->expire_dump_days());
    }

    if (slash::stringmatch(pattern.data(), "pidfile", 1)) {
        elements += 2;
        EncodeString(&config_body, "pidfile");
        EncodeString(&config_body, g_pika_conf->pidfile());
    }

    if (slash::stringmatch(pattern.data(), "maxclients", 1)) {
        elements += 2;
        EncodeString(&config_body, "maxclients");
        EncodeInt32(&config_body, g_pika_conf->maxclients());
    }

    if (slash::stringmatch(pattern.data(), "target-file-size-base", 1)) {
        elements += 2;
        EncodeString(&config_body, "target-file-size-base");
        EncodeInt32(&config_body, g_pika_conf->target_file_size_base());
    }

    if (slash::stringmatch(pattern.data(), "max-bytes-for-level-base", 1)) {
        elements += 2;
        EncodeString(&config_body, "max-bytes-for-level-base");
        EncodeInt32(&config_body, g_pika_conf->max_bytes_for_level_base());
    }

    if (slash::stringmatch(pattern.data(), "max-background-flushes", 1)) {
        elements += 2;
        EncodeString(&config_body, "max-background-flushes");
        EncodeInt32(&config_body, g_pika_conf->max_background_flushes());
    }

    if (slash::stringmatch(pattern.data(), "max-background-compactions", 1)) {
        elements += 2;
        EncodeString(&config_body, "max-background-compactions");
        EncodeInt32(&config_body, g_pika_conf->max_background_compactions());
    }

    if (slash::stringmatch(pattern.data(), "max-cache-files", 1)) {
        elements += 2;
        EncodeString(&config_body, "max-cache-files");
        EncodeInt32(&config_body, g_pika_conf->max_cache_files());
    }

    if (slash::stringmatch(pattern.data(), "max-bytes-for-level-multiplier", 1)) {
        elements += 2;
        EncodeString(&config_body, "max-bytes-for-level-multiplier");
        EncodeInt32(&config_body, g_pika_conf->max_bytes_for_level_multiplier());
    }

    if (slash::stringmatch(pattern.data(), "disable-auto-compactions", 1)) {
        elements += 2;
        EncodeString(&config_body, "disable-auto-compactions");
        EncodeInt32(&config_body, g_pika_conf->disable_auto_compactions());
    }

    if (slash::stringmatch(pattern.data(), "block-size", 1)) {
        elements += 2;
        EncodeString(&config_body, "block-size");
        EncodeInt32(&config_body, g_pika_conf->block_size());
    }

    if (slash::stringmatch(pattern.data(), "block-cache", 1)) {
        elements += 2;
        EncodeString(&config_body, "block-cache");
        EncodeInt64(&config_body, g_pika_conf->block_cache());
    }

    if (slash::stringmatch(pattern.data(), "share-block-cache", 1)) {
        elements += 2;
        EncodeString(&config_body, "share-block-cache");
        EncodeString(&config_body, g_pika_conf->share_block_cache() ? "yes" : "no");
    }

    if (slash::stringmatch(pattern.data(), "cache-index-and-filter-blocks", 1)) {
        elements += 2;
        EncodeString(&config_body, "cache-index-and-filter-blocks");
        EncodeString(&config_body, g_pika_conf->cache_index_and_filter_blocks() ? "yes" : "no");
    }

    if (slash::stringmatch(pattern.data(), "optimize-filters-for-hits", 1)) {
        elements += 2;
        EncodeString(&config_body, "optimize-filters-for-hits");
        EncodeString(&config_body, g_pika_conf->optimize_filters_for_hits() ? "yes" : "no");
    }

    if (slash::stringmatch(pattern.data(), "level-compaction-dynamic-level-bytes", 1)) {
        elements += 2;
        EncodeString(&config_body, "level-compaction-dynamic-level-bytes");
        EncodeString(&config_body, g_pika_conf->level_compaction_dynamic_level_bytes() ? "yes" : "no");
    }

    if (slash::stringmatch(pattern.data(), "max-subcompactions", 1)) {
        elements += 2;
        EncodeString(&config_body, "max-subcompactions");
        EncodeInt32(&config_body, g_pika_conf->max_subcompactions());
    }

    if (slash::stringmatch(pattern.data(), "expire-logs-days", 1)) {
        elements += 2;
        EncodeString(&config_body, "expire-logs-days");
        EncodeInt32(&config_body, g_pika_conf->expire_logs_days());
    }

    if (slash::stringmatch(pattern.data(), "expire-logs-nums", 1)) {
        elements += 2;
        EncodeString(&config_body, "expire-logs-nums");
        EncodeInt32(&config_body, g_pika_conf->expire_logs_nums());
    }

    if (slash::stringmatch(pattern.data(), "write-binlog", 1)) {
        elements += 2;
        EncodeString(&config_body, "write-binlog");
        EncodeString(&config_body, g_pika_conf->write_binlog() ? "yes" : "no");
    }

    if (slash::stringmatch(pattern.data(), "binlog-writer-queue-size", 1)) {
        elements += 2;
        EncodeString(&config_body, "binlog-writer-queue-size");
        EncodeInt32(&config_body, g_pika_conf->binlog_writer_queue_size());
    }

    if (slash::stringmatch(pattern.data(), "binlog-writer-method", 1)) {
        elements += 2;
        EncodeString(&config_body, "binlog-writer-method");
        EncodeString(&config_body, g_pika_conf->binlog_writer_method());
    }

    if (slash::stringmatch(pattern.data(), "binlog-writer-num", 1)) {
        elements += 2;
        EncodeString(&config_body, "binlog-writer-num");
        EncodeInt32(&config_body, g_pika_conf->binlog_writer_num());
    }

    if (slash::stringmatch(pattern.data(), "root-connection-num", 1)) {
        elements += 2;
        EncodeString(&config_body, "root-connection-num");
        EncodeInt32(&config_body, g_pika_conf->root_connection_num());
    }

    if (slash::stringmatch(pattern.data(), "slowlog-log-slower-than", 1)) {
        elements += 2;
        EncodeString(&config_body, "slowlog-log-slower-than");
        EncodeInt32(&config_body, g_pika_conf->slowlog_slower_than());
    }

    if (slash::stringmatch(pattern.data(), "slowlog-max-len", 1)) {
        elements += 2;
        EncodeString(&config_body, "slowlog-max-len");
        EncodeInt32(&config_body, g_pika_conf->slowlog_max_len());
    }

    if (slash::stringmatch(pattern.data(), "binlog-file-size", 1)) {
        elements += 2;
        EncodeString(&config_body, "binlog-file-size");
        EncodeInt32(&config_body, g_pika_conf->binlog_file_size());
    }

    if (slash::stringmatch(pattern.data(), "compression", 1)) {
        elements += 2;
        EncodeString(&config_body, "compression");
        EncodeString(&config_body, g_pika_conf->compression());
    }

    if (slash::stringmatch(pattern.data(), "slave-read-only", 1)) {
        elements += 2;
        EncodeString(&config_body, "slave-read-only");
        EncodeString(&config_body, g_pika_conf->readonly() ? "yes" : "no");
    }

    if (slash::stringmatch(pattern.data(), "slaveof", 1)) {
        elements += 2;
        EncodeString(&config_body, "slaveof");
        EncodeString(&config_body, g_pika_conf->slaveof());
    }

    if (slash::stringmatch(pattern.data(), "level0-file-num-compaction-trigger", 1)) {
        elements += 2;
        EncodeString(&config_body, "level0-file-num-compaction-trigger");
        EncodeInt32(&config_body, g_pika_conf->level0_file_num_compaction_trigger());
    }

    if (slash::stringmatch(pattern.data(), "level0-stop-writes-trigger", 1)) {
        elements += 2;
        EncodeString(&config_body, "level0-stop-writes-trigger");
        EncodeInt32(&config_body, g_pika_conf->level0_stop_writes_trigger());
    }

    if (slash::stringmatch(pattern.data(), "level0-slowdown-writes-trigger", 1)) {
        elements += 2;
        EncodeString(&config_body, "level0-slowdown-writes-trigger");
        EncodeInt32(&config_body, g_pika_conf->level0_slowdown_writes_trigger());
    }

    if (slash::stringmatch(pattern.data(), "slave-priority", 1)) {
        elements += 2;
        EncodeString(&config_body, "slave-priority");
        EncodeInt32(&config_body, g_pika_conf->slave_priority());
    }

    if (slash::stringmatch(pattern.data(), "cache-num", 1)) {
        elements += 2;
        EncodeString(&config_body, "cache-num");
        EncodeInt32(&config_body, g_pika_conf->cache_num());
    }

    if (slash::stringmatch(pattern.data(), "cache-model", 1)) {
        elements += 2;
        EncodeString(&config_body, "cache-model");
        EncodeInt32(&config_body, g_pika_conf->cache_model());
    }

    if (slash::stringmatch(pattern.data(), "cache-maxmemory", 1)) {
        elements += 2;
        EncodeString(&config_body, "cache-maxmemory");
        EncodeInt64(&config_body, g_pika_conf->cache_maxmemory());
    }

    if (slash::stringmatch(pattern.data(), "cache-maxmemory-policy", 1)) {
        elements += 2;
        EncodeString(&config_body, "cache-maxmemory-policy");
        EncodeInt32(&config_body, g_pika_conf->cache_maxmemory_policy());
    }

    if (slash::stringmatch(pattern.data(), "cache-maxmemory-samples", 1)) {
        elements += 2;
        EncodeString(&config_body, "cache-maxmemory-samples");
        EncodeInt32(&config_body, g_pika_conf->cache_maxmemory_samples());
    }

    if (slash::stringmatch(pattern.data(), "cache-lfu-decay-time", 1)) {
        elements += 2;
        EncodeString(&config_body, "cache-lfu-decay-time");
        EncodeInt32(&config_body, g_pika_conf->cache_lfu_decay_time());
    }

    if (slash::stringmatch(pattern.data(), "min-blob-size", 1)) {
        elements += 2;
        EncodeString(&config_body, "min-blob-size");
        EncodeInt64(&config_body, g_pika_conf->min_blob_size());
    }

    if (slash::stringmatch(pattern.data(), "rate-bytes-per-sec", 1)) {
        elements += 2;
        EncodeString(&config_body, "rate-bytes-per-sec");
        EncodeInt64(&config_body, g_pika_conf->rate_bytes_per_sec());
    }

    if (slash::stringmatch(pattern.data(), "disable-wal", 1)) {
        elements += 2;
        EncodeString(&config_body, "disable-wal");
        EncodeString(&config_body, g_pika_conf->disable_wal() ? "yes" : "no");
    }

    if (slash::stringmatch(pattern.data(), "use-direct-reads", 1)) {
        elements += 2;
        EncodeString(&config_body, "use-direct-reads");
        EncodeString(&config_body, g_pika_conf->use_direct_reads() ? "yes" : "no");
    }

    if (slash::stringmatch(pattern.data(), "use-direct-io-for-flush-and-compaction", 1)) {
        elements += 2;
        EncodeString(&config_body, "use-direct-io-for-flush-and-compaction");
        EncodeString(&config_body, g_pika_conf->use_direct_io_for_flush_and_compaction() ? "yes" : "no");
    }

    if (slash::stringmatch(pattern.data(), "check-free-mem-interval", 1)) {
        elements += 2;
        EncodeString(&config_body, "check-free-mem-interval");
        EncodeInt32(&config_body, g_pika_conf->check_free_mem_interval());
    }

    if (slash::stringmatch(pattern.data(), "min-system-free-mem", 1)) {
        elements += 2;
        EncodeString(&config_body, "min-system-free-mem");
        EncodeInt64(&config_body, g_pika_conf->min_system_free_mem());
    }

    if (slash::stringmatch(pattern.data(), "optimize-min-free-kbytes", 1)) {
        elements += 2;
        EncodeString(&config_body, "optimize-min-free-kbytes");
        EncodeString(&config_body, g_pika_conf->optimize_min_free_kbytes() ? "yes" : "no");
    }

    if (slash::stringmatch(pattern.data(), "max-gc-batch-size", 1)) {
        elements += 2;
        EncodeString(&config_body, "max-gc-batch-size");
        EncodeInt64(&config_body, g_pika_conf->max_gc_batch_size());
    }

    if (slash::stringmatch(pattern.data(), "blob-file-discardable-ratio", 1)) {
        elements += 2;
        EncodeString(&config_body, "blob-file-discardable-ratio");
        EncodeInt32(&config_body, g_pika_conf->blob_file_discardable_ratio());
    }

    if (slash::stringmatch(pattern.data(), "gc-sample-cycle", 1)) {
        elements += 2;
        EncodeString(&config_body, "gc-sample-cycle");
        EncodeInt64(&config_body, g_pika_conf->gc_sample_cycle());
    }

    if (slash::stringmatch(pattern.data(), "max-gc-queue-size", 1)) {
        elements += 2;
        EncodeString(&config_body, "max-gc-queue-size");
        EncodeInt32(&config_body, g_pika_conf->max_gc_queue_size());
    }

    if (slash::stringmatch(pattern.data(), "zset-auto-del-threshold", 1)) {
        elements += 2;
        EncodeString(&config_body, "zset-auto-del-threshold");
        EncodeInt32(&config_body, g_pika_conf->zset_auto_del_threshold());
    }

    if (slash::stringmatch(pattern.data(), "zset-auto-del-direction", 1)) {
        elements += 2;
        EncodeString(&config_body, "zset-auto-del-direction");
        EncodeInt32(&config_body, g_pika_conf->zset_auto_del_direction());
    }

    if (slash::stringmatch(pattern.data(), "zset-auto-del-num", 1)) {
        elements += 2;
        EncodeString(&config_body, "zset-auto-del-num");
        EncodeInt32(&config_body, g_pika_conf->zset_auto_del_num());
    }

    if (slash::stringmatch(pattern.data(), "zset-auto-del-cron", 1)) {
        elements += 2;
        EncodeString(&config_body, "zset-auto-del-cron");
        EncodeString(&config_body, g_pika_conf->zset_auto_del_cron());
    }

    if (slash::stringmatch(pattern.data(), "zset-auto-del-interval", 1)) {
        elements += 2;
        EncodeString(&config_body, "zset-auto-del-interval");
        EncodeInt32(&config_body, g_pika_conf->zset_auto_del_interval());
    }

    if (slash::stringmatch(pattern.data(), "zset-auto-del-cron-speed-factor", 1)) {
        elements += 2;
        EncodeString(&config_body, "zset-auto-del-cron-speed-factor");
        EncodeDouble(&config_body, g_pika_conf->zset_auto_del_cron_speed_factor());
    }

    if (slash::stringmatch(pattern.data(), "zset-auto-del-scan-round-num", 1)) {
        elements += 2;
        EncodeString(&config_body, "zset-auto-del-scan-round-num");
        EncodeInt32(&config_body, g_pika_conf->zset_auto_del_scan_round_num());
    }

    if (slash::stringmatch(pattern.data(), "use-thread-pool", 1)) {
        elements += 2;
        EncodeString(&config_body, "use-thread-pool");
        EncodeString(&config_body, g_pika_conf->use_thread_pool() ? "yes" : "no");
    }

    if (slash::stringmatch(pattern.data(), "fast-thread-pool-size", 1)) {
        elements += 2;
        EncodeString(&config_body, "fast-thread-pool-size");
        EncodeInt32(&config_body, g_pika_conf->fast_thread_pool_size());
    }

    if (slash::stringmatch(pattern.data(), "slow-thread-pool-size", 1)) {
        elements += 2;
        EncodeString(&config_body, "slow-thread-pool-size");
        EncodeInt32(&config_body, g_pika_conf->slow_thread_pool_size());
    }

    if (slash::stringmatch(pattern.data(), "slow-cmd-list", 1)) {
        elements += 2;
        EncodeString(&config_body, "slow-cmd-list");
        EncodeString(&config_body, g_pika_conf->slow_cmd_list());
    }

    std::stringstream resp;
    resp << "*" << std::to_string(elements) << "\r\n" << config_body;
    ret = resp.str();
}

void ConfigCmd::ConfigSet(std::string& ret) {
    std::string set_item = config_args_v_[1];
    if (set_item == "*") {
        ret = "*54\r\n";
        EncodeString(&ret, "loglevel");
        EncodeString(&ret, "max-log-size");
        EncodeString(&ret, "timeout");
        EncodeString(&ret, "fresh-info-interval");
        EncodeString(&ret, "requirepass");
        EncodeString(&ret, "masterauth");
        EncodeString(&ret, "slotmigrate");
        EncodeString(&ret, "userpass");
        EncodeString(&ret, "userblacklist");
        EncodeString(&ret, "dump-prefix");
        EncodeString(&ret, "maxclients");
        EncodeString(&ret, "dump-expire");
        EncodeString(&ret, "expire-logs-days");
        EncodeString(&ret, "expire-logs-nums");
        EncodeString(&ret, "write-binlog");
        EncodeString(&ret, "binlog-writer-queue-size");
        EncodeString(&ret, "root-connection-num");
        EncodeString(&ret, "slowlog-log-slower-than");
        EncodeString(&ret, "slave-read-only");
        EncodeString(&ret, "db-sync-speed");
        EncodeString(&ret, "compact-cron");
        EncodeString(&ret, "compact-interval");
        EncodeString(&ret, "write-buffer-size");
        EncodeString(&ret, "target-file-size-base");
        EncodeString(&ret, "max-bytes-for-level-base");
        EncodeString(&ret, "max-write-buffer-number");
        EncodeString(&ret, "disable-auto-compactions");
        EncodeString(&ret, "level0-file-num-compaction-trigger");
        EncodeString(&ret, "level0-slowdown-writes-trigger");
        EncodeString(&ret, "level0-stop-writes-trigger");
        EncodeString(&ret, "slave-priority");
        EncodeString(&ret, "cache-num");
        EncodeString(&ret, "cache-model");
        EncodeString(&ret, "cache-maxmemory");
        EncodeString(&ret, "cache-maxmemory-policy");
        EncodeString(&ret, "cache-maxmemory-samples");
        EncodeString(&ret, "cache-lfu-decay-time");
        EncodeString(&ret, "rate-bytes-per-sec");
        EncodeString(&ret, "disable-wal");
        EncodeString(&ret, "check-free-mem-interval");
        EncodeString(&ret, "min-system-free-mem");
        EncodeString(&ret, "optimize-min-free-kbytes");
        EncodeString(&ret, "max-gc-batch-size");
        EncodeString(&ret, "blob-file-discardable-ratio");
        EncodeString(&ret, "gc-sample-cycle");
        EncodeString(&ret, "max-gc-queue-size");
        EncodeString(&ret, "zset-auto-del-threshold");
        EncodeString(&ret, "zset-auto-del-direction");
        EncodeString(&ret, "zset-auto-del-num");
        EncodeString(&ret, "zset-auto-del-cron");
        EncodeString(&ret, "zset-auto-del-interval");
        EncodeString(&ret, "zset-auto-del-cron-speed-factor");
        EncodeString(&ret, "zset-auto-del-scan-round-num");
        EncodeString(&ret, "slow-cmd-list");
        return;
    }
    std::string value = config_args_v_[2];
    long int ival;
    if (set_item == "loglevel") {
        slash::StringToLower(value);
        if (value == "info") {
            ival = 0;
        } else if (value == "error") {
            ival = 1;
        } else {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'loglevel'\r\n";
            return;
        }
        g_pika_conf->SetLogLevel(ival);
        FLAGS_minloglevel = g_pika_conf->log_level();
        ret = "+OK\r\n";
    } else if (set_item == "max-log-size") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival <= 0 ) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'max-log-size'\r\n";
            return;
        }
        g_pika_conf->SetMaxLogSize(ival);
        FLAGS_max_log_size = ival;
        ret = "+OK\r\n";
    } else if (set_item == "timeout") {
        if (!slash::string2l(value.data(), value.size(), &ival)) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'timeout'\r\n";
            return;
        }
        g_pika_conf->SetTimeout(ival);
        ret = "+OK\r\n";
    } else if(set_item == "fresh-info-interval"){
        if (!slash::string2l(value.data(), value.size(), &ival) || ival <= 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'fresh-info-interval'\r\n";
            return;
        }
		g_pika_conf->SetFreshInfoInterval(ival);
		ret = "+OK\r\n";
    } else if (set_item == "requirepass") {
        g_pika_conf->SetRequirePass(value);
        ret = "+OK\r\n";
    } else if (set_item == "masterauth") {
        g_pika_conf->SetMasterAuth(value);
        ret = "+OK\r\n";
    } else if (set_item == "slotmigrate") {
        slash::StringToLower(value);
        bool is_migrate;
        if (value == "1" || value == "yes") {
            is_migrate = true;
        } else if (value == "0" || value == "no") {
            is_migrate = false;
        } else {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'slotmigrate'\r\n";
            return;
        }
        g_pika_conf->SetSlotMigrate(is_migrate);
        ret = "+OK\r\n";
    } else if (set_item == "slotmigrate-thread-num") {
        if (!slash::string2l(value.data(), value.size(), &ival)) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'slotmigrate-thread-num'\r\n";
            return;
        }
        long int migrate_thread_num = (1 > ival || 24 < ival) ? 8 : ival;
        g_pika_conf->SetSlotMigrateThreadNum(migrate_thread_num);
        ret = "+OK\r\n";
    } else if (set_item == "thread-migrate-keys-num") {
        if (!slash::string2l(value.data(), value.size(), &ival)) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'thread-migrate-keys-num'\r\n";
            return;
        }
        long int thread_migrate_keys_num = (8 > ival || 128 < ival) ? 64 : ival;
        g_pika_conf->SetThreadMigrateKeysNum(thread_migrate_keys_num);
        ret = "+OK\r\n";
    } else if (set_item == "userpass") {
        g_pika_conf->SetUserPass(value);
        ret = "+OK\r\n";
    } else if (set_item == "userblacklist") {
        g_pika_conf->SetUserBlackList(value);
        ret = "+OK\r\n";
    } else if (set_item == "dump-prefix") {
        g_pika_conf->SetBgsavePrefix(value);
        ret = "+OK\r\n";
    } else if (set_item == "maxclients") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival <= 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'maxclients'\r\n";
            return;
        }
        g_pika_conf->SetMaxConnection(ival);
        ret = "+OK\r\n";
    } else if (set_item == "dump-expire") {
        if (!slash::string2l(value.data(), value.size(), &ival)) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'dump-expire'\r\n";
            return;
        }
        g_pika_conf->SetExpireDumpDays(ival);
        ret = "+OK\r\n";
    } else if (set_item == "expire-logs-days") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival <= 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'expire-logs-days'\r\n";
            return;
        }
        g_pika_conf->SetExpireLogsDays(ival);
        ret = "+OK\r\n";
    } else if (set_item == "expire-logs-nums") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival <= 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'expire-logs-nums'\r\n";
            return;
        }
        g_pika_conf->SetExpireLogsNums(ival);
        ret = "+OK\r\n";
    } else if (set_item == "write-binlog") {
        slash::StringToLower(value);
        bool write_binlog;
        if (value == "1" || value == "yes") {
            write_binlog = true;
        } else if (value == "0" || value == "no") {
            write_binlog = false;
        } else {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'write-binlog'\r\n";
            return;
        }
        g_pika_conf->SetWriteBinlog(write_binlog);
        ret = "+OK\r\n";
    } else if (set_item == "binlog-writer-queue-size") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival <= 0 || ival > 10000) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'binlog-writer-queue-size'\r\n";
            return;
        }
        g_pika_conf->SetBinlogWriterQueueSize(ival);
        for (int i=0; i<g_pika_conf->binlog_writer_num(); i++) {
            g_pika_server->binlog_write_thread_[i]->SetMaxCmdsQueueSize(ival);
        }
        ret = "+OK\r\n";
    } else if (set_item == "root-connection-num") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival <= 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'root-connection-num'\r\n";
            return;
        }
        g_pika_conf->SetRootConnectionNum(ival);
        ret = "+OK\r\n";
    } else if (set_item == "slowlog-log-slower-than") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival <= 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'slowlog-log-slower-than'\r\n";
            return;
        }
        g_pika_conf->SetSlowlogSlowerThan(ival);
        ret = "+OK\r\n";
    } else if (set_item == "slowlog-max-len") {
        if (!slash::string2l(value.data(), value.size(), &ival)) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'slowlog-max-len'\r\n";
            return;
        }
        long int tmp_val = (100 > ival || 10000000 < ival) ? 12800 : ival;
        g_pika_conf->SetSlowlogMaxLen(tmp_val);
        g_pika_server->SlowlogTrim();
        ret = "+OK\r\n";
    } else if (set_item == "slave-read-only") {
        slash::StringToLower(value);
        bool is_readonly;
        if (value == "1" || value == "yes") {
            is_readonly = true;
        } else if (value == "0" || value == "no") {
            is_readonly = false;
        } else {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'slave-read-only'\r\n";
            return;
        }
        g_pika_conf->SetReadonly(is_readonly);
        ret = "+OK\r\n";
    } else if (set_item == "db-sync-speed") {
        if (!slash::string2l(value.data(), value.size(), &ival)) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'db-sync-speed(MB)'\r\n";
            return;
        }
        if (ival < 0 || ival > 125) {
            ival = 125;
        }
        g_pika_conf->SetDbSyncSpeed(ival);
        ret = "+OK\r\n";
    } else if (set_item == "compact-cron") {
        bool invalid = false;
        if (value != "") {
            std::string::size_type len = value.length();
            std::string::size_type colon = value.find("-");
            std::string::size_type underline = value.find("/");
            if (colon == std::string::npos || underline == std::string::npos ||
                    colon >= underline || colon + 1 >= len ||
                    colon + 1 == underline || underline + 1 >= len) {
                invalid = true;
            } else {
                int start = std::atoi(value.substr(0, colon).c_str());
                int end = std::atoi(value.substr(colon+1, underline).c_str());
                int usage = std::atoi(value.substr(underline+1).c_str());
                if (start < 0 || start > 23 || end < 0 || end > 23 || usage < 0 || usage > 100) {
                    invalid = true;
                }
            }
        }
        if (invalid) {
            ret = "-ERR invalid compact-cron\r\n";
            return;
        } else {
            g_pika_conf->SetCompactCron(value);
            ret = "+OK\r\n";
        }
    } else if (set_item == "compact-interval") {
        bool invalid = false;
        if (value != "") {
            std::string::size_type len = value.length();
            std::string::size_type slash = value.find("/");
            if (slash == std::string::npos || slash + 1 >= len) {
                invalid = true;
            } else {
                int interval = std::atoi(value.substr(0, slash).c_str());
                int usage = std::atoi(value.substr(slash+1).c_str());
                if (interval <= 0 || usage < 0 || usage > 100) {
                    invalid = true;
                }
            }
        }
        if (invalid) {
            ret = "-ERR invalid compact-interval\r\n";
            return;
        } else {
            g_pika_conf->SetCompactInterval(value);
            ret = "+OK\r\n";
        }
    } else if (set_item == "write-buffer-size") {
        long long ival = 0;
        if (!slash::string2ll(value.data(), value.size(), &ival) || ival <= 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'write-buffer-size'\r\n";
            return;
        }
        g_pika_conf->SetWriteBufferSize(ival);
        rocksdb::Status s = g_pika_server->db()->ResetOption("write_buffer_size", slash::StringToLower(value));
        if (!s.ok()) {
            ret = "-ERR Reset rocksdb 'write-buffer-size' failed\r\n";
            return;
        }
        ret = "+OK\r\n";
    } else if (set_item == "max-write-buffer-number") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival < 2) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'max-write-buffer-number'\r\n";
            return;
        }
        g_pika_conf->SetMaxWriteBufferNumber(ival);
        rocksdb::Status s = g_pika_server->db()->ResetOption("max_write_buffer_number", slash::StringToLower(value));
        if (!s.ok()) {
            ret = "-ERR Reset rocksdb 'max-write-buffer-number' failed\r\n";
            return;
        }
        ret = "+OK\r\n";
    } else if (set_item == "target-file-size-base") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival <= 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'target-file-size-base'\r\n";
            return;
        }
        g_pika_conf->SetTargetFileSizeBase(ival);
        rocksdb::Status s = g_pika_server->db()->ResetOption("target_file_size_base", slash::StringToLower(value));
        if (!s.ok()) {
            ret = "-ERR Reset rocksdb 'target-file-size-base' failed\r\n";
            return;
        }
        ret = "+OK\r\n";
    }  else if (set_item == "max-bytes-for-level-base") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival <= 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'max-bytes-for-level-base'\r\n";
            return;
        }
        g_pika_conf->SetMaxBytesForLevelBase(ival);
        rocksdb::Status s = g_pika_server->db()->ResetOption("max_bytes_for_level_base", slash::StringToLower(value));
        if (!s.ok()) {
            ret = "-ERR Reset rocksdb 'max-bytes-for-level-base' failed\r\n";
            return;
        }
        ret = "+OK\r\n";
    } else if (set_item == "disable-auto-compactions") {
        slash::StringToLower(value);
        bool disable_auto_compactions;
        if (value == "1" || value == "yes") {
            disable_auto_compactions = true;
        } else if (value == "0" || value == "no") {
            disable_auto_compactions = false;
        } else {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'disable-auto-compactions'\r\n";
            return;
        }
        g_pika_conf->SetDisableAutoCompactions(disable_auto_compactions);
        std::string key = "disable_auto_compactions";
        std::string new_value = disable_auto_compactions ? "true" : "false";
        rocksdb::Status s = g_pika_server->db()->ResetOption(key, new_value);
        if (!s.ok()) {
            ret = "-ERR Reset rocksdb 'disable-auto-compactions' failed\r\n";
            return;
        }
        ret = "+OK\r\n";
    } else if (set_item == "level0-file-num-compaction-trigger") {
        if (!slash::string2l(value.data(), value.size(), &ival)) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'level0-file-num-compaction-trigger'\r\n";
            return;
        }
        g_pika_conf->SetLevel0FileNumCompactionTrigger(ival);
        rocksdb::Status s = g_pika_server->db()->ResetOption("level0_file_num_compaction_trigger", slash::StringToLower(value));
        if (!s.ok()) {
            ret = "-ERR Reset rocksdb 'level0-file-num-compaction-trigger' failed\r\n";
            return;
        }
        ret = "+OK\r\n";
    } else if (set_item == "level0-slowdown-writes-trigger") {
        if (!slash::string2l(value.data(), value.size(), &ival)) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'level0-slowdown-writes-trigger'\r\n";
            return;
        }
        g_pika_conf->SetLevel0SlowdownWritesTrigger(ival);
        rocksdb::Status s = g_pika_server->db()->ResetOption("level0_slowdown_writes_trigger", slash::StringToLower(value));
        if (!s.ok()) {
            ret = "-ERR Reset rocksdb 'level0-slowdown-writes-trigger' failed\r\n";
            return;
        }
        ret = "+OK\r\n";
    } else if (set_item == "level0-stop-writes-trigger") {
        if (!slash::string2l(value.data(), value.size(), &ival) ||  ival <= 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'level0-stop-writes-trigger'\r\n";
            return;
        }
        g_pika_conf->SetLevel0StopWritesTrigger(ival);
        rocksdb::Status s = g_pika_server->db()->ResetOption("level0_stop_writes_trigger", slash::StringToLower(value));
        if (!s.ok()) {
            ret = "-ERR Reset rocksdb 'level0-stop-writes-trigger' failed\r\n";
            return;
        }
        ret = "+OK\r\n";
    } else if (set_item == "slave-priority") {
        if (!slash::string2l(value.data(), value.size(), &ival) ||  ival <= 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'slave-priority'\r\n";
            return;
        }
        g_pika_conf->SetSlavePriority(ival);
        ret = "+OK\r\n";
    } else if (set_item == "cache-num") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival < 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'cache-num'\r\n";
            return;
        }

        int cache_num = (0 >= ival || 48 < ival) ? 16 : ival;
        if (cache_num != g_pika_conf->cache_num()) {
            g_pika_conf->SetCacheNum(cache_num);
            g_pika_server->ResetCacheAsync(cache_num);
        }
        ret = "+OK\r\n";
    } else if (set_item == "cache-model") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival < 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'cache-model'\r\n";
            return;
        }
        if (PIKA_CACHE_NONE > ival || PIKA_CACHE_READ < ival) {
            ret = "-ERR Invalid cache model\r\n";
        } else {
            g_pika_conf->SetCacheModel(ival);
            if (PIKA_CACHE_NONE == ival) {
                g_pika_server->ClearCacheDbAsync();
            }
            ret = "+OK\r\n";
        }
    } else if (set_item == "cache-maxmemory") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival < 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'cache-maxmemory'\r\n";
            return;
        }
        int64_t cache_maxmemory = (PIKA_CACHE_SIZE_MIN > ival) ? PIKA_CACHE_SIZE_DEFAULT : ival;
        g_pika_conf->SetCacheMaxmemory(cache_maxmemory);
        g_pika_server->ResetCacheConfig();
        ret = "+OK\r\n";
    } else if (set_item == "cache-maxmemory-policy") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival < 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'cache-maxmemory-policy'\r\n";
            return;
        }
        int cache_maxmemory_policy_ = (0 > ival || 5 < ival) ? 3 : ival; // default allkeys-lru
        g_pika_conf->SetCacheMaxmemoryPolicy(cache_maxmemory_policy_);
        g_pika_server->ResetCacheConfig();
        ret = "+OK\r\n";
    } else if (set_item == "cache-maxmemory-samples") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival < 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'cache-maxmemory-samples'\r\n";
            return;
        }
        int cache_maxmemory_samples = (1 > ival) ? 5 : ival;
        g_pika_conf->SetCacheMaxmemorySamples(cache_maxmemory_samples);
        g_pika_server->ResetCacheConfig();
        ret = "+OK\r\n";
    } else if (set_item == "cache-lfu-decay-time") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival < 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'cache-lfu-decay-time'\r\n";
            return;
        }
        int cache_lfu_decay_time = (0 > ival) ? 1 : ival;
        g_pika_conf->SetCacheLFUDecayTime(cache_lfu_decay_time);
        g_pika_server->ResetCacheConfig();
        ret = "+OK\r\n";
    } else if (set_item == "rate-bytes-per-sec") {
        long long ival = 0;
        if (!slash::string2ll(value.data(), value.size(), &ival) || ival < 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'rate-bytes-per-sec'\r\n";
            return;
        }
        int64_t rate_bytes_per_sec = (1048576 > ival) ? 1048576 : ival;
        g_pika_conf->SetRateBytesPerSec(rate_bytes_per_sec);
        g_pika_server->db()->SetRateBytesPerSec(rate_bytes_per_sec);
        ret = "+OK\r\n";
    } else if (set_item == "disable-wal") {
        slash::StringToLower(value);
        bool disable_wal;
        if (value == "1" || value == "yes") {
            disable_wal = true;
        } else if (value == "0" || value == "no") {
            disable_wal = false;
        } else {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'disable-wal'\r\n";
            return;
        }
        g_pika_conf->SetDisableWAL(disable_wal);
        g_pika_server->db()->SetDisableWAL(disable_wal);
        ret = "+OK\r\n";
    } else if (set_item == "check-free-mem-interval") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival < 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'check-free-mem-interval'\r\n";
            return;
        }
        int check_free_mem_interval = (1 > ival) ? 60 : ival;
        g_pika_conf->SetCheckFreeMemInterval(check_free_mem_interval);
        ret = "+OK\r\n";
    } else if (set_item == "min-system-free-mem") {
        long long ival = 0;
        if (!slash::string2ll(value.data(), value.size(), &ival) || ival < 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'min-system-free-mem'\r\n";
            return;
        }
        int64_t min_system_free_mem = (1073741824 > ival) ? 0 : ival;
        g_pika_conf->SetMinSystemFreeMem(min_system_free_mem);
        ret = "+OK\r\n";
    } else if (set_item == "optimize-min-free-kbytes") {
        slash::StringToLower(value);
        bool optimize_min_free_kbytes;
        if (value == "1" || value == "yes") {
            optimize_min_free_kbytes = true;
        } else if (value == "0" || value == "no") {
            optimize_min_free_kbytes = false;
        } else {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'optimize-min-free-kbytes'\r\n";
            return;
        }
        g_pika_conf->SetOptimizeMinFreeKbytes(optimize_min_free_kbytes);
        if (optimize_min_free_kbytes) {
            slash::SetSysMinFreeKbytesRatio(0.03);
        }
        ret = "+OK\r\n";
    } else if (set_item == "max-gc-batch-size") {
        long long ival = 0;
        if (!slash::string2ll(value.data(), value.size(), &ival) || ival < 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'max-gc-batch-size'\r\n";
            return;
        }
        int64_t max_gc_batch_size = (1073741824 > ival) ? 1073741824 : ival;
        g_pika_conf->SetMaxGCBatchSize(max_gc_batch_size);
        g_pika_server->db()->SetMaxGCBatchSize(max_gc_batch_size);
        ret = "+OK\r\n";
    } else if (set_item == "blob-file-discardable-ratio") {
        if (!slash::string2l(value.data(), value.size(), &ival) ||  ival < 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'blob-file-discardable-ratio'\r\n";
            return;
        }
        int blob_file_discardable_ratio = (0 > ival || 100 < ival) ? 50 : ival;
        g_pika_conf->SetBlobFileDiscardableRatio(blob_file_discardable_ratio);
        g_pika_server->db()->SetBlobFileDiscardableRatio(static_cast<float>(blob_file_discardable_ratio) / 100);
        ret = "+OK\r\n";
    } else if (set_item == "gc-sample-cycle") {
        long long ival = 0;
        if (!slash::string2ll(value.data(), value.size(), &ival) ||  ival < 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'gc-sample-cycle'\r\n";
            return;
        }
        int64_t gc_sample_cycle = (0 > ival) ? 604800 : ival;;
        g_pika_conf->SetGCSampleCycle(gc_sample_cycle);
        g_pika_server->db()->SetGCSampleCycle(gc_sample_cycle);
        ret = "+OK\r\n";
    } else if (set_item == "max-gc-queue-size") {
        if (!slash::string2l(value.data(), value.size(), &ival) ||  ival < 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'max-gc-queue-size'\r\n";
            return;
        }
        int max_gc_queue_size = (1 > ival) ? 2 : ival;
        g_pika_conf->SetMaxGCQueueSize(max_gc_queue_size);
        g_pika_server->db()->SetMaxGCQueueSize(max_gc_queue_size);
        ret = "+OK\r\n";
    } else if (set_item == "zset-auto-del-threshold") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival < 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'zset-auto-del-threshold'\r\n";
            return;
        }
        int zset_auto_del_threshold = (0 > ival) ? 0 : ival;
        g_pika_conf->SetZsetAutoDelThreshold(zset_auto_del_threshold);
        ret = "+OK\r\n";
    } else if (set_item == "zset-auto-del-direction") {
        if (!slash::string2l(value.data(), value.size(), &ival)) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'zset-auto-del-direction'\r\n";
            return;
        }
        int zset_auto_del_direction = (0 != ival && -1 != ival) ? 0 : ival;
        g_pika_conf->SetZsetAutoDelDirection(zset_auto_del_direction);
        ret = "+OK\r\n";
    } else if (set_item == "zset-auto-del-num") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival < 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'zset-auto-del-num'\r\n";
            return;
        }
        int zset_auto_del_num = (0 >= ival) ? 1 : ival;
        g_pika_conf->SetZsetAutoDelNum(zset_auto_del_num);
        ret = "+OK\r\n";
    } else if (set_item == "zset-auto-del-cron") {
        bool invalid = false;
        if (value != "") {
            std::string::size_type len = value.length();
            std::string::size_type colon = value.find("-");
            if (colon == std::string::npos || colon + 1 >= len) {
                invalid = true;
            } else {
                int start = std::atoi(value.substr(0, colon).c_str());
                int end = std::atoi(value.substr(colon+1).c_str());
                if (start < 0 || start > 23 || end < 0 || end > 23) {
                    invalid = true;
                }
            }
        }
        if (invalid) {
            ret = "-ERR invalid zset-auto-del-cron\r\n";
            return;
        } else {
            g_pika_conf->SetZsetAutoDelCron(value);
            ret = "+OK\r\n";
        }
    } else if (set_item == "zset-auto-del-interval") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival < 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'zset-auto-del-interval'\r\n";
            return;
        }
        int zset_auto_del_interval = (0 > ival) ? 0 : ival;
        g_pika_conf->SetZsetAutoDelInterval(zset_auto_del_interval);
        ret = "+OK\r\n";
    } else if (set_item == "zset-auto-del-cron-speed-factor") {
        double ival;
        if (!slash::string2d(value.data(), value.size(), &ival) || ival < 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'zset-auto-del-cron-speed-factor'\r\n";
            return;
        }
        double zset_auto_del_cron_speed_factor = (0 > ival || 1000 < ival) ? 1 : ival;
        g_pika_conf->SetZsetAutoDelCronSpeedFactor(zset_auto_del_cron_speed_factor);
        ret = "+OK\r\n";
    } else if (set_item == "zset-auto-del-scan-round-num") {
        if (!slash::string2l(value.data(), value.size(), &ival) || ival < 0) {
            ret = "-ERR Invalid argument " + value + " for CONFIG SET 'zset-auto-del-scan-round-num'\r\n";
            return;
        }
        int zset_auto_del_scan_round_num = (0 >= ival) ? 10000 : ival;
        g_pika_conf->SetZsetAutoDelScanRoundNum(zset_auto_del_scan_round_num);
        ret = "+OK\r\n";
    } else if (set_item == "slow-cmd-list") {
        g_pika_conf->SetSlowCmdList(value);
        ret = "+OK\r\n";
    } else {
        ret = "-ERR No such configure item\r\n";
    }
}

void ConfigCmd::ConfigRewrite(std::string &ret) {
    g_pika_conf->ConfigRewrite();
    ret = "+OK\r\n";
}

void ConfigCmd::ConfigResetstat(std::string &ret) {
    g_pika_server->ResetStat();
    ret = "+OK\r\n";
}

void MonitorCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    (void)ptr_info;
    if (argv.size() != 1) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameMonitor);
        return;
    }
}

void MonitorCmd::Do() {
}

void DbsizeCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    (void)ptr_info;
    if (argv.size() != 1) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameDbsize);
        return;
    }
}

void DbsizeCmd::Do() {
    if (g_pika_conf->slotmigrate()){
        int64_t dbsize = 0;
        for (int i = 0; i < HASH_SLOTS_SIZE; ++i){
            int32_t card = 0;
            rocksdb::Status s = g_pika_server->db()->SCard(SlotKeyPrefix+std::to_string(i), &card);
            //card = g_pika_server->db()->SCard(SlotKeyPrefix+std::to_string(i));
            if (s.ok() && card >= 0) {
                dbsize += card;
            }else {
                res_.SetRes(CmdRes::kErrOther, "Get dbsize error");
                return;
            }
        }
        res_.AppendInteger(dbsize);
        return;
    }

    PikaServer::KeyScanInfo key_scan_info = g_pika_server->key_scan_info();
    std::vector<uint64_t> &key_nums_v = key_scan_info.key_nums_v;
    if (key_scan_info.key_nums_v.size() != 5) {
        res_.SetRes(CmdRes::kErrOther, "keyspace error");
        return;
    }
    int64_t dbsize = key_nums_v[0] + key_nums_v[1] + key_nums_v[2] + key_nums_v[3] + key_nums_v[4];
    res_.AppendInteger(dbsize);
}

void TimeCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    (void)ptr_info;
    if (argv.size() != 1) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameTime);
        return;
    }
}

void TimeCmd::Do() {
    struct timeval tv;
    if (gettimeofday(&tv, NULL) == 0) {
        res_.AppendArrayLen(2);
        char buf[32];
        int32_t len = slash::ll2string(buf, sizeof(buf), tv.tv_sec);
        res_.AppendStringLen(len);
        res_.AppendContent(buf);

        len = slash::ll2string(buf, sizeof(buf), tv.tv_usec);
        res_.AppendStringLen(len);
        res_.AppendContent(buf);
    } else {
        res_.SetRes(CmdRes::kErrOther, strerror(errno));
    }
}

void DelbackupCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    (void)ptr_info;
    if (argv.size() != 1) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameDelbackup);
        return;
    }
}

void DelbackupCmd::Do() {
    std::string db_sync_prefix = g_pika_conf->bgsave_prefix();
    std::string db_sync_path = g_pika_conf->bgsave_path();
    std::vector<std::string> dump_dir;

    // Dump file is not exist
    if (!slash::FileExists(db_sync_path)) {
        res_.SetRes(CmdRes::kOk);
        return;
    }
    // Directory traversal
    if (slash::GetChildren(db_sync_path, dump_dir) != 0) {
        res_.SetRes(CmdRes::kOk);
        return;
    }

    int len = dump_dir.size();
    for (size_t i = 0; i < dump_dir.size(); i++) {
        if (dump_dir[i].substr(0, db_sync_prefix.size()) != db_sync_prefix || dump_dir[i].size() != (db_sync_prefix.size() + 8)) {
            continue;
        }

        std::string str_date = dump_dir[i].substr(db_sync_prefix.size(), (dump_dir[i].size() - db_sync_prefix.size()));
        char *end = NULL;
        std::strtol(str_date.c_str(), &end, 10);
        if (*end != 0) {
            continue;
        }

        std::string dump_dir_name = db_sync_path + dump_dir[i];
        if (g_pika_server->CountSyncSlaves() == 0) {
            LOG(INFO) << "Not syncing, delete dump file: " << dump_dir_name;
            slash::DeleteDirIfExist(dump_dir_name);
            len--;
        } else if (g_pika_server->bgsave_info().path != dump_dir_name){
            LOG(INFO) << "Syncing, delete expired dump file: " << dump_dir_name;
            slash::DeleteDirIfExist(dump_dir_name);
            len--;
        } else {
            LOG(INFO) << "Syncing, can not delete " << dump_dir_name << " dump file" << std::endl;
        }
    }
    if (len == 0) {
        g_pika_server->bgsave_info().Clear();
    }

    res_.SetRes(CmdRes::kOk);
    return;
}

#ifdef TCMALLOC_EXTENSION
void TcmallocCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    (void)ptr_info;
    if (argv.size() != 2 && argv.size() != 3) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameTcmalloc);
        return;
    }
    rate_ = -1;
    if (!strcasecmp(argv[1].data(), "stats")) {
        type_ = 0;
    } else if (!strcasecmp(argv[1].data(), "rate")) {
        type_ = 1;
        if (argv.size() == 3) {
            if (!slash::string2d(argv[2].data(), argv[2].size(), &rate_)) {
                res_.SetRes(CmdRes::kSyntaxErr, kCmdNameTcmalloc);
            }
        }
    } else if (!strcasecmp(argv[1].data(), "list")) {
        type_ = 2;
    } else if (!strcasecmp(argv[1].data(), "free")) {
        type_ = 3;
    } else {
        res_.SetRes(CmdRes::kInvalidParameter, kCmdNameTcmalloc);
        return;
    }

}

void TcmallocCmd::Do() {
    std::vector<MallocExtension::FreeListInfo> fli;
    std::vector<std::string> elems;
    switch(type_) {
        case 0:
            char stats[1024];
            MallocExtension::instance()->GetStats(stats, 1024);
            slash::StringSplit(stats, '\n', elems);
            res_.AppendArrayLen(elems.size());
            for (auto& i : elems) {
                res_.AppendString(i);
            }
            break;
        case 1:
            if (rate_ >= 0) {
                MallocExtension::instance()->SetMemoryReleaseRate(rate_);
            }
            res_.AppendInteger(MallocExtension::instance()->GetMemoryReleaseRate());
            break;
        case 2:
            MallocExtension::instance()->GetFreeListSizes(&fli);
            res_.AppendArrayLen(fli.size());
            for (auto& i : fli) {
                res_.AppendString("type: " + std::string(i.type) + ", min: " + std::to_string(i.min_object_size) +
                    ", max: " + std::to_string(i.max_object_size) + ", total: " + std::to_string(i.total_bytes_free));
            }
            break;
        case 3:
            MallocExtension::instance()->ReleaseFreeMemory();
            res_.SetRes(CmdRes::kOk);
    }
}
#endif

void EchoCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameEcho);
    return;
  }

  echomsg_ = argv[1];
}
void EchoCmd::Do() {
  res_.AppendString(echomsg_);
}

void SlowlogCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info)
{
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameSlowlog);
        return;
    }

    if (argv.size() == 2 && !strcasecmp(argv[1].data(), "reset")) {
        condition_ = SlowlogCmd::kRESET;
    }
    else if (argv.size() == 2 && !strcasecmp(argv[1].data(), "len")) {
        condition_ = SlowlogCmd::kLEN;
    }
    else if ((argv.size() == 2 || argv.size() == 3) && !strcasecmp(argv[1].data(), "get")) {
        condition_ = SlowlogCmd::kGET;
        if (argv.size() == 3 && !slash::string2l(argv[2].data(), argv[2].size(), &number_)) {
            res_.SetRes(CmdRes::kInvalidInt);
            return;
        }
    }
    else {
        res_.SetRes(CmdRes::kErrOther, "Unknown SLOWLOG subcommand or wrong # of args. Try GET, RESET, LEN.");
        return;
    }
}

void SlowlogCmd::Do() {
    if (condition_ == SlowlogCmd::kRESET) {
        g_pika_server->SlowlogReset();
        res_.SetRes(CmdRes::kOk);
    }
    else if (condition_ ==  SlowlogCmd::kLEN) {
        res_.AppendInteger(g_pika_server->SlowlogLen());
    }
    else {
        std::vector<SlowlogEntry> slowlogs;
        g_pika_server->SlowlogObtain(number_, &slowlogs);
        res_.AppendArrayLen(slowlogs.size());
        for (const auto& slowlog : slowlogs) {
            res_.AppendArrayLen(4);
            res_.AppendInteger(slowlog.id);
            res_.AppendInteger(slowlog.start_time);
            res_.AppendInteger(slowlog.duration);
            res_.AppendArrayLen(slowlog.argv.size());
            for (const auto& arg : slowlog.argv) {
                res_.AppendString(arg);
            }
        }
    }
    return;
}

void CacheCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameCache);
    return;
  }

  if (!strcasecmp(argv[1].data(), "clear")) {
    if (!strcasecmp(argv[2].data(), "db")) {
      condition_ = kCLEAR_DB;
    } else if (!strcasecmp(argv[2].data(), "hitratio")) {
      condition_ = kCLEAR_HITRATIO;
    } else {
      res_.SetRes(CmdRes::kErrOther, "Unknown cache subcommand or wrong # of args.");
    }
  } else if (!strcasecmp(argv[1].data(), "del")) {
    condition_ = kDEL_KEYS;
    std::vector<std::string>::const_iterator iter = argv.begin();
    keys_.assign(iter + 2, argv.end());
  } else if (!strcasecmp(argv[1].data(), "randomkey")) {
    condition_ = kRANDOM_KEY;
  } else {
    res_.SetRes(CmdRes::kErrOther, "Unknown cache subcommand or wrong # of args.");
  }
  return;
}

void CacheCmd::Do() {
  slash::Status s;
  std::string key;
  switch (condition_) {
    case kCLEAR_DB:
      g_pika_server->ClearCacheDbAsync();
      res_.SetRes(CmdRes::kOk);
      break;
    case kCLEAR_HITRATIO:
      g_pika_server->ClearHitRatio();
      res_.SetRes(CmdRes::kOk);
      break;
    case kDEL_KEYS:
      for (auto& key : keys_) {
        g_pika_server->Cache()->Del(key);
      }
      res_.SetRes(CmdRes::kOk);
      break;
    case kRANDOM_KEY:
      s = g_pika_server->Cache()->RandomKey(&key);
      if (!s.ok()) {
        res_.AppendStringLen(-1);
      } else {
        res_.AppendStringLen(key.size());
        res_.AppendContent(key);
      } 
      break;
    default:
      res_.SetRes(CmdRes::kErrOther, "Unknown cmd");
      break;
  }
  return;
}

void ZsetAutoDelCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
  if (!ptr_info->CheckArg(argv.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameZsetAutoDel);
    return;
  }

  if (!slash::string2l(argv[1].data(), argv[1].size(), &cursor_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (!slash::string2d(argv[2].data(), argv[2].size(), &speed_factor_)) {
    res_.SetRes(CmdRes::kInvalidFloat);
    return;
  }
  speed_factor_ = (0 > speed_factor_ || 1000 < speed_factor_) ? 1 : speed_factor_;
}

void ZsetAutoDelCmd::Do() {
    slash::Status s = g_pika_server->ZsetAutoDel(cursor_, speed_factor_);
    if (!s.ok()) {
        res_.SetRes(CmdRes::kErrOther, s.ToString());
        return;
    }
    res_.SetRes(CmdRes::kOk);
}

void ZsetAutoDelOffCmd::DoInitial(const PikaCmdArgsType &argv, const CmdInfo* const ptr_info) {
    if (!ptr_info->CheckArg(argv.size())) {
        res_.SetRes(CmdRes::kWrongNum, kCmdNameZsetAutoDelOff);
        return;
    }
}

void ZsetAutoDelOffCmd::Do() {
    slash::Status s = g_pika_server->ZsetAutoDelOff();
    if (!s.ok()) {
        res_.SetRes(CmdRes::kErrOther, s.ToString());
        return;
    }
    res_.SetRes(CmdRes::kOk);
}
