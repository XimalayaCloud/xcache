#ifndef SLASH_RSYNC_H_
#define SLASH_RSYNC_H_

#include <string>

namespace slash {
const std::string kRsyncConfFile = "slash_rsync.conf";
const std::string kRsyncLogFile = "slash_rsync.log";
const std::string kRsyncPidFile = "slash_rsync.pid";
const std::string kRsyncLockFile = "slash_rsync.lock";
const std::string kRsyncSubDir = "rsync";
struct RsyncRemote {
  std::string host;
  int port;
  std::string module;
  int kbps; //speed limit
  RsyncRemote(const std::string& _host, const int _port,
      const std::string& _module, const int _kbps)
  : host(_host), port(_port), module(_module), kbps(_kbps) {}
};

int StartRsync(const std::string& rsync_path, const std::string& module, const std::string& ip, const int port);
int StopRsync(const std::string& path);
int RsyncSendFile(const std::string& local_file_path, const std::string& remote_file_path,
    const RsyncRemote& remote);
int RsyncSendClearTarget(const std::string& local_dir_path, const std::string& remote_dir_path,
    const RsyncRemote& remote);

}
#endif
