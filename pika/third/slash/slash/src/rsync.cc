#include <fstream>
#include <sstream>
#include <string>

#include "slash/include/env.h"
#include "slash/include/rsync.h"
#include "slash/include/xdebug.h"

namespace slash {
// Clean files for rsync info, such as the lock, log, pid, conf file
static bool CleanRsyncInfo(const std::string& path) {
  return slash::DeleteDirIfExist(path + kRsyncSubDir);
}

int StartRsync(const std::string& raw_path, const std::string& module, const std::string& ip, const int port) {
  // Sanity check  
  if (raw_path.empty() || module.empty()) {
    return -1;
  }
  std::string path(raw_path);
  if (path.back() != '/') {
    path += "/";
  }
  std::string rsync_path = path + kRsyncSubDir + "/";
  CreatePath(rsync_path);

  // Generate conf file
  std::string conf_file(rsync_path + kRsyncConfFile);
  std::ofstream conf_stream(conf_file.c_str());
  if (!conf_stream) {
    log_warn("Open rsync conf file failed!");
    return -1;
  }
  conf_stream << "uid = root" << std::endl;
  conf_stream << "gid = root" << std::endl;
  conf_stream << "use chroot = no" << std::endl;
  conf_stream << "max connections = 10" << std::endl;
  conf_stream << "lock file = " << rsync_path + kRsyncLockFile << std::endl;
  conf_stream << "log file = " << rsync_path + kRsyncLogFile << std::endl;
  conf_stream << "pid file = " << rsync_path + kRsyncPidFile << std::endl;
  conf_stream << "list = no" << std::endl;
  conf_stream << "strict modes = no" << std::endl;
  conf_stream << "[" << module << "]" << std::endl;
  conf_stream << "path = " << path << std::endl;
  conf_stream << "read only = no" << std::endl;
  conf_stream.close();

  // Execute rsync command
  std::stringstream ss;
  ss << "rsync --daemon --config=" << conf_file;
  ss << " --address=" << ip;
  if (port != 873) {
    ss << " --port=" << port;
  }
  std::string rsync_start_cmd = ss.str();
  int ret = system(rsync_start_cmd.c_str());
  if (ret == 0 || (WIFEXITED(ret) && !WEXITSTATUS(ret))) {
    return 0;
  }
  log_warn("Start rsync deamon failed : %d!", ret);
  return ret;
}

int StopRsync(const std::string& raw_path) {
  // Sanity check  
  if (raw_path.empty()) {
    log_warn("empty rsync path!");
    return -1;
  }
  std::string path(raw_path);
  if (path.back() != '/') {
    path += "/";
  }

  std::string pid_file(path + kRsyncSubDir + "/" + kRsyncPidFile);
  if (!FileExists(pid_file)) {
    log_warn("no rsync pid file found");
    return 0; // Rsync deamon is not exist
  }

  // Kill Rsync
  std::string rsync_stop_cmd = "kill -9 `cat " + pid_file + "`";
  int ret = system(rsync_stop_cmd.c_str());
  if (ret == 0 || (WIFEXITED(ret) && !WEXITSTATUS(ret))) {
    log_info("Stop rsync success!");
  } else {
    log_warn("Stop rsync deamon failed : %d!", ret);
  }
  CleanRsyncInfo(path);
  return ret;
}

int RsyncSendFile(const std::string& local_file_path, const std::string& remote_file_path, const RsyncRemote& remote) {
  std::stringstream ss;
  ss << """rsync -avP --bwlimit=" << remote.kbps
    << " --port=" << remote.port
    << " " << local_file_path
    << " " << remote.host
    << "::" << remote.module << "/" << remote_file_path;

  std::string rsync_cmd = ss.str();
  int ret = system(rsync_cmd.c_str());
  if (ret == 0 || (WIFEXITED(ret) && !WEXITSTATUS(ret))) {
    return 0;
  }
  log_warn("Rsync send file failed : %d!", ret);
  return ret;
}

int RsyncSendClearTarget(const std::string& local_dir_path, const std::string& remote_dir_path, const RsyncRemote& remote) {
  if (local_dir_path.empty() || remote_dir_path.empty()) {
    return -2;
  }
  std::string local_dir(local_dir_path), remote_dir(remote_dir_path);
  if (local_dir_path.back() != '/') {
    local_dir.append("/");
  }
  if (remote_dir_path.back() != '/') {
    remote_dir.append("/");
  }
  std::stringstream ss;
  ss << "rsync -avP --delete --port=" << remote.port
    << " " << local_dir
    << " " << remote.host
    << "::" << remote.module << "/" << remote_dir;
  std::string rsync_cmd = ss.str();
  int ret = system(rsync_cmd.c_str());
  if (ret == 0 || (WIFEXITED(ret) && !WEXITSTATUS(ret))) {
    return 0;
  }
  log_warn("Rsync send file failed : %d!", ret);
  return ret;
}

}  // namespace slash
