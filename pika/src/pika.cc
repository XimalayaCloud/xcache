// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

//signal.h: before include in nemo.h
#include <signal.h>
#include <glog/logging.h>
#include <sys/resource.h>
//fcntl.h: before include in pubsub.h
#include <fcntl.h>

#include "slash/include/env.h"
#include "pika_server.h"
#include "pika_command.h"
#include "pika_conf.h"
#include "pika_define.h"
#include "pika_slot.h"
#include "pika_version.h"
#include "pika_cmd_table_manager.h"
#include "pika_commonfunc.h"

#ifdef TCMALLOC_EXTENSION
#include <gperftools/malloc_extension.h>
#endif

#include <string>
#include <fstream>

PikaConf *g_pika_conf;

PikaServer* g_pika_server;

PikaCmdTableManager* g_pika_cmd_table_manager;

static void version() {
    char version[32];
    snprintf(version, sizeof(version), "%d.%d.%d-%d.%d", PIKA_MAJOR,
        PIKA_MINOR, PIKA_PATCH, PIKA_XMLY_MAJOR, PIKA_XMLY_MINOR);
    printf("-----------Pika server %s ----------\n", version);
}

static void PikaConfInit(const std::string& path) {
  printf("path : %s\n", path.c_str());
  g_pika_conf = new PikaConf(path);
  if (g_pika_conf->Load() != 0) {
    LOG(FATAL) << "pika load conf error";
  }
  version();
  printf("-----------Pika config list----------\n");
  g_pika_conf->DumpConf();
  printf("-----------Pika config end----------\n");
}

static void signalHandle(const char *data, int size) {
    std::string core_path = FLAGS_log_dir + "/pika_coredump.log";
    std::ofstream fs(core_path.c_str(), std::ios::app);
    std::string str = std::string(data, size);
    fs << str;
    fs.close();
    LOG(ERROR) << str;
}

static void PikaGlogInit() {
  if (!slash::FileExists(g_pika_conf->log_path())) {
    slash::CreatePath(g_pika_conf->log_path()); 
  }

  if (!g_pika_conf->daemonize()) {
    FLAGS_alsologtostderr = true;
  }
  FLAGS_log_dir = g_pika_conf->log_path();
  FLAGS_minloglevel = g_pika_conf->log_level();
  //FLAGS_max_log_size = 1800;
  FLAGS_max_log_size = g_pika_conf->max_log_size();
  FLAGS_logbufsecs = 0;
  ::google::InitGoogleLogging("pika");
  ::google::InstallFailureSignalHandler();
  ::google::InstallFailureWriter(&signalHandle);
}

static void daemonize() {
  if (fork() != 0) exit(0); /* parent exits */
  setsid(); /* create a new session */
}

static void close_std() {
  int fd;
  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, STDIN_FILENO);
    dup2(fd, STDOUT_FILENO);
    dup2(fd, STDERR_FILENO);
    close(fd);
  }
}

static void create_pid_file(void) {
  /* Try to write the pid file in a best-effort way. */
  std::string path(g_pika_conf->pidfile());

  size_t pos = path.find_last_of('/');
  if (pos != std::string::npos) {
    // mkpath(path.substr(0, pos).c_str(), 0755);
    slash::CreateDir(path.substr(0, pos));
  } else {
    path = kPikaPidFile;
  }

  FILE *fp = fopen(path.c_str(), "w");
  if (fp) {
    fprintf(fp,"%d\n",(int)getpid());
    fclose(fp);
  }
}

static void IntSigHandle(const int sig) {
  LOG(INFO) << "Catch Signal " << sig << ", cleanup...";
  g_pika_server->Exit();
}

static void PikaSignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

static void usage()
{
    char version[32];
    snprintf(version, sizeof(version), "%d.%d.%d-%d.%d", PIKA_MAJOR,
        PIKA_MINOR, PIKA_PATCH, PIKA_XMLY_MAJOR, PIKA_XMLY_MINOR);
    fprintf(stderr,
            "Pika module %s\n"
            "usage: pika [-hv] [-c conf/file]\n"
            "\t-h               -- show this help\n"
            "\t-c conf/file     -- config file \n"
            "  example: ./output/bin/pika -c ./conf/pika.conf\n",
            version
           );
}

int main(int argc, char *argv[]) {
  if (argc != 2 && argc != 3) {
    usage();
    exit(-1);
  }

  bool path_opt = false;
  char c;
  char path[1024];
  while (-1 != (c = getopt(argc, argv, "c:hv"))) {
    switch (c) {
      case 'c':
        snprintf(path, 1024, "%s", optarg);
        path_opt = true;
        break;
      case 'h':
        usage();
        return 0;
      case 'v':
        version();
        return 0;
      default:
        usage();
        return 0;
    }
  }

  if (path_opt == false) {
    fprintf (stderr, "Please specify the conf file path\n" );
    usage();
    exit(-1);
  }
#ifdef TCMALLOC_EXTENSION
  MallocExtension::instance()->Initialize();
#endif
  PikaConfInit(path);

  rlimit limit;
  if (getrlimit(RLIMIT_NOFILE,&limit) == -1) {
    LOG(WARNING) << "getrlimit error: " << strerror(errno);
  } else if (limit.rlim_cur < static_cast<unsigned int>(g_pika_conf->maxclients() + PIKA_MIN_RESERVED_FDS)) {
    rlim_t old_limit = limit.rlim_cur;
    rlim_t best_limit = g_pika_conf->maxclients() + PIKA_MIN_RESERVED_FDS;
    limit.rlim_cur = best_limit > limit.rlim_max ? limit.rlim_max-1 : best_limit;
    limit.rlim_max = best_limit > limit.rlim_max ? limit.rlim_max-1 : best_limit;
    if (setrlimit(RLIMIT_NOFILE,&limit) != -1) {
      LOG(WARNING) << "your 'limit -n ' of " << old_limit << " is not enough for Redis to start. pika have successfully reconfig it to " << limit.rlim_cur;
    } else {
      LOG(FATAL) << "your 'limit -n ' of " << old_limit << " is not enough for Redis to start. pika can not reconfig it(" << strerror(errno) << "), do it by yourself";
    }
  }

  // daemonize if needed
  if (g_pika_conf->daemonize()) {
    daemonize();
    create_pid_file();
  }


  PikaGlogInit();
  PikaSignalSetup();
  InitCmdInfoTable();
  PikaCommonFunc::InitCRC32Table();

  LOG(INFO) << "Server at: " << path;
  g_pika_server = new PikaServer();
  g_pika_cmd_table_manager = new PikaCmdTableManager();

  if (g_pika_conf->daemonize()) {
    close_std();
  }

  g_pika_server->Start();
  
  if (g_pika_conf->daemonize()) {
    unlink(g_pika_conf->pidfile().c_str());
  }

  delete g_pika_server;
  delete g_pika_cmd_table_manager;
  DestoryCmdInfoTable();
  ::google::ShutdownGoogleLogging();
  delete g_pika_conf;
  
  return 0;
}
