#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <string>
#include <fstream>
#include <sstream>
#include <ctime>
#include "aof_sender.h"

#define VERSION "1.2.0"
#define EOFMSG "EOAOF"

long long g_block_size = 0;
long long g_block_sleep = 0;

static std::time_t begin_time;
static AOFSender *sender = NULL;

static void split_send(const std::string &line){
    static std::string send_buf_;
    if (EOFMSG == line){
        if (!send_buf_.empty()) { // end of the AOF file
            sender->message_add(send_buf_);
            send_buf_.clear();
        }
        return;
    }

    // Record
    send_buf_ += line;
    
    // Split and send
    std::size_t pos = 0;
    std::string str_block;
    while (send_buf_.size() - pos >= MSG_BLOCK_MAX){
        str_block.assign(send_buf_.substr(pos, MSG_BLOCK_MAX));
        sender->message_add(str_block);
        pos += MSG_BLOCK_MAX;
    }
    if (pos != 0)
        send_buf_.assign(send_buf_.substr(pos));
}

static void print_cur_time(){
    std::time_t now = time(0);
    struct tm * time = localtime( &now);
    std::stringstream ss;
    ss <<(time->tm_mon + 1) << '-'
        << time->tm_mday << " "
        << time->tm_hour  << ":"
        << time->tm_min  << "."
        << time->tm_sec;
    LOG_INFO(ss.str());

}

static int file_read(const std::string &path, std::ios::streampos start_pos)
{
    std::ifstream ifs(path.c_str(), std::ios::binary | std::ios::in);
    if (!ifs || !ifs.is_open()){
        LOG_ERR("ERROR: opening aof file: " + path);
        return 1;
    }

    LOG_INFO("Begin reading.... ");
    print_cur_time();

    std::ios::streampos gpos = start_pos == 0 ? ifs.tellg() : start_pos;
    std::string line;
    bool dirty = false;
    while(true)
    {
        ifs.clear();
        ifs.seekg(gpos);

        while(std::getline(ifs, line)){
            dirty = true;
            gpos = ifs.tellg();
            split_send(line + '\n');
            line.clear();
        }
      
        if (!ifs.eof()) {
            break;
        }

        if (dirty) {
            // Flush current send buf
            split_send(EOFMSG);
            dirty = false;
            print_cur_time();
        } else {
            LOG_INFO("Read the end of aof...");
        }
        usleep(1000 * 1000);
    }

    ifs.close();
    return 0;
}

static void* msg_process(void *p){
    long long offset = *(long long *)p;
    sender->process(offset);
    pthread_exit(NULL);
}

void intHandler(int sig) {
    LOG_ERR("Catched");
    std::time_t current = time(0);
    double diff  = difftime(current, begin_time);
    std::stringstream ss;
    ss << "Elapse time(s): " << diff;
    LOG_ERR(ss.str());
    exit(0);
}

bool isNum(std::string s){
    std::stringstream sin(s);
    double t;
    char p;
    if(!(sin >> t))
        return false;
    if(sin >> p)
        return false;
    else
        return true;
}

static void usage()
{
    fprintf(stderr,
            "aof transfor tool %s\n"
            "Parameters: \n"
            "   -i: aof file \n"
            "   -h: the target host \n"
            "   -p: the target port \n"
            "   -a: the target auth \n"
            "   -o: the start offset \n"
            "   -b: the block size(Byte) \n"
            "   -s: the block sleep(us) \n"
            "   -v: show more information\n"
            "Example: ./aof_to_pika -i ./appendonly.aof -h 127.0.0.1 -p 6379 -a abc -o 0 -b 1024 -s 10 -v\n", VERSION
           );
}

int main(int argc, char **argv)
{
    char c;
    std::string path, host, port, auth, offset_str, block_size, block_sleep;
    long long offset = -1;
    bool verbose = false;
    while (-1 != (c = getopt(argc, argv, "i:h:p:a:o:b:s:v"))) {
        switch (c) {
            case 'i':
                path.assign(optarg);
                break;
            case 'h':
                host.assign(optarg);
                break;
            case 'p':
                port.assign(optarg);
                break;
            case 'a':
                auth.assign(optarg);
                break;
            case 'o':
                offset_str.assign(optarg);
                break;
            case 'b':
                block_size.assign(optarg);
                break;
            case 's':
                block_sleep.assign(optarg);
                break;
            case 'v':
                verbose = true;
                break;
            default:
                usage();
                return 0;
        }
    }

    if (isNum(offset_str)) {
        offset = std::stoll(offset_str, NULL, 0);
        if (offset < 0) {
            printf("offset must be greater or equal to 0\n");
            return 0;
        }
    } else {
        if ("" == offset_str){
            offset = 0;
        } else {
            printf("offset must be a number\n");
            return 0;
        }
    }

    if (isNum(block_size)) {
        g_block_size = std::stoll(block_size, NULL, 0);
        if (g_block_size < 0) {
            printf("block_size must be greater or equal to 0\n");
            return 0;
        }
    } else {
        if ("" == block_size){
            g_block_size = 0;
        } else {
            printf("block_size must be a number\n");
            return 0;
        }
    }

    if (isNum(block_sleep)) {
        g_block_sleep = std::stoll(block_sleep, NULL, 0);
        if (g_block_sleep < 0) {
            printf("block_sleep must be greater or equal to 0\n");
            return 0;
        }
    } else {
        if ("" == block_sleep){
            g_block_sleep = 0;
        } else {
            printf("block_sleep must be a number\n");
            return 0;
        }
    }

    if (path.empty() || host.empty() || port.empty()) {
        usage();
        return 0;
    }

    int info_level = verbose ? AOF_LOG_TRACE : AOF_LOG_INFO;
    set_info_level(info_level);

    std::ifstream ifs(path.c_str(), std::ios::binary);
    if(!ifs){
        LOG_ERR("Error aof file path");
        usage();
        return 0;
    }
    ifs.close();
    
    sender = new AOFSender();
    if (false == sender->rconnect(host, port, auth)) {
        LOG_ERR("Failed to connect remote server! host: " + host + " port : " + port);
        delete sender;
        return -1;
    }

    // We need output something when ctrl c catched
    begin_time = time(0);
    signal(SIGINT, intHandler);

    int ret = 0;
    pthread_t send_thread;
    // Send thread
    ret = pthread_create(&send_thread, NULL, msg_process, &offset);
    if (0 != ret)
    {
        LOG_ERR("Failed to create assist_watcher_thread!");
        delete sender;
        return -1;
    }

    // Main thread
    file_read(path, offset);

    pthread_join(send_thread, NULL);
    delete sender;
    return 0;
}
