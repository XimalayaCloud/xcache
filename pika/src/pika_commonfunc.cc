#include <glog/logging.h>

#include "pika_commonfunc.h"
#include "pink/include/redis_conn.h"
#include "pink/include/redis_cli.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"
#include "slash/include/slash_string.h"

const std::string kInnerReplOk = "ok";

// crc
static const uint32_t IEEE_POLY = 0xedb88320;
static const uint32_t CAST_POLY = 0x82f63b78;
static const uint32_t KOOP_POLY = 0xeb31d82e;

static uint32_t crc32tab[256];

static void CRC32TableInit(uint32_t poly) {
    int i, j;
    for (i = 0; i < 256; i ++) {
        uint32_t crc = i;
        for (j = 0; j < 8; j ++) {
            if (crc & 1) {
                crc = (crc >> 1) ^ poly;
            } else {
                crc = (crc >> 1);
            }
        }
        crc32tab[i] = crc;
    }
}

void
PikaCommonFunc::InitCRC32Table(void)
{
	CRC32TableInit(IEEE_POLY);
}

uint32_t
PikaCommonFunc::CRC32Update(uint32_t crc, const char *buf, int len)
{
    crc = ~crc;
    for (int i = 0; i < len; i++) {
        crc = crc32tab[(uint8_t)((char)crc ^ buf[i])] ^ (crc >> 8);
    }
    return ~crc;
}

uint32_t
PikaCommonFunc::CRC32CheckSum(const char *buf, int len)
{
    return CRC32Update(0, buf, len);
}

bool
PikaCommonFunc::DoAuth(pink::PinkCli *client, const std::string requirepass)
{
    if (NULL == client) {
        return false;
    }

    pink::RedisCmdArgsType argv;
    std::string wbuf_str;
    if (requirepass != "") {
        argv.push_back("auth");
        argv.push_back(requirepass);
        pink::SerializeRedisCommand(argv, &wbuf_str);

        slash::Status s;
        s = client->Send(&wbuf_str);
        if (!s.ok()) {
            LOG(WARNING) << "PikaCommonFunc::DoAuth Slot Migrate auth error: " << strerror(errno);
            return false;
        }

        // Recv
        s = client->Recv(&argv);
        if (!s.ok()) {
            LOG(WARNING) << "PikaCommonFunc::DoAuth Slot Migrate auth Recv error: " << strerror(errno);
            return false;
        }

        if (kInnerReplOk != slash::StringToLower(argv[0])) {
            LOG(ERROR) << "PikaCommonFunc::DoAuth auth error";
            return false;
        }
    }
    return true;
}
