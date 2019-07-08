#ifndef PIKA_COMMONFUNC_H_
#define PIKA_COMMONFUNC_H_

#include <cstdint>

#include "pink/include/pink_cli.h"


class PikaCommonFunc
{
public:
    static void InitCRC32Table(void);
    static uint32_t CRC32Update(uint32_t crc, const char *buf, int len);
    static uint32_t CRC32CheckSum(const char *buf, int len);

    static bool DoAuth(pink::PinkCli *client, const std::string requirepass);
    
private:
    PikaCommonFunc();
    ~PikaCommonFunc();
};

#endif