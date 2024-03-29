// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"bytes"
	"hash/crc32"
	"strconv"
	"strings"
	"sync"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

var charmap [256]byte
var cmdsflag sync.RWMutex

func init() {
	for i := range charmap {
		c := byte(i)
		switch {
		case c >= 'A' && c <= 'Z':
			charmap[i] = c
		case c >= 'a' && c <= 'z':
			charmap[i] = c - 'a' + 'A'
		}
	}
}

type OpFlag uint32
type OpFlagMonitor uint32  //监控大key，大value的标志位

//标志位：读写，快慢命令判断
func (f OpFlag) IsNotAllowed() bool {
	return (f & FlagNotAllow) != 0
}

func (f OpFlag) IsReadOnly() bool {
	const mask = FlagWrite | FlagMayWrite
	return (f & mask) == 0
}

func (f OpFlag) IsMasterOnly() bool {
	const mask = FlagWrite | FlagMayWrite | FlagMasterOnly
	return (f & mask) != 0
}

//只要没有慢标志就认为是快命令
func (f OpFlag) IsQuick() bool {
	const mask = FlagSureSlow | FlagMaySlow
	return (f & mask) == 0
}

func (f OpFlag) IsSureQuick() bool {
	const mask = FlagSureQuick
	return (f & mask) != 0
}

func (f OpFlag) IsMayQuick() bool {
	const mask = FlagMayQuick
	return (f & mask) != 0
}

//标志位：大key，大value判断
func (f OpFlagMonitor) NeedCheckBatchsizeOfRequest() bool {
	const mask = FlagReqKeys | FlagReqKeyFields
	return (f & mask) != 0
}

func (f OpFlagMonitor) NeedCheckContentOfRequest() bool {
	const mask = FlagReqValues | FlagReqKeyValues | FlagReqKeyFieldValues | FlagReqKeyTtlValue
	return (f & mask) != 0
}

func (f OpFlagMonitor) NeedCheckSingleValueOfResp () bool {
	const mask = FlagRespReturnSingleValue
	return (f & mask) != 0
}

func (f OpFlagMonitor) NeedCheckNumberOfResp() bool {
	const mask = FlagRespReturnArraysize | FlagRespReturnValuesize
	return (f & mask) != 0
}

func (f OpFlagMonitor) NeedCheckArrayOfResp() bool {
	const mask = FlagRespReturnArray | FlagRespReturnArrayByPair | FlagRespCheckArrayLength | FlagRespCheckArrayLengthByPair
	return (f & mask) != 0
}

func (f OpFlagMonitor) IsHighRisk() bool {
	const mask = FlagHighRisk
	return (f & mask) != 0
}

type CustomCheckFunc interface {
	CheckRequest(r *Request, s *Session) bool //return true表示检查过了
	CheckResponse(r *Request, s *Session, delay int64) bool //return true表示检查过了
}

type OpInfo struct {
	Name string
	Flag OpFlag
	FlagMonitor OpFlagMonitor
	CustomCheckFunc
}

const (
	FlagWrite = 1 << iota   //1
	FlagMasterOnly			//2
	FlagMayWrite			//4
	FlagNotAllow			//8
	FlagSureQuick			//16
	FlagMayQuick			//32
	FlagSureSlow			//64
	FlagMaySlow				//128
)

const (
	// -- 请求部分
	// 1. 请求包含多个key或多个成员，没有值，即不需要考虑数据量的大小
	FlagReqKeys = 1 << iota              // 1     参数为：CMD Key1 ~ KeyN
	FlagReqKeyFields                     // 2     参数为：CMD KEY Field1 ~ FieldN

	// 2. 请求不光包含key或成员，还操作了响应的值，需要检查数据量
	FlagReqValues                        // 4     参数为：CMD KEY Value1 ~ ValueN
	FlagReqKeyValues                     // 8     参数为：CMD (Key1,Value1) ~ (KeyN,ValueN)
	FlagReqKeyFieldValues                // 16    参数为：CMD KEY (Field1,Value1) ~ (FieldN,ValueN)
	FlagReqKeyTtlValue					 // 32    参数为：CMD KEY ttl value

	// -- 响应部分
	// 1. 响应返回的是单个数字，代表key的成员数量或者key本身大小（如果key是string类型），即对key的统计结果
	FlagRespReturnArraysize              // 64    返回值是数组的长度
	FlagRespReturnValuesize              //	128    返回值是string的长度

	// 2. 响应返回的是查询结果，代表key本身或者一部分
	FlagRespReturnSingleValue            // 256   返回值是单个值
	FlagRespReturnArray                  // 512   返回为数组，一个为一组
	FlagRespReturnArrayByPair            // 1024   返回为数组，两个为一组

	// 3. 响应返回的是数组，但是只要检查数组大小，数组的内容不要检查
	FlagRespCheckArrayLength             // 2048  返回为数组，一个为一组, 只检查数组的长度，不检查数组内容
	FlagRespCheckArrayLengthByPair       // 4096  返回为数组，两个为一组, 只检查数组的长度，不检查数组内容

	// -- 命令本身是高危操作，高危操作一定要被记录，即使不一定有风险
	FlagHighRisk                         // 8192  高风险命令
)

var opTable = make(map[string]*OpInfo, 256)

func init() {
	var cmds = []OpInfo{
		{"APPEND", FlagWrite, FlagReqKeyValues | FlagRespReturnValuesize, nil},
		{"ASKING", FlagNotAllow, 0, nil},
		{"AUTH", 0, 0, nil},
		{"BGREWRITEAOF", FlagNotAllow, 0, nil},
		{"BGSAVE", FlagNotAllow, 0, nil},
		{"BITCOUNT", 0, 0, nil},
		{"BITFIELD", FlagWrite, 0, nil},
		{"BITOP", FlagWrite | FlagNotAllow, 0, nil},
		{"BITPOS", 0, 0, nil},
		{"BLPOP", FlagWrite | FlagNotAllow, 0, nil},
		{"BRPOP", FlagWrite | FlagNotAllow, 0, nil},
		{"BRPOPLPUSH", FlagWrite | FlagNotAllow, 0, nil},
		{"CLIENT", FlagNotAllow, 0, nil},
		{"CLUSTER", 0, 0, nil},
		{"COMMAND", 0, 0, nil},
		{"CONFIG", FlagNotAllow, 0, nil},
		{"DBSIZE", FlagNotAllow, 0, nil},
		{"DEBUG", FlagNotAllow, 0, nil},
		{"DECR", FlagWrite, 0, nil},
		{"DECRBY", FlagWrite, 0, nil},
		{"DEL", FlagWrite, FlagReqKeys, nil},
		{"DISCARD", FlagNotAllow, 0, nil},
		{"DUMP", 0, 0, nil},
		{"ECHO", 0, 0, nil},
		{"EVAL", FlagWrite, 0, nil},
		{"EVALSHA", FlagWrite, 0, nil},
		{"EXEC", FlagNotAllow, 0, nil},
		{"EXISTS", 0, 0, nil},
		{"EXPIRE", FlagWrite, 0, nil},
		{"EXPIREAT", FlagWrite, 0, nil},
		{"FLUSHALL", FlagWrite | FlagNotAllow, 0, nil},
		{"FLUSHDB", FlagWrite | FlagNotAllow, 0, nil},
		{"GEOADD", FlagWrite, 0, nil},
		{"GEODIST", 0, 0, nil},
		{"GEOHASH", 0, 0, nil},
		{"GEOPOS", 0, 0, nil},
		{"GEORADIUS", FlagWrite, 0, nil},
		{"GEORADIUSBYMEMBER", FlagWrite, 0, nil},
		{"GET", 0, FlagRespReturnSingleValue, nil},
		{"GETBIT", 0, 0, nil},
		{"GETRANGE", 0, 0, nil},
		{"GETSET", FlagWrite, FlagReqKeyValues | FlagRespReturnSingleValue, nil},
		{"HDEL", FlagWrite, FlagReqKeyFields, nil},
		{"HEXISTS", 0, 0, nil},
		{"HGET", 0, 0, &CheckHGET{}},
		{"HGETALL", 0, FlagRespReturnArrayByPair | FlagHighRisk, nil},
		{"HINCRBY", FlagWrite, 0, nil},
		{"HINCRBYFLOAT", FlagWrite, 0, nil},
		{"HKEYS", 0, FlagRespCheckArrayLength | FlagHighRisk, nil},
		{"HLEN", 0, FlagRespReturnArraysize, nil},
		{"HMGET", 0, FlagReqKeyFields | FlagRespReturnArray, nil},
		{"HMSET", FlagWrite, FlagReqKeyFieldValues, nil},
		{"HSCAN", FlagMasterOnly, 0, nil},
		{"HSET", FlagWrite, FlagReqKeyFieldValues, nil},
		{"HSETNX", FlagWrite, FlagReqKeyFieldValues, nil},
		{"HSTRLEN", 0, 0, nil},
		{"HVALS", 0, 0, nil},
		{"INCR", FlagWrite, 0, nil},
		{"INCRBY", FlagWrite, 0, nil},
		{"INCRBYFLOAT", FlagWrite, 0, nil},
		{"INFO", 0, 0, nil},
		{"KEYS", FlagNotAllow, 0, nil},
		{"LASTSAVE", FlagNotAllow, 0, nil},
		{"LATENCY", FlagNotAllow, 0, nil},
		{"LINDEX", 0, 0, nil},
		{"LINSERT", FlagWrite, 0, nil},
		{"LLEN", 0, FlagRespReturnArraysize, nil},
		{"LPOP", FlagWrite, 0, nil},
		{"LPUSH", FlagWrite, FlagReqKeyFields | FlagRespReturnArraysize, nil},
		{"LPUSHX", FlagWrite, FlagReqKeyFields | FlagRespReturnArraysize, nil},
		{"LRANGE", 0, FlagRespCheckArrayLength, &CheckLRANGE{}},
		{"LREM", FlagWrite, FlagRespReturnArraysize, nil},
		{"LSET", FlagWrite, 0, nil},
		{"LTRIM", FlagWrite, 0, nil},
		{"MGET", 0, FlagReqKeys, &CheckMGET{}},
		{"MIGRATE", FlagWrite | FlagNotAllow, 0, nil},
		{"MONITOR", FlagNotAllow, 0, nil},
		{"MOVE", FlagWrite | FlagNotAllow, 0, nil},
		{"MSET", FlagWrite, FlagReqKeyValues, nil},
		{"MSETNX", FlagWrite | FlagNotAllow, FlagReqKeyValues, nil},
		{"MULTI", FlagNotAllow, 0, nil},
		{"OBJECT", FlagNotAllow, 0, nil},
		{"PERSIST", FlagWrite, 0, nil},
		{"PEXPIRE", FlagWrite, 0, nil},
		{"PEXPIREAT", FlagWrite, 0, nil},
		{"PFADD", FlagWrite, 0, nil},
		{"PFCOUNT", 0, 0, nil},
		{"PFDEBUG", FlagWrite, 0, nil},
		{"PFMERGE", FlagWrite, 0, nil},
		{"PFSELFTEST", 0, 0, nil},
		{"PING", 0, 0, nil},
		{"PSETEX", FlagWrite, FlagReqKeyTtlValue, nil},
		{"PSUBSCRIBE", FlagNotAllow, 0, nil},
		{"PSYNC", FlagNotAllow, 0, nil},
		{"PTTL", 0, 0, nil},
		{"PUBLISH", FlagNotAllow, 0, nil},
		{"PUBSUB", 0, 0, nil},
		{"PUNSUBSCRIBE", FlagNotAllow, 0, nil},
		{"QUIT", 0, 0, nil},
		{"RANDOMKEY", FlagNotAllow, 0, nil},
		{"READONLY", FlagNotAllow, 0, nil},
		{"READWRITE", FlagNotAllow, 0, nil},
		{"RENAME", FlagWrite | FlagNotAllow, 0, nil},
		{"RENAMENX", FlagWrite | FlagNotAllow, 0, nil},
		{"REPLCONF", FlagNotAllow, 0, nil},
		{"RESTORE", FlagWrite | FlagNotAllow, 0, nil},
		{"RESTORE-ASKING", FlagWrite | FlagNotAllow, 0, nil},
		{"ROLE", 0, 0, nil},
		{"RPOP", FlagWrite, 0, nil},
		{"RPOPLPUSH", FlagWrite, 0, nil},
		{"RPUSH", FlagWrite, FlagReqKeyFields | FlagRespReturnArraysize, nil},
		{"RPUSHX", FlagWrite, FlagReqKeyFields | FlagRespReturnArraysize, nil},
		{"SADD", FlagWrite, FlagReqKeyFields, nil},
		{"SAVE", FlagNotAllow, 0, nil},
		{"SCAN", FlagMasterOnly | FlagNotAllow, 0, nil},
		{"SCARD", 0, FlagRespReturnArraysize, nil},
		{"SCRIPT", FlagNotAllow, 0, nil},
		{"SDIFF", 0, FlagReqKeys, &CheckSDIFF{}},
		{"SDIFFSTORE", FlagWrite, 0, nil},
		{"SELECT", 0, 0, nil},
		{"SET", FlagWrite, 0, &CheckSET{}},
		{"SETBIT", FlagWrite, 0, nil},
		{"SETEX", FlagWrite, FlagReqKeyTtlValue, nil},  // 特殊，因为包含特殊参数ttl
		{"SETNX", FlagWrite, FlagReqKeyValues, nil},
		{"SETRANGE", FlagWrite, FlagReqKeyFieldValues | FlagRespReturnValuesize, nil}, // 特殊，因为包含特殊参数offset
		{"SHUTDOWN", FlagNotAllow, 0, nil},
		{"SINTER", 0, FlagReqKeys, &CheckSETCOMPARE{}},
		{"SINTERSTORE", FlagWrite, FlagReqKeys, &CheckSETCOMPAREANDSTORE{}},
		{"SISMEMBER", 0, 0, nil},
		{"SLAVEOF", FlagNotAllow, 0, nil},
		{"SLOTSCHECK", FlagNotAllow, 0, nil},
		{"SLOTSDEL", FlagWrite | FlagNotAllow, 0, nil},
		{"SLOTSHASHKEY", 0, 0, nil},
		{"SLOTSINFO", FlagMasterOnly, 0, nil},
		{"SLOTSMAPPING", 0, 0, nil},
		{"SLOTSMGRTONE", FlagWrite | FlagNotAllow, 0, nil},
		{"SLOTSMGRTSLOT", FlagWrite | FlagNotAllow, 0, nil},
		{"SLOTSMGRTTAGONE", FlagWrite | FlagNotAllow, 0, nil},
		{"SLOTSMGRTTAGSLOT", FlagWrite | FlagNotAllow, 0, nil},
		{"SLOTSRESTORE", FlagWrite, 0, nil},
		{"SLOTSMGRTONE-ASYNC", FlagWrite | FlagNotAllow, 0, nil},
		{"SLOTSMGRTSLOT-ASYNC", FlagWrite | FlagNotAllow, 0, nil},
		{"SLOTSMGRTTAGONE-ASYNC", FlagWrite | FlagNotAllow, 0, nil},
		{"SLOTSMGRTTAGSLOT-ASYNC", FlagWrite | FlagNotAllow, 0, nil},
		{"SLOTSMGRT-ASYNC-FENCE", FlagNotAllow, 0, nil},
		{"SLOTSMGRT-ASYNC-CANCEL", FlagNotAllow, 0, nil},
		{"SLOTSMGRT-ASYNC-STATUS", FlagNotAllow, 0, nil},
		{"SLOTSMGRT-EXEC-WRAPPER", FlagWrite | FlagNotAllow, 0, nil},
		{"SLOTSRESTORE-ASYNC", FlagWrite | FlagNotAllow, 0, nil},
		{"SLOTSRESTORE-ASYNC-AUTH", FlagWrite | FlagNotAllow, 0, nil},
		{"SLOTSRESTORE-ASYNC-ACK", FlagWrite | FlagNotAllow, 0, nil},
		{"SLOTSSCAN", FlagMasterOnly, 0, nil},
		{"SLOWLOG", FlagNotAllow, 0, nil},
		{"SMEMBERS", 0, FlagRespCheckArrayLength | FlagHighRisk, nil},
		{"SMOVE", FlagWrite, 0, nil},
		{"SORT", FlagWrite, 0, nil},
		{"SPOP", FlagWrite, FlagRespCheckArrayLength, &CheckSPOP{}},
		{"SRANDMEMBER", 0, FlagRespCheckArrayLength, &CheckSRANDMEMBER{}},
		{"SREM", FlagWrite, FlagReqKeyFields, nil},
		{"SSCAN", FlagMasterOnly, 0, nil},
		{"STRLEN", 0, FlagRespReturnValuesize, nil},
		{"SUBSCRIBE", FlagNotAllow, 0, nil},
		{"SUBSTR", 0, 0, nil},
		{"SUNION", 0, FlagReqKeys, &CheckSETCOMPARE{}},
		{"SUNIONSTORE", FlagWrite, FlagReqKeys, &CheckSETCOMPAREANDSTORE{}},
		{"SYNC", FlagNotAllow, 0, nil},
		{"TIME", FlagNotAllow, 0, nil},
		{"TOUCH", FlagWrite, 0, nil},
		{"TTL", 0, 0, nil},
		{"TYPE", 0, 0, nil},
		{"UNSUBSCRIBE", FlagNotAllow, 0, nil},
		{"UNWATCH", FlagNotAllow, 0, nil},
		{"WAIT", FlagNotAllow, 0, nil},
		{"WATCH", FlagNotAllow, 0, nil},
		{"XSLOWLOG", 0, 0, nil},
		{"XMONITOR", 0, 0, nil},
		{"XCONFIG", 0, 0, nil},
		{"ZADD", FlagWrite, 0, nil},  //特殊，因为需要解析，版本较高时接收多种参数
		{"ZCARD", 0, FlagRespReturnArraysize, nil},
		{"ZCOUNT", 0, 0, nil},
		{"ZINCRBY", FlagWrite, 0, nil},           //特殊，因为需要解析
		{"ZINTERSTORE", FlagWrite, 0, nil},           //特殊，因为需要解析，解析较复杂
		{"ZLEXCOUNT", 0, 0, nil},
		{"ZRANGE", 0, 0, &CheckZRANGE{}},
		{"ZRANGEBYLEX", 0, 0, nil},
		{"ZRANGEBYSCORE", 0, 0, nil},
		{"ZRANK", 0, 0, nil},
		{"ZREM", FlagWrite, FlagReqKeyFields, nil},
		{"ZREMRANGEBYLEX", FlagWrite, 0, nil},
		{"ZREMRANGEBYRANK", FlagWrite, 0, nil},
		{"ZREMRANGEBYSCORE", FlagWrite, 0, nil},
		{"ZREVRANGE", 0, 0, nil},
		{"ZREVRANGEBYLEX", 0, 0, nil},
		{"ZREVRANGEBYSCORE", 0, 0, nil},
		{"ZREVRANK", 0, 0, nil},
		{"ZSCAN", FlagMasterOnly, 0, nil},
		{"ZSCORE", 0, 0, nil},
		{"ZUNIONSTORE", FlagWrite, 0, nil}, //特殊，因为需要解析，解析较复杂
		{"EHSET", FlagWrite, 0, nil},
		{"EHSETNX", FlagWrite, 0, nil},
		{"EHSETEX", FlagWrite, 0, nil},
		{"EHEXPIRE", FlagWrite, 0, nil},
		{"EHEXPIREAT", FlagWrite, 0, nil},
		{"EHTTL", 0, 0, nil},
		{"EHPERSIST", FlagWrite, 0, nil},
		{"EHGET", 0, 0, nil},
		{"EHEXISTS", 0, 0, nil},
		{"EHDEL", FlagWrite, 0, nil},
		{"EHLEN", 0, 0, nil},
		{"EHSTRLEN", 0, 0, nil},
		{"EHINCRBY", FlagWrite, 0, nil},
		{"EHINCRBYFLOAT", FlagWrite, 0, nil},
		{"EHMSET", FlagWrite, 0, nil},
		{"EHMSETEX", FlagWrite, 0, nil},
		{"EHMGET", 0, 0, nil},
		{"EHKEYS", 0, 0, nil},
		{"EHVALS", 0, 0, nil},
		{"EHGETALL", 0, 0, nil},
		{"EHSCAN", FlagMasterOnly, 0, nil},
	}
	for i, _ := range cmds {
		opTable[cmds[i].Name] = &cmds[i]
	}
}

var (
	ErrBadMultiBulk = errors.New("bad multi-bulk for command")
	ErrBadOpStrLen  = errors.New("bad command length, too short or too long")
)

const MaxOpStrLen = 64

func getOpInfo(multi []*redis.Resp) (string, OpFlag, OpFlagMonitor, CustomCheckFunc, error) {
	if len(multi) < 1 {
		return "", 0, 0, nil, ErrBadMultiBulk
	}

	var upper [MaxOpStrLen]byte

	var op = multi[0].Value
	if len(op) == 0 || len(op) > len(upper) {
		return "", 0, 0, nil, ErrBadOpStrLen
	}
	for i := range op {
		if c := charmap[op[i]]; c != 0 {
			upper[i] = c
		} else {
			return strings.ToUpper(string(op)), FlagMayWrite, 0, nil, nil
		}
	}
	op = upper[:len(op)]
	if r, ok := opTable[string(op)]; ok {
		return r.Name, r.Flag, r.FlagMonitor, r.CustomCheckFunc, nil
	}
	return string(op), FlagMayWrite, 0, nil, nil
}

func getCmdFlag() *redis.Resp{
	//array must init, or array is nil when there is no slowlog
	var array []*redis.Resp = make([]*redis.Resp, 0, 32)
	const mask = FlagSureQuick | FlagMayQuick | FlagSureSlow | FlagMaySlow

	for _, r := range opTable {
		if r.Flag & mask != 0 {
			retstr := r.Name + " : Flag[" + strconv.Itoa(int(r.Flag)) + "]"

			if r.Flag & FlagSureQuick != 0 {
				retstr += ", FlagSureQuick"
			}

			if r.Flag & FlagMayQuick != 0 {
				retstr += ", FlagMayQuick"
			}

			if r.Flag & FlagSureSlow != 0 {
				retstr += ", FlagSureSlow"
			}

			if r.Flag & FlagMaySlow != 0 {
				retstr += ", FlagMaySlow"
			}

			array = append(array, redis.NewBulkBytes([]byte(retstr)))
		}
	}

	return redis.NewArray(array)
}

func setQuickCmdList(cmdlist string) error {
	//设置快慢标志时加锁，防止命令被同时设置快命令和慢命令
	cmdsflag.Lock()
	defer cmdsflag.Unlock()
	//首先需要清除所有命令的快标志
	for _, r := range opTable {
		r.Flag = r.Flag &^ FlagSureQuick
	}

	if cmdlist == "" {
		return nil
	}

	cmdlist = strings.ToUpper(cmdlist)
	cmds := strings.Split(cmdlist, ",")
	for i := 0; i < len(cmds); i++ {
		if r, ok := opTable[strings.TrimSpace(cmds[i])]; ok {
			log.Infof("before setQuickCmdList: r.Name[%s], r.Flag[%d]", r.Name, r.Flag)
			//如果命令被设置了慢标志则返回失败
			if r.Flag & FlagSureSlow == 0 {
				r.Flag = r.Flag &^ FlagMayQuick
				r.Flag = r.Flag &^ FlagMaySlow
				r.Flag = r.Flag | FlagSureQuick
				log.Infof("after setSlowCmdList: r.Name[%s], r.Flag[%d]", r.Name, r.Flag)
			} else {
				log.Warnf("cmd[%s] is FlagSureSlow command.", cmds[i])
				return errors.Errorf("cmd[%s] is FlagSureSlow command.", cmds[i])
			}
		} else {
			//这里不能新建命令，session每次请求都要无锁操作opTable，这里添加新命令会存在多线程安全问题
			log.Warnf("cant find [%s] command.", cmds[i])
			return errors.Errorf("cant find [%s] command.", cmds[i])
		}
	}

	return nil
}

//用于proxy启动时通过配置文件为不存在的命令设置快慢命令标志
func setQuickCmdListForStart(cmdlist string) error {
	//设置快慢标志时加锁，防止命令被同时设置快命令和慢命令
	cmdsflag.Lock()
	defer cmdsflag.Unlock()

	//首先需要清除所有命令的快标志
	for _, r := range opTable {
		r.Flag = r.Flag &^ FlagSureQuick
	}

	if cmdlist == "" {
		return nil
	}

	cmdlist = strings.ToUpper(cmdlist)
	cmds := strings.Split(cmdlist, ",")
	for i := 0; i < len(cmds); i++ {
		if r, ok := opTable[strings.TrimSpace(cmds[i])]; ok {
			log.Infof("before setQuickCmdList: r.Name[%s], r.Flag[%d]", r.Name, r.Flag)
			//如果命令被设置了慢标志则返回失败
			if r.Flag & FlagSureSlow == 0 {
				r.Flag = r.Flag &^ FlagMayQuick
				r.Flag = r.Flag &^ FlagMaySlow
				r.Flag = r.Flag | FlagSureQuick
				log.Infof("after setSlowCmdList: r.Name[%s], r.Flag[%d]", r.Name, r.Flag)
			} else {
				log.Warnf("cmd[%s] is FlagSureSlow command.", cmds[i])
				return errors.Errorf("cmd[%s] is FlagSureSlow command.", cmds[i])
			}
		} else {
			log.Warnf("cant find [%s] command, new a command.", cmds[i])
			//return errors.Errorf("cant find [%s] command.", cmds[i])
			cmd := &OpInfo{}
			cmd.Name = strings.TrimSpace(cmds[i])
			cmd.Flag = FlagMayWrite | FlagSureQuick
			opTable[cmd.Name] = cmd
		}
	}

	return nil
}

func setSlowCmdList(cmdlist string) error {
	//设置快慢标志时加锁，防止命令被同时设置快命令和慢命令
	cmdsflag.Lock()
	defer cmdsflag.Unlock()
	//首先需要清除所有命令的慢标志
	for _, r := range opTable {
		r.Flag = r.Flag &^ FlagSureSlow
	}

	if cmdlist == "" {
		return nil
	}

	cmdlist = strings.ToUpper(cmdlist)
	cmds := strings.Split(cmdlist, ",")
	for i := 0; i < len(cmds); i++ {
		if r, ok := opTable[strings.TrimSpace(cmds[i])]; ok {
			log.Infof("before setSlowCmdList: r.Name[%s], r.Flag[%d]", r.Name, r.Flag)
			//如果命令被设置了快标志则返回失败
			if r.Flag & FlagSureQuick == 0 {
				r.Flag = r.Flag &^ FlagMayQuick
				r.Flag = r.Flag &^ FlagMaySlow
				r.Flag = r.Flag | FlagSureSlow
				log.Infof("after setSlowCmdList: r.Name[%s], r.Flag[%d]", r.Name, r.Flag)
			} else {
				log.Warnf("cmd[%s] is FlagSureQuick command.", cmds[i])
				return errors.Errorf("cmd[%s] is FlagSureQuick command.", cmds[i])
			}
		} else {
			//这里不能新建命令，session每次请求都要无锁操作opTable，这里添加新命令会存在多线程安全问题
			log.Warnf("cant find [%s] command.", cmds[i])
			return errors.Errorf("cant find [%s] command.", cmds[i])
		}
	}

	return nil
}

//用于proxy启动时通过配置文件为不存在的命令设置快慢命令标志
func setSlowCmdListForStart(cmdlist string) error {
	//设置快慢标志时加锁，防止命令被同时设置快命令和慢命令
	cmdsflag.Lock()
	defer cmdsflag.Unlock()
	//首先需要清除所有命令的慢标志
	for _, r := range opTable {
		r.Flag = r.Flag &^ FlagSureSlow
	}

	if cmdlist == "" {
		return nil
	}

	cmdlist = strings.ToUpper(cmdlist)
	cmds := strings.Split(cmdlist, ",")
	for i := 0; i < len(cmds); i++ {
		if r, ok := opTable[strings.TrimSpace(cmds[i])]; ok {
			log.Infof("before setSlowCmdList: r.Name[%s], r.Flag[%d]", r.Name, r.Flag)
			//如果命令被设置了快标志则返回失败
			if r.Flag & FlagSureQuick == 0 {
				r.Flag = r.Flag &^ FlagMayQuick
				r.Flag = r.Flag &^ FlagMaySlow
				r.Flag = r.Flag | FlagSureSlow
				log.Infof("after setSlowCmdList: r.Name[%s], r.Flag[%d]", r.Name, r.Flag)
			} else {
				log.Warnf("cmd[%s] is FlagSureQuick command.", cmds[i])
				return errors.Errorf("cmd[%s] is FlagSureQuick command.", cmds[i])
			}
		} else {
			log.Warnf("cant find [%s] command, new a command.", cmds[i])
			//return errors.Errorf("cant find [%s] command.", cmds[i])
			cmd := &OpInfo{}
			cmd.Name = strings.TrimSpace(cmds[i])
			cmd.Flag = FlagMayWrite | FlagSureSlow
			opTable[cmd.Name] = cmd
		}
	}

	return nil
}

//加读写锁保证全局设置quicklist和slowlist的时候，flag不会被其他操作修改
func setMaySlowOpFlag(op string) error {
	cmdsflag.RLock()
	defer cmdsflag.RUnlock()
	//如果命令设置为FlagSureQuick或FlagSureSlow者则不允许命令设置为慢命令
	const mask = FlagSureQuick | FlagSureSlow
	if r, ok := opTable[strings.ToUpper(op)]; ok {
		if r.Flag & mask == 0 {
			r.Flag = r.Flag | FlagMaySlow
		}
	} else {
		return errors.Errorf("cant find %s.", op)
	}

	return nil
}

func clearMaySlowOpFlag(op string) error {
	cmdsflag.RLock()
	defer cmdsflag.RUnlock()
	//如果命令设置为FlagSureQuick或FlagSureSlow者则不需要清除慢标志
	const mask = FlagSureQuick | FlagSureSlow
	if r, ok := opTable[strings.ToUpper(op)]; ok {
		//如果命令设置为sureslow则不需要清除命令慢标志
		if r.Flag & mask == 0 {
			r.Flag = r.Flag &^ FlagMaySlow
		}
	} else {
		return errors.Errorf("cant find %s.", op)
	}

	return nil
}

func Hash(key []byte) uint32 {
	const (
		TagBeg = '{'
		TagEnd = '}'
	)
	if beg := bytes.IndexByte(key, TagBeg); beg >= 0 {
		if end := bytes.IndexByte(key[beg+1:], TagEnd); end >= 0 {
			key = key[beg+1 : beg+1+end]
		}
	}
	return crc32.ChecksumIEEE(key)
}

func getHashKey(multi []*redis.Resp, opstr string) []byte {
	var index = 1
	switch opstr {
	case "ZINTERSTORE", "ZUNIONSTORE", "EVAL", "EVALSHA":
		index = 3
	}
	if index < len(multi) {
		return multi[index].Value
	}
	return nil
}

func getWholeCmd(multi []*redis.Resp, cmd []byte) int {
	var index = 0
	var bytes = 0

	for i := 0; i < len(multi); i++ {
		//cmd是固定大小切片，index最大等于cmd切片大小
		if index < len(cmd) {
			index += copy(cmd[index:], multi[i].Value)

			if i < len(multi) - 1 {
				index += copy(cmd[index:], []byte(" "))
			}
		}

		bytes += len(multi[i].Value)

		//遍历所有元素后，如果cmd切片被填满则添加统计信息
		if i == len(multi) - 1 && index == len(cmd) {
			more := []byte("... " + strconv.Itoa(len(multi)) + " elements " + strconv.Itoa(bytes) + " bytes.")
			index = len(cmd) - len(more)
			if index < 0 {
				index = 0
			}

			index += copy(cmd[index:], more)
			break;
		}
	}

	return index
}
