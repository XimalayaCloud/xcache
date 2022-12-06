// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	utilredis "github.com/CodisLabs/codis/pkg/utils/redis"
	"github.com/CodisLabs/codis/pkg/utils/rpc"
	"github.com/CodisLabs/codis/pkg/utils/unsafe2"
)

type Proxy struct {
	mu sync.Mutex

	xauth string
	model *models.Proxy

	exit struct {
		C chan struct{}
	}
	online bool
	closed bool

	config *Config
	router *Router
	ignore []byte

	lproxy net.Listener
	ladmin net.Listener

	ha struct {
		monitor *utilredis.Sentinel
		masters map[int]string
		servers []string
	}
	jodis *Jodis
}

var ErrClosedProxy = errors.New("use of closed proxy")

func New(config *Config) (*Proxy, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Trace(err)
	}
	if err := models.ValidateProduct(config.ProductName); err != nil {
		return nil, errors.Trace(err)
	}

	s := &Proxy{}
	s.config = config
	s.exit.C = make(chan struct{})
	s.router = NewRouter(config)
	s.ignore = make([]byte, config.ProxyHeapPlaceholder.Int64())

	s.model = &models.Proxy{
		StartTime: time.Now().String(),
	}
	s.model.ProductName = config.ProductName
	s.model.DataCenter = config.ProxyDataCenter
	s.model.Pid = os.Getpid()
	s.model.Pwd, _ = os.Getwd()
	if b, err := exec.Command("uname", "-a").Output(); err != nil {
		log.WarnErrorf(err, "run command uname failed")
	} else {
		s.model.Sys = strings.TrimSpace(string(b))
	}
	s.model.Hostname = utils.Hostname

	if err := s.setup(config); err != nil {
		s.Close()
		return nil, err
	}

	log.Warnf("[%p] create new proxy:\n%s", s, s.model.Encode())

	unsafe2.SetMaxOffheapBytes(config.ProxyMaxOffheapBytes.Int64())

	go s.serveAdmin()
	go s.serveProxy()
	go s.AutoPurgeLog()

	return s, nil
}

func (s *Proxy) setup(config *Config) error {
	proto := config.ProtoType
	if l, err := net.Listen(proto, config.ProxyAddr); err != nil {
		return errors.Trace(err)
	} else {
		s.lproxy = l

		x, err := utils.ReplaceUnspecifiedIP(proto, l.Addr().String(), config.HostProxy)
		if err != nil {
			return err
		}
		s.model.ProtoType = proto
		s.model.ProxyAddr = x
	}

	proto = "tcp"
	if l, err := net.Listen(proto, config.AdminAddr); err != nil {
		return errors.Trace(err)
	} else {
		s.ladmin = l

		x, err := utils.ReplaceUnspecifiedIP(proto, l.Addr().String(), config.HostAdmin)
		if err != nil {
			return err
		}
		s.model.AdminAddr = x
	}

	s.model.Token = rpc.NewToken(
		config.ProductName,
		s.lproxy.Addr().String(),
		s.ladmin.Addr().String(),
	)
	s.xauth = rpc.NewXAuth(
		config.ProductName,
		config.ProductAuth,
		s.model.Token,
	)

	s.model.JodisAddr = config.JodisAddr
	if config.JodisAddr != "" {
		c, err := models.NewClient(config.JodisName, config.JodisAddr, config.JodisAuth, config.JodisTimeout.Duration())
		if err != nil {
			return err
		}
		if config.JodisCompatible {
			if len(config.JodisProxySubDir) != 0 {
				s.model.JodisPath = filepath.Join("/zk/codis", fmt.Sprintf("db_%s", config.ProductName), "proxy", config.JodisProxySubDir, s.model.Token)
			} else {
				s.model.JodisPath = filepath.Join("/zk/codis", fmt.Sprintf("db_%s", config.ProductName), "proxy", s.model.Token)
			}
		} else {
			if len(config.JodisProxySubDir) != 0 {
				s.model.JodisPath = models.JodisPath(config.ProductName + "/" + config.JodisProxySubDir, s.model.Token)
			} else {
				s.model.JodisPath = models.JodisPath(config.ProductName, s.model.Token)
			}
		}
		s.jodis = NewJodis(c, s.model)
	}

	return nil
}

func (s *Proxy) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedProxy
	}
	if s.online {
		return nil
	}
	s.online = true
	s.router.Start()
	if s.jodis != nil {
		s.jodis.Start()
	}
	return nil
}

func (s *Proxy) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.exit.C)

	if s.jodis != nil {
		s.jodis.Close()
	}
	if s.ladmin != nil {
		s.ladmin.Close()
	}
	if s.lproxy != nil {
		s.lproxy.Close()
	}
	if s.router != nil {
		s.router.Close()
	}
	if s.ha.monitor != nil {
		s.ha.monitor.Cancel()
	}
	return nil
}

func (s *Proxy) XAuth() string {
	return s.xauth
}

func (s *Proxy) Model() *models.Proxy {
	return s.model
}

// 这里加锁，方式xconfig get、set命令与http的overview请求并发操作
func (s *Proxy) Config() Config {
	s.mu.Lock()
	defer s.mu.Unlock()

	return *s.config
}

//这个接口不再使用，只是保留给http接口用
func (s *Proxy) SetConfig(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch key {
	case "proxy_max_clients":
		n, err := strconv.Atoi(value)
		if err != nil {
			return err
		}

		if n <= 0 {
			return errors.New("invalid proxy_max_clients")
		} else {
			s.config.ProxyMaxClients = n
		}

	case "proxy_refresh_state_period":
		p := &(s.config.ProxyRefreshStatePeriod)
		err :=  p.UnmarshalText([]byte(value))
		if err != nil {
			return err
		}

		if d := s.config.ProxyRefreshStatePeriod.Duration(); d < 0 {
			return errors.New("invalid proxy_refresh_state_period")
		} else {
			StatsSetRefreshPeriod(d)
		}

	case "backend_primary_quick":
		n, err := strconv.Atoi(value)
		if err != nil {
			return err
		}

		if n < 0 {
			return errors.New("invalid backend_primary_quick")
		} else {
			s.config.BackendPrimaryQuick = n
			s.router.SetPrimaryQuickConn(s.config.BackendPrimaryQuick)
		}

	case "backend_replica_quick":
		n, err := strconv.Atoi(value)
		if err != nil {
			return err
		}

		if n < 0 {
			return errors.New("invalid backend_replica_quick")
		} else {
			s.config.BackendReplicaQuick = n
			s.router.SetReplicaQuickConn(s.config.BackendReplicaQuick)
		}

	case "slowlog_log_slower_than":
		i64, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}

		if i64 < 0 {
			return errors.New("invalid slowlog_log_slower_than")
		} else {
			s.config.SlowlogLogSlowerThan = i64
			StatsSetLogSlowerThan( i64 )
		}
		
	case "slowlog_max_len":
		i64, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}

		if i64 < 0 {
			return errors.New("invalid slowlog_max_len")
		} else {
			s.config.SlowlogMaxLen = i64
			if s.config.SlowlogMaxLen > 0 {
				XSlowlogSetMaxLen(s.config.SlowlogMaxLen)
			}
		}
	case "expire_log_days":
		intValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}

		if intValue < 0 {
			return errors.New("invalid expire_log_days")
		}
		s.config.ExpireLogDays = intValue

	default:
		return errors.New("invalid key")
	}

	return utils.RewriteConf(*(s.config), s.config.ConfigFileName, "=", true)
}

func (s *Proxy) ConfigSet(key, value string) *redis.Resp {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch key {
	case "proxy_max_clients":
		n, err := strconv.Atoi(value)
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}

		if n <= 0 {
			return redis.NewErrorf("invalid proxy_max_clients")
		} else {
			s.config.ProxyMaxClients = n
			return redis.NewString([]byte("OK"))
		}

	case "proxy_refresh_state_period":
		p := &(s.config.ProxyRefreshStatePeriod)
		err :=  p.UnmarshalText([]byte(value))
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}

		if d := s.config.ProxyRefreshStatePeriod.Duration(); d < 0 {
			return redis.NewErrorf("invalid proxy_refresh_state_period")
		} else {
			StatsSetRefreshPeriod(d)
			return redis.NewString([]byte("OK"))
		}

	case "backend_primary_only":
		return redis.NewErrorf("support as soon as possible.")

	case "backend_primary_quick":
		n, err := strconv.Atoi(value)
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}

		//至少留一个后端连接给慢命令使用
		if n < 0 || n >= s.config.BackendPrimaryParallel {
			return redis.NewErrorf("invalid backend_primary_quick")
		} else {
			s.config.BackendPrimaryQuick = n
			s.router.SetPrimaryQuickConn(s.config.BackendPrimaryQuick)
			return redis.NewString([]byte("OK"))
		}

	case "backend_replica_quick":
		n, err := strconv.Atoi(value)
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}

		//至少留一个后端连接给慢命令使用
		if n < 0 || n >= s.config.BackendReplicaParallel {
			return redis.NewErrorf("invalid backend_replica_quick")
		} else {
			s.config.BackendReplicaQuick = n
			s.router.SetReplicaQuickConn(s.config.BackendReplicaQuick)
			return redis.NewString([]byte("OK"))
		}

	case "slowlog_log_slower_than":
		i64, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}

		if i64 < 0 {
			return redis.NewErrorf("invalid slowlog_log_slower_than")
		} else {
			s.config.SlowlogLogSlowerThan = i64
			StatsSetLogSlowerThan( i64 )
			return redis.NewString([]byte("OK"))
		}
		
	case "slowlog_max_len":
		i64, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}

		if i64 < 0 {
			return redis.NewErrorf("invalid slowlog_max_len")
		} else {
			s.config.SlowlogMaxLen = i64
			if s.config.SlowlogMaxLen > 0 {
				XSlowlogSetMaxLen(s.config.SlowlogMaxLen)
			}
			return redis.NewString([]byte("OK"))
		}

	case "quick_cmd_list":
		err := setQuickCmdList(value)
		if err != nil {
			//恢复原来的命令设置
			log.Warnf("setQuickCmdList config[%s] failed, recover old config[%s].", value, s.config.QuickCmdList)
			setQuickCmdList(s.config.QuickCmdList)
			return redis.NewErrorf("err：%s.", err)
		}
		s.config.QuickCmdList = value
		return redis.NewString([]byte("OK"))

	case "slow_cmd_list":
		err := setSlowCmdList(value)
		if err != nil {
			//恢复原来的命令设置
			log.Warnf("setSlowCmdList config[%s] failed, recover old config[%s].", value, s.config.SlowCmdList)
			setSlowCmdList(s.config.SlowCmdList)
			return redis.NewErrorf("err：%s.", err)
		}
		s.config.SlowCmdList = value
		return redis.NewString([]byte("OK"))

	case "auto_set_slow_flag":
		boolValue, err := strconv.ParseBool(value)
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}
		StatsSetAutoSetSlowFlag(boolValue)
		s.config.AutoSetSlowFlag = boolValue
		return redis.NewString([]byte("OK"))

	case "expire_log_days":
		intValue, err := strconv.Atoi(value)
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}

		if intValue < 0 {
			return redis.NewErrorf("invalid expire_log_days")
		}
		s.config.ExpireLogDays = intValue
		return redis.NewString([]byte("OK"))
	case "monitor_enabled":
		i64, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}
		if i64!=0 && i64!=1{
			return redis.NewErrorf("invalid state for xmonitor. Try 0 or 1")
		}
		XMonitorSetMonitorState(i64)
		s.config.MonitorEnabled = i64
		return redis.NewString([]byte("OK"))
	case "monitor_max_value_len":
		i64, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}
		if i64 < 0 {
			return redis.NewErrorf("invalid monitor_max_value_len")
		} else {
			s.config.MonitorMaxValueLen = i64
			XMonitorSetMaxLengthOfValue(i64)
			return redis.NewString([]byte("OK"))
		}
	case "monitor_max_batchsize":
		i64, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}
		if i64 < 0 {
			return redis.NewErrorf("invalid monitor_max_batchsize")
		} else {
			s.config.MonitorMaxBatchsize = i64
			XMonitorSetMaxBatchsize(i64)
			return redis.NewString([]byte("OK"))
		}
	case "monitor_max_cmd_info":
		i64, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}
		if i64 < MAX_CMD_INFO_LENGTH_DEFAULT {
			return redis.NewErrorf("invalid monitor_max_cmd_info, no less than %v", MAX_CMD_INFO_LENGTH_DEFAULT)
		} else {
			s.config.MonitorMaxCmdInfo = i64
			XMonitorSetMaxLengthOfCmdInfo(i64)
			return redis.NewString([]byte("OK"))
		}
	case "monitor_log_max_len":
		i64, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}
		if i64 < 0 {
			return redis.NewErrorf("invalid monitor_log_max_len")
		} else {
			s.config.MonitorLogMaxLen = i64
			MonitorLogSetMaxLen(i64)
			return redis.NewString([]byte("OK"))
		}
	case "monitor_result_set_size":
		i64, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}
		if i64 < 0 {
			return redis.NewErrorf("invalid monitor_result_set_size")
		} else {
			s.config.MonitorResultSetSize = i64
			XMonitorSetResultSetSize(i64)
			return redis.NewString([]byte("OK"))
		}
	case "breaker_enabled":
		i64, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}
		if i64!=0 && i64!=1{
			return redis.NewErrorf("invalid state for breaker state. Try 0 or 1")
		}
		s.config.BreakerEnabled = i64
		BreakerSetState(i64)
		return redis.NewString([]byte("OK"))
	case "breaker_degradation_probability":
		i64, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}
		if i64 < 0 || i64 > 100 {
			return redis.NewErrorf("invalid breaker_degradation_probability, no less than 0 or more than 100")
		}
		s.config.BreakerDegradationProbability = i64
		BreakerSetProbability(i64)
		return redis.NewString([]byte("OK"))
	case "breaker_qps_limitation":
		i64, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return redis.NewErrorf("err：%s.", err)
		}
		s.config.BreakerQpsLimitation = i64
		BreakerSetTokenBucket(i64)
		return redis.NewString([]byte("OK"))
	case "breaker_cmd_white_list":
		StoreCmdWhiteListByBatch(value)
		s.config.BreakerCmdWhiteList = value
		return redis.NewString([]byte("OK"))
	case "breaker_cmd_black_list":
		StoreCmdBlackListByBatch(value)
		s.config.BreakerCmdBlackList = value
		return redis.NewString([]byte("OK"))
	case "breaker_key_white_list":
		StoreKeyWhiteListByBatch(value)
		s.config.BreakerKeyWhiteList = value
		return redis.NewString([]byte("OK"))
	case "breaker_key_black_list":
		StoreKeyBlackListByBatch(value)
		s.config.BreakerKeyBlackList = value
		return redis.NewString([]byte("OK"))
	case "*":
		return redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte("proxy_max_clients")),
			redis.NewBulkBytes([]byte("proxy_refresh_state_period")),
			redis.NewBulkBytes([]byte("backend_primary_quick")),
			redis.NewBulkBytes([]byte("backend_replica_quick")),
			redis.NewBulkBytes([]byte("slowlog_log_slower_than")),
			redis.NewBulkBytes([]byte("slowlog_max_len")),
			redis.NewBulkBytes([]byte("quick_cmd_list")),
			redis.NewBulkBytes([]byte("slow_cmd_list")),
			redis.NewBulkBytes([]byte("auto_set_slow_flag")),
			redis.NewBulkBytes([]byte("expire_log_days")),
			redis.NewBulkBytes([]byte("monitor_enabled")),
			redis.NewBulkBytes([]byte("monitor_max_value_len")),
			redis.NewBulkBytes([]byte("monitor_max_batchsize")),
			redis.NewBulkBytes([]byte("monitor_max_cmd_info")),
			redis.NewBulkBytes([]byte("monitor_log_max_len")),
			redis.NewBulkBytes([]byte("monitor_result_set_size")),
			redis.NewBulkBytes([]byte("breaker_enabled")),
			redis.NewBulkBytes([]byte("breaker_degradation_probability")),
			redis.NewBulkBytes([]byte("breaker_degradation_strategy")),
			redis.NewBulkBytes([]byte("breaker_list_max_length")),
			redis.NewBulkBytes([]byte("breaker_qps_limitation")),
			redis.NewBulkBytes([]byte("breaker_cmd_white_list_enabled")),
			redis.NewBulkBytes([]byte("breaker_cmd_white_list")),
			redis.NewBulkBytes([]byte("breaker_cmd_black_list_enabled")),
			redis.NewBulkBytes([]byte("breaker_cmd_black_list")),
			redis.NewBulkBytes([]byte("breaker_key_white_list_enabled")),
			redis.NewBulkBytes([]byte("breaker_key_white_list")),
			redis.NewBulkBytes([]byte("breaker_key_black_list_enabled")),
			redis.NewBulkBytes([]byte("breaker_key_black_list")),
		})
	default:
		return redis.NewErrorf("unsurport key.")
	}
}

func (s *Proxy) ConfigGet(key string) *redis.Resp {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch key {
	case "proxy_max_clients":
		return redis.NewBulkBytes([]byte(strconv.Itoa(s.config.ProxyMaxClients)))
	case "proxy_refresh_state_period":
		if text, err := s.config.ProxyRefreshStatePeriod.MarshalText(); err == nil {
			return redis.NewBulkBytes(text)
		} else {
			return redis.NewErrorf("cant get proxy_refresh_state_period value.")
		}
	case "backend_primary_only":
		return redis.NewBulkBytes([]byte(strconv.FormatBool(s.config.BackendPrimaryOnly)))
	case "backend_primary_parallel":
		return redis.NewBulkBytes([]byte(strconv.Itoa(s.config.BackendPrimaryParallel)))
	case "backend_primary_quick":
		return redis.NewBulkBytes([]byte(strconv.Itoa(s.config.BackendPrimaryQuick)))
	case "backend_replica_parallel":
		return redis.NewBulkBytes([]byte(strconv.Itoa(s.config.BackendReplicaParallel)))
	case "backend_replica_quick":
		return redis.NewBulkBytes([]byte(strconv.Itoa(s.config.BackendReplicaQuick)))
	case "slowlog_log_slower_than":
		return redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.SlowlogLogSlowerThan,10)))
	case "slowlog_max_len":
		return redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.SlowlogMaxLen, 10)))
	case "quick_cmd_list":
		return redis.NewBulkBytes([]byte(s.config.QuickCmdList))
	case "slow_cmd_list":
		return redis.NewBulkBytes([]byte(s.config.SlowCmdList))
	case "quick_slow_cmd":
		return getCmdFlag()
	case "auto_set_slow_flag":
		return redis.NewBulkBytes([]byte(strconv.FormatBool(s.config.AutoSetSlowFlag)))
	case "expire_log_days":
		return redis.NewBulkBytes([]byte(strconv.Itoa(s.config.ExpireLogDays)))
	case "monitor_enabled":
		return redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.MonitorEnabled, 10)))
	case "monitor_max_value_len":
		return redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.MonitorMaxValueLen, 10)))
	case "monitor_max_batchsize":
		return redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.MonitorMaxBatchsize, 10)))
	case "monitor_max_cmd_info":
		return redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.MonitorMaxCmdInfo, 10)))
	case "monitor_log_max_len":
		return redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.MonitorLogMaxLen, 10)))
	case "monitor_result_set_size":
		return redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.MonitorResultSetSize, 10)))
	case "breaker_enabled":
		return redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.BreakerEnabled, 10)))
	case "breaker_degradation_probability":
		return redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.BreakerDegradationProbability, 10)))
	case "breaker_qps_limitation":
		return redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.BreakerQpsLimitation, 10)))
	case "breaker_cmd_white_list":
		return redis.NewBulkBytes([]byte(s.config.BreakerCmdWhiteList))
	case "breaker_cmd_black_list":
		return redis.NewBulkBytes([]byte(s.config.BreakerCmdBlackList))
	case "breaker_key_white_list":
		return redis.NewBulkBytes([]byte(s.config.BreakerKeyWhiteList))
	case "breaker_key_black_list":
		return redis.NewBulkBytes([]byte(s.config.BreakerKeyBlackList))
	case "*":
		var proxy_refresh_state_period_value string
		if text, err := s.config.ProxyRefreshStatePeriod.MarshalText(); err == nil {
			proxy_refresh_state_period_value = string(text[:])
		} else {
			proxy_refresh_state_period_value = "cant get proxy_refresh_state_period value."
		}
		return redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte("proxy_max_clients")),
			redis.NewBulkBytes([]byte(strconv.Itoa(s.config.ProxyMaxClients))),
			redis.NewBulkBytes([]byte("proxy_refresh_state_period")),
			redis.NewBulkBytes([]byte(proxy_refresh_state_period_value)),
			redis.NewBulkBytes([]byte("backend_primary_only")),
			redis.NewBulkBytes([]byte(strconv.FormatBool(s.config.BackendPrimaryOnly))),
			redis.NewBulkBytes([]byte("backend_primary_parallel")),
			redis.NewBulkBytes([]byte(strconv.Itoa(s.config.BackendPrimaryParallel))),
			redis.NewBulkBytes([]byte("backend_primary_quick")),
			redis.NewBulkBytes([]byte(strconv.Itoa(s.config.BackendPrimaryQuick))),
			redis.NewBulkBytes([]byte("backend_replica_parallel")),
			redis.NewBulkBytes([]byte(strconv.Itoa(s.config.BackendReplicaParallel))),
			redis.NewBulkBytes([]byte("backend_replica_quick")),
			redis.NewBulkBytes([]byte(strconv.Itoa(s.config.BackendReplicaQuick))),
			redis.NewBulkBytes([]byte("slowlog_log_slower_than")),
			redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.SlowlogLogSlowerThan,10))),
			redis.NewBulkBytes([]byte("slowlog_max_len")),
			redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.SlowlogMaxLen, 10))),
			redis.NewBulkBytes([]byte("quick_cmd_list")),
			redis.NewBulkBytes([]byte(s.config.QuickCmdList)),
			redis.NewBulkBytes([]byte("slow_cmd_list")),
			redis.NewBulkBytes([]byte(s.config.SlowCmdList)),
			redis.NewBulkBytes([]byte("quick_slow_cmd")),
			redis.NewBulkBytes([]byte("do it alone")),
			redis.NewBulkBytes([]byte("auto_set_slow_flag")),
			redis.NewBulkBytes([]byte(strconv.FormatBool(s.config.AutoSetSlowFlag))),
			redis.NewBulkBytes([]byte("expire_log_days")),
			redis.NewBulkBytes([]byte(strconv.Itoa(s.config.ExpireLogDays))),
			redis.NewBulkBytes([]byte("monitor_enabled")),
			redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.MonitorEnabled, 10))),
			redis.NewBulkBytes([]byte("monitor_max_value_len")),
			redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.MonitorMaxValueLen, 10))),
			redis.NewBulkBytes([]byte("monitor_max_batchsize")),
			redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.MonitorMaxBatchsize, 10))),
			redis.NewBulkBytes([]byte("monitor_max_cmd_info")),
			redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.MonitorMaxCmdInfo, 10))),
			redis.NewBulkBytes([]byte("monitor_log_max_len")),
			redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.MonitorLogMaxLen, 10))),
			redis.NewBulkBytes([]byte("monitor_result_set_size")),
			redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.MonitorResultSetSize, 10))),
			redis.NewBulkBytes([]byte("breaker_enabled")),
			redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.BreakerEnabled, 10))),
			redis.NewBulkBytes([]byte("breaker_degradation_probability")),
			redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.BreakerDegradationProbability, 10))),
			redis.NewBulkBytes([]byte("breaker_qps_limitation")),
			redis.NewBulkBytes([]byte(strconv.FormatInt(s.config.BreakerQpsLimitation, 10))),
			redis.NewBulkBytes([]byte("breaker_cmd_white_list")),
			redis.NewBulkBytes([]byte(s.config.BreakerCmdWhiteList)),
			redis.NewBulkBytes([]byte("breaker_cmd_black_list")),
			redis.NewBulkBytes([]byte(s.config.BreakerCmdBlackList)),
			redis.NewBulkBytes([]byte("breaker_key_white_list")),
			redis.NewBulkBytes([]byte(s.config.BreakerKeyWhiteList)),
			redis.NewBulkBytes([]byte("breaker_key_black_list")),
			redis.NewBulkBytes([]byte(s.config.BreakerKeyBlackList)),
		})
	default:
		return redis.NewErrorf("unsurport key[%s].", key)
	}
}

func (s *Proxy) ClusterSlots() *redis.Resp {
	slots := s.Slots()
	if len(slots) == 0 {
		return redis.NewErrorf("cant get slots info.")
	}

	type Node struct {
		IP 		string
		Port 	string
		RunId 	string
	}

	type NodeSlots struct {
		Start int
		End int
		Master *Node
		Slaves  []*Node
	}

	type GroupNodes struct {
		Master *Node
		Slaves  []*Node
	}

	results := make([]*NodeSlots, 0)

	//找出所有的master节点信息,目前暂时没有获取slave节点信息
	//proxy上只有读写分离时才有slave节点信息
	masters := make(map[string]*GroupNodes)
	for i := range slots {
		slot := slots[i]
		if slot == nil {
			return redis.NewErrorf("cant get slots info.")
		}
		
		//第一次添加节点信息
		if _, ok := masters[slot.BackendAddr]; !ok {
			host, port, err := net.SplitHostPort(slot.BackendAddr)
			if err != nil {
				return redis.NewErrorf("paser addr[%s] faild, %v", slot.BackendAddr, err)
			}

			masters[slot.BackendAddr] = &GroupNodes{
				Master:&Node{
					IP:host,
					Port:port,
					RunId:slot.BackendAddr,
				},
			}
		}
	}

	//获取每个master中的slot信息
	for k, v := range masters{
		var start = -1

		for i := range slots {
			slot := slots[i]

			//第一次找到属于当前节点的slot
			if slot.BackendAddr == k && start == -1 {
				start = i
			}

			//遍历到不属于当前节点是slot或遍历结束
			if start != -1 && (slot.BackendAddr != k || i == MaxSlotNum - 1) {
				nodeSlots := &NodeSlots{
					Start:start,
					Master:v.Master,
					Slaves:v.Slaves,
				}

				//最后一个slot属于当前节点
				if slot.BackendAddr == k && i == MaxSlotNum - 1 {
					i++
				}

				if start == i-1 {
					nodeSlots.End = start
				} else {
					nodeSlots.End = i-1
				}

				results = append(results, nodeSlots)

				start = -1
			}
		}
	}

	var array = make([]*redis.Resp, len(results))
	for i := range results {
		array[i] = redis.NewArray([]*redis.Resp{
		redis.NewInt([]byte(strconv.Itoa(results[i].Start))),
		redis.NewInt([]byte(strconv.Itoa(results[i].End))),
		redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte(results[i].Master.IP)),
			redis.NewInt([]byte(results[i].Master.Port)),
			redis.NewBulkBytes([]byte(results[i].Master.RunId)),
		}),
		})
	}
	return redis.NewArray(array)
}

// cluster nodes
// runid ip:port@cport master|slave -|master_runid ping_sent pong_received configEpoch connected|disconnected slots_info
func (s *Proxy) ClusterNodes() *redis.Resp {
	slots := s.Slots()
	if len(slots) == 0 {
		return redis.NewErrorf("cant get slots info.")
	}

	type Node struct {
		IP 		string
		Port 	string
		RunId 	string
	}

	type NodeSlots struct {
		Start int
		End int
		Master *Node
		Slaves  []*Node
	}

	type GroupNodes struct {
		Master *Node
		Slaves  []*Node
	}

	results := make([]string, 0)

	//找出所有的master节点信息,目前暂时没有获取slave节点信息
	//proxy上只有读写分离时才有slave节点信息
	masters := make(map[string]*GroupNodes)
	for i := range slots {
		slot := slots[i]
		if slot == nil {
			return redis.NewErrorf("cant get slots info.")
		}

		//第一次添加节点信息
		if _, ok := masters[slot.BackendAddr]; !ok {
			host, port, err := net.SplitHostPort(slot.BackendAddr)
			if err != nil {
				return redis.NewErrorf("paser addr[%s] faild, %v", slot.BackendAddr, err)
			}

			masters[slot.BackendAddr] = &GroupNodes{
				Master:&Node{
					IP:host,
					Port:port,
					RunId:slot.BackendAddr,
				},
			}
		}
	}

	//获取每个master中的slot信息
	for k, v := range masters{
		nodeInfo := fmt.Sprintf("%.40s %s:%s@%s master - 0 0 0 connected", v.Master.RunId, v.Master.IP, v.Master.Port, v.Master.Port)
		var start = -1

		for i := range slots {
			slot := slots[i]

			//第一次找到属于当前节点的slot
			if slot.BackendAddr == k && start == -1 {
				start = i
			}

			//遍历到不属于当前节点是slot或遍历结束
			if start != -1 && (slot.BackendAddr != k || i == MaxSlotNum - 1) {
				nodeSlots := &NodeSlots{
					Start:start,
					Master:v.Master,
					Slaves:v.Slaves,
				}

				//最后一个slot属于当前节点
				if slot.BackendAddr == k && i == MaxSlotNum - 1 {
					i++
				}

				if start == i-1 {
					nodeSlots.End = start
					nodeInfo += " " + strconv.Itoa(start)
				} else {
					nodeSlots.End = i-1
					nodeInfo += " " + strconv.Itoa(start) + "-" + strconv.Itoa(nodeSlots.End)
				}

				start = -1
			}
		}

		results = append(results, nodeInfo)
	}

	var retStr = ""
	for i := range results {
		retStr += results[i] + "\n"
	}

	return redis.NewBulkBytes([]byte(retStr))
}

func (s *Proxy) ConfigRewrite() *redis.Resp {
	s.mu.Lock()
	defer s.mu.Unlock()

	utils.RewriteConf(*(s.config), s.config.ConfigFileName, "=", true)
	return redis.NewString([]byte("OK"))
}

func (s *Proxy) IsOnline() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.online && !s.closed
}

func (s *Proxy) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Proxy) HasSwitched() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.router.HasSwitched()
}

func (s *Proxy) Slots() []*models.Slot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.router.GetSlots()
}

func (s *Proxy) FillSlot(m *models.Slot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedProxy
	}
	return s.router.FillSlot(m)
}

func (s *Proxy) FillSlots(slots []*models.Slot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedProxy
	}
	for _, m := range slots {
		if err := s.router.FillSlot(m); err != nil {
			return err
		}
	}
	return nil
}

func (s *Proxy) SwitchMasters(masters map[int]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedProxy
	}
	s.ha.masters = masters

	if len(masters) != 0 {
		s.router.SwitchMasters(masters)
	}
	return nil
}

func (s *Proxy) GetSentinels() ([]string, map[int]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil
	}
	return s.ha.servers, s.ha.masters
}

func (s *Proxy) SetSentinels(servers []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedProxy
	}
	s.ha.servers = servers
	log.Warnf("[%p] set sentinels = %v", s, s.ha.servers)

	s.rewatchSentinels(s.ha.servers)
	return nil
}

func (s *Proxy) RewatchSentinels() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedProxy
	}
	log.Warnf("[%p] rewatch sentinels = %v", s, s.ha.servers)

	s.rewatchSentinels(s.ha.servers)
	return nil
}

func (s *Proxy) rewatchSentinels(servers []string) {
	if s.ha.monitor != nil {
		s.ha.monitor.Cancel()
		s.ha.monitor = nil
		s.ha.masters = nil
	}
	if len(servers) != 0 {
		s.ha.monitor = utilredis.NewSentinel(s.config.ProductName, s.config.ProductAuth)
		s.ha.monitor.LogFunc = log.Warnf
		s.ha.monitor.ErrFunc = log.WarnErrorf
		go func(p *utilredis.Sentinel) {
			var trigger = make(chan struct{}, 1)
			delayUntil := func(deadline time.Time) {
				for !p.IsCanceled() {
					var d = deadline.Sub(time.Now())
					if d <= 0 {
						return
					}
					time.Sleep(math2.MinDuration(d, time.Second))
				}
			}
			go func() {
				defer close(trigger)
				callback := func() {
					select {
					case trigger <- struct{}{}:
					default:
					}
				}
				for !p.IsCanceled() {
					timeout := time.Minute * 15
					retryAt := time.Now().Add(time.Second * 10)
					if !p.Subscribe(servers, timeout, callback) {
						delayUntil(retryAt)
					} else {
						callback()
					}
				}
			}()
			go func() {
				for range trigger {
					var success int
					for i := 0; i != 10 && !p.IsCanceled() && success != 2; i++ {
						timeout := time.Second * 5
						masters, err := p.Masters(servers, timeout)
						if err != nil {
							log.WarnErrorf(err, "[%p] fetch group masters failed", s)
						} else {
							if !p.IsCanceled() {
								s.SwitchMasters(masters)
							}
							success += 1
						}
						delayUntil(time.Now().Add(time.Second * 5))
					}
				}
			}()
		}(s.ha.monitor)
	}
}

func (s *Proxy) serveAdmin() {
	if s.IsClosed() {
		return
	}
	defer s.Close()

	log.Warnf("[%p] admin start service on %s", s, s.ladmin.Addr())

	eh := make(chan error, 1)
	go func(l net.Listener) {
		h := http.NewServeMux()
		h.Handle("/", newApiServer(s))
		hs := &http.Server{Handler: h}
		eh <- hs.Serve(l)
	}(s.ladmin)

	select {
	case <-s.exit.C:
		log.Warnf("[%p] admin shutdown", s)
	case err := <-eh:
		log.ErrorErrorf(err, "[%p] admin exit on error", s)
	}
}

func (s *Proxy) serveProxy() {
	if s.IsClosed() {
		return
	}
	defer s.Close()

	log.Warnf("[%p] proxy start service on %s", s, s.lproxy.Addr())

	eh := make(chan error, 1)
	go func(l net.Listener) (err error) {
		defer func() {
			eh <- err
		}()
		for {
			c, err := s.acceptConn(l)
			if err != nil {
				return err
			}
			NewSession(c, s.config, s).Start(s.router)
		}
	}(s.lproxy)

	if d := s.config.BackendPingPeriod.Duration(); d != 0 {
		go s.keepAlive(d)
	}

	//设置命令快慢标志
	if err := setQuickCmdListForStart(s.config.QuickCmdList); err != nil {
		//终止启动
		log.PanicErrorf(err, "setQuickCmdList [%s] failed", s.config.QuickCmdList)
	}
	if err := setSlowCmdListForStart(s.config.SlowCmdList); err != nil {
		//终止启动
		log.PanicErrorf(err, "setSlowCmdList [%s] failed", s.config.SlowCmdList)
	}

	//设置延迟统计相关参数
	StatsSetRefreshPeriod(s.config.ProxyRefreshStatePeriod.Duration())
	StatsSetLogSlowerThan(s.config.SlowlogLogSlowerThan)
	StatsSetAutoSetSlowFlag(s.config.AutoSetSlowFlag)

	//设置内存慢日志参数
	XSlowlogSetMaxLen(s.config.SlowlogMaxLen)

	//设置监控参数
	XMonitorSetMaxLengthOfValue(s.config.MonitorMaxValueLen)
	XMonitorSetMaxBatchsize(s.config.MonitorMaxBatchsize)
	XMonitorSetMaxLengthOfCmdInfo(s.config.MonitorMaxCmdInfo)
	XMonitorSetMonitorState(s.config.MonitorEnabled)
	MonitorLogSetMaxLen(s.config.MonitorLogMaxLen)
	XMonitorSetResultSetSize(s.config.MonitorResultSetSize)

	//设置熔断参数
	BreakerSetState(s.config.BreakerEnabled)
	BreakerSetProbability(s.config.BreakerDegradationProbability)
	BreakerNewQpsLimiter(s.config.BreakerQpsLimitation)

	StoreCmdBlackListByBatch(s.config.BreakerCmdBlackList)
	StoreCmdWhiteListByBatch(s.config.BreakerCmdWhiteList)
	StoreKeyBlackListByBatch(s.config.BreakerKeyBlackList)
	StoreKeyWhiteListByBatch(s.config.BreakerKeyWhiteList)

	select {
	case <-s.exit.C:
		log.Warnf("[%p] proxy shutdown", s)
	case err := <-eh:
		log.ErrorErrorf(err, "[%p] proxy exit on error", s)
	}
}

func (s *Proxy) keepAlive(d time.Duration) {
	var ticker = time.NewTicker(math2.MaxDuration(d, time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-s.exit.C:
			return
		case <-ticker.C:
			s.router.KeepAlive()
		}
	}
}

func (s *Proxy) acceptConn(l net.Listener) (net.Conn, error) {
	var delay = &DelayExp2{
		Min: 10, Max: 500,
		Unit: time.Millisecond,
	}
	for {
		c, err := l.Accept()
		if err != nil {
			if e, ok := err.(net.Error); ok && e.Temporary() {
				log.WarnErrorf(err, "[%p] proxy accept new connection failed", s)
				delay.Sleep()
				continue
			}
		}
		return c, err
	}
}

type Overview struct {
	Version string         `json:"version"`
	Compile string         `json:"compile"`
	Config  Config         `json:"config,omitempty"`
	Model   *models.Proxy  `json:"model,omitempty"`
	Stats   *Stats         `json:"stats,omitempty"`
	Slots   []*models.Slot `json:"slots,omitempty"`
}

type CmdInfo struct {
	Total int64 `json:"total"`
	Fails int64 `json:"fails"`
	Redis struct {
		Errors int64 `json:"errors"`
	} `json:"redis"`
	QPS int64      `json:"qps"`
	Cmd []*OpStats `json:"cmd,omitempty"`
}

type Stats struct {
	Online bool `json:"online"`
	Closed bool `json:"closed"`

	Sessions struct {
		Total int64 `json:"total"`
		Alive int64 `json:"alive"`
	} `json:"sessions"`

	Rusage struct {
		Now string       `json:"now"`
		CPU float64      `json:"cpu"`
		Mem int64        `json:"mem"`
		Raw *utils.Usage `json:"raw,omitempty"`
	} `json:"rusage"`

	Backend struct {
		PrimaryOnly bool `json:"primary_only"`
	} `json:"backend"`

	Runtime *RuntimeStats `json:"runtime,omitempty"`

	Sentinels struct {
		Servers  []string          `json:"servers,omitempty"`
		Masters  map[string]string `json:"masters,omitempty"`
		Switched bool              `json:"switched,omitempty"`
	} `json:"sentinels"`

	Ops struct {
		Total int64 `json:"total"`
		Fails int64 `json:"fails"`
		Redis struct {
			Errors int64 `json:"errors"`
		} `json:"redis"`
		QPS int64      `json:"qps"`
		Cmd []*OpStats `json:"cmd,omitempty"`
	} `json:"ops"`
}

type RuntimeStats struct {
	General struct {
		Alloc   uint64 `json:"alloc"`
		Sys     uint64 `json:"sys"`
		Lookups uint64 `json:"lookups"`
		Mallocs uint64 `json:"mallocs"`
		Frees   uint64 `json:"frees"`
	} `json:"general"`

	Heap struct {
		Alloc   uint64 `json:"alloc"`
		Sys     uint64 `json:"sys"`
		Idle    uint64 `json:"idle"`
		Inuse   uint64 `json:"inuse"`
		Objects uint64 `json:"objects"`
	} `json:"heap"`

	GC struct {
		Num          uint32  `json:"num"`
		CPUFraction  float64 `json:"cpu_fraction"`
		TotalPauseMs uint64  `json:"total_pausems"`
	} `json:"gc"`

	NumProcs      int   `json:"num_procs"`
	NumGoroutines int   `json:"num_goroutines"`
	NumCgoCall    int64 `json:"num_cgo_call"`
	MemOffheap    int64 `json:"mem_offheap"`
}

type StatsFlags uint32

func (s StatsFlags) HasBit(m StatsFlags) bool {
	return (s & m) != 0
}

const (
	StatsCmds = StatsFlags(1 << iota)
	StatsSlots
	StatsRuntime

	StatsFull = StatsFlags(^uint32(0))
)

func (s *Proxy) Overview(flags StatsFlags) *Overview {
	o := &Overview{
		Version: utils.Version,
		Compile: utils.Compile,
		Config:  s.Config(),
		Model:   s.Model(),
		Stats:   s.Stats(flags),
	}
	if flags.HasBit(StatsSlots) {
		o.Slots = s.Slots()
	}
	return o
}

func (s *Proxy) CmdInfo(interval int64) *CmdInfo {
	cmdInfo := &CmdInfo{}

	cmdInfo.Total = OpTotal()
	cmdInfo.Fails = OpFails()
	cmdInfo.Redis.Errors = OpRedisErrors()
	cmdInfo.QPS = OpQPS()

	cmdInfo.Cmd = GetOpStatsByInterval(interval)

	return cmdInfo
}

func (s *Proxy) Stats(flags StatsFlags) *Stats {
	stats := &Stats{}
	stats.Online = s.IsOnline()
	stats.Closed = s.IsClosed()

	servers, masters := s.GetSentinels()
	if servers != nil {
		stats.Sentinels.Servers = servers
	}
	if masters != nil {
		stats.Sentinels.Masters = make(map[string]string)
		for gid, addr := range masters {
			stats.Sentinels.Masters[strconv.Itoa(gid)] = addr
		}
	}
	stats.Sentinels.Switched = s.HasSwitched()

	stats.Ops.Total = OpTotal()
	stats.Ops.Fails = OpFails()
	stats.Ops.Redis.Errors = OpRedisErrors()
	stats.Ops.QPS = OpQPS()

	//if flags.HasBit(StatsCmds) {
	//stats.Ops.Cmd = GetOpStatsAll()GetOpStatsByInterval(interval)
	stats.Ops.Cmd = GetOpStatsByInterval(1)
	//}

	stats.Sessions.Total = SessionsTotal()
	stats.Sessions.Alive = SessionsAlive()

	if u := GetSysUsage(); u != nil {
		stats.Rusage.Now = u.Now.String()
		stats.Rusage.CPU = u.CPU
		stats.Rusage.Mem = u.MemTotal()
		stats.Rusage.Raw = u.Usage
	}

	stats.Backend.PrimaryOnly = s.Config().BackendPrimaryOnly

	if flags.HasBit(StatsRuntime) {
		var r runtime.MemStats
		runtime.ReadMemStats(&r)

		stats.Runtime = &RuntimeStats{}
		stats.Runtime.General.Alloc = r.Alloc
		stats.Runtime.General.Sys = r.Sys
		stats.Runtime.General.Lookups = r.Lookups
		stats.Runtime.General.Mallocs = r.Mallocs
		stats.Runtime.General.Frees = r.Frees
		stats.Runtime.Heap.Alloc = r.HeapAlloc
		stats.Runtime.Heap.Sys = r.HeapSys
		stats.Runtime.Heap.Idle = r.HeapIdle
		stats.Runtime.Heap.Inuse = r.HeapInuse
		stats.Runtime.Heap.Objects = r.HeapObjects
		stats.Runtime.GC.Num = r.NumGC
		stats.Runtime.GC.CPUFraction = r.GCCPUFraction
		stats.Runtime.GC.TotalPauseMs = r.PauseTotalNs / uint64(time.Millisecond)
		stats.Runtime.NumProcs = runtime.GOMAXPROCS(0)
		stats.Runtime.NumGoroutines = runtime.NumGoroutine()
		stats.Runtime.NumCgoCall = runtime.NumCgoCall()
		stats.Runtime.MemOffheap = unsafe2.OffheapBytes()
	}
	return stats
}

func (s *Proxy)AutoPurgeLog() {
	for {
		if s.IsClosed() {
			return
		}

		if err := s.PurgeLog(s.config.Log, s.config.ExpireLogDays); err != nil {
			log.WarnErrorf(err, "proxy purge log failed")
		}

		time.Sleep(time.Second * 60 * 60 * 24)
	}
}

func (s *Proxy) PurgeLog(logfile string, expireLogDays int) error {
	if logfile == "" || expireLogDays == 0 {
		return nil
	}
	log.Warnf("AutoPurgeLog")
	logPath := ""
	logPrefix := ""
	logPathList := strings.Split(logfile, "/")
	logPathLen := len(logPathList)
	if logPathLen == 0 {
		return nil
	}

	for i:=0; i<logPathLen; i++ {
		if i == logPathLen - 1 {
			logPrefix = logPathList[i]
			break;
		} else {
			logPath += logPathList[i] + "/"
		}
	}
	if logPath == "" {
		logPath = "./"
	}
	log.Warnf("log_path: %s", logPath)
	log.Warnf("log_prefix: %s", logPrefix)

	fileList, err := ioutil.ReadDir(logPath)
	if err != nil {
		log.Warnf("read dir error")
		return err
	}
	logNum := 0
	for _, v := range fileList {
		if strings.Contains(v.Name(), logPrefix) {
			logNum++
		}
	}
	if logNum <= expireLogDays {
		return nil
	}

	for _, v := range fileList {
		if strings.Contains(v.Name(), logPrefix) {
			//DelExpireLog(v.Name(), log_path, log_prefix)
			logSuffix := strings.Split(v.Name(), logPrefix + ".")
			if len(logSuffix) != 0 {
				logDate := logSuffix[len(logSuffix) - 1]
				if IsExpireDate(logDate, int64(expireLogDays)) {
					log.Warnf("rm logfile: %s", v.Name())
					os.Remove(logPath + v.Name());
				}
			}
		}
	}
	return nil
}

func IsExpireDate(logDate string, expireLogDays int64) bool {
	timeLayout := "2006-01-02"
	localTime, _ := time.LoadLocation("Local")
	theTime, _ := time.ParseInLocation(timeLayout, logDate, localTime)
	timeStamp := theTime.Unix() 
	if (time.Now().Unix() - timeStamp) > expireLogDays * 24 * 60 * 60 {
		return true
	} 
	return false
}
