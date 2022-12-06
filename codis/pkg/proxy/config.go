// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"bytes"

	"github.com/BurntSushi/toml"

	"github.com/CodisLabs/codis/pkg/utils/bytesize"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/timesize"
)

const DefaultConfig = `
##################################################
#                                                #
#                  Codis-Proxy                   #
#                                                #
##################################################


# Set runtime.GOMAXPROCS to N, default is 1.
ncpu = 1

# Set path/name of daliy rotated log file.
log = "proxy.log"

# Expire-log-days, 0 means never expire
expire_log_days = 30

# Set the log-level, should be INFO,WARN,DEBUG or ERROR, default is INFO.
log_level = "info"

# Set pidfile
pidfile = "proxy.pid"

# Set Codis Product Name/Auth.
product_name = "codis-demo"
product_auth = ""

# Set auth for client session
#   1. product_auth is used for auth validation among codis-dashboard,
#      codis-proxy and codis-server.
#   2. session_auth is different from product_auth, it requires clients
#      to issue AUTH <PASSWORD> before processing any other commands.
session_auth = ""

# Set bind address for admin(rpc), tcp only.
admin_addr = "0.0.0.0:11080"

# Set bind address for proxy, proto_type can be "tcp", "tcp4", "tcp6", "unix" or "unixpacket".
proto_type = "tcp4"
proxy_addr = "0.0.0.0:19000"

# Set jodis address & session timeout
#   1. jodis_name is short for jodis_coordinator_name, only accept "zookeeper" & "etcd".
#   2. jodis_addr is short for jodis_coordinator_addr
#   3. jodis_auth is short for jodis_coordinator_auth, for zookeeper/etcd, "user:password" is accepted.
#   4. proxy will be registered as node:
#        if jodis_compatible = true (not suggested):
#          /zk/codis/db_{PRODUCT_NAME}/proxy-{HASHID} (compatible with Codis2.0)
#          /zk/codis/db_{PRODUCT_NAME}/proxy/{jodis_proxy_subdir}/{HASHID} (compatible with Codis2.0)
#        or else
#          /jodis/{PRODUCT_NAME}/proxy-{HASHID}
#          /jodis/{PRODUCT_NAME}/{jodis_proxy_subdir}/proxy-{HASHID}
jodis_name = ""
jodis_addr = ""
jodis_auth = ""
jodis_timeout = "20s"
jodis_compatible = true
jodis_proxy_subdir = ""

# Set datacenter of proxy.
proxy_datacenter = ""

# Set max number of alive sessions.
proxy_max_clients = 50000

# Set max offheap memory size. (0 to disable)
proxy_max_offheap_size = "1024mb"

# Set heap placeholder to reduce GC frequency.
proxy_heap_placeholder = "256mb"

# Proxy will refresh state in a predefined interval. (0 to disable)
proxy_refresh_state_period = "1s"

# Proxy will ping backend redis (and clear 'MASTERDOWN' state) in a predefined interval. (0 to disable)
backend_ping_period = "5s"

# Set backend recv buffer size & timeout.
backend_recv_bufsize = "128kb"
backend_recv_timeout = "3s"

# Set backend send buffer & timeout.
backend_send_bufsize = "128kb"
backend_send_timeout = "3s"

# Set backend pipeline buffer size.
backend_max_pipeline = 20480

# Set backend never read replica groups, default is true
backend_primary_only = true

# Set backend parallel connections per server
backend_primary_parallel = 8
backend_primary_quick = 0
backend_replica_parallel = 8
backend_replica_quick = 0

# Set backend tcp keepalive period. (0 to disable)
backend_keepalive_period = "75s"

# Set number of databases of backend.
backend_number_databases = 1

# If there is no request from client for a long time, the connection will be closed. (0 to disable)
# Set session recv buffer size & timeout.
session_recv_bufsize = "128kb"
session_recv_timeout = "30m"

# Set session send buffer size & timeout.
session_send_bufsize = "64kb"
session_send_timeout = "30s"

# Make sure this is higher than the max number of requests for each pipeline request, or your client may be blocked.
# Set session pipeline buffer size.
session_max_pipeline = 10000

# Set session tcp keepalive period. (0 to disable)
session_keepalive_period = "75s"

# Set session to be sensitive to failures. Default is false, instead of closing socket, proxy will send an error response to client.
session_break_on_failure = false

# Slowlog-log-slower-than(us), from receive command to send response, 0 is allways print slow log
slowlog_log_slower_than = 100000
# set the number of slowlog in memory, max len is 10000000. (0 to disable)
slowlog_max_len = 128000
# quick command list
quick_cmd_list = ""
# slow command list
slow_cmd_list = ""
# auto set slow flag for command, when command timeout
auto_set_slow_flag = false

# monitor big key big value
# max length of single value
monitor_max_value_len = 4096
# max batchsize of single request or response from redis client
monitor_max_batchsize = 200
# when writing the record into xmonitor log, max length of cmd info
monitor_max_cmd_info = 256
# set the number of xmonitor log in memory, max length is 100000, about 8GB
monitor_log_max_len = 10000
# set the limitation of result set for xmonitor log
monitor_result_set_size = 20
# switch for xmonitor，0 is disabled, 1 is enabled
monitor_enabled = 0

# breaker
# switch for breaker, 0 is disabled, 1 is enabled
breaker_enabled = 0
# default probability of degradation if degrade service with probability
breaker_degradation_probability = 0
# limitation of qps of cmd executed, if equals nagative or 0, means no limitation
breaker_qps_limitation = 0

# cmd white list
breaker_cmd_white_list = ""
# cmd black list
breaker_cmd_black_list = ""
# key white list
breaker_key_white_list = ""
# key black list
breaker_key_black_list = ""
`

type Config struct {
	Ncpu           int     `toml:"ncpu"`
	Log            string  `toml:"log"`
	ExpireLogDays  int     `toml:"expire_log_days"`
	LogLevel       string  `toml:"log_level"`
	PidFile        string  `toml:"pidfile"`

	ConfigFileName	string    		`toml:"-" json:"config_file_name"`

	ProtoType string `toml:"proto_type" json:"proto_type"`
	ProxyAddr string `toml:"proxy_addr" json:"proxy_addr"`
	AdminAddr string `toml:"admin_addr" json:"admin_addr"`

	HostProxy string `toml:"-" json:"-"`
	HostAdmin string `toml:"-" json:"-"`

	JodisName       string            `toml:"jodis_name" json:"jodis_name"`
	JodisAddr       string            `toml:"jodis_addr" json:"jodis_addr"`
	JodisAuth       string            `toml:"jodis_auth" json:"jodis_auth"`
	JodisTimeout    timesize.Duration `toml:"jodis_timeout" json:"jodis_timeout"`
	JodisCompatible bool              `toml:"jodis_compatible" json:"jodis_compatible"`
	JodisProxySubDir  string         `toml:"jodis_proxy_subdir" json:"jodis_proxy_subdir"`

	ProductName string `toml:"product_name" json:"product_name"`
	ProductAuth string `toml:"product_auth" json:"-"`
	SessionAuth string `toml:"session_auth" json:"-"`

	ProxyDataCenter      string         `toml:"proxy_datacenter" json:"proxy_datacenter"`
	ProxyMaxClients      int            `toml:"proxy_max_clients" json:"proxy_max_clients"`
	ProxyMaxOffheapBytes bytesize.Int64 `toml:"proxy_max_offheap_size" json:"proxy_max_offheap_size"`
	ProxyHeapPlaceholder bytesize.Int64 `toml:"proxy_heap_placeholder" json:"proxy_heap_placeholder"`
	ProxyRefreshStatePeriod timesize.Duration `toml:"proxy_refresh_state_period" json:"proxy_refresh_state_period"`

	BackendPingPeriod      timesize.Duration `toml:"backend_ping_period" json:"backend_ping_period"`
	BackendRecvBufsize     bytesize.Int64    `toml:"backend_recv_bufsize" json:"backend_recv_bufsize"`
	BackendRecvTimeout     timesize.Duration `toml:"backend_recv_timeout" json:"backend_recv_timeout"`
	BackendSendBufsize     bytesize.Int64    `toml:"backend_send_bufsize" json:"backend_send_bufsize"`
	BackendSendTimeout     timesize.Duration `toml:"backend_send_timeout" json:"backend_send_timeout"`
	BackendMaxPipeline     int               `toml:"backend_max_pipeline" json:"backend_max_pipeline"`
	BackendPrimaryOnly     bool              `toml:"backend_primary_only" json:"backend_primary_only"`
	BackendPrimaryParallel int               `toml:"backend_primary_parallel" json:"backend_primary_parallel"`
	BackendPrimaryQuick    int               `toml:"backend_primary_quick" json:"backend_primary_quick"`
	BackendReplicaParallel int               `toml:"backend_replica_parallel" json:"backend_replica_parallel"`
	BackendReplicaQuick    int               `toml:"backend_replica_quick" json:"backend_replica_quick"`
	BackendKeepAlivePeriod timesize.Duration `toml:"backend_keepalive_period" json:"backend_keepalive_period"`
	BackendNumberDatabases int32             `toml:"backend_number_databases" json:"backend_number_databases"`

	SessionRecvBufsize     bytesize.Int64    `toml:"session_recv_bufsize" json:"session_recv_bufsize"`
	SessionRecvTimeout     timesize.Duration `toml:"session_recv_timeout" json:"session_recv_timeout"`
	SessionSendBufsize     bytesize.Int64    `toml:"session_send_bufsize" json:"session_send_bufsize"`
	SessionSendTimeout     timesize.Duration `toml:"session_send_timeout" json:"session_send_timeout"`
	SessionMaxPipeline     int               `toml:"session_max_pipeline" json:"session_max_pipeline"`
	SessionKeepAlivePeriod timesize.Duration `toml:"session_keepalive_period" json:"session_keepalive_period"`
	SessionBreakOnFailure  bool              `toml:"session_break_on_failure" json:"session_break_on_failure"`

	SlowlogLogSlowerThan   int64 			 `toml:"slowlog_log_slower_than" json:"slowlog_log_slower_than"`
	SlowlogMaxLen          int64 			 `toml:"slowlog_max_len" json:"slowlog_max_len"`
	QuickCmdList		   string            	 `toml:"quick_cmd_list" json:"quick_cmd_list"`
	SlowCmdList		   	   string        `toml:"slow_cmd_list" json:"slow_cmd_list"`
	AutoSetSlowFlag		   bool			 `toml:"auto_set_slow_flag" json:"auto_set_slow_flag"`

	MonitorMaxValueLen         int64   `toml:"monitor_max_value_len" json:"monitor_max_value_len"`
	MonitorMaxBatchsize        int64   `toml:"monitor_max_batchsize" json:"monitor_max_batchsize"`
	MonitorMaxCmdInfo          int64   `toml:"monitor_max_cmd_info" json:"monitor_max_cmd_info"`
	MonitorLogMaxLen           int64   `toml:"monitor_log_max_len" json:"monitor_log_max_len"`
	MonitorResultSetSize       int64   `toml:"monitor_result_set_size" json:"monitor_result_set_size"`
	MonitorEnabled             int64   `toml:"monitor_enabled" json:"monitor_enabled"`

	BreakerEnabled                 int64   `toml:"breaker_enabled" json:"breaker_enabled"`
	BreakerDegradationProbability  int64   `toml:"breaker_degradation_probability" json:"breaker_degradation_probability"`
	BreakerQpsLimitation           int64   `toml:"breaker_qps_limitation" json:"breaker_qps_limitation"`
	BreakerCmdWhiteList            string  `toml:"breaker_cmd_white_list" json:"breaker_cmd_white_list"`
	BreakerCmdBlackList            string  `toml:"breaker_cmd_black_list" json:"breaker_cmd_black_list"`
	BreakerKeyWhiteList            string  `toml:"breaker_key_white_list" json:"breaker_key_white_list"`
	BreakerKeyBlackList            string  `toml:"breaker_key_black_list" json:"breaker_key_black_list"`
}

func NewDefaultConfig() *Config {
	c := &Config{}
	if _, err := toml.Decode(DefaultConfig, c); err != nil {
		log.PanicErrorf(err, "decode toml failed")
	}
	if err := c.Validate(); err != nil {
		log.PanicErrorf(err, "validate config failed")
	}
	return c
}

func (c *Config) LoadFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	if err != nil {
		return errors.Trace(err)
	}
	return c.Validate()
}

func (c *Config) String() string {
	var b bytes.Buffer
	e := toml.NewEncoder(&b)
	e.Indent = "    "
	e.Encode(c)
	return b.String()
}

func (c *Config) Validate() error {
	if c.ProtoType == "" {
		return errors.New("invalid proto_type")
	}
	if c.ProxyAddr == "" {
		return errors.New("invalid proxy_addr")
	}
	if c.AdminAddr == "" {
		return errors.New("invalid admin_addr")
	}
	if c.JodisName != "" {
		if c.JodisAddr == "" {
			return errors.New("invalid jodis_addr")
		}
		if c.JodisTimeout < 0 {
			return errors.New("invalid jodis_timeout")
		}
	}
	if c.ProductName == "" {
		return errors.New("invalid product_name")
	}
	if c.ProxyMaxClients <= 0 {
		return errors.New("invalid proxy_max_clients")
	}

	const MaxInt = bytesize.Int64(^uint(0) >> 1)

	if d := c.ProxyMaxOffheapBytes; d < 0 || d > MaxInt {
		return errors.New("invalid proxy_max_offheap_size")
	}
	if d := c.ProxyHeapPlaceholder; d < 0 || d > MaxInt {
		return errors.New("invalid proxy_heap_placeholder")
	}
	if c.ProxyRefreshStatePeriod < 0 {
		return errors.New("invalid proxy_refresh_state_period")
	}
	if c.BackendPingPeriod < 0 {
		return errors.New("invalid backend_ping_period")
	}

	if d := c.BackendRecvBufsize; d < 0 || d > MaxInt {
		return errors.New("invalid backend_recv_bufsize")
	}
	if c.BackendRecvTimeout < 0 {
		return errors.New("invalid backend_recv_timeout")
	}
	if d := c.BackendSendBufsize; d < 0 || d > MaxInt {
		return errors.New("invalid backend_send_bufsize")
	}
	if c.BackendSendTimeout < 0 {
		return errors.New("invalid backend_send_timeout")
	}
	if c.BackendMaxPipeline < 0 {
		return errors.New("invalid backend_max_pipeline")
	}
	if c.BackendPrimaryParallel <= 0 {
		return errors.New("invalid backend_primary_parallel")
	}
	if c.BackendPrimaryQuick < 0 || c.BackendPrimaryQuick >= c.BackendPrimaryParallel {
		return errors.New("invalid backend_primary_quick")
	}
	if c.BackendReplicaParallel <= 0 {
		return errors.New("invalid backend_replica_parallel")
	}
	if c.BackendReplicaQuick < 0 || c.BackendReplicaQuick >= c.BackendReplicaParallel {
		return errors.New("invalid backend_replica_quick")
	}
	if c.BackendKeepAlivePeriod < 0 {
		return errors.New("invalid backend_keepalive_period")
	}
	if c.BackendNumberDatabases < 1 {
		return errors.New("invalid backend_number_databases")
	}

	if d := c.SessionRecvBufsize; d < 0 || d > MaxInt {
		return errors.New("invalid session_recv_bufsize")
	}
	if c.SessionRecvTimeout < 0 {
		return errors.New("invalid session_recv_timeout")
	}
	if d := c.SessionSendBufsize; d < 0 || d > MaxInt {
		return errors.New("invalid session_send_bufsize")
	}
	if c.SessionSendTimeout < 0 {
		return errors.New("invalid session_send_timeout")
	}
	if c.SessionMaxPipeline < 0 {
		return errors.New("invalid session_max_pipeline")
	}
	if c.SessionKeepAlivePeriod < 0 {
		return errors.New("invalid session_keepalive_period")
	}

	if c.SlowlogLogSlowerThan < 0 {
		return errors.New("invalid slowlog_log_slower_than")
	}
	if c.SlowlogMaxLen < 0 {
		return errors.New("invalid slowlog_max_len")
	}
	if c.Ncpu <= 0 {
		return errors.New("invalid ncpu")
	}
	if c.Log == "" {
		return errors.New("invalid log")
	}
	if c.ExpireLogDays < 0 {
		return errors.New("invalid expire_log_days")
	}
	if c.LogLevel == "" {
		return errors.New("invalid log_level")
	}
	if c.PidFile == "" {
		return errors.New("invalid pidfile")
	}
	if c.MonitorMaxValueLen < 0 {
		return errors.New("invalid monitor_max_value_len")
	}
	if c.MonitorMaxBatchsize < 0 {
		return errors.New("invalid monitor_max_batchsize")
	}
	if c.MonitorMaxCmdInfo < 0 {
		return errors.New("invalid monitor_max_cmd_info")
	}
	if c.MonitorLogMaxLen < 0 {
		return errors.New("invalid monitor_log_max_len")
	}
	if c.MonitorResultSetSize < 0 {
		return errors.New("invalid monitor_result_set_size")
	}
	if c.BreakerEnabled != 0 && c.BreakerEnabled != 1{
		return errors.New("invalid breaker_enabled")
	}
	if c.BreakerDegradationProbability < 0 {
		return errors.New("invalid breaker_degradation_probability")
	}
	if c.BreakerQpsLimitation < 0 {
		return errors.New("invalid breaker_qps_limitation")
	}
	return nil
}
