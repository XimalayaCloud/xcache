// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"container/list"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"github.com/CodisLabs/codis/pkg/utils/redis"
	"github.com/CodisLabs/codis/pkg/utils/rpc"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
	"github.com/CodisLabs/codis/pkg/proxy"
)

type Topom struct {
	mu sync.Mutex

	xauth string
	model *models.Topom
	store *models.Store
	slaveStore *models.Store
	cache struct {
		hooks list.List
		slots []*models.SlotMapping
		group map[int]*models.Group
		proxy map[string]*models.Proxy

		sentinel *models.Sentinel
	}

	exit struct {
		C chan struct{}
	}

	config *Config
	online bool
	closed bool

	ladmin net.Listener

	action struct {
		redisp *redis.Pool

		interval   atomic2.Int64
		disabled   atomic2.Int64

		progress struct {
			status atomic.Value
		}
		executor atomic2.Int64
	}

	stats struct {
		redisp *redis.Pool

		servers map[string]*RedisStats
		proxies map[string]*ProxyStats
	}

	ha struct {
		redisp *redis.Pool

		monitor *redis.Sentinel
		masters map[int]string
	}
}

var ErrClosedTopom = errors.New("use of closed topom")

func New(client models.Client, config *Config) (*Topom, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Trace(err)
	}
	if err := models.ValidateProduct(config.ProductName); err != nil {
		return nil, errors.Trace(err)
	}
	s := &Topom{}
	s.config = config
	s.exit.C = make(chan struct{})
	s.action.redisp = redis.NewPool(config.ProductAuth, config.MigrationTimeout.Duration())
	s.action.progress.status.Store("")

	s.ha.redisp = redis.NewPool("", time.Second*5)

	s.model = &models.Topom{
		StartTime: time.Now().String(),
	}
	s.model.ProductName = config.ProductName
	s.model.Pid = os.Getpid()
	s.model.Pwd, _ = os.Getwd()
	if b, err := exec.Command("uname", "-a").Output(); err != nil {
		log.WarnErrorf(err, "run command uname failed")
	} else {
		s.model.Sys = strings.TrimSpace(string(b))
	}
	s.store = models.NewStore(client, config.ProductName)

	if config.MasterProduct != "" {
		if config.MasterProduct != config.ProductName {
			log.Panicf("MasterProduct:%s not same with ProductName:%s", config.MasterProduct, config.ProductName)
		} else {
			slaveClient, err := models.NewSqlClient(config.MasterMysqlAddr, config.MasterMysqlUsername, config.MasterMysqlPassword, config.MasterMysqlDatabase)
			if err != nil {
				log.PanicErrorf(err, "create '%s' client to '%s' failed", "Mysql", config.MasterMysqlAddr)
			}
			s.slaveStore = models.NewStore(slaveClient, config.MasterProduct)
			log.Warnf("Became slave_dashboard and get Slots_info from master_dashboard-[%s]", config.MasterProduct)
		}
	}

	s.stats.redisp = redis.NewPool(config.ProductAuth, time.Second*5)
	s.stats.servers = make(map[string]*RedisStats)
	s.stats.proxies = make(map[string]*ProxyStats)

	if err := s.setup(config); err != nil {
		s.Close()
		return nil, err
	}

	log.Warnf("create new topom:\n%s", s.model.Encode())

	go s.serveAdmin()

	s.startMetricsInfluxdb()

	return s, nil
}

func (s *Topom) setup(config *Config) error {
	if l, err := net.Listen("tcp", config.AdminAddr); err != nil {
		return errors.Trace(err)
	} else {
		s.ladmin = l

		x, err := utils.ReplaceUnspecifiedIP("tcp", l.Addr().String(), s.config.HostAdmin)
		if err != nil {
			return err
		}
		s.model.AdminAddr = x
	}

	s.model.Token = rpc.NewToken(
		config.ProductName,
		s.ladmin.Addr().String(),
	)
	s.xauth = rpc.NewXAuth(config.ProductName)

	return nil
}

func (s *Topom) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.exit.C)

	if s.ladmin != nil {
		s.ladmin.Close()
	}
	for _, p := range []*redis.Pool{
		s.action.redisp, s.stats.redisp, s.ha.redisp,
	} {
		if p != nil {
			p.Close()
		}
	}

	defer s.store.Close()

	if s.online {
		if err := s.store.Release(); err != nil {
			log.ErrorErrorf(err, "store: release lock of %s failed", s.config.ProductName)
			return errors.Errorf("store: release lock of %s failed", s.config.ProductName)
		}
	}
	return nil
}

func (s *Topom) Start(routines bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedTopom
	}
	if s.online {
		return nil
	} else {
		if err := s.store.Acquire(s.model); err != nil {
			log.ErrorErrorf(err, "store: acquire lock of %s failed", s.config.ProductName)
			return errors.Errorf("store: acquire lock of %s failed", s.config.ProductName)
		}
		s.online = true
	}

	if !routines {
		return nil
	}
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	s.rewatchSentinels(ctx.sentinel.Servers)

	go func() {
		for !s.IsClosed() {
			if s.IsOnline() {
				w, _ := s.RefreshRedisStats(time.Second)
				if w != nil {
					w.Wait()
				}
			}
			time.Sleep(time.Second)
		}
	}()

	go func() {
		for !s.IsClosed() {
			if s.IsOnline() {
				w, _ := s.RefreshProxyStats(time.Second)
				if w != nil {
					w.Wait()
				}
			}
			time.Sleep(time.Second)
		}
	}()

	go func() {
		for !s.IsClosed() {
			if s.IsOnline() {
				if err := s.ProcessSlotAction(); err != nil {
					log.WarnErrorf(err, "process slot action failed")
					time.Sleep(time.Second * 5)
				}
			}
			time.Sleep(time.Second)
		}
	}()

	go func() {
		for !s.IsClosed() {
			if s.IsOnline() {
				if err := s.ProcessSyncAction(); err != nil {
					log.WarnErrorf(err, "process sync action failed")
					time.Sleep(time.Second * 5)
				}
			}
			time.Sleep(time.Second)
		}
	}()

	// 定期刷新proxy的延时信息
	go func() {
		var loops int64 = 0 
		for !s.IsClosed() {
			if s.IsOnline() {
				w, _ := s.RefreshProxyCmdStats(time.Second, loops)
				if w != nil {
					w.Wait()
				}
			}
			loops++
			if loops >= proxy.IntervalMark[len(proxy.IntervalMark)-1] {
				loops = 0
			}
			time.Sleep(time.Second)
		}
	}()

	// 定时刷新redis的延时信息
	go func() {
		var loops int64 = 0 
		for !s.IsClosed() {
			if s.IsOnline() {
				w, _ := s.RefreshRedisCmdStats(time.Second, loops)
				if w != nil {
					w.Wait()
				}
			}
			loops++
			if loops >= proxy.IntervalMark[len(proxy.IntervalMark)-1] {
				loops = 0
			}
			time.Sleep(time.Second)
		}
	}()

	go func() {
		for !s.IsClosed() {
			if s.IsOnline() {
				if err := s.AutoPurgeLog(s.config.Log, s.config.ExpireLogDays); err != nil {
					log.WarnErrorf(err, "AutoDelExpireLog failed")
					time.Sleep(time.Second * 5)
				}
			}
			time.Sleep(time.Second * 60 * 60 * 24)
		}
	}()

	return nil
}

func (s *Topom) XAuth() string {
	return s.xauth
}

func (s *Topom) Model() *models.Topom {
	return s.model
}

var ErrNotOnline = errors.New("topom is not online")

func (s *Topom) newContext() (*context, error) {
	if s.closed {
		return nil, ErrClosedTopom
	}
	if s.online {
		if err := s.refillCache(); err != nil {
			return nil, err
		} else {
			ctx := &context{}
			ctx.slots = s.cache.slots
			ctx.group = s.cache.group
			ctx.proxy = s.cache.proxy
			ctx.sentinel = s.cache.sentinel
			ctx.hosts.m = make(map[string]net.IP)
			ctx.method, _ = models.ParseForwardMethod(s.config.MigrationMethod)
			return ctx, nil
		}
	} else {
		return nil, ErrNotOnline
	}
}

func (s *Topom) Stats() (*Stats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}

	stats := &Stats{}
	stats.Closed = s.closed

	stats.Slots = ctx.slots

	stats.Group.Models = models.SortGroup(ctx.group)
	stats.Group.Stats = map[string]*RedisStats{}
	for _, g := range ctx.group {
		for _, x := range g.Servers {
			if v := s.stats.servers[x.Addr]; v != nil {
				stats.Group.Stats[x.Addr] = v
			}
		}
	}

	stats.Proxy.Models = models.SortProxy(ctx.proxy)
	stats.Proxy.Stats = s.stats.proxies

	stats.SlotAction.Interval = s.action.interval.Int64()
	stats.SlotAction.Disabled = s.action.disabled.Int64()
	stats.SlotAction.Progress.Status = s.action.progress.status.Load().(string)
	stats.SlotAction.Executor = s.action.executor.Int64()

	stats.HA.Model = ctx.sentinel
	stats.HA.Stats = map[string]*RedisStats{}
	for _, server := range ctx.sentinel.Servers {
		if v := s.stats.servers[server]; v != nil {
			stats.HA.Stats[server] = v
		}
	}
	stats.HA.Masters = make(map[string]string)
	if s.ha.masters != nil {
		for gid, addr := range s.ha.masters {
			stats.HA.Masters[strconv.Itoa(gid)] = addr
		}
	}
	return stats, nil
}

type Stats struct {
	Closed bool `json:"closed"`

	Slots []*models.SlotMapping `json:"slots"`

	Group struct {
		Models []*models.Group        `json:"models"`
		Stats  map[string]*RedisStats `json:"stats"`
	} `json:"group"`

	Proxy struct {
		Models []*models.Proxy        `json:"models"`
		Stats  map[string]*ProxyStats `json:"stats"`
	} `json:"proxy"`

	SlotAction struct {
		Interval int64 `json:"interval"`
		Disabled int64  `json:"disabled"`

		Progress struct {
			Status string `json:"status"`
		} `json:"progress"`

		Executor int64 `json:"executor"`
	} `json:"slot_action"`

	HA struct {
		Model   *models.Sentinel       `json:"model"`
		Stats   map[string]*RedisStats `json:"stats"`
		Masters map[string]string      `json:"masters"`
	} `json:"sentinels"`
}

func (s *Topom) Config() *Config {
	return s.config
}

func (s *Topom) IsOnline() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.online && !s.closed
}

func (s *Topom) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Topom) GetSlotActionInterval() int {
	return s.action.interval.AsInt()
}

func (s *Topom) SetSlotActionInterval(us int) {
	us = math2.MinMaxInt(us, 0, 1000*1000)
	s.action.interval.Set(int64(us))
	log.Warnf("set action interval = %d", us)
}

func (s *Topom) GetSlotActionDisabled() int {
	return s.action.disabled.AsInt()
}

func (s *Topom) SetSlotActionDisabled(value int) {
	//0:enabled  1:immediate stop  2:wait stop
	s.action.disabled.Set(int64(value))
	log.Warnf("set action disabled = %d", value)
}

func (s *Topom) Slots() ([]*models.Slot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}
	return ctx.toSlotSlice(ctx.slots, nil), nil
}

func (s *Topom) Reload() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.newContext()
	if err != nil {
		return err
	}
	//defer s.dirtyCacheAll()
	s.dirtyCacheAll()

	// 当使用mysql存储集群节点并且dashboard为slave时，强制将slots最新状态同步到所有proxy
	if s.config.MasterProduct != "" {
		s.ReinitAllProxy()
	}

	return nil
}

func (s *Topom) serveAdmin() {
	if s.IsClosed() {
		return
	}
	defer s.Close()

	log.Warnf("admin start service on %s", s.ladmin.Addr())

	eh := make(chan error, 1)
	go func(l net.Listener) {
		h := http.NewServeMux()
		h.Handle("/", newApiServer(s))
		hs := &http.Server{Handler: h}
		eh <- hs.Serve(l)
	}(s.ladmin)

	select {
	case <-s.exit.C:
		log.Warnf("admin shutdown")
	case err := <-eh:
		log.ErrorErrorf(err, "admin exit on error")
	}
}

type Overview struct {
	Version string        `json:"version"`
	Compile string        `json:"compile"`
	Config  *Config       `json:"config,omitempty"`
	Model   *models.Topom `json:"model,omitempty"`
	Stats   *Stats        `json:"stats,omitempty"`
}

func (s *Topom) Overview() (*Overview, error) {
	if stats, err := s.Stats(); err != nil {
		return nil, err
	} else {
		return &Overview{
			Version: utils.Version,
			Compile: utils.Compile,
			Config:  s.Config(),
			Model:   s.Model(),
			Stats:   stats,
		}, nil
	}
}

func (s *Topom) SetConfig(key, value string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var nextStep string
	strings.ToLower(value)
	strings.ToLower(key)
	
	switch key {
	case "migration_method":
		nextStep = "migrate"
		if (value != "semi-async" && value != "sync") {
			return "", errors.New("invalid value for migration_method")
		}
		s.config.MigrationMethod = value

	case "migration_parallel_slots":
		nextStep = "migrate"
		ret,err := strconv.Atoi(value)
		if err != nil {
			return nextStep, err
		}
		s.config.MigrationParallelSlots = ret

	case "migration_async_maxbulks":
		nextStep = "migrate"
		ret,err := strconv.Atoi(value)
		if err != nil {
			return nextStep, err
		}
		s.config.MigrationAsyncMaxBulks = ret

	case "migration_async_maxbytes":
		nextStep = "migrate"
		p := &(s.config.MigrationAsyncMaxBytes)
		err :=  p.UnmarshalText([]byte(value))
		if err != nil {
			return nextStep, err
		}

	case "migration_async_numkeys":
		nextStep = "migrate"
		ret,err := strconv.Atoi(value)
		if err != nil {
			return nextStep, err
		}
		s.config.MigrationAsyncNumKeys = ret

	case "migration_timeout":
		nextStep = "migrate"
		p := &(s.config.MigrationTimeout)
		err :=  p.UnmarshalText([]byte(value))
		if err != nil {
			return nextStep, err
		}

	case "sentinel_client_timeout":
		nextStep = "sentinel"
		p := &(s.config.SentinelClientTimeout)
		err :=  p.UnmarshalText([]byte(value))
		if err != nil {
			return nextStep, err
		}

	case "sentinel_quorum":
		nextStep = "sentinel"
		ret,err := strconv.Atoi(value)
		if err != nil {
			return nextStep, err
		}
		s.config.SentinelQuorum = ret

	case "sentinel_parallel_syncs":
		nextStep = "sentinel"
		ret,err := strconv.Atoi(value)
		if err != nil {
			return nextStep, err
		}
		s.config.SentinelParallelSyncs = ret

	case "sentinel_down_after":
		nextStep = "sentinel"
		p := &(s.config.SentinelDownAfter)
		err :=  p.UnmarshalText([]byte(value))
		if err != nil {
			return nextStep, err
		}

	case "sentinel_failover_timeout":
		nextStep = "sentinel"
		p := &(s.config.SentinelFailoverTimeout)
		err :=  p.UnmarshalText([]byte(value))
		if err != nil {
			return nextStep, err
		}

	case "sentinel_notification_script":
		nextStep = "sentinel"
		s.config.SentinelNotificationScript = value

	case "sentinel_client_reconfig_script":
		nextStep = "sentinel"
		s.config.SentinelClientReconfigScript = value

	case "expire_log_days":
		nextStep = "log"
		intValue, err := strconv.Atoi(value)
		if err != nil {
			return nextStep, err
		}
		if intValue < 0 {
			return nextStep, errors.New("invalid expire_log_days")
		}
		s.config.ExpireLogDays = intValue

	default:
		return "", errors.New("invalid key")
	}
	return nextStep, utils.RewriteConf(*(s.config), s.config.ConfigName, "=", true)
}

func NewZkToMysql(client models.Client, config *Config) (*Topom, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Trace(err)
	}
	if err := models.ValidateProduct(config.ProductName); err != nil {
		return nil, errors.Trace(err)
	}
	s := &Topom{}
	s.config = config

	
	s.store = models.NewStore(client, config.ProductName)

	return s, nil
}
