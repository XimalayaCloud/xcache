// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"time"
	"encoding/json"
	//"fmt"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/proxy"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/redis"
	"github.com/CodisLabs/codis/pkg/utils/rpc"
	"github.com/CodisLabs/codis/pkg/utils/sync2"
)

type redisOpStats struct {
	OpStr          string `json:"opstr"`
	Interval       int64  `json:"interval"`
	TotalCalls     int64  `json:"total_calls"`
	TotalUsecs     int64  `json:"total_usecs"`
	UsecsPercall   int64  `json:"usecs_percall"`

	Calls          int64  `json:"calls"`
	Usecs          int64  `json:"usecs"`
	RedisErrType   int64  `json:"errors"`
	QPS 		   int64  `json:"qps"`
	AVG            int64  `json:"avg"`
	TP90  		   int64  `json:"tp90"`
	TP99  		   int64  `json:"tp99"`
	TP999  		   int64  `json:"tp999"`
	TP9999  	   int64  `json:"tp9999"`
	TP100          int64  `json:"tp100"`

	Delay50ms    int64  `json:"delay50ms"`
	Delay100ms   int64  `json:"delay100ms"`
	Delay200ms   int64  `json:"delay200ms"`
	Delay300ms   int64  `json:"delay300ms"`
	Delay500ms   int64  `json:"delay500ms"`
	Delay1s      int64  `json:"delay1s"`
	Delay2s      int64  `json:"delay2s"`
	Delay3s      int64  `json:"delay3s"`
}


type  RedisCmdStats struct {
	Total  int64 `json:"total"`
	Errors int64 `json:"errors"`
	QPS    int64   `json:"qps"`
	Cmd []*redisOpStats `json:"cmd",omitempty`
}

type  RedisCmdList struct {
	CmdList []*RedisCmdStats  `json:"cmd"`
}

type RedisStats struct {
	Stats map[string]string `json:"stats,omitempty"`
	CmdStats *RedisCmdList  `json:"rediscmdstats"`

	Error *rpc.RemoteError  `json:"error,omitempty"`

	Sentinel map[string]*redis.SentinelGroup `json:"sentinel,omitempty"`

	UnixTime int64 `json:"unixtime"`
	Timeout  bool  `json:"timeout,omitempty"`
}

func (s *Topom) newRedisStats(addr string, timeout time.Duration, do func(addr string) (*RedisStats, error)) *RedisStats {
	var ch = make(chan struct{})
	stats := &RedisStats{}

	go func() {
		defer close(ch)
		p, err := do(addr)
		if err != nil {
			stats.Error = rpc.NewRemoteError(err)
		} else {
			stats.Stats, stats.Sentinel = p.Stats, p.Sentinel
		}
	}()

	select {
	case <-ch:
		return stats
	case <-time.After(timeout):
		return &RedisStats{Timeout: true}
	}
}

func (s *Topom) newRedisCmdStats(addr string, timeout time.Duration, interval int64,  do func(addr string, interval int64) (string, error)) string {
	var ch = make(chan struct{})
	resp := ""
	go func() {
		defer close(ch)
		p, err := do(addr, interval)
		if err != nil {
			resp = ""
		} else {
			resp = p
		}
	}()

	select {
	case <-ch:
		return resp
	case <-time.After(timeout):
		return ""
	}
}

func (s *Topom) RefreshRedisStats(timeout time.Duration) (*sync2.Future, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}
	var fut sync2.Future
	goStats := func(addr string, do func(addr string) (*RedisStats, error)) {
		fut.Add()
		go func() {
			stats := s.newRedisStats(addr, timeout, do)
			stats.UnixTime = time.Now().Unix()
			fut.Done(addr, stats)
		}()
	}
	for _, g := range ctx.group {
		for _, x := range g.Servers {
			goStats(x.Addr, func(addr string) (*RedisStats, error) {
				m, err := s.stats.redisp.InfoFull(addr)
				if err != nil {
					return nil, err
				}
				return &RedisStats{Stats: m}, nil
			})
		}
	}
	for _, server := range ctx.sentinel.Servers {
		goStats(server, func(addr string) (*RedisStats, error) {
			c, err := s.ha.redisp.GetClient(addr)
			if err != nil {
				return nil, err
			}
			defer s.ha.redisp.PutClient(c, err)
			m, err := c.Info()
			if err != nil {
				return nil, err
			}
			sentinel := redis.NewSentinel(s.config.ProductName, s.config.ProductAuth)
			p, err := sentinel.MastersAndSlavesClient(c)
			if err != nil {
				return nil, err
			}
			return &RedisStats{Stats: m, Sentinel: p}, nil
		})
	}
	go func() {
		stats := make(map[string]*RedisStats)
		for k, v := range fut.Wait() {
			stats[k] = v.(*RedisStats)
			s.mu.Lock()
			if _, ok := s.stats.servers[k]; ok {
				stats[k].CmdStats = s.stats.servers[k].CmdStats
			}
			s.mu.Unlock()
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.stats.servers = stats
	}()
	return &fut, nil
}

func (s *Topom) RefreshRedisCmdStats(timeout time.Duration, loops int64) (*sync2.Future, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}
	var fut sync2.Future
	goStats := func(addr string, do func(addr string, interval int64) (string, error)) {
		fut.Add()
		go func() {
			redisCmdList := &RedisCmdList{}
			for i := 0; i < len(proxy.IntervalMark); i++ {
				if loops % proxy.IntervalMark[i] != 0 {
					redisCmdList.CmdList = append(redisCmdList.CmdList, nil)
					continue
				}
				redisCmdstats := &RedisCmdStats{}
				stats := s.newRedisCmdStats(addr, timeout, proxy.IntervalMark[i], do)
				if err := json.Unmarshal([]byte(stats), redisCmdstats); err != nil {
					log.Debugf("get redis cmd stats error: redis-[%s] interval-[%d]", addr, proxy.IntervalMark[i])
					redisCmdstats = nil
				}

				redisCmdList.CmdList = append(redisCmdList.CmdList, redisCmdstats)
			}
			fut.Done(addr, redisCmdList)
		}()
	}
	for _, g := range ctx.group {
		for _, x := range g.Servers {
			goStats(x.Addr, func(addr string, interval int64) (string, error) {
				m, err := s.stats.redisp.InfoDelay(addr, interval)
				if err != nil {
					return "", err
				}
				return m, nil
			})
		}
	}

	go func() {
		for k, v := range fut.Wait() {
			s.mu.Lock()
			redisStats, ok := s.stats.servers[k]
			if !ok {
				s.mu.Unlock()
				continue
			} 

			if redisStats.CmdStats == nil {
				redisStats.CmdStats  = &RedisCmdList{CmdList: make([]*RedisCmdStats, 5, 5)}
			}

			for i:=0; i<len(proxy.IntervalMark); i++ {
				cmdInfo := v.(*RedisCmdList).CmdList[i]
				if cmdInfo == nil && loops % proxy.IntervalMark[i] != 0 {
					continue
				}
				
				redisStats.CmdStats.CmdList[i] = cmdInfo
			}

			s.mu.Unlock()
		}
		//s.stats.servers = stats
	}()
	return &fut, nil
}

type  ProxyCmdStats struct {
	CmdList []*proxy.CmdInfo  `json:"cmd"`
}

type ProxyStats struct {
	Stats *proxy.Stats     `json:"stats,omitempty"`
	CmdStats *ProxyCmdStats `json:"-"`

	Error *rpc.RemoteError `json:"error,omitempty"`

	UnixTime int64 `json:"unixtime"`
	Timeout  bool  `json:"timeout,omitempty"`
}

func (s *Topom) newProxyStats(p *models.Proxy, timeout time.Duration) *ProxyStats {
	var ch = make(chan struct{})
	stats := &ProxyStats{}

	go func() {
		defer close(ch)
		x, err := s.newProxyClient(p).StatsSimple()
		if err != nil {
			stats.Error = rpc.NewRemoteError(err)
		} else {
			stats.Stats = x
		}
	}()

	select {
	case <-ch:
		return stats
	case <-time.After(timeout):
		return &ProxyStats{Timeout: true}
	}
}

func (s *Topom) newProxyCmdStats(p *models.Proxy, loops int64, timeout time.Duration) *proxy.CmdInfo {
	var ch = make(chan struct{})
	stats := &proxy.CmdInfo{}

	go func() {
		defer close(ch)
		x, err := s.newProxyClient(p).CmdInfo(loops)
		if err != nil {
			stats = nil
			//stats.Error = rpc.NewRemoteError(err)
		} else {
			stats = x
		}
	}()

	select {
	case <-ch:
		return stats
	case <-time.After(timeout):
		return nil
	}
}

func (s *Topom) RefreshProxyStats(timeout time.Duration) (*sync2.Future, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}
	var fut sync2.Future
	for _, p := range ctx.proxy {
		fut.Add()
		go func(p *models.Proxy) {
			stats := s.newProxyStats(p, timeout)
			stats.UnixTime = time.Now().Unix()
			fut.Done(p.Token, stats)

			switch x := stats.Stats; {
			case x == nil:
			case x.Closed || x.Online:
			default:
				if err := s.OnlineProxy(p.AdminAddr); err != nil {
					log.WarnErrorf(err, "auto online proxy-[%s] failed", p.Token)
				}
			}
		}(p)
	}
	go func() {
		stats := make(map[string]*ProxyStats)
		for k, v := range fut.Wait() {
			stats[k] = v.(*ProxyStats)
			s.mu.Lock()
			if _, ok := s.stats.proxies[k]; ok {
				stats[k].CmdStats = s.stats.proxies[k].CmdStats
			}
			s.mu.Unlock()
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.stats.proxies = stats
	}()
	return &fut, nil
}

func (s *Topom) RefreshProxyCmdStats(timeout time.Duration, loops int64) (*sync2.Future, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}
	var fut sync2.Future
	for _, p := range ctx.proxy {
		fut.Add()
		go func(p *models.Proxy) {
			proxyCmdStats := &ProxyCmdStats{}
			for i := 0; i < len(proxy.IntervalMark); i++ {
				if loops % proxy.IntervalMark[i] != 0 {
					proxyCmdStats.CmdList = append(proxyCmdStats.CmdList, nil)
					continue
				}

				stats := s.newProxyCmdStats(p, proxy.IntervalMark[i], timeout)
				if stats == nil {
					log.Warnf("newProxyCmdStats failed: proxy-[%s], interval-[%d]", p.ProxyAddr, proxy.IntervalMark[i])	
				}

				proxyCmdStats.CmdList = append(proxyCmdStats.CmdList, stats)
			}
			
			fut.Done(p.Token, proxyCmdStats)
		}(p)
	}
	go func() {
		//stats := make(map[string]*ProxyStats)
		for k, v := range fut.Wait() {
			s.mu.Lock()
			//fmt.Println(k)
			proxyStats, ok := s.stats.proxies[k]
			if !ok {
				s.mu.Unlock()
				continue
			}
			if proxyStats.CmdStats == nil {
				proxyStats.CmdStats  = &ProxyCmdStats{CmdList: make([]*proxy.CmdInfo, 5, 5)}
			}
			
			for i:=0; i<len(proxy.IntervalMark); i++ {
				cmdInfo := v.(*ProxyCmdStats).CmdList[i]
				// 如果到了统计周期但 cmdInfo 为 nil，说明此次统计结果获取失败
				if cmdInfo == nil && loops % proxy.IntervalMark[i] != 0 {
					continue
				}

				proxyStats.CmdStats.CmdList[i] = cmdInfo
			}
			s.mu.Unlock()
		}
	}()
	return &fut, nil
}
