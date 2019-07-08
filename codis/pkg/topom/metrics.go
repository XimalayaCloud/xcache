// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"time"
	"fmt"
	"strings"
	"strconv"
	"math"

	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"

	client "github.com/influxdata/influxdb/client/v2"
)

type TopomInfluxdbStats struct {
	Ops struct {
		Total int64             `json:"total"`
		Fails int64             `json:"fails"`
		Qps   int64             `json:"qps"`
	} `json:"ops"`

	Sessions struct {
		Total int64 `json:"total"`
		Alive int64 `json:"alive"`
	} `json:"sessions"`

	Servers struct {
		TotalKeys			int64		`json:"total_keys"`
		UsedMemory			int64		`json:"used_memory"`
		MaxMemory			int64		`json:"max_memory"`
		TotalCommandsProcessed 		int64		`json:"total_commands_processed"`
		ConnectedClients 		int64 		`json:"connected_clients"`
		TotalConnectionsReceived 	int64		`json:"total_connections_received"`
		InstantaneousOpsPerSec 		int64		`json:"instantaneous_ops_per_sec"`
		CacheMemory 				int64 		`json:"cache_memory"`
		CacheKeys 					int64 		`json:"cache_keys"`
		ReadCmdPerSec 				int64 		`json:"read_cmd_per_sec"`
		HitsPerSec 					int64 		`json:"hits_per_sec"`
		CmdHitRate 					int64 		`json:"cmd_hit_rate"`
	} `json:"servers"`

	/*Caches struct {
		RedisKeys			int64		`json:"redis_keys"`
		UsedMemory			int64		`json:"used_memory"`
		TotalCommandsProcessed 		int64		`json:"total_commands_processed"`
		ConnectedClients 		int64 		`json:"connected_clients"`
		TotalConnectionsReceived 	int64		`json:"total_connections_received"`
		InstantaneousOpsPerSec 		int64		`json:"instantaneous_ops_per_sec"`
		ReadCmdPerSec				int64  `json:"read_cmd_per_sec"`
		ReadCmdHit					int64  `json:"read_cmd_hit"`
		CmdHitRate					int64  `json:"cmd_hit_rate"`
	} `json:"caches"`*/
}

type TopomOpStats struct {
	OpStr        string `json:"opstr"`
	Calls        int64  `json:"calls"`
	Usecs        int64  `json:"usecs"`
	UsecsPercall int64  `json:"usecs_percall"`
	Fails        int64  `json:"fails"`
	RedisErrType int64  `json:"redis_errtype"`
	QPS 		 int64  `json:"qps"`
	AVG          float64  `json:"avg"`
	TP90  		 float64  `json:"tp90"`
	TP99  		 float64  `json:"tp99"`
	TP999  		 float64  `json:"tp999"`
	TP9999  	 float64  `json:"tp9999"`
	TP100        int64    `json:"tp100"`
}

func (p *Topom) startMetricsReporter(d time.Duration, do, cleanup func() error) {
	go func() {
		if cleanup != nil {
			defer cleanup()
		}
		var ticker = time.NewTicker(d)
		defer ticker.Stop()
		var delay = &DelayExp2{
			Min: 1, Max: 15,
			Unit: time.Second,
		}
		for !p.IsClosed() {
			<-ticker.C
			if err := do(); err != nil {
				log.WarnErrorf(err, "report metrics failed")
				delay.SleepWithCancel(p.IsClosed)
			} else {
				delay.Reset()
			}
		}
	}()
}

func (p *Topom) startMetricsInfluxdb() {
	server := p.config.MetricsReportInfluxdbServer
	period := p.config.MetricsReportInfluxdbPeriod.Duration()
	if server == "" {
		return
	}
	period = math2.MaxDuration(time.Second, period)

	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     server,
		Username: p.config.MetricsReportInfluxdbUsername,
		Password: p.config.MetricsReportInfluxdbPassword,
		Timeout:  time.Second * 5,
	})
	if err != nil {
		log.WarnErrorf(err, "create influxdb client failed")
		return
	}

	database := p.config.MetricsReportInfluxdbDatabase

	p.startMetricsReporter(period, func() error {
		batch, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  database,
			Precision: "ns",
		})
		if err != nil {
			return errors.Trace(err)
		}

		stats, err := p.Stats();
		if err != nil {
			return errors.Trace(err)
		}

		topomStats := &TopomInfluxdbStats{}
		topomOpStats := make(map[string] *TopomOpStats)

		err = p.GenProxyPoints(stats, batch, topomStats)
		if err != nil {
			return errors.Trace(err)
		}

		err = p.GenGroupServerPoints(stats, batch, topomStats)
		if err != nil {
			return errors.Trace(err)
		}

		err = p.GenProxyCmdInfoPoints(stats, batch, topomOpStats)
		if err != nil {
			return errors.Trace(err)
		}

		err = p.GenTopomCmdInfoPoint(batch, topomOpStats)
		if err != nil {
			return errors.Trace(err)
		}

		err = p.GenTopomPoint(batch, topomStats)
		if err != nil {
			return errors.Trace(err)
		}

		if len(batch.Points()) > 0 {
			return c.Write(batch)
		} else {
			return nil
		}
	}, func() error {
		return c.Close()
	})
}

func (p *Topom) GenTopomCmdInfoPoint(batch client.BatchPoints, OpStats map[string] *TopomOpStats) error{
	model := p.Model()
	if model == nil {
		return fmt.Errorf("GenTopomCmdInfoPoint model is nil")
	}

	for cmd, cmdInfo := range OpStats {
		tags := map[string]string{
			"product_name": model.ProductName,
			"cmd_name": 			cmd,
		}

		fields := map[string]interface{}{
			"calls":		   cmdInfo.Calls,
			"Usecs":		   cmdInfo.Usecs,
			"UsecsPercall":	   cmdInfo.UsecsPercall,
			"fails":           cmdInfo.Fails,
			"redis_errtype":   cmdInfo.RedisErrType,
			"qps":         	   cmdInfo.QPS,
			"tp90":            int64(math.Ceil(cmdInfo.TP90)),
			"tp99":            int64(math.Ceil(cmdInfo.TP99)),
			"tp999":           int64(math.Ceil(cmdInfo.TP999)),
			"tp9999":          int64(math.Ceil(cmdInfo.TP9999)),
			"tp100":           cmdInfo.TP100,
			"avg":             int64(math.Ceil(cmdInfo.AVG)),
		}
		table := getTableName("dashboard_", model.AdminAddr) + "_cmd_info"
		//table = table + "_cmd_info"
		point, err := client.NewPoint(table, tags, fields, time.Now())
		if err != nil {
			log.WarnErrorf(err, "GenTopomCmdInfoPoint NewPoint[%s] error", model.AdminAddr)
			return fmt.Errorf("GenTopomCmdInfoPoint NewPoint[%s] error", model.AdminAddr)
		}

		batch.AddPoint(point)
	}
	return nil
}

func (p *Topom) GenTopomPoint(batch client.BatchPoints, topomStats *TopomInfluxdbStats) error{
	model := p.Model()
	if model == nil {
		return fmt.Errorf("GenTopomPoint model is nil")
	}

	tags := map[string]string{
		"product_name": model.ProductName,
		"admin_addr":   model.AdminAddr,
	}

	if (topomStats.Servers.ReadCmdPerSec == 0) {
		topomStats.Servers.CmdHitRate = 0
	} else {
		topomStats.Servers.CmdHitRate = (topomStats.Servers.HitsPerSec * 100.0) / topomStats.Servers.ReadCmdPerSec
	}
	fields := map[string]interface{}{
		"ops_total":         					topomStats.Ops.Total,
		"ops_fails":                			topomStats.Ops.Fails,
		"ops_qps":                  			topomStats.Ops.Qps,
		"sessions_total":           			topomStats.Sessions.Total,
		"sessions_alive":           			topomStats.Sessions.Alive,
		"redis_keys":							topomStats.Servers.TotalKeys,
		"used_memory":							topomStats.Servers.UsedMemory,
		"used_ssd":								topomStats.Servers.MaxMemory,
		"total_commands_processed":				topomStats.Servers.TotalCommandsProcessed,
		"connected_clients": 					topomStats.Servers.ConnectedClients,
		"total_connections_received":			topomStats.Servers.TotalConnectionsReceived,
		"instantaneous_ops_per_sec":			topomStats.Servers.InstantaneousOpsPerSec,
		"cache_memory":							topomStats.Servers.CacheMemory,
		"cache_keys":							topomStats.Servers.CacheKeys,
		"cache_read_qps":						topomStats.Servers.ReadCmdPerSec,
		"cache_hit_rate":						topomStats.Servers.CmdHitRate,
	}

	table := getTableName("dashboard_", model.AdminAddr)
	point, err := client.NewPoint(table, tags, fields, time.Now())
	if err != nil {
		log.WarnErrorf(err, "GenTopomPoint NewPoint[%s] error", model.AdminAddr)
		return fmt.Errorf("GenTopomPoint NewPoint[%s] error", model.AdminAddr)
	}

	batch.AddPoint(point)

	return nil
}

func (p *Topom) GenProxyPoints(stats *Stats, batch client.BatchPoints, topomStats *TopomInfluxdbStats) error{
	Pmodels := stats.Proxy.Models
	Pstats := stats.Proxy.Stats
	Plen := len(Pmodels)

	for i := 0; i < Plen; i++ {
		if Pmodels[i] == nil {
			log.Warnf("GenProxyPoints error index[%d] is nil", i)
			continue
		}

		p, ok := Pstats[Pmodels[i].Token]
		if ok && p != nil && p.Stats != nil {

			if !p.Stats.Online || p.Stats.Closed {
				log.Infof("GenProxyPoints proxy[%s] is closed or offline", Pmodels[i].ProxyAddr)
				continue
			}

			//collection proxy stats to topom
			topomStats.Ops.Total += p.Stats.Ops.Total
			topomStats.Ops.Fails += p.Stats.Ops.Fails
			topomStats.Ops.Qps += p.Stats.Ops.QPS
			topomStats.Sessions.Total += p.Stats.Sessions.Total
			topomStats.Sessions.Alive += p.Stats.Sessions.Alive

			tags := map[string]string{
				"product_name": Pmodels[i].ProductName,
				"admin_addr":   Pmodels[i].AdminAddr,
			}

			fields := map[string]interface{}{
				"ops_total":         		p.Stats.Ops.Total,
				"ops_fails":                p.Stats.Ops.Fails,
				"ops_qps":                  p.Stats.Ops.QPS,
				"sessions_total":           p.Stats.Sessions.Total,
				"sessions_alive":           p.Stats.Sessions.Alive,
			}

			table := getTableName("proxy_", Pmodels[i].ProxyAddr)
			point, err := client.NewPoint(table, tags, fields, time.Now())
			if err != nil {
				log.WarnErrorf(err, "GenProxyPoints NewPoint[%s] error", Pmodels[i].ProxyAddr)
				return fmt.Errorf("GenProxyPoints NewPoint[%s] error", Pmodels[i].ProxyAddr)
			}

			batch.AddPoint(point)
		} else {
			log.Warnf("GenProxyPoints get stats[%s] error", Pmodels[i].Token)
			continue
		}
	}

	return nil
}

func (p *Topom) GenGroupServerPoints(stats *Stats, batch client.BatchPoints, topomStats *TopomInfluxdbStats) error{
	Tmodel := p.Model()
	if Tmodel == nil {
		return fmt.Errorf("GenGroupServerPoints model is nil")
	}

	Gmodels := stats.Group.Models
	Gstats := stats.Group.Stats
	Glen := len(Gmodels)

	for i := 0; i < Glen; i++ {
		if Gmodels[i] == nil {
			log.Warnf("GenGroupServerPoints error index[%d] is nil", i)
			continue
		}

		Slen := len(Gmodels[i].Servers)
		for j := 0; j < Slen; j++{
			if Gmodels[i].Servers[j] == nil {
				log.Warnf("GenGroupServerPoints error Gmodels[%d].Servers[%d] is nil", i, j)
				continue
			}

			s, ok := Gstats[Gmodels[i].Servers[j].Addr]
			if ok && s.Stats != nil {
				var redis_keys int64
				db_keys, ok := s.Stats["db0"]
				if ok {
					redis_keys = getServerKeys(db_keys)
				} else {
					log.Debugf("GenGroupServerPoints cant get db0 from %s", Gmodels[i].Servers[j].Addr)
					redis_keys = 0
				}

				used_memory := getServerInt64Field(s.Stats, "used_memory")
				max_memory := getServerInt64Field(s.Stats, "maxmemory")
				total_commands_processed := getServerInt64Field(s.Stats, "total_commands_processed")
				connected_clients := getServerInt64Field(s.Stats, "connected_clients")
				total_connections_received := getServerInt64Field(s.Stats, "total_connections_received")
				instantaneous_ops_per_sec := getServerInt64Field(s.Stats, "instantaneous_ops_per_sec")
				cache_keys := getServerInt64Field(s.Stats, "cache_keys")
				cache_memory := getServerInt64Field(s.Stats, "cache_memory")
				read_cmd_per_sec := getServerInt64Field(s.Stats, "read_cmd_per_sec")
				hits_per_sec := getServerInt64Field(s.Stats, "hits_per_sec")
				cmd_hit_rate := 0.0
				if (read_cmd_per_sec > 0) {
					cmd_hit_rate = float64(hits_per_sec * 100.0) / float64(read_cmd_per_sec)
				}
				//collection master-server stats to topom
				if j == 0 {
					topomStats.Servers.TotalKeys += redis_keys
					topomStats.Servers.UsedMemory += used_memory
					topomStats.Servers.MaxMemory += max_memory
					topomStats.Servers.TotalCommandsProcessed += total_commands_processed
					topomStats.Servers.TotalConnectionsReceived += total_connections_received
					topomStats.Servers.ConnectedClients += connected_clients
					topomStats.Servers.InstantaneousOpsPerSec += instantaneous_ops_per_sec
					topomStats.Servers.CacheMemory += cache_memory
					topomStats.Servers.CacheKeys += cache_keys
					topomStats.Servers.ReadCmdPerSec += read_cmd_per_sec
					topomStats.Servers.HitsPerSec += hits_per_sec
				}

				tags := map[string]string{
					"product_name": 	Tmodel.ProductName,
					"server_addr":   	Gmodels[i].Servers[j].Addr,
				}

				fields := map[string]interface{}{
					"redis_keys":								redis_keys,
					"used_memory":								used_memory,
					"used_ssd":									max_memory,
					"total_commands_processed":					total_commands_processed,
					"connected_clients":						connected_clients,
					"total_connections_received":				total_connections_received,
					"instantaneous_ops_per_sec":				instantaneous_ops_per_sec,
					"cache_keys":								cache_keys,
					"cache_memory":								cache_memory,
					"cache_read_qps":							read_cmd_per_sec,
					"cache_hits_qps":							hits_per_sec,
					"cache_hit_rate":							cmd_hit_rate,
				}

				table := getTableName("server_", Gmodels[i].Servers[j].Addr)
				point, err := client.NewPoint(table, tags, fields, time.Now())
				if err != nil {
					log.WarnErrorf(err, "GenGroupServerPoints NewPoint[%s] error", Gmodels[i].Servers[j].Addr)
					return fmt.Errorf("GenGroupServerPoints NewPoint[%s] error", Gmodels[i].Servers[j].Addr)
				}

				batch.AddPoint(point)
			} else {
				log.Warnf("GenGroupServerPoints get stats[%s] error", Gmodels[i].Servers[j].Addr)
				continue
			}
		}
	}

	return nil
}

/*func (p *Topom) GenGroupCachePoints(stats *Stats, batch client.BatchPoints, topomStats *TopomInfluxdbStats) error{
	Tmodel := p.Model()
	if Tmodel == nil {
		return fmt.Errorf("GenGroupCachePoints model is nil")
	}

	Gmodels := stats.Group.Models
	Gstats := stats.Group.Stats
	Glen := len(Gmodels)

	for i := 0; i < Glen; i++ {
		if Gmodels[i] == nil {
			log.Warnf("GenGroupCachePoints error index[%d] is nil", i)
			continue
		}

		Slen := len(Gmodels[i].Caches)
		for j := 0; j < Slen; j++{
			if Gmodels[i].Caches[j] == nil {
				log.Warnf("GenGroupCachePoints error Gmodels[%d].Caches[%d] is nil", i, j)
				continue
			}

			s, ok := Gstats[Gmodels[i].Caches[j].Addr]
			if ok && s.Stats != nil {
				var redis_keys int64
				db_keys, ok := s.Stats["db0"]
				if ok {
					redis_keys = getServerKeys(db_keys)
				} else {
					log.Debugf("GenGroupCachePoints cant get db0 from %s", Gmodels[i].Caches[j].Addr)
					redis_keys = 0
				}

				used_memory := getServerInt64Field(s.Stats, "used_memory")
				total_commands_processed := getServerInt64Field(s.Stats, "total_commands_processed")
				connected_clients := getServerInt64Field(s.Stats, "connected_clients")
				total_connections_received := getServerInt64Field(s.Stats, "total_connections_received")
				instantaneous_ops_per_sec := getServerInt64Field(s.Stats, "instantaneous_ops_per_sec")
				read_cmd_per_sec := getServerInt64Field(s.Stats, "read_cmd_per_sec")
				read_cmd_hit := getServerInt64Field(s.Stats, "run_in_redis_cmd_per_sec")
				var cmd_hit_rate int64 = 0
				if (read_cmd_per_sec != 0) {
					cmd_hit_rate = (read_cmd_hit * 100) / read_cmd_per_sec
				}

				//collection master-server stats to topom
			
				topomStats.Caches.RedisKeys += redis_keys
				topomStats.Caches.UsedMemory += used_memory
				topomStats.Caches.TotalCommandsProcessed += total_commands_processed
				topomStats.Caches.TotalConnectionsReceived += total_connections_received
				topomStats.Caches.ConnectedClients += connected_clients
				topomStats.Caches.InstantaneousOpsPerSec += instantaneous_ops_per_sec
				topomStats.Caches.ReadCmdPerSec += read_cmd_per_sec
				topomStats.Caches.ReadCmdHit += read_cmd_hit

				tags := map[string]string{
					"product_name": 	Tmodel.ProductName,
					"server_addr":   	Gmodels[i].Caches[j].Addr,
				}

				fields := map[string]interface{}{
					"redis_keys":								redis_keys,
					"cache_used_memory":						used_memory,
					"total_commands_processed":					total_commands_processed,
					"connected_clients":						connected_clients,
					"total_connections_received":				total_connections_received,
					"cache_qps":								instantaneous_ops_per_sec,
					"read_cmd_per_sec":							read_cmd_per_sec,
					"read_cmd_hit":								read_cmd_hit,
					"cmd_hit_rate":								cmd_hit_rate,
				}

				table := getTableName("cache_", Gmodels[i].Caches[j].Addr)
				point, err := client.NewPoint(table, tags, fields, time.Now())
				if err != nil {
					log.WarnErrorf(err, "GenGroupCachePoints NewPoint[%s] error", Gmodels[i].Caches[j].Addr)
					return fmt.Errorf("GenGroupCachePoints NewPoint[%s] error", Gmodels[i].Caches[j].Addr)
				}

				batch.AddPoint(point)
			} else {
				log.Warnf("GenGroupCachePoints get stats[%s] error", Gmodels[i].Caches[j].Addr)
				continue
			}
		}
	}

	return nil
}*/

func (p *Topom) GenProxyCmdInfoPoints(stats *Stats, batch client.BatchPoints, OpStats map[string] *TopomOpStats) error{
	Pmodels := stats.Proxy.Models
	Pstats := stats.Proxy.Stats
	Plen := len(Pmodels)

	for i := 0; i < Plen; i++ {
		if Pmodels[i] == nil {
			log.Warnf("GenProxyCmdInfoPoints error index[%d] is nil", i)
			continue
		}

		p, ok := Pstats[Pmodels[i].Token]
		if ok && p != nil && p.Stats != nil {

			if !p.Stats.Online || p.Stats.Closed {
				log.Infof("GenProxyCmdInfoPoints proxy[%s] is closed or offline", Pmodels[i].ProxyAddr)
				continue
			}
			CmdLen := len(p.Stats.Ops.Cmd)
			for j := 0; j < CmdLen; j++ {
				CmdReponse := p.Stats.Ops.Cmd[j]
				if CmdReponse != nil {
					cmd := CmdReponse.OpStr
					if _, ok := OpStats[cmd]; ok {
						OpStats[cmd].Calls += CmdReponse.Calls
						OpStats[cmd].Usecs += CmdReponse.Usecs
						if (OpStats[cmd].Calls > 0) {
							OpStats[cmd].UsecsPercall = OpStats[cmd].Usecs / OpStats[cmd].Calls
						} else {
							OpStats[cmd].UsecsPercall = 0
						}
						OpStats[cmd].Fails += CmdReponse.Fails
						OpStats[cmd].RedisErrType += CmdReponse.RedisErrType
						OpStats[cmd].TP90 = mergeCmdTP(OpStats[cmd].QPS, OpStats[cmd].TP90, CmdReponse.QPS, CmdReponse.TP90)
						OpStats[cmd].TP99 = mergeCmdTP(OpStats[cmd].QPS, OpStats[cmd].TP99, CmdReponse.QPS, CmdReponse.TP99)
						OpStats[cmd].TP999 = mergeCmdTP(OpStats[cmd].QPS, OpStats[cmd].TP999, CmdReponse.QPS, CmdReponse.TP999)
						OpStats[cmd].TP9999 = mergeCmdTP(OpStats[cmd].QPS, OpStats[cmd].TP9999, CmdReponse.QPS, CmdReponse.TP9999)
						OpStats[cmd].TP100 = maxCmdTP(OpStats[cmd].TP100, CmdReponse.TP100)
						OpStats[cmd].AVG = mergeCmdTP(OpStats[cmd].QPS, OpStats[cmd].AVG, CmdReponse.QPS, CmdReponse.AVG)
						OpStats[cmd].QPS += CmdReponse.QPS
					} else {
						//insert new cmd info to OpStats
						OpStats[cmd] = &TopomOpStats{}
						OpStats[cmd].Calls = CmdReponse.Calls
						OpStats[cmd].Usecs = CmdReponse.Usecs
						OpStats[cmd].UsecsPercall = CmdReponse.UsecsPercall
						OpStats[cmd].Fails = CmdReponse.Fails
						OpStats[cmd].RedisErrType = CmdReponse.RedisErrType
						OpStats[cmd].TP90 = float64(CmdReponse.TP90)
						OpStats[cmd].TP99 = float64(CmdReponse.TP99)
						OpStats[cmd].TP999 = float64(CmdReponse.TP999)
						OpStats[cmd].TP9999 = float64(CmdReponse.TP9999)
						OpStats[cmd].TP100 = CmdReponse.TP100
						OpStats[cmd].AVG = float64(CmdReponse.AVG)
						OpStats[cmd].QPS = CmdReponse.QPS
					}

					tags := map[string]string{
						"admin_addr":   Pmodels[i].AdminAddr,
						"cmd_name":   CmdReponse.OpStr,
					}

					fields := map[string]interface{}{
						"calls":		   CmdReponse.Calls,
						"Usecs":		   CmdReponse.Usecs,
						"UsecsPercall":	   CmdReponse.UsecsPercall,
						"fails":           CmdReponse.Fails,
						"redis_errtype":   CmdReponse.RedisErrType,
						"qps":         	   CmdReponse.QPS,
						"tp90":            CmdReponse.TP90,
						"tp99":            CmdReponse.TP99,
						"tp999":           CmdReponse.TP999,
						"tp9999":          CmdReponse.TP9999,
						"tp100":           CmdReponse.TP100,
						"avg":             CmdReponse.AVG,
					}

					//table_prefix := "proxy_"
					table := getTableName("proxy_", Pmodels[i].ProxyAddr) + "_cmd_info"
					//table = getTableName(table, "_cmd_info")
					point, err := client.NewPoint(table, tags, fields, time.Now())
					if err != nil {
						log.WarnErrorf(err, "GenProxyCmdInfoPoints NewPoint Proxy[%s] Cmd[%s] error", Pmodels[i].ProxyAddr, CmdReponse.OpStr)
						return fmt.Errorf("GenProxyCmdInfoPoints NewPoint Proxy[%s] Cmd[%s] error", Pmodels[i].ProxyAddr, CmdReponse.OpStr)
					}

					batch.AddPoint(point)
				}
			}
		} else {
			log.Warnf("GenProxyCmdInfoPoints get stats[%s] error", Pmodels[i].Token)
			continue
		}
	}

	return nil
}

func getServerKeys(dbkeys string) int64{
	var keys int64 = 0

	//db0:keys=1,expires=0,avg_ttl=0
	fields := strings.Split(dbkeys, ",")
	flen := len(fields)

	for i := 0; i < flen; i++ {
		if strings.Index(fields[i], "keys=") < 0 {
			continue
		}

		num, err := strconv.ParseInt(string([]byte(fields[i])[5:]), 10, 64)
		if err == nil{
			keys = num
		}

		break
	}

	return keys
}

func getServerInt64Field(info map[string]string, field string) int64{
	s, ok := info[field]
	if !ok {
		return 0
	}

	num, err := strconv.ParseInt(s, 10, 64)
	if err != nil{
		return 0
	}

	return num
}

func getTableName(prefix, addr string) string{
	var table string

	addr = strings.Replace(addr, ".", "_", -1)
	addr = strings.Replace(addr, ":", "_", -1)
	table = prefix + addr

	return table
}

func (p *Topom) QueryInfluxdb(sql string) (*client.Response, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	server := p.config.MetricsReportInfluxdbServer
	database := p.config.MetricsReportInfluxdbDatabase
	if server == "" {
		return nil, fmt.Errorf("influxdb server is nil")
	}

	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     server,
		Username: p.config.MetricsReportInfluxdbUsername,
		Password: p.config.MetricsReportInfluxdbPassword,
		Timeout:  time.Second * 5,
	})
	if err != nil {
		log.WarnErrorf(err, "QueryInfluxdb NewHTTPClient error")
		return nil, err
	}

	defer c.Close()
	//var now = time.Now().Unix()
	//q := client.NewQuery("SELECT "+qps+" FROM codis_dashboard where time > now() - "+start_time+"000s and time < now() - "+end_time+"000s", "test", "ms")
	return c.Query(client.NewQuery(sql, database, "ms"))
}
 
func mergeCmdTP (current_qps int64, current_tp float64, cmd_qps, cmd_tp int64) float64 {
	qps_all := current_qps + cmd_qps
	if (qps_all == 0) {
		return 0
	} else {
		return (float64(current_qps) * current_tp + float64(cmd_qps * cmd_tp)) / float64(qps_all)
	}
}

func maxCmdTP (a, b int64) int64 {
	if (a < b) {
		return b
	}
	return a
}

/*func floatCeil (a float64) int64 {
	if (0 == int64(a * 10) % 10) {
		return int64(a)
	} else {
		return int64(a + 1)
	}
}*/
