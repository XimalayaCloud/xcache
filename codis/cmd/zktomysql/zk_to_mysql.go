// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"runtime"
	"time"

	"github.com/docopt/docopt-go"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/topom"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/log"
)


func main() {
	const usage = `
Usage:
	czk-to-mysql  -c CONF

Options:
	-c CONF, --config=CONF      run with the specific configuration.
`

	d, err := docopt.Parse(usage, nil, true, "", false)
	if err != nil {
		log.PanicError(err, "parse arguments failed")
	}

	config := topom.NewDefaultConfig()
	if s, ok := utils.Argument(d, "--config"); ok {
		if err := config.LoadFromFile(s); err != nil {
			log.PanicErrorf(err, "load config %s failed", s)
		}
		log.Warnf("load config success")
		config.ConfigName = s
	}

	w, err := log.NewRollingFile(config.Log, log.DailyRolling)
	if err != nil {
		log.PanicErrorf(err, "open log file %s failed", config.Log)
	} else {
		log.StdLog = log.New(w, "")
	}
	log.SetLevel(log.LevelInfo)

	
	if !log.SetLevelString(config.LogLevel) {
		log.Panicf("option --log-level = %s", config.LogLevel)
	}

	if (config.Ncpu >= 1) {
		runtime.GOMAXPROCS(config.Ncpu)
	} else {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}
	log.Warnf("set ncpu = %d", runtime.GOMAXPROCS(0))

	//需要两个客户端: zk客户端 mysql客户端
	zk_client, err := NewZkClient(config)
	if err != nil {
		log.PanicErrorf(err, "create ZK client to '%s' failed", config.CoordinatorAddr)
	}
	defer zk_client.Close()

	sql_client, err := NewSqlClient(config)
	if err != nil {
		log.PanicErrorf(err, "create mysql client to '%s' failed", config.MysqlAddr)
	}
	defer sql_client.Close()
	log.Warnf("create zk_client and sql_client success")

	s, err := topom.NewZkToMysql(zk_client, config)
	if err != nil {
		log.PanicErrorf(err, "create topom with config file failed\n%s", config)
	}
	

	//从zk中获取集群节点信息
	if err := s.RefillCache(); err != nil {
		log.PanicError(err, "get codis info from zk faild")
	}
	log.Warnf("load node from zk success")

	//将集群节点信息存储到mysql中
	sql_store := models.NewStore(sql_client, config.ProductName)
	if err := s.ZkToMysql(sql_store); err != nil {
		log.PanicError(err, "zk to mysql faild!")
	}
	

	log.Warnf("zk to mysql success ...")
}

func NewSqlClient(config *topom.Config) (models.Client, error) {
	return models.NewSqlClient(config.MysqlAddr, config.MysqlUsername, config.MysqlPassword, config.MysqlDatabase)
}

func NewZkClient(config *topom.Config) (models.Client, error) {
	return models.NewClient(config.CoordinatorName, config.CoordinatorAddr, config.CoordinatorAuth, time.Minute)
}