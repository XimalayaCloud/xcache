// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"time"
	"strings"

	"github.com/docopt/docopt-go"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/topom"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

func killProcess(pidfile string) error {
	var ErrNoSuchProcess = "no such process"

	pf, err := os.Open(pidfile)
	if err != nil {
		return err
	}
	defer pf.Close()

	pd, err := ioutil.ReadAll(pf)
	if err != nil {
		return err
	}

	pid, err := strconv.Atoi( strings.TrimSpace(string(pd)) )
	if err := syscall.Kill(pid, 0); err == nil {
		log.Warnf("fe[%d] is running, prepare to stop it.", pid)
		//kill
		if err := syscall.Kill(pid, syscall.SIGTERM); err == nil {

			//等待进程退出
			count := 0
			for count < 30 {
				if err := syscall.Kill(pid, 0); err == nil {
					time.Sleep(100 * time.Millisecond)
					count++
					continue
				} 
				break
			}
			if err := syscall.Kill(pid, 0); err.Error() != ErrNoSuchProcess { 
				log.Warnf("stop fe[%d] failed, error : %v.", pid, err)
				return fmt.Errorf("stop fe[%d] failed, error : %v.", pid, err)
			}
			log.Warnf("stop fe[%d] success.", pid)
			return nil
		} else {
			log.Warnf("stop fe[%d] failed, error : %v.", pid, err)
			return err
		}
	} else if err.Error() == ErrNoSuchProcess {
		//进程不存在删除pid文件
		log.Warnf("fe is not exist, prepare to remove pidfile.")
		if err := os.Remove(pidfile); err != nil {
			log.WarnErrorf(err, "remove pidfile = '%s' failed", pidfile)
			return err
		}
		log.Warnf("remove pidfile[%s] success.", pidfile)
		return nil
	} else {
		return err
	}

	return nil
}

func main() {
	const usage = `
Usage:
	codis-dashboard [--ncpu=N] [--config=CONF] [--log=FILE] [--log-level=LEVEL] [--host-admin=ADDR] [--pidfile=FILE] [--zookeeper=ADDR|--etcd=ADDR|--filesystem=ROOT] [--product_name=NAME] [--product_auth=AUTH] [--remove-lock]
	codis-dashboard  --default-config
	codis-dashboard  -c CONF [-s (start|stop|restart)]
	codis-dashboard  --version

Options:
	--ncpu=N                    set runtime.GOMAXPROCS to N, default is runtime.NumCPU().
	-c CONF, --config=CONF      run with the specific configuration.
	-l FILE, --log=FILE         set path/name of daliy rotated log file.
	--log-level=LEVEL           set the log-level, should be INFO,WARN,DEBUG or ERROR, default is INFO.
`

	d, err := docopt.Parse(usage, nil, true, "", false)
	if err != nil {
		log.PanicError(err, "parse arguments failed")
	}

	switch {

	case d["--default-config"]:
		fmt.Println(topom.DefaultConfig)
		return

	case d["--version"].(bool):
		fmt.Println("version:", utils.Version)
		fmt.Println("compile:", utils.Compile)
		return

	}

	config := topom.NewDefaultConfig()
	if s, ok := utils.Argument(d, "--config"); ok {
		if err := config.LoadFromFile(s); err != nil {
			log.PanicErrorf(err, "load config %s failed", s)
		}
		log.Warnf("load config success")
		config.ConfigName = s
	}

	if s, ok := utils.Argument(d, "--log"); ok {
		config.Log = s
	}
	w, err := log.NewRollingFile(config.Log, log.DailyRolling)
	if err != nil {
		log.PanicErrorf(err, "open log file %s failed", config.Log)
	} else {
		log.StdLog = log.New(w, "")
	}
	log.SetLevel(log.LevelInfo)

	if s, ok := utils.Argument(d, "--log-level"); ok {
		config.LogLevel = s
	}
	if !log.SetLevelString(config.LogLevel) {
		log.Panicf("option --log-level = %s", config.LogLevel)
	}

	if n, ok := utils.ArgumentInteger(d, "--ncpu"); ok {
		config.Ncpu = n
		//runtime.GOMAXPROCS(n)
	}
	if (config.Ncpu >= 1) {
		runtime.GOMAXPROCS(config.Ncpu)
	} else {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}
	log.Warnf("set ncpu = %d", runtime.GOMAXPROCS(0))

	/*config := topom.NewDefaultConfig()
	if s, ok := utils.Argument(d, "--config"); ok {
		if err := config.LoadFromFile(s); err != nil {
			log.PanicErrorf(err, "load config %s failed", s)
		}
		config.ConfigName = s
	}*/
	if s, ok := utils.Argument(d, "--host-admin"); ok {
		config.HostAdmin = s
		log.Warnf("option --host-admin = %s", s)
	}

	switch {
	case d["--zookeeper"] != nil:
		config.CoordinatorName = "zookeeper"
		config.CoordinatorAddr = utils.ArgumentMust(d, "--zookeeper")
		log.Warnf("option --zookeeper = %s", config.CoordinatorAddr)

	case d["--etcd"] != nil:
		config.CoordinatorName = "etcd"
		config.CoordinatorAddr = utils.ArgumentMust(d, "--etcd")
		log.Warnf("option --etcd = %s", config.CoordinatorAddr)

	case d["--filesystem"] != nil:
		config.CoordinatorName = "filesystem"
		config.CoordinatorAddr = utils.ArgumentMust(d, "--filesystem")
		log.Warnf("option --filesystem = %s", config.CoordinatorAddr)

	}

	if s, ok := utils.Argument(d, "--product_name"); ok {
		config.ProductName = s
		log.Warnf("option --product_name = %s", s)
	}
	if s, ok := utils.Argument(d, "--product_auth"); ok {
		config.ProductAuth = s
		log.Warnf("option --product_auth = %s", s)
	}

	client, err := models.NewClient(config.CoordinatorName, config.CoordinatorAddr, config.CoordinatorAuth, time.Minute)
	if err != nil {
		log.PanicErrorf(err, "create '%s' client to '%s' failed", config.CoordinatorName, config.CoordinatorAddr)
	}
	defer client.Close()

	if d["--remove-lock"].(bool) {
		store := models.NewStore(client, config.ProductName)
		defer store.Close()

		log.Warnf("force remove-lock")
		if err := store.Release(); err != nil {
			log.WarnErrorf(err, "force remove-lock failed")
		} else {
			log.Warnf("force remove-lock OK")
		}
	}

	binpath, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.PanicErrorf(err, "get path of binary failed")
	}
	if config.PidFile == "" {
		log.Panicf("pidfile is nil")
	}

	pidfile := filepath.Join(binpath, config.PidFile)
	log.Warnf("pidfile is %s.", pidfile)
	if d["-s"].(bool) {
		if d["stop"].(bool) {
			if err := killProcess(pidfile); err != nil {
				log.WarnErrorf(err, "stop dashboard failed.")
			} else {
				log.Warnf("stop dashboard success.")
			}

			return
		}
		if d["restart"].(bool) {
			if err := killProcess(pidfile); err != nil {
				log.WarnErrorf(err, "stop dashboard failed.")
				if _, err := os.Stat(pidfile); err == nil || os.IsExist(err) {
					log.Warnf("pidfile[%s] is exist, but stop dashboard failed.", pidfile)
					return
				}

				//if pidfile is not exist, do nothing
			} else {
				log.Warnf("stop dashboard success, prepare to start dashboard.")
			}
		}

		if d["start"].(bool) {
			//do nothing
		}
	}

	s, err := topom.New(client, config)
	if err != nil {
		log.PanicErrorf(err, "create topom with config file failed\n%s", config)
	}
	defer s.Close()

	log.Warnf("create topom with config\n%s", config)

	if s, ok := utils.Argument(d, "--pidfile"); ok {
		config.PidFile = s
	}
	if pidfile, err := filepath.Abs(config.PidFile); err != nil {
		log.WarnErrorf(err, "parse pidfile = '%s' failed", config.PidFile)
	} else if err := ioutil.WriteFile(pidfile, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
		log.WarnErrorf(err, "write pidfile = '%s' failed", pidfile)
	} else {
		defer func() {
			if err := os.Remove(pidfile); err != nil {
				log.WarnErrorf(err, "remove pidfile = '%s' failed", pidfile)
			}
		}()
		log.Warnf("option --pidfile = %s", pidfile)
	}

	go func() {
		defer s.Close()
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)

		sig := <-c
		log.Warnf("[%p] dashboard receive signal = '%v'", s, sig)
	}()

	for i := 0; !s.IsClosed() && !s.IsOnline(); i++ {
		if err := s.Start(true); err != nil {
			if i <= 15 {
				log.Warnf("[%p] dashboard online failed [%d]", s, i)
			} else {
				log.Panicf("dashboard online failed, give up & abort :'(")
			}
			time.Sleep(time.Second * 2)
		}
	}

	log.Warnf("[%p] dashboard is working ...", s)

	for !s.IsClosed() {
		time.Sleep(time.Second)
	}

	log.Warnf("[%p] dashboard is exiting ...", s)
}
