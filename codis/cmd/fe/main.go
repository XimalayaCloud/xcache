// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"
	"database/sql"
	"strings"
	"crypto/md5"
	"encoding/hex"
	"syscall"

	"github.com/docopt/docopt-go"
	"github.com/go-martini/martini"
	"github.com/martini-contrib/render"

	"github.com/CodisLabs/codis/pkg/fe"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/rpc"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"

	"github.com/coopernurse/gorp"
	"github.com/martini-contrib/binding"
	"github.com/martini-contrib/sessionauth"
	"github.com/martini-contrib/sessions"
	_ "github.com/go-sql-driver/mysql"
)

var roundTripper http.RoundTripper
var dbmap *gorp.DbMap
var md5Salt = "ximalaya_salt_cnp553w69u87ju3b"

type UserModel struct {
	Id            int64  `form:"id" db:"id"`
	Username      string `form:"name" db:"username"`
	Password      string `form:"password" db:"password"`
	UserType      int64  `form:"usertype" db:"usertype"`
	Comment       string `form:"comment" db:"comment"`
	authenticated bool   `form:"-" db:"-"`
}

type UserCodis struct {
	Id            int64  `form:"id" db:"id"`
	UserId        int64  `form:"userid" db:"userid"`
	CodisId       int64  `form:"codisid" db:"codisid"`
	Iswrite       bool   `form:"iswrite" db:"iswrite"`
}

type CodisModel struct {
	Id             int64  `form:"id" db:"id"`
	CodisName      string `form:"name" db:"codisname"`
	Dashboard      string `form:"dashboard" db:"dashboard"`
	Comment        string `form:"comment" db:"comment"`
}

func GenerateAnonymousUser() sessionauth.User {
	return &UserModel{}
}

func (u *UserModel) Login() {
	u.authenticated = true
}

func (u *UserModel) Logout() {
	u.authenticated = false
}

func (u *UserModel) IsAuthenticated() bool {
	return u.authenticated
}

func (u *UserModel) UniqueId() interface{} {
	return u.Id
}

func (u *UserModel) GetById(id interface{}) error {
	err := dbmap.SelectOne(u, "SELECT * FROM users WHERE id = ?", id)
	if err != nil {
		return err
	}

	return nil
}

func initDb(addr string, user string , pswd string , database string) (*gorp.DbMap, error) {
	if (addr == "" || user == "" || pswd == "" || database == "") {
		return nil, errors.New("invalid params")
	}

	var netAddr = fmt.Sprintf("%s(%s)", "tcp", addr)
	var dsn = fmt.Sprintf("%s:%s@%s/%s", user, pswd, netAddr, database)
	//db, err := sql.Open("mysql", user + ":" + pswd + "@tcp(" + addr + ")/" + database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.ErrorErrorf(err, "faile to open database")
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		log.ErrorErrorf(err, "faile to connect database")
		return nil, err
	}

	//create tables to store user information
	dbmap := &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{}}
	return dbmap, nil
}

func init() {
	var dials atomic2.Int64
	tr := &http.Transport{}
	tr.Dial = func(network, addr string) (net.Conn, error) {
		c, err := net.DialTimeout(network, addr, time.Second*10)
		if err == nil {
			log.Debugf("rpc: dial new connection to [%d] %s - %s",
				dials.Incr()-1, network, addr)
		}
		return c, err
	}
	go func() {
		for {
			time.Sleep(time.Minute)
			tr.CloseIdleConnections()
		}
	}()
	roundTripper = tr
}

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
	codis-fe --config=CONF --listen=ADDR [--ncpu=N] [--log=FILE] [--log-level=LEVEL] [--assets-dir=PATH] [--pidfile=FILE] (--dashboard-list=FILE|--zookeeper=ADDR [--zookeeper-auth=USR:PWD]|--etcd=ADDR [--etcd-auth=USR:PWD]|--filesystem=ROOT)
	codis-fe --config=CONF --listen=ADDR [--ncpu=N] [--log=FILE] [--log-level=LEVEL] [--assets-dir=PATH] [--pidfile=FILE]
	codis-fe -c CONF [-s (start|stop|restart)]
	codis-fe --version
	
Options:
	--ncpu=N                        set runtime.GOMAXPROCS to N, default is runtime.NumCPU().
	-c CONF, --config=CONF          run with the specific configuration.
	-d FILE, --dashboard-list=FILE  set list of dashboard, can be generated by codis-admin.
	-l FILE, --log=FILE             set path/name of daliy rotated log file.
	--log-level=LEVEL               set the log-level, should be INFO,WARN,DEBUG or ERROR, default is INFO.
	--listen=ADDR                   set the listen address.
`
	d, err := docopt.Parse(usage, nil, true, "", false)
	if err != nil {
		log.PanicError(err, "parse arguments failed")
	}

	if d["--version"].(bool) {
		fmt.Println("version:", utils.Version)
		fmt.Println("compile:", utils.Compile)
		return
	}

	config := fe.NewDefaultConfig()
	if s, ok := utils.Argument(d, "--config"); ok {
		if err := config.LoadFromFile(s); err != nil {
			log.PanicErrorf(err, "load config %s failed", s)
		}
	} else {
		log.Panicf("fe start failed: must use config file")
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
				log.WarnErrorf(err, "stop fe failed.")
			} else {
				log.Warnf("stop fe success.")
			}

			return
		}

		if d["restart"].(bool) {
			if err := killProcess(pidfile); err != nil {
				log.WarnErrorf(err, "stop fe failed.")
				if _, err := os.Stat(pidfile); err == nil || os.IsExist(err) {
					log.Warnf("pidfile[%s] is exist, but stop fe failed.", pidfile)
					return
				}

				//if pidfile is not exist, do nothing
			} else {
				log.Warnf("stop fe success, prepare to start fe.")
			}
		}

		if d["start"].(bool) {
			//do nothing
		}
	}
	//判断pid文件是否已经存在，如果存在则启动失败
	if _, err := os.Stat(pidfile); err == nil || os.IsExist(err) {
		log.Panicf("parse pidfile = %s has exist.", pidfile)
	}

	if n, ok := utils.ArgumentInteger(d, "--ncpu"); ok {
		config.Ncpu = n
	} 
	if (config.Ncpu >= 1) {
		runtime.GOMAXPROCS(config.Ncpu)
	} else {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}
	log.Warnf("set ncpu = %d", runtime.GOMAXPROCS(0))

	listen, ok := utils.Argument(d, "--listen")
	if !ok {
		listen = config.Listen
		if listen == "" {
			log.Panicf("listen %s is null", listen)
		}
	}
	log.Warnf("set listen = %s", listen)

	var assets string
	if s, ok := utils.Argument(d, "--assets-dir"); ok {
		abspath, err := filepath.Abs(s)
		if err != nil {
			log.PanicErrorf(err, "get absolute path of %s failed", s)
		}
		assets = abspath
	} else {
		binpath, err := filepath.Abs(filepath.Dir(os.Args[0]))
		if err != nil {
			log.PanicErrorf(err, "get path of binary failed")
		}
		assets = filepath.Join(binpath, "assets")
	}
	log.Warnf("set assets = %s", assets)

	indexFile := filepath.Join(assets, "index.tmpl")
	if _, err := os.Stat(indexFile); err != nil {
		log.PanicErrorf(err, "get stat of %s failed", indexFile)
	}

	var loader ConfigLoader
	if d["--dashboard-list"] != nil {
		file := utils.ArgumentMust(d, "--dashboard-list")
		loader = &StaticLoader{file}
		log.Warnf("set --dashboard-list = %s", file)
	} else {
		loader = &StaticLoader{config.DashboardList}
		if (config.DashboardList != "") {
			log.Warnf("set --dashboard-list = %s", config.DashboardList)
		} else {
			log.Warnf("get dashboard_list from MySql")
		}
		/*var coordinator struct {
			name string
			addr string
			auth string
		}

		switch {
		case d["--zookeeper"] != nil:
			coordinator.name = "zookeeper"
			coordinator.addr = utils.ArgumentMust(d, "--zookeeper")
			if d["--zookeeper-auth"] != nil {
				coordinator.auth = utils.ArgumentMust(d, "--zookeeper-auth")
			}

		case d["--etcd"] != nil:
			coordinator.name = "etcd"
			coordinator.addr = utils.ArgumentMust(d, "--etcd")
			if d["--etcd-auth"] != nil {
				coordinator.auth = utils.ArgumentMust(d, "--etcd-auth")
			}

		case d["--filesystem"] != nil:
			coordinator.name = "filesystem"
			coordinator.addr = utils.ArgumentMust(d, "--filesystem")

		default:
			log.Panicf("invalid coordinator")
		}

		log.Warnf("set --%s = %s", coordinator.name, coordinator.addr)

		c, err := models.NewClient(coordinator.name, coordinator.addr, coordinator.auth, time.Minute)
		if err != nil {
			log.PanicErrorf(err, "create '%s' client to '%s' failed", coordinator.name, coordinator.addr)
		}
		defer c.Close()

		loader = &DynamicLoader{c}*/
	}

	router := NewReverseProxy(loader)

	store := sessions.NewCookieStore([]byte("codis_secret"))
	store.Options(sessions.Options{
		MaxAge: 0,
	})

	utils.DeployInit(assets)
	dbmap, err = initDb(config.MysqlAddr, config.MysqlUsername, config.MysqlPassword, config.MysqlDatabase)
	if err != nil {
		log.PanicErrorf(err, "open mysql db failed")
	}
	defer dbmap.Db.Close()
	renderop := render.Options{Directory: assets, Extensions: []string{".tmpl", ".html"},}

	martini.Env = martini.Prod
	m := martini.New()
	m.Use(martini.Recovery())
	m.Use(render.Renderer(renderop))
	m.Use(martini.Static(assets, martini.StaticOptions{SkipLogging: true}))
	m.Use(sessions.Sessions("codis_login_" + strings.Split(listen, ":")[1], store))
	m.Use(sessionauth.SessionUser(GenerateAnonymousUser))

	sessionauth.RedirectUrl = "/login"
	sessionauth.RedirectParam = "new-next"

	r := martini.NewRouter()
	r.Get("/list", sessionauth.LoginRequired, func(user sessionauth.User) (int, string) {
		loginuser := &UserModel{}
		err := loginuser.GetById(user.UniqueId())
		if err != nil {
			return rpc.ApiResponseError(err)
		}
		codiss := router.GetNames(dbmap, loginuser.Username) 
		sort.Sort(sort.StringSlice(codiss))
		return rpc.ApiResponseJson(codiss)
	})

	r.Get("/", func(r render.Render) {
		r.Redirect("index")
	})

	r.Get("/admin", sessionauth.LoginRequired, func(r render.Render, user sessionauth.User, req *http.Request) {
		loginuser := &UserModel{}
		err := loginuser.GetById(user.UniqueId())
		if (err != nil || loginuser.UserType != 1) {
			params := req.URL.Query()
			r.Redirect(params.Get(sessionauth.RedirectParam))
		}
	}, func(r render.Render) {
		r.HTML(200, "admin", nil)
	})

	r.Get("/auth/:codis", sessionauth.LoginRequired, func(user sessionauth.User, params martini.Params) (int, string) {
		userid := user.UniqueId()
		codis := params["codis"]
		auth := false
		err := dbmap.SelectOne(&auth, "SELECT iswrite FROM user_codis, users, codis WHERE users.id = ? and users.id = user_codis.userid and codis.codisname = ? and user_codis.codisid = codis.id", userid, codis)
		if err != nil {
			auth = false
		}

		return rpc.ApiResponseJson(auth)
	})

	r.Get("/isadmin", sessionauth.LoginRequired, func(user sessionauth.User) (int, string) {
		userid := user.UniqueId()
		isadmin := false
		usertype := 0
		err := dbmap.SelectOne(&usertype, "SELECT usertype FROM users WHERE id = ?", userid)
		if err != nil {
			usertype = 0
		}

		if usertype == 1 {
			isadmin = true
		}

		return rpc.ApiResponseJson(isadmin)
	})

	r.Get("/username", sessionauth.LoginRequired, func(user sessionauth.User) (int, string) {
		loginuser := &UserModel{}
		err := loginuser.GetById(user.UniqueId())
		if err != nil {
			return rpc.ApiResponseError(err)
		}
		return rpc.ApiResponseJson(loginuser.Username)
	})

	r.Get("/sql/:sql", sessionauth.LoginRequired, func(r render.Render, user sessionauth.User, req *http.Request) {
		loginuser := &UserModel{}
		err := loginuser.GetById(user.UniqueId())
		if (err != nil || loginuser.UserType != 1) {
			r.Redirect(sessionauth.RedirectUrl)
		}
	}, SqlQuery)

	r.Post("/add/user", sessionauth.LoginRequired, binding.Bind(UserModel{}), func(postUser UserModel, r render.Render, user sessionauth.User, req *http.Request) (int, string) {
		loginuser := &UserModel{}
		err := loginuser.GetById(user.UniqueId())
		if (err != nil || loginuser.UserType != 1) {
			r.Redirect(sessionauth.RedirectUrl)
		}

		if ok, info := IsValidUser(postUser); !ok {
			return 800, info
		}

		md5_pswd := md5.New()
		md5_pswd.Write([]byte(postUser.Password))
		md5_pswd.Write([]byte(string(md5Salt)))
		password := hex.EncodeToString(md5_pswd.Sum(nil))

		sql := "insert into users(username,password,usertype,comment) values('"
		sql = sql + postUser.Username + "','" 
		sql = sql + password + "'," 
		sql = sql + strconv.FormatInt(postUser.UserType,10) + ",'"
		sql = sql + postUser.Comment + "')"
		return ExecSql(sql)
	})

	r.Post("/update/user", sessionauth.LoginRequired, binding.Bind(UserModel{}), func(postUser UserModel, r render.Render, user sessionauth.User, req *http.Request) (int, string) {
		loginuser := &UserModel{}
		err := loginuser.GetById(user.UniqueId())
		if (err != nil || loginuser.UserType != 1) {
			r.Redirect(sessionauth.RedirectUrl)
		}

		if ok, info := IsValidUser(postUser); !ok {
			return 800, info
		}

		md5_pswd := md5.New()
		md5_pswd.Write([]byte(postUser.Password))
		md5_pswd.Write([]byte(string(md5Salt)))
		password := hex.EncodeToString(md5_pswd.Sum(nil))

		sql := "update users set username='" + postUser.Username
		if (postUser.Password != "") {
			sql = sql + "', password='" + password
		}
		sql = sql + "', usertype=" +  strconv.FormatInt(postUser.UserType,10)
		sql = sql + ", comment='" +  postUser.Comment
		sql = sql + "' where id=" + strconv.FormatInt(postUser.Id,10)
		return ExecSql(sql)
	})

	r.Get("/login",func(r render.Render){
		r.HTML(200,"login",nil)
	})

	r.Post("/login", binding.Bind(UserModel{}), func(session sessions.Session, postUser UserModel, r render.Render, req *http.Request) (int, string) {
		// You should verify credentials against a database or some other mechanism at this point.
		// Then you can authenticate this session.
		user := UserModel{}
		md5_pswd := md5.New()
		md5_pswd.Write([]byte(postUser.Password))
		md5_pswd.Write([]byte(string(md5Salt)))
		password := hex.EncodeToString(md5_pswd.Sum(nil))
		err := dbmap.SelectOne(&user, "SELECT * FROM users WHERE username = ? and password = ?", postUser.Username, password)
		if err != nil {
			return rpc.ApiResponseJson(false)
		} else {
			err := sessionauth.AuthenticateSession(session, &user)
			if err != nil {
				return rpc.ApiResponseError(err)
			}
			return rpc.ApiResponseJson(true)
		}
	})

	r.Get("/index", sessionauth.LoginRequired, func(r render.Render, user sessionauth.User) {
		r.HTML(200, "index", nil)
	})

	r.Get("/logout",sessionauth.LoginRequired,func(session sessions.Session,user sessionauth.User,r render.Render){
		sessionauth.Logout(session, user)
		r.Redirect("/")
	})

	r.Get("/serverInfo",sessionauth.LoginRequired,func(r render.Render){
		r.HTML(200, "serverInfo", nil)
	})

	r.Get("/proxyInfo",sessionauth.LoginRequired,func(r render.Render){
		r.HTML(200, "proxyInfo", nil)
	})

	r.Group("/deploy", func(r martini.Router) {
		r.Group("/dashboard", func(r martini.Router) {
			r.Post("/create", sessionauth.LoginRequired, binding.Bind(utils.DashBoardConf{}), utils.CreateDashboard)
			r.Get("/remote/:addr", sessionauth.LoginRequired, utils.GetDashboardConf)
			r.Get("/dashboard.toml", sessionauth.LoginRequired, utils.GetDashboardDefaultConf)
		})
		r.Group("/proxy", func(r martini.Router) {
			r.Post("/create", sessionauth.LoginRequired, binding.Bind(utils.ProxyConf{}), utils.CreateProxy)
			r.Get("/remote/:addr", sessionauth.LoginRequired, utils.GetProxyConf)
			r.Get("/proxy.toml", sessionauth.LoginRequired, utils.GetProxyDefaultConf)
		})
		r.Group("/pika", func(r martini.Router) {
			r.Post("/create", sessionauth.LoginRequired, binding.Bind(utils.PikaConf{}), utils.CreatePika)
			r.Get("/remote/:addr", sessionauth.LoginRequired, utils.GetPikaConf)
			r.Get("/pika.conf", sessionauth.LoginRequired, utils.GetPikaDefaultConf)
			r.Put("/destroy/:addr",sessionauth.LoginRequired, utils.DestroyPika)
		})
		r.Group("/redis", func(r martini.Router) {
			r.Post("/create", sessionauth.LoginRequired, binding.Bind(utils.RedisConf{}), utils.CreateRedis)
			r.Get("/remote/:addr", sessionauth.LoginRequired, utils.GetRedisConf)
			r.Get("/redis.conf", sessionauth.LoginRequired, utils.GetRedisDefaultConf)
			r.Put("/destroy/:addr",sessionauth.LoginRequired, utils.DestroyRedis)
		})
	})

	r.Any("/**", sessionauth.LoginRequired, func(w http.ResponseWriter, req *http.Request) {
		name := req.URL.Query().Get("forward")
		if p := router.GetProxy(name); p != nil {
			p.ServeHTTP(w, req)
		} else {
			w.WriteHeader(http.StatusForbidden)
		}
	})

	m.MapTo(r, (*martini.Routes)(nil))
	m.Action(r.Handle)

	l, err := net.Listen("tcp", listen)
	if err != nil {
		log.PanicErrorf(err, "listen %s failed", listen)
	}
	defer l.Close()

	if pidfile != "" {
		if err := ioutil.WriteFile(pidfile, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
			log.WarnErrorf(err, "write pidfile = '%s' failed", pidfile)
		} else {
			defer func() {
				if err := os.Remove(pidfile); err != nil {
					log.WarnErrorf(err, "remove pidfile = '%s' failed", pidfile)
				}
			}()
			log.Warnf("option --pidfile = %s", pidfile)
		}
	}

	h := http.NewServeMux()
	h.Handle("/", m)
	hs := &http.Server{Handler: h}
	defer hs.Close()
	go func() {
		if err := hs.Serve(l); err != nil {
			log.PanicErrorf(err, "serve %s failed", listen)
		}
	}()

	go func() {
		for {
			AutoPurgeLog(config.Log, config.ExpireLogDays)
			time.Sleep(time.Second * 60 * 60 * 24)
		}
	}()

	log.Warnf("fe is working ...")

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)
	sig := <-c
	log.Warnf("fe receive signal = '%v'", sig)

	log.Warnf("fe is exiting ...")
}

type ConfigLoader interface {
	Reload(*gorp.DbMap, string) (map[string]string, error)
}

type StaticLoader struct {
	path string
}


func (l *StaticLoader) Reload(dbmap *gorp.DbMap, username string) (map[string]string, error) {
	var m = make(map[string]string)
	//将codis.json中新增的集群添加到mysql中
	if (l.path != "") {
		var list []*struct {
			Name      string `json:"name"`
			Dashboard string `json:"dashboard"`
		}

		b, err := ioutil.ReadFile(l.path)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if err := json.Unmarshal(b, &list); err != nil {
			return nil, errors.Trace(err)
		}
	
		sql := "select codisname, dashboard from codis"
		rows, err := dbmap.Db.Query(sql)
		if err != nil {
			return nil, errors.Trace(err)
		}
		defer rows.Close()
		for rows.Next() {
			var codis_name string
			var codis_dashboard string
			err := rows.Scan(&codis_name, &codis_dashboard)
			if err != nil {
				log.WarnErrorf(err, "rows.Scan() failed: %s", sql)
				return nil, errors.Trace(err)
			}
			m[codis_name] = codis_dashboard
		}

		for _, e := range list {
			if m[e.Name] == "" {
				//add codis
				sql := "insert into codis(codisname, dashboard) values('" + e.Name + "', '" + e.Dashboard + "')"
				affect, err := sqlExec(dbmap, sql)
				if err != nil {
					log.WarnErrorf(err,"do '%s' failed", sql)
					break
				}

				log.Infof("insert %s[%s] return %d.", e.Name, e.Dashboard, affect)
			} 
		}

		for k, _ := range m {
			delete(m, k)
		}
	}

	sql := "select codisname, dashboard from codis, users, user_codis where users.username = '" + username + "' and user_codis.userid = users.id and codis.id = user_codis.codisid"
	rows, err := dbmap.Db.Query(sql)
	if err != nil {
		log.WarnErrorf(err,"Db.Query failed: %s", sql)
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	for rows.Next() {
		var codis_name string
		var codis_dashboard string
		err := rows.Scan(&codis_name, &codis_dashboard)
		if err != nil {
			log.WarnErrorf(err,"rows.Scan failed: %s", sql)
			return nil, errors.Trace(err)
		}
		m[codis_name] = codis_dashboard
	}
	return m, nil
}

type DynamicLoader struct {
	client models.Client
}

func (l *DynamicLoader) Reload(dbmap *gorp.DbMap, username string) (map[string]string, error) {
	var m = make(map[string]string)
	list, err := l.client.List(models.CodisDir, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, path := range list {
		product := filepath.Base(path)
		if b, err := l.client.Read(models.LockPath(product), false); err != nil {
			log.WarnErrorf(err, "read topom of product %s failed", product)
		} else if b != nil {
			var t = &models.Topom{}
			if err := json.Unmarshal(b, t); err != nil {
				log.WarnErrorf(err, "decode json failed")
			} else {
				m[product] = t.AdminAddr
			}
		}
	}
	return m, nil
}

type ReverseProxy struct {
	sync.Mutex
	loadAt time.Time
	loader ConfigLoader
	routes map[string]*httputil.ReverseProxy
}

func NewReverseProxy(loader ConfigLoader) *ReverseProxy {
	r := &ReverseProxy{}
	r.loader = loader
	r.routes = make(map[string]*httputil.ReverseProxy)
	return r
}

func (r *ReverseProxy) reload(d time.Duration, dbmap *gorp.DbMap, username string) {
	if time.Now().Sub(r.loadAt) < d {
		return
	}
	r.routes = make(map[string]*httputil.ReverseProxy)
	if m, err := r.loader.Reload(dbmap, username); err != nil {
		log.WarnErrorf(err, "reload reverse proxy failed")
	} else {
		for name, host := range m {
			if name == "" || host == "" {
				continue
			}
			u := &url.URL{Scheme: "http", Host: host}
			p := httputil.NewSingleHostReverseProxy(u)
			p.Transport = roundTripper
			r.routes[name] = p
		}
	}
	r.loadAt = time.Now()
}

func (r *ReverseProxy) GetProxy(name string) *httputil.ReverseProxy {
	r.Lock()
	defer r.Unlock()
	return r.routes[name]
}

func (r *ReverseProxy) GetNames(dbmap *gorp.DbMap, username string) []string {
	r.Lock()
	defer r.Unlock()
	r.reload(time.Second * 0, dbmap, username)
	var names []string
	for name, _ := range r.routes {
		names = append(names, name)
	}
	return names
}

func SqlQuery(params martini.Params) (int, string) {
	sql := params["sql"]
	if sql == "" {
		return 800, "invalid sql"
	}
	return ExecSql(sql)
}

func ExecSql(sql string) (int, string) {
	if sql == "" {
		return 800, "invalid sql"
	}
	strings.ToLower(sql)
	//sql = SqlConvert(sql)

	if (strings.Contains(sql, "select")) {
		rows, err := dbmap.Db.Query(sql)
		defer rows.Close()
		if (err != nil) {
			log.WarnErrorf(err,"exec '%s' failed", sql)
			return rpc.ApiResponseError(err)
		}

		records := make([]map[string]interface{}, 0)
		columns, err := rows.Columns()
		if err != nil {
			log.WarnErrorf(err,"parse columns failed: %s", sql)
			return rpc.ApiResponseError(err)
		}

		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		for rows.Next() {
			err := rows.Scan(valuePtrs...)
			if err != nil {
				log.WarnErrorf(err, "rows.Scan() failed: %s", sql)
				return rpc.ApiResponseError(err)
			}
			record := make(map[string]interface{})
			for i, col := range values {
				if col != nil {
					var v interface{}
					b, ok := col.([]byte)
					if ok {
						v = string(b)
					} else {
						v = col
					}
					record[columns[i]] = v
				}
			}
			records = append(records, record)
		}

		return rpc.ApiResponseJson(records)
	} else {
		res, err := dbmap.Db.Exec(sql)
		if err != nil {
			log.WarnErrorf(err,"exec '%s' failed", sql)
			return rpc.ApiResponseError(err)
		}

		affect, err := res.RowsAffected()
		if err != nil {
			log.WarnErrorf(err,"get Affected '%s' failed", sql)
			return rpc.ApiResponseError(err)
		}

		return rpc.ApiResponseJson(affect)
	}
}

func sqlExec(dbmap *gorp.DbMap, sql string) (int64, error) {
	res, err := dbmap.Db.Exec(sql)
	if err != nil {
		log.WarnErrorf(err,"exec '%s' failed", sql)
		return 0, errors.Trace(err)
	}

	affect, err := res.RowsAffected()
	if err != nil {
		log.WarnErrorf(err,"get Affected '%s' failed", sql)
		return 0, errors.Trace(err)
	}

	return affect, nil
}

func SqlConvert(sql string) string {
	if strings.Contains(sql, "user") {
		strings.Replace(sql, "user", "user", -1)
	}
	if strings.Contains(sql, "codis") {
		strings.Replace(sql, "codis", "codis", -1)
	}
	if strings.Contains(sql, "user_codis") {
		strings.Replace(sql, "user_codis", "user_codis", -1)
	}

	return sql
}

func IsValidUser(postUser UserModel) (bool, string) {
	if postUser.Username == "" {
		return false, "username is null"
	}
	if postUser.Password == "" {
		return false, "password is null"
	}
	if postUser.UserType != 0 &&  postUser.UserType != 1 {
		return false, "invalid user_type, user_type must be 0 or 1"
	}
	return true, ""
}

func AutoPurgeLog(logfile string, expireLogDays int) error {
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

