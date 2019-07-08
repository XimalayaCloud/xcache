package utils

import (
	"fmt"
	"bufio"
	"io"
	"os"
	"os/exec"
	"strings"
	"strconv"
	"mime/multipart"
	"reflect"
	"io/ioutil"
	"path/filepath"

	"github.com/go-martini/martini"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/timesize"
	"github.com/CodisLabs/codis/pkg/utils/bytesize"
	"github.com/CodisLabs/codis/pkg/utils/rpc"

	"github.com/martini-contrib/render"

)

//.toml文件使用“=”分隔key和value，存在空行
//pika.conf文件使用“:”分隔key和value，存在空行
//redis.conf文件使用“ ”分隔key和value，存在空行
var dashboardSep = "="
var proxySep = "="
var pikaSep = ":"
var redisSep = " "

var binDir = "./assets/bin"
var fabfile = filepath.Join(binDir, "fabfile.py")
var upLoadDir = "upload"
var downLoadDir = "download"
var dashboardConfName = "dashboard.toml"
var proxyConfName = "proxy.toml"
var pikaConfName = "pika.conf"
var redisConfName = "redis.conf"

//type DashBoardConf topom.Config

type DashBoardConf struct {
	CoordinatorAddr		string 			`form:"coordinator_addr" conf:"coordinator_addr"`
	ProductName 		string 			`form:"product_name" conf:"product_name"`
	ProductAuth 		string 			`form:"product_auth" conf:"product_auth"`
	AdminAddr 			string 			`form:"admin_addr" conf:"admin_addr"`
	InfluxdbServer 		string 			`form:"influxdb_server" conf:"metrics_report_influxdb_server"`
	InfluxdbPeriod 		string 			`form:"influxdb_period" conf:"metrics_report_influxdb_period"`
	InfluxdbUsername 	string 			`form:"influxdb_username" conf:"metrics_report_influxdb_username"`
	InfluxdbPassword 	string 			`form:"influxdb_password" conf:"metrics_report_influxdb_password"`
	InfluxdbDatabase 	string 			`form:"influxdb_database" conf:"metrics_report_influxdb_database"`

	Ncpu 		int 					`form:"ncpu"`
	LogLevel 	string 					`form:"logLevel"`
	ConfFile 	*multipart.FileHeader 	`form:"confFile"`
}

type ProxyConf struct {
	ProductName 		string 			`form:"product_name" conf:"product_name"`
	ProductAuth 		string 			`form:"product_auth" conf:"product_auth"`
	AdminAddr 			string 			`form:"admin_addr" conf:"admin_addr"`
	ProxyAddr 			string 			`form:"proxy_addr" conf:"proxy_addr"`
	JodisAddr 			string 			`form:"jodis_addr" conf:"jodis_addr"`
	MaxClients 			int 			`form:"max_clients" conf:"proxy_max_clients"`
	BackendParallel 	int 			`form:"backend_parallel" conf:"backend_primary_parallel"`
	BackendMaxPipeline 	int 			`form:"backend_max_pipeline" conf:"backend_max_pipeline"`
	SessionMaxPipeline 	int 			`form:"session_max_pipeline" conf:"session_max_pipeline"`

	Ncpu 		int 					`form:"ncpu"`
	LogLevel 	string 					`form:"logLevel"`
	ConfFile 	*multipart.FileHeader 	`form:"confFile"`
}

type PikaConf struct {
	RemoteIP 			string 			`form:"remote_ip"`
	Port 				int 			`form:"port" conf:"port"`
	ThreadNum 			int  			`form:"thread_num" conf:"thread-num"`
	Masterauth 			string 			`form:"masterauth" conf:"masterauth"`
	Requirepass 		string 			`form:"requirepass" conf:"requirepass"`
	Userpass 			string 			`form:"userpass" conf:"userpass"`
	MaxClients 			int 			`form:"maxclients" conf:"maxclients"`

	//TargetFileSizeBase 	int 			`form:"target_file_size_base" conf:"target-file-size-base"`
	//ExpireLogsDays 		int 			`form:"expire_logs_days" conf:"expire-logs-days"`
	//ExpireLogsNums 		int 			`form:"expire_logs_nums" conf:"expire-logs-nums"`

	ConfFile 	*multipart.FileHeader 	`form:"confFile"`
}


type RedisConf struct {
	RemoteIP 			string 			`form:"remote_ip"`
	Port 				int 			`form:"port" conf:"port"`
	Databases 			int 			`form:"databases" conf:"databases"`
	Masterauth 			string 			`form:"masterauth" conf:"masterauth"`
	Requirepass 		string 			`form:"requirepass" conf:"requirepass"`
	MaxClients 			int 			`form:"maxclients" conf:"maxclients"`
	MaxMemory 			string 			`form:"maxmemory" conf:"maxmemory"`
	Appendonly 			string 			`form:"appendonly" conf:"appendonly"`
	Appendfsync 		string 			`form:"appendfsync" conf:"appendfsync"`

	ConfFile 	*multipart.FileHeader 	`form:"confFile"`
}

type ServerFileConf struct {
	Port 				int 			`form:"port" conf:"port"`
}

type ProxyFileConf struct {
	ProductName 		string 			`form:"product_name" conf:"product_name"`
	ProxyAddr 			string 			`form:"proxy_addr" conf:"proxy_addr"`
	AdminAddr 			string 			`form:"admin_addr" conf:"admin_addr"`
}

type DashBoardFileConf struct {
	ProductName 		string 			`form:"product_name" conf:"product_name"`
	AdminAddr 			string 			`form:"admin_addr" conf:"admin_addr"`
}

type RemoteInfo struct {
	RemoteIP 			string 			`form:"remote_ip"`
	Port 				int 			`form:"port"`
}

const(
	TypeConf = iota
	TypeComment
)

type ConfItem struct{
	confType	int  	// 0 means conf, 1 means comment
	name		string	
	value		string
}

//items保存注释和配置项，confMap只保存配置项，共有相同的指针，修改confMap同时可以修改items
type DeployConfig struct {
	items		[]*ConfItem
	confMap  	map[string]*ConfItem
	sep 		string 					//配置项中key、value分隔符
}


func DeployInit(path string) {
	binDir = filepath.Join(path, "bin")
	fabfile = filepath.Join(binDir, "fabfile.py")
}

func (c DeployConfig) Show() {
	log.Infof("Show config, len = %d\n", len(c.items))
	for index, item := range c.items {
		if item.confType == TypeComment {
			log.Infof("%d: %s\n", index, item.name)
		}else{
			if len(strings.TrimSpace(c.sep)) > 0 { 	//如果分隔符部署空格则在分隔符两边加空格
				log.Infof("%d: %s %s %s\n", index, item.name, c.sep, item.value)
			}else{
				log.Infof("%d: %s%s%s\n", index, item.name, c.sep, item.value)
			}
		}
	}
}


func (c *DeployConfig) Init(path string, sep string) error {
	c.confMap = make(map[string]*ConfItem)
	c.sep = sep

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		b, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		line := strings.TrimSpace(string(b))

		item := &ConfItem{}
		//第一个字符为“#”或者空行都认为是注释
		if strings.Index(line, "#") == 0 || len(line) == 0 {
			item.confType = TypeComment
			item.name = line
			c.items = append(c.items, item)
			continue
		}

		//不完整的配置项跳过
		index := strings.Index(line, sep)
		if index <= 0 {
			continue
		}

		//分隔符为空格时跳过了只有key没有value的配置项

		//key不为空，value可以为空
		frist := strings.TrimSpace(line[:index])
		second := strings.TrimSpace(line[index+1:])
		if len(frist) == 0 {
			continue
		}

		item.confType = TypeConf
		item.name = frist
		item.value = second
		c.items = append(c.items, item)
		c.confMap[item.name] = item
	}
}

func (c DeployConfig) Get(key string) string {
	item, found := c.confMap[key]
	if !found {
		return ""
	}
	return item.value
}

func (c *DeployConfig) Set(key string, value string) error {
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)

	log.Infof("Set key : %s, value: %s\n", key, value)


	if len(key) == 0 || len(value) == 0 {
		return errors.New("key or value is null")
	}

	item, found := c.confMap[key]
	if found {
		item.value = value
	} else{
		item := &ConfItem{}
		item.confType = TypeConf
		item.name = key
		item.value = value
		c.items = append(c.items, item)
		c.confMap[item.name] = item
	}

	return nil
}

//isWrap为1，表示value需要使用“”
func (c *DeployConfig) Reset(conf interface{}, isWrap bool) {
	obj := reflect.ValueOf(conf)//.Elem() // the struct variable 指针时才用Elem()
	for i := 0; i < obj.NumField(); i++ {
		fieldInfo := obj.Type().Field(i) // a reflect.StructField
		name := fieldInfo.Tag.Get("toml")         // a reflect.StructTag
		if name == "" || name == "-" {
			continue
		}

		//fmt.Printf("name : %s(%s), value: %v\n", name, fieldInfo.Name, obj.Field(i).Interface())
		var value string
		switch v := obj.Field(i).Interface().(type) {
			case string:
				value = strings.Trim(strings.TrimSpace(v), "\"")
				if value == "" {
					continue
				}

				if isWrap {
					c.Set(name, "\"" + value + "\"")
				}else{
					c.Set(name, value)
				}

			case int:
				value = strconv.Itoa(v)
				c.Set(name, value)

			case int32:
				value = strconv.FormatInt(int64(v),10)
				c.Set(name, value)

			case int64:
				value = strconv.FormatInt(v,10)
				c.Set(name, value)

			case bool:
				if (v) {
					c.Set(name, "true")
				} else {
					c.Set(name, "false")
				}

			case timesize.Duration:
				if ret, err := v.MarshalText(); err != nil{
					log.WarnErrorf(err, "config set %s failed.\n", name)
				} else {
					value = string(ret[:])
					c.Set(name, "\"" + value + "\"")
				}

			case bytesize.Int64: 
				if ret, err := v.MarshalText(); err != nil{
					log.WarnErrorf(err, "config set %s failed.\n", name)
				} else {
					value = string(ret[:])
					c.Set(name, "\"" + value + "\"")
				}
				
			default:
				log.Warnf("value error: %v\n", v)
				continue
		}
	}
}

func (c DeployConfig) ReWrite(confName string) error {
	f, err := os.Create(confName)
	if err != nil {
		log.WarnErrorf(err, "create %s failed.\n", confName)
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	for _, item := range c.items {
		var lineStr string
		if item.confType == TypeComment {
			lineStr = fmt.Sprintf("%s", item.name)
		}else {
			if len(strings.TrimSpace(c.sep)) > 0 {
				lineStr = fmt.Sprintf("%s %s %s", item.name, c.sep, item.value)
			}else{
				lineStr = fmt.Sprintf("%s%s%s", item.name, c.sep, item.value)
			}
		}
		fmt.Fprintln(w, lineStr)
	}
	return w.Flush()
}

func RewriteConf(postConf interface{}, defaultConf string, sep string, isWrap bool) error {
	conf := new(DeployConfig)
	err := conf.Init(defaultConf, sep)
	if err != nil {
		log.WarnErrorf(err, "open  %s file failed.\n", defaultConf)
		return err
	}

	conf.Reset(postConf, isWrap)
	conf.Show()
	var newConf = defaultConf + ".tmp"
	if err = conf.ReWrite(newConf); err != nil {
		return err
	} else {
		if err = os.Remove(defaultConf); err != nil {
			return err
		} else {
			return os.Rename(newConf, defaultConf)
		}
	}
}

func genNewConfByConf(postConf interface{}, defaultConf string, newConf string, sep string) error {
	conf := new(DeployConfig)
	err := conf.Init(defaultConf, sep)
	if err != nil {
		log.WarnErrorf(err, "open  %s file failed.\n", defaultConf)
		return err
	}

	var isWrap bool		//value值是否需要“”包围
	switch postConf.(type) {
		case DashBoardConf, DashBoardFileConf:
			isWrap = true
		case ProxyConf, ProxyFileConf:
			isWrap = true
		default:
			isWrap = false
	}

	conf.Reset(postConf, isWrap)
	conf.Show()
	return conf.ReWrite(newConf)
}

func genNewConfByFile(postConfFile *multipart.FileHeader, newConf string) error {
	confFile, err := postConfFile.Open()
	if err != nil{
		log.WarnErrorf(err, "open postConf.ConfFile failed")
		return err
	}
	defer confFile.Close()

	//生成配置文件，os.Create如果文件存在则清空
	f, err := os.Create(newConf)
	if err != nil{
		log.WarnErrorf(err,"create %s failed", newConf)
		return err
	}
	defer f.Close()

	io.Copy(f, confFile)

	return nil
}

func doExecmd(cmd string) (bool, string) {
	var detail string
	var result bool = false

	out, err := exec.Command("bash", "-c", cmd).CombinedOutput()
	if err != nil {
		if strings.Contains(string(out), "Fatal error:") {
			detail = strings.Split(string(out), "Fatal error:")[1]
			detail = strings.Split(detail, "Aborting.")[0]
			detail = strings.TrimSpace(detail)
		} else{
			detail = string(out)
		}

		result = false
		log.Infof("Failed to do cmd[%s]\n", cmd)
	}else{
		if strings.Contains(string(out), "deploy_success") {
			detail = "Success"
			result = true

			log.Infof("Success to do cmd[%s]\n", cmd)
		}else{
			detail = "Failed"
			result = false

			log.Infof("Failed to do cmd[%s]\n", cmd)
		}
	}

	if !result {
		log.Warnf("doExecmd out: .", string(out))
	}

	return result, detail
}

func CreateDashboard(postConf DashBoardConf) (int, string) {
	addr := strings.TrimSpace(postConf.AdminAddr)
	ncpu := postConf.Ncpu
	logLevel := strings.TrimSpace(postConf.LogLevel)

	host, port := parseAddr(addr)
	if host == "" || ( port <= 0 || port >= 65535) {
		return rpc.ApiResponseError(errors.New("host or port is invalid"))
	}

	if ncpu <= 0 {
		return rpc.ApiResponseError(errors.New("ncpu is invalid"))
	}
	if logLevel == "" {
		logLevel = "INFO"
	}

	//保存新配置文件
	defaultConf := filepath.Join(binDir, dashboardConfName)
	confName := "dashboard_" +  host + "_" + strconv.Itoa(port) + ".toml"
	newConf := filepath.Join(binDir, upLoadDir, confName)

	var err error
	if postConf.ConfFile == nil{
		err = genNewConfByConf(postConf, defaultConf, newConf, dashboardSep)
	}else{
		var fileConf DashBoardFileConf
		err = genNewConfByFile(postConf.ConfFile, newConf)
		fileConf.AdminAddr = postConf.AdminAddr
		fileConf.ProductName = postConf.ProductName
		if err == nil {
			err = genNewConfByConf(fileConf, newConf, newConf, dashboardSep)
		}
	}
	if err != nil {
		log.WarnErrorf(err,"create %s failed", newConf)
		return rpc.ApiResponseError(err)
	} 

	log.Infof("create %s success.\n", newConf)

	cmd := "fab -f " + fabfile + " -H " + host + " deploy_dashboard:port='" + strconv.Itoa(port) + 
			"',ncpu='" + strconv.Itoa(ncpu) + "',log_level='" + logLevel + "',conf_name='" + confName + "'"

	ok, detail := doExecmd(cmd)
	if ok  {
		log.Infof("Success to start dashboard[%s]\n", addr)
	} else {
		log.Warnf("Failed to start dashboard[%s]\n", addr)
		log.Warnf("out : %s\n", detail)
		return rpc.ApiResponseError(errors.New(detail))
	}

	//删除配置文件,先留着以便查询，不占空间，写时可以覆盖
	/*
	err = os.Remove(newConf)
	if err != nil {
		log.WarnErrorf(err, "delete %s failed", newConf)
	}*/

	return rpc.ApiResponseJson(detail)
}

func CreateProxy(postConf ProxyConf) (int, string) {
	adminAddr := strings.TrimSpace(postConf.AdminAddr)
	proxyAddr := strings.TrimSpace(postConf.ProxyAddr)
	ncpu := postConf.Ncpu
	logLevel := strings.TrimSpace(postConf.LogLevel)

	adminHost, adminPort := parseAddr(adminAddr)
	if adminHost == "" || ( adminPort <= 0 || adminPort >= 65535) {
		return rpc.ApiResponseError(errors.New("adminHost or adminPort is invalid"))
	}
	proxyHost, proxyPort := parseAddr(proxyAddr)
	if proxyHost == "" || ( proxyPort <= 0 || proxyPort >= 65535) {
		return rpc.ApiResponseError(errors.New("proxyHost or proxyPort is invalid"))
	}
	if adminHost != proxyHost {
		return rpc.ApiResponseError(errors.New("adminHost and proxyHost must be same"))
	}
	if adminPort == proxyPort {
		return rpc.ApiResponseError(errors.New("adminPort and proxyPort must be not same"))
	}

	if ncpu <= 0 {
		return rpc.ApiResponseError(errors.New("ncpu is invalid"))
	}
	if logLevel == "" {
		logLevel = "INFO"
	}

	//保存新配置文件
	defaultConf := filepath.Join(binDir, proxyConfName)
	confName := "proxy_" +  adminHost + "_" + strconv.Itoa(adminPort) + ".toml"
	newConf := filepath.Join(binDir, upLoadDir, confName)

	var err error
	if postConf.ConfFile == nil{
		err = genNewConfByConf(postConf, defaultConf, newConf, proxySep)
	}else{
		var fileConf ProxyFileConf
		err = genNewConfByFile(postConf.ConfFile, newConf)
		fileConf.AdminAddr = postConf.AdminAddr
		fileConf.ProxyAddr = postConf.ProxyAddr
		fileConf.ProductName = postConf.ProductName
		if err == nil {
			err = genNewConfByConf(fileConf, newConf, newConf, proxySep)
		}
	}
	if err != nil {
		log.WarnErrorf(err,"create %s failed", newConf)
		return rpc.ApiResponseError(err)
	}

	log.Infof("create %s success.\n", newConf)

	cmd := "fab -f " + fabfile + " -H " + adminHost + " deploy_proxy:port='" + strconv.Itoa(adminPort) + 
			"',ncpu='" + strconv.Itoa(ncpu) + "',log_level='" + logLevel + "',conf_name='" + confName + "'"
	ok, detail := doExecmd(cmd)
	if ok  {
		log.Infof("Success to start proxy[%s]\n", adminAddr)
	} else {
		log.Warnf("Failed to start proxy[%s]\n", adminAddr)
		log.Warnf("out : %s\n", detail)
		return rpc.ApiResponseError(errors.New(detail))
	}

	//删除配置文件,先留着以便查询，不占空间，写时可以覆盖
	/*
	err = os.Remove(newConf)
	if err != nil {
		log.WarnErrorf(err, "delete %s failed", newConf)
	}*/

	return rpc.ApiResponseJson(detail)
}

func CreatePika(postConf PikaConf) (int, string) {
	host := strings.TrimSpace(postConf.RemoteIP)
	port := postConf.Port

	//判断配置项是否为空
	if host == "" || ( port <= 0 || port >= 65535) {
		return rpc.ApiResponseError(errors.New("host or port is invalid"))
	}

	//保存新配置文件
	defaultConf := filepath.Join(binDir, pikaConfName)
	confName := "pika_" +  host + "_" + strconv.Itoa(port) + ".conf"
	newConf := filepath.Join(binDir, upLoadDir, confName)

	var err error
	if postConf.ConfFile == nil{
		err = genNewConfByConf(postConf, defaultConf, newConf, pikaSep)
	}else{
		var fileConf ServerFileConf
		err = genNewConfByFile(postConf.ConfFile, newConf)
		fileConf.Port = postConf.Port
		if err == nil {
			err = genNewConfByConf(fileConf, newConf, newConf, pikaSep)
		}
	}
	if err != nil {
		log.WarnErrorf(err,"create %s failed", newConf)
		return rpc.ApiResponseError(err)
	}

	log.Infof("create %s success.\n", newConf)

	cmd := "fab -f " + fabfile + " -H " + host + " deploy_pika:port='" + strconv.Itoa(port) + 
			"',conf_name='" + confName + "'"

	ok, detail := doExecmd(cmd)
	if ok  {
		log.Infof("Success to start pika[%s:%d]\n", host, port)
	} else {
		log.Warnf("Failed to start pika[%s:%d]\n", host, port)
		log.Warnf("out : %s\n", detail)
		return rpc.ApiResponseError(errors.New(detail))
	}

	//删除配置文件,先留着以便查询，不占空间，写时可以覆盖
	/*
	err = os.Remove(newConf)
	if err != nil {
		log.WarnErrorf(err, "delete %s failed", newConf)
	}*/

	return rpc.ApiResponseJson(detail)
}

func CreateRedis(postConf RedisConf) (int, string) {
	host := strings.TrimSpace(postConf.RemoteIP)
	port := postConf.Port

	//判断配置项是否为空
	if host == "" || ( port <= 0 || port >= 65535) {
		return rpc.ApiResponseError(errors.New("host or port is invalid"))
	}

	//保存新配置文件
	//binDir = "./assets/bin"  redisConfName = "redis.conf"  upLoadDir = "upLoadDir"
	defaultConf := filepath.Join(binDir, redisConfName)
	confName := "redis_" +  host + "_" + strconv.Itoa(port) + ".conf"
	newConf := filepath.Join(binDir, upLoadDir, confName)

	//redisSep = " "
	var err error
	if postConf.ConfFile == nil{
		err = genNewConfByConf(postConf, defaultConf, newConf, redisSep)
	}else{
		var fileConf ServerFileConf
		err = genNewConfByFile(postConf.ConfFile, newConf)
		fileConf.Port = postConf.Port
		if err == nil {
			err = genNewConfByConf(fileConf, newConf, newConf, redisSep)
		}
	}
	if err != nil {
		log.WarnErrorf(err,"create %s failed", newConf)
		return rpc.ApiResponseError(err)
	}

	log.Infof("create %s success.\n", newConf)

	cmd := "fab -f " + fabfile + " -H " + host + " deploy_redis:port='" + strconv.Itoa(port) + 
			"',conf_name='" + confName + "'"

	ok, detail := doExecmd(cmd)
	if ok  {
		log.Infof("Success to start redis[%s:%d]\n", host, port)
	} else {
		log.Warnf("Failed to start redis[%s:%d]\n", host, port)
		log.Warnf("out : %s\n", detail)
		return rpc.ApiResponseError(errors.New(detail))
	}

	//删除配置文件,先留着以便查询，不占空间，写时可以覆盖
	/*
	err = os.Remove(newConf)
	if err != nil {
		log.WarnErrorf(err, "delete %s failed", newConf)
	}*/

	return rpc.ApiResponseJson(detail)
}

func sendFile(r render.Render, file string){
	buf, err := ioutil.ReadFile(file)
	if err != nil {
		log.WarnErrorf(err, "ReadFile %s failed", file)
		r.Text(rpc.ApiResponseError(err))
		return
	}
	r.Data(200, buf)
}

func GetDashboardDefaultConf(r render.Render) {
	conf := filepath.Join(binDir, dashboardConfName)
	sendFile(r, conf)
}

func GetProxyDefaultConf(r render.Render) {
	conf := filepath.Join(binDir, proxyConfName)
	sendFile(r, conf)
}

func GetPikaDefaultConf(r render.Render) {
	conf := filepath.Join(binDir, pikaConfName)
	sendFile(r, conf)
}

func GetRedisDefaultConf(r render.Render) {
	conf := filepath.Join(binDir, redisConfName)
	sendFile(r, conf)
}

func downLoadConf(host string, port int, serviceType string) (string, error){
	if host == "" || ( port <= 0 || port >= 65535) {
		return "", errors.New("host or port is invalid")
	}

	var confName string
	var fabFunc	string

	switch serviceType {
		case "dashboard":
			confName = "dashboard_" +  host + "_" + strconv.Itoa(port) + ".toml"
			fabFunc = "get_dashboard_conf"
		case "proxy":
			confName = "proxy_" +  host + "_" + strconv.Itoa(port) + ".toml"
			fabFunc = "get_proxy_conf"
		case "pika":
			confName = "pika_" +  host + "_" + strconv.Itoa(port) + ".conf"
			fabFunc = "get_pika_conf"
		case "redis":
			confName = "redis_" +  host + "_" + strconv.Itoa(port) + ".conf"
			fabFunc = "get_redis_conf"
		default:
			detail := "cant kown " + serviceType + " type"
			return "", errors.New(detail)
	}

	//保存新配置文件
	downloadConf := filepath.Join(binDir, downLoadDir, confName)
	cmd := "fab -f " + fabfile + " -H " + host + " " + fabFunc +":port='" + strconv.Itoa(port) + 
			"',conf_name='" + confName + "'"
	ok, detail := doExecmd(cmd)
	if ok  {
		log.Infof("Success to download %s\n", downloadConf)
	} else {
		log.Warnf("Failed to download %s\n", downloadConf)
		log.Warnf("out : %s\n", detail)
		return "", errors.New(detail)
	}

	return downloadConf, nil
}

//当下载文件失败时，通过r.Text返回状态码和失败信息
func GetDashboardConf(r render.Render, params martini.Params) {
	addr := params["addr"]
	host, port := parseAddr(addr)

	if host == "" || ( port <= 0 || port >= 65535) {
		r.Text(rpc.ApiResponseError(errors.New("host or port is invalid")))
		return
	}

	downloadConf, err := downLoadConf(host, port, "dashboard")
	if err != nil {
		r.Text(rpc.ApiResponseError(err))
		return
	}

	sendFile(r, downloadConf)
}

func GetProxyConf(r render.Render, params martini.Params) {
	addr := params["addr"]
	host, port := parseAddr(addr)

	if host == "" || ( port <= 0 || port >= 65535) {
		r.Text(rpc.ApiResponseError(errors.New("host or port is invalid")))
		return
	}

	downloadConf, err := downLoadConf(host, port, "proxy")
	if err != nil {
		r.Text(rpc.ApiResponseError(err))
		return
	}

	sendFile(r, downloadConf)
}

func GetPikaConf(r render.Render, params martini.Params) {
	addr := params["addr"]
	host, port := parseAddr(addr)

	if host == "" || ( port <= 0 || port >= 65535) {
		r.Text(rpc.ApiResponseError(errors.New("host or port is invalid")))
		return
	}

	downloadConf, err := downLoadConf(host, port,  "pika")
	if err != nil {
		r.Text(rpc.ApiResponseError(err))
		return
	}

	sendFile(r, downloadConf)
}

func GetRedisConf(r render.Render, params martini.Params) {
	addr := params["addr"]
	host, port := parseAddr(addr)

	if host == "" || ( port <= 0 || port >= 65535) {
		r.Text(rpc.ApiResponseError(errors.New("host or port is invalid")))
		return
	}

	downloadConf, err := downLoadConf(host, port, "redis")
	if err != nil {
		r.Text(rpc.ApiResponseError(err))
		return
	}

	sendFile(r, downloadConf)
}

func DestroyPika(r render.Render, params martini.Params) (int, string){
	addr := params["addr"]
	host, port := parseAddr(addr)

	if host == "" || ( port <= 0 || port >= 65535) {
		return rpc.ApiResponseError(errors.New("host or port is invalid"))
	}

	ok, detail := destroyServer(host, port, "pika")
	if ok  {
		log.Infof("Success to destroy Pika [%s]\n", addr)
	} else {
		log.Warnf("Failed to destroy Pika [%s]\n", addr)
		log.Warnf("out : %s\n", detail)
		return rpc.ApiResponseError(errors.New(detail))
	}
	return rpc.ApiResponseJson(detail)
}

func DestroyRedis(r render.Render, params martini.Params) (int, string){
	addr := params["addr"]
	host, port := parseAddr(addr)

	if host == "" || ( port <= 0 || port >= 65535) {
		return rpc.ApiResponseError(errors.New("host or port is invalid"))
	}

	ok, detail := destroyServer(host, port, "redis")
	if ok  {
		log.Infof("Success to destroy Redis [%s]\n", addr)
	} else {
		log.Warnf("Failed to destroy Redis [%s]\n", addr)
		log.Warnf("out : %s\n", detail)
		return rpc.ApiResponseError(errors.New(detail))
	}
	
	return rpc.ApiResponseJson(detail)
}

func destroyServer(host string, port int, serviceType string) (bool, string) {
	var fabFunc string

	switch serviceType {
		case "redis":
			fabFunc = "destroy_redis"
		case "pika":
			fabFunc = "destroy_pika"
		default:
			detail := "cant kown " + serviceType + " type"
			return false, detail
	}

	cmd := "fab -f " + fabfile + " -H " + host + " " + fabFunc +":port='" + strconv.Itoa(port) + "'"
	return doExecmd(cmd)
}

func parseAddr(addr string) (string, int) {
	addrArray := strings.Split(strings.TrimSpace(addr), ":")
	if (len(addrArray) != 2) {
		return "", -1
	}

	port, err := strconv.Atoi(addrArray[1])
	if err != nil {
		port = -1
	}

	return addrArray[0], port
}
