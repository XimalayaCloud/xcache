# codis
## 3.2.2-1.10 (2022-09-20)
### New Features
* xmonitor名中增加子命令getbigkey、getriskcmd；

## 3.2.2-1.9 (2022-09-16)
### New Features
* 增加对setex、psetex命令进行大value监控；

## 3.2.2-1.8 (2022-08-31)
### bug fix
* 修复xconfig get部分配置项返回错误的问题

## 3.2.2-1.7 (2022-02-17)
### New Features
* 新增服务降级功能，可根据配置命令和key的黑白名单，对相应流量进行降级，并可根据是否为大请求和QPS对流量进行限制；
* 支持xconfig对降级操作进行配置，包括开启/启停降级，配置黑白名单，配置QPS限流大小；
### optimize
* 去除监控记录冗余字段，不再纳入哈希类型成员的名称，将监控记录详情控制在key级别；
* 优化监控类型，重新定义监控类型，分为大请求，大响应，大key和高风险四种类型；
* 简化时间戳字段，并精确到纳秒级别；
* 减少部分命令的监控，如果在请求阶段可以判断为大请求或大key时，不在响应阶段再进行监控；
* 监控响应时加入延时信息；
### bug fix
* 修复部分命令监控信息统计异常的问题；

## 3.2.2-1.6 (2021-11-19)
### New Features
* 新增redis大key，大value的监控，对redis的异常请求以及响应进行记录
* 新增xmonitor命令，用于获取监控记录
* 支持xconfig对xmonitor进行配置，包括启停/关闭监控，监控相关参数的设置


## 3.2.2-1.5 (2020-11-13)
### optimize
* 解决后端server不可用时，proxy切换到slave节点慢的问题
* 修改proxy backend后端连接读写超时时间为3s
* 修改dashboard中sentinel检测server节点默认超时时间为15s


## 3.2.2-1.4 (2020-10-28)
### New Features
* 在proxy中增加cluster命令，兼容部分redis-cluster功能


## 3.2.2-1.3 (2020-10-10)
### New Features
* 支持proxy信息通过fe转发获取
* 支持添加redis cluser节点
* 备机房和主机房名称一致性校验

### bug fix
* 修复向sentinel添加master节点后，master被判定为主观下线bug

### optimize
* proxy慢日志中添加元素数量和字节数量统计信息
* 使用go-1.15.3编译

## 3.2.2-1.2 (2020-04-15)
### New Features
* proxy命令列表中增加ehash相关命令

### optimize
* 允许proxy启动时对不在命令列表中的命令设置快慢标志

## 3.2.2-1.1 (2020-03-31)
### New Features
* proxy后端连接支持快命令和慢命令连接功能
* 增加xconfig命令动态获取和修改proxy参数
* 增加命令延时信息统计功能

### bug fix
* 修复用户信息修改权限时必须修改密码的bug
* 修复timeout状态proxy无法强制删除的bug


## 3.2.2-1.0 (2019-12-27)
### New Features
* 使用mysql代替zookeeper
* 支持主备机房同步slots状态
* 支持添加dashboard时自动向所有管理员用户分配管理权限
* 增加zk_to_mysql转换工具


## 3.2.2-0.8 (2019-07-31)
### bug fix
* 将主从切换中slaveof命令的超时时间改为100ms，减少proxy阻塞时间
* 修复历史数据中Server QPS点击后小图按钮切换异常的bug

## 3.2.2-0.7 (2019-07-10)
### New Features
* 在codis-server中增加设置TCP_NODLEY失败后打印日志信息


## 3.2.2-0.6 (2019-06-16)
### New Features
* dashboard和proxy支持日志定期删除，过期时间可通过配置文件进行配置
* 简化dashboard和proxy的启动方式
* dashboard支持自动升级
* 优化codis与sentinel兼容性
* 在proxy中增加更详细的慢日志时间信息

## 3.2.2-0.5 (2019-04-25)
### New Features
* 在codis-fe中增加-s参数，可以通过向codis-fe发送信号来关闭进程
* 修改当proxy中SessionAuth为空时，使用ProductAuth进行认证
* 管理系统支持集群自动成倍扩容

## 3.2.2-0.4 (2019-03-29)
### bug fix
* 修改页面发送的sql语句中包含‘-’字符出现的解析错误的问题
* 修改pika页面中hit_rate对应的历史数据展示图像不能缩放的bug

## 3.2.2-0.3 (2018-12-17)
### New Features
* 支持server实例中慢日志、配置项、和客户端连接信息的查询
* 支持主从切换不丢key

### bug fix
* 修改 xslowlog get 命令返回值类型
* 在server故障的情况下也可以查看历史数据
* 修改proxy统计响应时间单位错误的问题

### Performance Improvements
* 修改proxy直接处理ping命令向客户端返回pong
* 修改proxy在flush之后再打印慢日志


## 3.2.2-0.2 (2018-12-07)
### New Features
* 修改配置项名称和默认值

## 3.2.2-0.1 (2018-12-07)
### New Features
* proxy支持慢日志查询和命令响应时间统计
* proxy支持通过jodis_proxy_subdir配置项将同一集群中不同业务连接到不同proxy
* proxy中proxy_max_clients、proxy_refresh_state_period、slowlog_log_slower_than、slowlog_max_len配置项可以通过http接口在线修改

### bug fix
* 修复客户端同一key的命令哈希到同一个proxy与后端server的连接

### Performance Improvements
* 将proxy启动参数中的maxcpu默认设置ncpu

### Public API changes
* 
