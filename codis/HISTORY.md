# codis
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
