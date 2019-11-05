# xcache
xcache是喜马拉雅内部使用的一套分布式缓存解决方案，它是基于360公司的开源项目[Pika](https://github.com/Qihoo360/pika)以及豌豆荚的开源项目[codis](https://github.com/CodisLabs/codis)做的定制开发。

## 整体架构
xchace的整体架构和coids-redis架构保持一致，最主要的区别是将codis-server组件替换成了Pika组件。组件图如下：  

![component](https://github.com/XimalayaCloud/xcache/blob/master/doc/pictures/component.png)

---
在pika组件中，我们引入了一个缓存层(MEM-cache)，该特性是可配置的，在读多写少的场景下，可以缓存热key到内存中，从而提高QPS以及降低命令延时。整体的命令调用流程如下：

![](https://github.com/XimalayaCloud/xcache/blob/master/doc/pictures/flowchart.png)

## 版本说明
xcache的存储引擎是基于pika3.0.4代码做的开发，自定义的版本号使用两位有效数字，即主版本号和次版本号，如V3.2。该版本号和pika的版本号没有任何关系，请做区分。

## xcache都有什么特点
### string数据类型支持key和value分离存储
在value较大的情况下，kv分离存储，可以明显减少LSM的写放大问题，从而降低磁盘的IO，延长磁盘的使用寿命，并且还可以降低命令的延时。测试数据如下：  
kv分离前
![kv分离前](https://github.com/XimalayaCloud/xcache/blob/master/doc/pictures/blobkv/nemo_rw_proxy.png)


kv分离后
![kv分离后](https://github.com/XimalayaCloud/xcache/blob/master/doc/pictures/blobkv/optimize.png)

可以看出kv分离前，tp9999 100ms以上延时非常多，kv分离后，延时降低到20ms左右。

### 支持缓存热key
xcache使用redis模块做缓存，支持缓存的打开和关闭。在读多写少的场景下，缓存不仅可以有效的降低命令访问延时，还可以提高QPS。

### 新增ehash数据类型
该数据类型是在hash数据类型的基础上，支持了field的过期时间设置。

### 磁盘限速
支持对rocksdb底层的flush，compact以及kv分离后的blob文件gc的限速，避免产出大量突发的磁盘IO。

### 针对codis做的新功能开发及优化
- proxy支持慢日志查询和命令响应时间统计
- proxy支持通过jodis_proxy_subdir配置项将同一集群中不同业务连接到不同proxy
- proxy中proxy_max_clients、proxy_refresh_state_period、slowlog_log_slower_than、slowlog_max_len配置项可以通过http接口在线修改
- 支持server实例中慢日志、配置项、和客户端连接信息的查询
- 支持主从切换不丢key
- 在codis-fe中增加-s参数，可以通过向codis-fe发送信号来关闭进程
- 修改当proxy中SessionAuth为空时，使用ProductAuth进行认证
- 管理系统支持集群自动成倍扩容
- dashboard和proxy支持日志定期删除，过期时间可通过配置文件进行配置
- 简化dashboard和proxy的启动方式
- dashboard支持自动升级
- 优化codis与sentinel兼容性

### 其它优化
- 修复pink库没有设置nodelay标志，某些场景导致40ms延迟的问题
- 写binlog流程优化，使用异步线程去写binlog，worker线程只是创建一个写binlog任务，并加入到异步线程队列
- binlog每生成8M数据时sync一次，避免数据一次性落盘，产生大量磁盘IO
- 将info命令中耗时操作放到异步线程中去做
- 支持静态编译pika，运行时不再需要额外的动态库

## 联系我们
xcache的具体使用可以参考wiki，我们会持续更新。QQ交流群：914191190，欢迎随时骚扰:-)
