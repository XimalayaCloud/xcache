# pika for codis
## 3.0.4-3.4 (2019-12-27)
### New Features
* zset数据类型支持限长功能
* 支持动态设置检测操作系统free内存周期

### optimize
* 调整min_free_kbytes为系统总内存的3%，目的是加快系统cache内存回收，避免free内存太少，导致写QPS较高时产生延时毛刺
* zcount/zrangebyscore/zremrangebyscore/zrevrangebyscore接口从用户传入的min值开始迭代，而不是从score最小值开始迭代
* zrange接口，当起始索引大于元素总量的70%时采用反向迭代；zrevrange接口，当起始索引大于元素总量30%，采用正向迭代

### bug fix
* 修复scan命令，如果每次使用相同的游标，会导致存储游标的list持续增大，发生内存泄露

## 3.0.4-3.3 (2019-10-31)
### optimize
* 优化GC算法，每次GC任务结束后继续递归判断是否需要GC，而不是依赖compact触发GC任务，提升GC速度
* 支持动态设置GC每次采样数据总量，文件采样检查周期，垃圾回收比例以及GC任务队列大小

## 3.0.4-3.2 (2019-10-23)
### bug fix
* 修复服务重启后，每个blob的可回收率的分数被清为0，只有被访问到的sst文件中的blob数据会重新计算，导致很多blob文件分数虽然高，但采样时又不满足回收条件的文件一直被选中，后面的无效数据很多但分数为0的真正需要回收的blob文件无法被删除，从而导致gc速度就变慢的bug

## 3.0.4-3.1 (2019-10-10)
### New Features
* 周期检测操作系统free内存剩余大小，如果小于用户设定值，则清理。只有master时执行该操作

### optimize
* 使用限速器对flush和compact磁盘限速，避免瞬间产生大量磁盘IO
* 支持动态打开和关闭rocksdb的写WAL
* 配置中支持使用direct IO
* blob文件gc时，先判断blobindex中的ttl是否过期，如果过期则直接删除，避免回查sst文件
* binlog每生成8M数据时sync一次，避免数据一次性落盘，产生大量磁盘IO
* 在info中显示cache是否打开状态，如果关闭则不现实cache信息
* 统计key数量时，只读取sst文件，不需要读取blob文件
* 增大select命令db上限

### bug fix
* 修复incr/incrby/incrbyfloat/decr/decrby会把key的expire属性消除的bug

## 3.0.4-3.0 (2019-08-12)
### New Features
* string数据类型支持key和value分离存储

## 3.0.4-2.7 (2019-08-06)
### bug fix
* 修复ehsetex命令没有将meta中的count值加1

## 3.0.4-2.6 (2019-08-06)
### bug fix
* 修复bgsave命令没有dump出ehash数据类型的备份目录

## 3.0.4-2.5 (2019-07-16)
### bug fix
* 修复缓存模式下，ZREVRANGEBYSCORE当score不在元素范围内时返回错误而不是空列表问题

## 3.0.4-2.4 (2019-07-16)
### bug fix
* 修复缓存模式下，ZREVRANGEBYSCORE返回member数据被截断问题

## 3.0.4-2.3 (2019-06-10)
### bug fix
* 修复pink库没有设置nodelay标志，某些场景导致40ms延迟的问题
* 修复pika在极少数binlog文件写坏导致coredump的问题
* 修复写binlog过程中由于文件创建失败直接coredump的bug。出现异常时打印错误日志

## 3.0.4-2.2 (2019-05-09)
### New Features
* 添加ehash数据类型，支持field过期时间设置

## 3.0.4-2.1 (2019-05-06)
### optimize
* 支持静态编译pika，运行时不再需要额外的动态库
* 将info命令中耗时操作放到异步线程中去做

### bug fix
* 为配置项设置默认值，防止配置项注释掉后，被赋予随机值

## 3.0.4-2.0 (2019-04-02)
### New Features
* 添加redis缓存模块，支持所有数据类型的缓存读写
* 添加发布订阅功能
* 添加慢日志功能
* 写binlog流程优化，使用异步线程去写binlog，worker线程只是创建一个写binlog任务，并加入到异步线程队列

### bug fix
* 修复block-cache配置读取异常的bug

## 3.0.4-1.3 (2019-01-25)
### bug fix
* 修复slotsdel命令处理不存在的slotsinfo key时直接退出的bug
* 修复slotsdel命令删除key失败的bug

## 3.0.4-1.2 (2018-12-27)
### New Features
* 增加block_cache相关配置
* 增加max_subcompactions配置项
* 支持动态修改rocksdb配置

### bug fix
* 修改hset命令不写slotinfo信息

## 3.0.4-1.1 (2018-11-26)
### New Features
* 增加block_cache相关配置
* 增加max_subcompactions配置项
* 支持动态修改rocksdb配置

### bug fix
* 修改string类型expireat命令当timestamp为0时的行为与redis不一致
* 修改hset命令不写slotinfo信息
* 修改全量同步过程中resync database文件两次的bug

## 3.0.4-1.0 (2018-10-25)
### New Features
* rocksdb 中 max_subcompactions 参数默认值设为3
* 升级至最新引擎black_widow,新引擎读写性能更优
* 新引擎与pika-2.2.6数据不兼容
* 在线修改rocksdb配置不生效

## 2.2.6-3.6 (2018-07-11)
### New Features
* 支持发布订阅
* aof_to_pika工具增加启动参数-b和-s,用于限制发送速度

## 3.5-2.2.6 (2018-03-20)
### New Features
* 新增max-log-size配置项
* 新增disable-auto-compactions配置项
* 新增max-write-buffer-number配置项
* 新增max-bytes-for-level-base配置项
* 新增level0-file-num-compaction-trigger配置项
* 新增level0-slowdown-writes-trigger配置项
* 新增level0-stop-writes-trigger配置项
* config rewrite 将注释的配置项追加到配置文件末尾
* glog日志文件按文件大小切分
* aof_to_pika工具支持断点续传

## 2.2.6-4(xmly) (2018-03-13)
### bug fix
* 兼容与pika-2.1.0进行主从同步

## 2.2.6-3(xmly) (2018-02-05)
### bug fix
* bgsave_thread_在执行DoDBSync()时获取已经销毁的db_sync_protector_导致coredump

## 2.2.6-2(xmly) (2018-01-16)
### Performance Improvements
* 删除多余日志

## 2.2.6-1(xmly) (2017-12-18)
### New Features
* 增加slotsrestore命令，支持redis向pika迁移数据
* slave状态中增加了repl_down_since_字段(master宕机codis选slave时使用)
* slot操作增加tag功能
* 增加key数量显示
* 修改定期compact逻辑，同时满足compact-cron和compact-interval才会执行
* 修改通过config get maxmemory时返回pika占用磁盘空间

* third/slash中添加trylock接口

### bug fix
* pike_trysync_thread.cc中RecvProc()函数获取是否需要认证时使用masterauth()
* PikaTrysyncThread::Send()删除多余的获取requirepass的逻辑

* third/slash中将条件变量等待时间从系统时间改为开机时间CLOCK_REALTIME  to CLOCK_MONOTONIC，防止修改系统时间导致timewait执行错误

* 修复pink中redis_cli.cc中解析redis响应遇到REDIS_HALF是类型错误的bug
* pink中默认rbuf大小为64M改为2M

* 系统接收kill信号处理
* 修复磁盘空间满时，pika coredump问题(pika-2.3.0修复，增加写binlog失败释放锁)
* DBSyncSendFile()函数判断GetChildren()返回值错误bug修复
* 修复PurgeFiles()函数中purgebinlog日志异常bug

* 修复数据迁移建立连接使用masterauth密码，改为使用userpass密码
* 去掉Trysync命令必须是requirepass才允许执行的现在(这样只有命令黑名单中的命令需要requirepass认证才能执行)
* 修复bitset和bitget命令当key过期后，依然可以获取到key的value的bug
* 修复lpushx、rpushx在key不存在时也去写slots的错误逻辑，将ok()和isnotfound()分开处理
* 为lset、linsert增加slots操作，当执行ok时写slot

### Performance Improvements
* 优化info命令，每隔60秒才去统计一次磁盘空间使用情况
* 数据迁移复用连接
* 将slotsdel放到后台运行，并支持范围删除
* SerializeRedisCommand redis.cc中argv类型应该传引用更节省内存
* 修复hgetall不存在的key非常耗时的问题，执行hgetall之前先判断一下key是否存在


### Public API changes
* 
