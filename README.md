# xcache介绍
## 1.xcache是什么
xcache是由喜马拉雅系统架构团队基于开源项目[codis](https://github.com/CodisLabs/codis)，[redis](https://github.com/antirez/redis)和[pika](https://github.com/Qihoo360/pika)深度定制开发的一套分布式KV持久化存储系统。该系统主要有以下几个特点：
1. 完全支持redis协议，用户不需要修改任何代码，就可以将服务迁移至xcache
2. 数据存储在磁盘上，解决了redis由于存储数据量巨大而导致内存容量瓶颈的问题
3. 集群化部署，支持高并发，高可用，弹性伸缩容

## 2.xcache整体架构
xchace的整体架构和coids-redis架构保持一致，最主要的区别是将codis-server组件替换成了pika组件，如下图：
![component](https://github.com/XimalayaCloud/xcache/blob/master/doc/pictures/top/component.png)

---
在pika组件中，我们引入了一个缓存层(MEM-cache，该模块是基于redis实现)，当存在冷热数据时，可以缓存热key到内存中，从而提高QPS以及降低命令延时。整体的命令调用流程如下：

![flowchart](https://github.com/XimalayaCloud/xcache/blob/master/doc/pictures/top/flowerchart.png)

## 3.xcache有什么特点
### 3.1 支持大value存储
xcahe底层存储引擎支持string数据类型的kv分离存储，在value较大时，可以有效降低LSM的写放大问题，从而降低磁盘IO，减少命令延时。下图是我们对线上存储大value服务优化的效果。
![kv](https://github.com/XimalayaCloud/xcache/blob/master/doc/pictures/top/kv_sperate.png)
优化前每天有10w左右的300ms超时报警，优化后每天1000左右，降低99%以上。

### 3.2 支持缓存热key
早期数据都是存储在磁盘上，这样就造成了内存的浪费。本着对资源充分利用的目的，xcache引入了一个缓存层，该模块是基于redis实现，支持动态开关。测试缓存命中时，可以大大提升QPS，并且降低访问延时，吞吐量测试数据如下：
![cache](https://github.com/XimalayaCloud/xcache/blob/master/doc/pictures/top/cache.png)
缓存命中时，QPS大概提升了50%左右。

### 3.3 新增ehash数据类型
ehash是一种可为field设置过期时间的hash类型数据结构。支持和redis hash一样丰富的数据接口，极大地提高了hash数据结构的灵活性，简化了很多场景下的业务开发工作。

主要特性：
- field支持单独设置过期时间
- field过期后支持高效删除
- 使用语法和原生redis hash数据类型类似

### 3.4 支持zset数据元素个数限长
xcache支持设置zset存储的最大元素个数，当超过用户设置的最大元素个数时，可以自动清理zset中不需要的数据，非常适合只保存定量历史数据的业务场景。

主要特性：
- 支持设置zset保存的最大元素个数
- 支持头部和尾部删除策略
- 支持动态设置执行删除任务周期，错峰删除，避免影响线上业务
- 支持手动执行删除任务

### 3.5 支持快慢命令分离
xcache支持将快命令和慢命令分离执行，这样可以有效降低命令之间的相互影响，避免执行较慢的命令阻塞执行较快的命令。下图是测试set和zadd命令，设置set为快命令，zadd为慢命令。set命令QPS为1w，zadd命令QPS为2w。

set命令延时如下：
![set](https://github.com/XimalayaCloud/xcache/blob/master/doc/pictures/top/fash_slow_thread_pool_set.png)

zadd命令延时如下：
![zadd](https://github.com/XimalayaCloud/xcache/blob/master/doc/pictures/top/fast_slow_thread_pool_zadd.png)

可以看出快慢命令分离前，set和zadd的延时毛刺都差不多，因为zadd和set命令在相同的执行队列中，zadd会阻塞set命令的执行。快慢命令分离后，zadd命令不会阻塞set命令，所以set的延时毛刺降低了很多，从200ms降低到了20ms左右，降低了90%。

## 4.xcache和redis性能对比
### 4.1 环境配置
- CPU: 48核，Intel(R) Xeon(R) Gold 6126 CPU @ 2.60GHz
- 内存：128G
- 磁盘：1.5T（NVMe SSD）
- OS：CentOS Linux release 7.7.1908 (Core)
### 4.2 测试过程
- key长度：20字节
- value长度：100字节（随机字符串)
- redis数据容量大小：20G （string，list，hash，zset各5G）
- xcache数据容量大小：400G（string，list，hash，zset各100G）

PS：考虑到pika底层hash结构和set结构存储协议一致，所以没有测试set数据类型。
### 4.3 测试结果
正常压力，每个命令测试1个小时
![normal_qps](https://github.com/XimalayaCloud/xcache/blob/master/doc/pictures/top/xcache_vs_redis_normal_qps.png)

最大吞吐量测试
![max_qps](https://github.com/XimalayaCloud/xcache/blob/master/doc/pictures/top/xcache_vs_redis_max_qps.png)
### 4.4 测试结论
1. 正常压力下，redis的tp9999在10ms左右，xcache的tp9999在30ms左右。
2. xcache的tp100有延时抖动，可能会出现300ms左右的毛刺（测试1个小时，出现4条超过300ms以上延时毛刺），但redis比较稳定，没有超过40毫秒的延时。
3. 单实例redis的最大QPS在10w左右，xcache的最大QPS可以达到20w左右，比redis高出50%

## 5.xcache适用场景
1. 大数据容量（数据超过百GB，甚至TB级别）。目前xcache在喜马拉雅线上部署了2000+个实例，承载的数据总量约120TB。
2. 高并发。xcache底层是多线程实现，相比redis有着更高的吞吐量，测试一般情况下，QPS比redis高出一倍。
3. 对延时要求不是特别高。xcache数据是存储在磁盘上的，读命令时会去读磁盘，有可能会产生延时毛刺，但写命令会直接写内存，速度较快。测试xcache的tp9999大概在30ms左右，tp100有可能产生上百毫秒延时抖动。
4. 大value存储。对于string数据类型，xcache支持KB级别的value存储，并且有较好的性能表现。
5. 存在冷热数据。xcache可以将热数据缓存到内部的多个redis db中，相当于redis的多线程版本，在读多写少的场景下，对性能有较高的提升。

## 6.其它说明
xcache的存储引擎是基于pika3.0.4代码做的开发，自定义的版本号使用两位有效数字，即主版本号和次版本号，如V3.2。该版本号和pika的版本号没有任何关系，请做区分。

## 7.联系我们
xcache的具体使用可以参考wiki，我们会持续更新。QQ交流群：914191190，欢迎随时骚扰:-)
