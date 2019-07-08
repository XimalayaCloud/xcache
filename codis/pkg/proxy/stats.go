// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)

const TPFirstGrade = 5				//5ms - 200ms
const TPFirstGradeSize = 40
const TPSecondGrade = 25		    //225ms - 700ms
const TPSecondGradeSize = 20
const TPThirdGrade = 250			    //950ms - 3200ms
const TPThirdGradeSize = 10
const TPMaxNum = TPFirstGradeSize + TPSecondGradeSize + TPThirdGradeSize


type opStats struct {
	opstr 		string
	totalCalls 	atomic2.Int64
	totalNsecs 	atomic2.Int64
	totalFails 	atomic2.Int64

	calls 		atomic2.Int64
	nsecs 		atomic2.Int64
	nsecsmax  	atomic2.Int64
	qps 		atomic2.Int64
	avg 		int64
	tp    	[TPMaxNum]atomic2.Int64
	tp90  	int64
	tp99  	int64
	tp999 	int64
	tp9999 	int64
	tp100 	int64
	redis 	struct {
		errors atomic2.Int64
	}
}

type OpStats struct {
	OpStr        string `json:"opstr"`
	Calls        int64  `json:"calls"`
	Usecs        int64  `json:"usecs"`
	UsecsPercall int64  `json:"usecs_percall"`
	Fails        int64  `json:"fails"`
	RedisErrType int64  `json:"redis_errtype"`
	QPS 		 int64  `json:"qps"`
	AVG          int64  `json:"avg"`
	TP90  		 int64  `json:"tp90"`
	TP99  		 int64  `json:"tp99"`
	TP999  		 int64  `json:"tp999"`
	TP9999  	 int64  `json:"tp9999"`
	TP100        int64  `json:"tp100"`

}

var cmdstats struct {
	sync.RWMutex 				//仅仅对opmap进行加锁

	opmap map[string]*opStats
	total atomic2.Int64
	fails atomic2.Int64
	redis struct {
		errors atomic2.Int64
	}

	qps atomic2.Int64
	tpdelay		[TPMaxNum]int64   //us
	refreshPeriod atomic2.Int64
}

func init() {
	cmdstats.opmap = make(map[string]*opStats, 128)
	cmdstats.refreshPeriod.Set(int64(time.Second))

	//init tp delay array
	for i := 0; i < TPMaxNum; i++ {
		if i < TPFirstGradeSize {
			cmdstats.tpdelay[i] = int64(i + 1) * TPFirstGrade
		} else if i < TPFirstGradeSize + TPSecondGradeSize {
			cmdstats.tpdelay[i] = TPFirstGradeSize * TPFirstGrade + int64(i - TPFirstGradeSize + 1) * TPSecondGrade
		} else {
			cmdstats.tpdelay[i] = TPFirstGradeSize * TPFirstGrade + TPSecondGradeSize * TPSecondGrade  + int64(i - TPFirstGradeSize -  TPSecondGradeSize + 1) * TPThirdGrade
		}
	}

	//log.Debugf("cmdstats.tpdelay: %v", cmdstats.tpdelay)

	go func() {
		for {
			if cmdstats.refreshPeriod.Int64() <= 0 {
				time.Sleep(time.Second)
				continue
			}

			start := time.Now()
			total := cmdstats.total.Int64()
			if cmdstats.refreshPeriod.Int64() <= int64(time.Second) {
				time.Sleep(time.Second)
			} else {
				time.Sleep( time.Duration(cmdstats.refreshPeriod.Int64()) )
			}
			delta := cmdstats.total.Int64() - total
			normalized := math.Max(0, float64(delta)) / float64(time.Since(start)) * float64(time.Second) 
			cmdstats.qps.Set(int64(normalized + 0.5))

			cmdstats.RLock()
			for _, v := range cmdstats.opmap{
				normalized := math.Max(0, float64(v.calls.Int64())) / float64(time.Since(start)) * float64(time.Second)
				v.qps.Set(int64(normalized + 0.5))
				v.tp90, v.tp99, v.tp999, v.tp9999 = v.Get4TP(0.9, 0.99, 0.999, 0.9999)
				v.tp100 = v.nsecsmax.Int64() / 1e6

				if calls := v.calls.Int64(); calls != 0 {
					v.avg = v.nsecs.Int64() / 1e6 / calls
				} else {
					v.avg = 0
				}

				v.calls.Set(0)
				v.nsecs.Set(0)
				v.nsecsmax.Set(0)
				v.tp = [TPMaxNum]atomic2.Int64{0}	//对原来数组进行初始化，不重新分配内存,  是否存在非原子操作问题
			}
			cmdstats.RUnlock()
		}
	}()
}

//SetTP()中duration单位为毫秒
func (s *opStats) SetTP(duration int64) {
	if duration <= 0 {
		s.tp[0].Incr()
	}else if duration <= TPFirstGrade*TPFirstGradeSize {
		index := (duration + TPFirstGrade - 1) / TPFirstGrade - 1
		s.tp[index].Incr()
	} else if duration <= TPFirstGrade*TPFirstGradeSize + TPSecondGrade*TPSecondGradeSize {
		index := (duration - TPFirstGrade*TPFirstGradeSize + TPSecondGrade - 1) / TPSecondGrade + TPFirstGradeSize - 1
		s.tp[index].Incr()
	} else if duration <= TPFirstGrade*TPFirstGradeSize + TPSecondGrade*TPSecondGradeSize + TPThirdGrade*TPThirdGradeSize {
		index := (duration - TPFirstGrade*TPFirstGradeSize - TPSecondGrade*TPSecondGradeSize + TPThirdGrade - 1) / TPThirdGrade + TPFirstGradeSize + TPSecondGradeSize - 1
		s.tp[index].Incr()
	} else {
		s.tp[TPMaxNum - 1].Incr()
	}
}

//persents support 0 < persents <= 1 only
func (s *opStats) GetTP(persents float64) int64{
	if s.calls.Int64() == 0 || persents <= 0 || persents > 1 {
		return 0
	}

	tpnum := int64( float64(s.calls.Int64()) * persents )
	var count int64
	var index int

	for i, v := range s.tp {
		count += v.Int64()
		if count >= tpnum || i == len(s.tp)-1 {
			index = i
			break
		}
	}

	if index >= 0 && index < TPMaxNum {
		return cmdstats.tpdelay[index]
	}

	return -1
}

//persents support 0 < persents <= 1 only
func (s *opStats) Get4TP(persents1, persents2, persents3, persents4 float64) (int64, int64, int64, int64){
	if s.calls.Int64() == 0 {
		return 0, 0, 0, 0
	}

	if !(persents1 > 0 && persents2 >= persents1 && persents3 >= persents2 && persents4 >= persents3 && persents4 <= 1.0) {
		return -1, -1, -1, -1 
	}

	tpnum1 := int64( float64(s.calls.Int64()) * persents1 )
	tpnum2 := int64( float64(s.calls.Int64()) * persents2 )
	tpnum3 := int64( float64(s.calls.Int64()) * persents3 )
	tpnum4 := int64( float64(s.calls.Int64()) * persents4 )

	var index1, index2, index3, index4 int
	var count int64
	var i 	  int

	for i = 0; i < len(s.tp); i++ {
		count += s.tp[i].Int64()
		if count >= tpnum1 || i == len(s.tp)-1 {
			index1 = i
			break
		}
	}

	if count >= tpnum2 || i == len(s.tp)-1 {
		index2 = i
	} else {
		for i = i+1; i < len(s.tp); i++ {
			count += s.tp[i].Int64()
			if count >= tpnum2 || i == len(s.tp)-1 {
				index2 = i
				break
			}
		}
	}

	if count >= tpnum3 || i == len(s.tp)-1 {
		index3 = i
	} else {
		for i = i+1; i < len(s.tp); i++ {
			count += s.tp[i].Int64()
			if count >= tpnum3 || i == len(s.tp)-1 {
				index3 = i
				break
			}
		}
	}

	if count >= tpnum4 || i == len(s.tp)-1 {
		index4 = i
	} else {
		for i = i+1; i < len(s.tp); i++ {
			count += s.tp[i].Int64()
			if count >= tpnum4 || i == len(s.tp)-1 {
				index4 = i
				break
			}
		}
	}

	if index1 >= 0 && index2 >= index1 && index3 >= index2 && index4 >= index3 && index4 < TPMaxNum {
		return cmdstats.tpdelay[index1], cmdstats.tpdelay[index2], cmdstats.tpdelay[index3], cmdstats.tpdelay[index4]
	}

	return -1, -1, -1, -1
}

func (s *opStats) OpStats() *OpStats {
	o := &OpStats{
		OpStr: s.opstr,
		Calls: s.totalCalls.Int64(),
		Usecs: s.totalNsecs.Int64() / 1e3,
		Fails: s.totalFails.Int64(),
		QPS:   s.qps.Int64(),
		AVG:   s.avg,
		TP90:  s.tp90,
		TP99:  s.tp99,
		TP999:   s.tp999,
		TP9999:  s.tp9999,
		TP100:	 s.tp100,
	}
	if o.Calls != 0 {
		o.UsecsPercall = o.Usecs / o.Calls
	}
	o.RedisErrType = s.redis.errors.Int64()

	return o
}

func (s *opStats)incrOpStats(responseTime int64, t redis.RespType) {
	s.totalCalls.Incr()
	s.totalNsecs.Add(responseTime)
	s.calls.Incr()
	s.nsecs.Add(responseTime)
	s.nsecsmax.Set( int64(math.Max( float64(responseTime), float64(s.nsecsmax.Int64()) )) )

	switch t {
		case redis.TypeError:
			s.redis.errors.Incr()
	}
	s.SetTP( responseTime/1e6 )
}

func SetRefreshPeriod(d time.Duration) {
	if d >= 0 {
		cmdstats.refreshPeriod.Set( int64(d) )
	}
}

func OpTotal() int64 {
	return cmdstats.total.Int64()
}

func OpFails() int64 {
	return cmdstats.fails.Int64()
}

func OpRedisErrors() int64 {
	return cmdstats.redis.errors.Int64()
}

func OpQPS() int64 {
	return cmdstats.qps.Int64()
}

func getOpStats(opstr string, create bool) *opStats {
	cmdstats.RLock()
	s := cmdstats.opmap[opstr]
	cmdstats.RUnlock()

	if s != nil || !create {
		return s
	}

	cmdstats.Lock()
	s = cmdstats.opmap[opstr]
	if s == nil {
		s = &opStats{opstr: opstr}
		cmdstats.opmap[opstr] = s
	}
	cmdstats.Unlock()
	return s
}

type sliceOpStats []*OpStats

func (s sliceOpStats) Len() int {
	return len(s)
}

func (s sliceOpStats) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sliceOpStats) Less(i, j int) bool {
	return s[i].OpStr < s[j].OpStr
}

func GetOpStatsAll() []*OpStats {
	var all = make([]*OpStats, 0, 128)
	cmdstats.RLock()
	for _, s := range cmdstats.opmap {
		all = append(all, s.OpStats())
	}
	cmdstats.RUnlock()
	sort.Sort(sliceOpStats(all))
	return all
}

func ResetStats() {
	//由于session已经获取到了cmdstats.opmap中的结构体，所以这里不能重新分配只能置零
	//因此reset后命令数量不会减少
	cmdstats.RLock()
	for _, v := range cmdstats.opmap{
		v.totalCalls.Set(0)
		v.totalNsecs.Set(0)
		v.totalFails.Set(0)
		v.redis.errors.Set(0)
	}
	cmdstats.RUnlock()

	cmdstats.total.Set(0)
	cmdstats.fails.Set(0)
	cmdstats.redis.errors.Set(0)
	sessions.total.Set(sessions.alive.Int64())
}

func incrOpTotal() {
	cmdstats.total.Incr()
}

func incrOpRedisErrors() {
	cmdstats.redis.errors.Incr()
}

func incrOpFails(r *Request, err error) {
	if r != nil {
		var s *opStats
		s = getOpStats(r.OpStr, true)
		s.totalFails.Incr()
		s = getOpStats("ALL", true)
		s.totalFails.Incr()
	} 

	cmdstats.fails.Incr()
}

func incrOpStats(r *Request, t redis.RespType) {
	if r != nil {
		var s *opStats
		responseTime := time.Now().UnixNano() - r.ReceiveTime

		s = getOpStats(r.OpStr, true)
		s.incrOpStats(responseTime, t)
		s = getOpStats("ALL", true)
		s.incrOpStats(responseTime, t)

		switch t {
			case redis.TypeError:
				cmdstats.redis.errors.Incr()
		}
	}
}

var sessions struct {
	total atomic2.Int64
	alive atomic2.Int64
}

func incrSessions() int64 {
	sessions.total.Incr()
	return sessions.alive.Incr()
}

func decrSessions() {
	sessions.alive.Decr()
}

func SessionsTotal() int64 {
	return sessions.total.Int64()
}

func SessionsAlive() int64 {
	return sessions.alive.Int64()
}

type SysUsage struct {
	Now time.Time
	CPU float64
	*utils.Usage
}

var lastSysUsage atomic.Value

func init() {
	go func() {
		for {
			cpu, usage, err := utils.CPUUsage(time.Second)
			if err != nil {
				lastSysUsage.Store(&SysUsage{
					Now: time.Now(),
				})
			} else {
				lastSysUsage.Store(&SysUsage{
					Now: time.Now(),
					CPU: cpu, Usage: usage,
				})
			}
			if err != nil {
				time.Sleep(time.Second * 5)
			}
		}
	}()
}

func GetSysUsage() *SysUsage {
	if p := lastSysUsage.Load(); p != nil {
		return p.(*SysUsage)
	}
	return nil
}
