// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"time"
	"strings"
	"strconv"
	"os/exec"
	"os"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/redis"
)

func (s *Topom) CreateGroup(gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if gid <= 0 || gid > models.MaxGroupId {
		return errors.Errorf("invalid group id = %d, out of range", gid)
	}
	if ctx.group[gid] != nil {
		return errors.Errorf("group-[%d] already exists", gid)
	}
	defer s.dirtyGroupCache(gid)

	g := &models.Group{
		Id:      gid,
		Servers: []*models.GroupServer{},
	}
	return s.storeCreateGroup(g)
}

func (s *Topom) RemoveGroup(gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	if len(g.Servers) != 0 {
		return errors.Errorf("group-[%d] isn't empty", gid)
	}
	defer s.dirtyGroupCache(g.Id)

	return s.storeRemoveGroup(g)
}

func (s *Topom) ResyncGroup(gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}

	if err := s.resyncSlotMappingsByGroupId(ctx, gid); err != nil {
		log.Warnf("group-[%d] resync-group failed", g.Id)
		return err
	}
	defer s.dirtyGroupCache(gid)

	g.OutOfSync = false
	return s.storeUpdateGroup(g)
}

func (s *Topom) ResyncGroupAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	for _, g := range ctx.group {
		if ctx.isGroupInUse(g.Id) == false {
			log.Infof("group-[%d] not in used, skip.", g.Id)
			continue
		}
		if err := s.resyncSlotMappingsByGroupId(ctx, g.Id); err != nil {
			log.Warnf("group-[%d] resync-group failed", g.Id)
			return err
		}
		defer s.dirtyGroupCache(g.Id)

		g.OutOfSync = false
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}
	}
	return nil
}

func (s *Topom) GroupAddServer(gid int, dc, addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if addr == "" {
		return errors.Errorf("invalid server address")
	}

	for _, g := range ctx.group {
		for _, x := range g.Servers {
			if x.Addr == addr {
				return errors.Errorf("server-[%s] already exists", addr)
			}
		}
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}

	if p := ctx.sentinel; len(p.Servers) != 0 {
		defer s.dirtySentinelCache()
		p.OutOfSync = true
		if err := s.storeUpdateSentinel(p); err != nil {
			return err
		}
	}
	defer s.dirtyGroupCache(g.Id)

	g.Servers = append(g.Servers, &models.GroupServer{Addr: addr, DataCenter: dc})
	return s.storeUpdateGroup(g)
}

func (s *Topom) GroupDelServer(gid int, addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	index, err := ctx.getGroupIndex(g, addr)
	if err != nil {
		return err
	}

	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}

	if index == 0 {
		if len(g.Servers) != 1 || ctx.isGroupInUse(g.Id) {
			return errors.Errorf("group-[%d] can't remove master, still in use", g.Id)
		}
	}

	if p := ctx.sentinel; len(p.Servers) != 0 {
		defer s.dirtySentinelCache()
		p.OutOfSync = true
		if err := s.storeUpdateSentinel(p); err != nil {
			return err
		}

		if err := s.SentinelRemoveGroupNoLock(gid); err != nil {
			log.WarnErrorf(err, "sentinel remove group-[%d] failed.", gid)
			return err
		}

		//send slaveof no one to slave
		c, err := redis.NewClient(addr, s.config.ProductAuth, time.Second)
		if err != nil {
			log.WarnErrorf(err, "creat slave-[%s] client failed", addr)	
		}
		defer c.Close()

		if c != nil {
			if _, err := c.Do("slaveof", "no", "one"); err != nil {
				log.WarnErrorf(err, "set slave-[%s] slaveof no one failed", addr)
			}
		}

		if err := s.ResyncSentinelsNoLock(); err != nil {
			log.WarnErrorf(err, "resync sentinel failed.", gid)
			return err
		}

		p.OutOfSync = false
		if err := s.storeUpdateSentinel(p); err != nil {
			return err
		}
	}
	defer s.dirtyGroupCache(g.Id)

	if index != 0 && g.Servers[index].ReplicaGroup {
		g.OutOfSync = true
	}

	var slice = make([]*models.GroupServer, 0, len(g.Servers))
	for i, x := range g.Servers {
		if i != index {
			slice = append(slice, x)
		}
	}
	if len(slice) == 0 {
		g.OutOfSync = false
	}

	g.Servers = slice

	return s.storeUpdateGroup(g)
}

func (s *Topom) GroupPromoteServer(gid int, addr string, force bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	index, err := ctx.getGroupIndex(g, addr)
	if err != nil {
		return err
	}

	if g.Promoting.State != models.ActionNothing {
		if index != g.Promoting.Index {
			return errors.Errorf("group-[%d] is promoting index = %d", g.Id, g.Promoting.Index)
		}
	} else {
		if index == 0 {
			return errors.Errorf("group-[%d] can't promote master", g.Id)
		}
	}
	if n := s.action.executor.Int64(); n != 0 {
		return errors.Errorf("slots-migration is running = %d", n)
	}

	//一键主从切换
	if (!force) {
		master_addr := ctx.getGroupMaster(gid)
		c_master, err := redis.NewClient(master_addr, s.config.ProductAuth, time.Second)
		if err != nil {
			log.WarnErrorf(err, "creat master-[%s] client failed", master_addr)
			return errors.New("Client connection failed, Master may be down!")	
		}
		defer c_master.Close()

		c_slave, err := redis.NewClient(addr, s.config.ProductAuth, time.Second)
		if err != nil {
			log.WarnErrorf(err, "creat slave-[%s] client failed", addr)
			return errors.New("Client connection failed, Slave may be down!")
		}
		defer c_slave.Close()

		//获取偏移量（为什么不去overview中直接获取？因为overview中的状态每秒才会更新一次）
		master_filenum, master_offset, err := c_master.BinlogOffset()
		if err != nil {
			log.WarnErrorf(err, "get master-[%s] binlog offset failed", master_addr)
			return errors.New("Failed to get binlog offset, Master may be down!")
		}
		slave_filenum_beg, slave_offset_beg, err := c_slave.BinlogOffset()
		if err != nil {
			log.WarnErrorf(err, "get slave-[%s] binlog offset failed", addr)
			return errors.New("Failed to get binlog offset, Slave may be down!")
		}

		if master_filenum != slave_filenum_beg || master_offset != slave_offset_beg {
			//104857600: max size of each binlog file
			diff :=  (master_filenum - slave_filenum_beg) * 104857600 + master_offset - slave_offset_beg

			time.Sleep(500 * time.Millisecond)

			slave_filenum_end, slave_offset_end, err := c_slave.BinlogOffset()
			if err != nil {
				log.WarnErrorf(err, "get slave-[%s] binlog offset failed", addr)
				return errors.New("Failed to get binlog offset, Slave may be down!")
			}

			//speed 1000ms 500ms
			speed := ((slave_filenum_end - slave_filenum_beg) * 104857600 + (slave_offset_end - slave_offset_beg)) * 1000 / 500 

			if  (speed == 0 && diff != 0) || (speed * 3 < diff) {
				return errors.New("Master and Slave offset cannot be same within the time limit!")
			}
		} 

		//master停写后再次获取主从偏移量,用于计算主从切换需要的时长
		can_promote := false
		if _, err := c_master.Do("config", "set", "slave-read-only", "yes"); err != nil {
			log.WarnErrorf(err, "set master-[%s] slave-read-only yes failed", master_addr)
			return errors.New("Setting read-only mode failed, Master may be down!")
		}
		defer func() {
			if _, err := c_master.Do("config", "set", "slave-read-only", "no"); err != nil {
				log.WarnErrorf(err, "set master-[%s] slave-read-only no failed", master_addr)
			}
		}()

		if (slave_filenum_beg == master_filenum && master_offset == slave_offset_beg) {
			can_promote = true
		} else {
			master_filenum, master_offset, err = c_master.BinlogOffset()
			if err != nil {
				log.WarnErrorf(err, "get master-[%s] binlog offset failed", master_addr)
				return errors.New("Failed to get binlog offset, Master may be down!")
			}
			for i:=0; i<60; i++ {
				time.Sleep(50 * time.Millisecond)
				slave_filenum_beg, slave_offset_beg, err = c_slave.BinlogOffset()
				if err != nil {
					log.WarnErrorf(err, "get slave-[%s] binlog offset failed", addr)
					return errors.New("Failed to get binlog offset, Slave may be down!")
				}
				if (slave_filenum_beg == master_filenum && master_offset == slave_offset_beg) {
					can_promote = true
					break
				}
			}
		}
		
		if (!can_promote) {
			return errors.New("Master and Slave offset cannot be same within the time limit!")
		}
	}

	switch g.Promoting.State {

	case models.ActionNothing:

		defer s.dirtyGroupCache(g.Id)

		log.Warnf("group-[%d] will promote index = %d", g.Id, index)

		g.Promoting.Index = index
		g.Promoting.State = models.ActionPreparing
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}

		fallthrough

	case models.ActionPreparing:

		defer s.dirtyGroupCache(g.Id)

		log.Warnf("group-[%d] resync to prepared", g.Id)

		slots := ctx.getSlotMappingsByGroupId(g.Id)

		g.Promoting.State = models.ActionPrepared
		if err := s.resyncSlotMappings(ctx, slots...); err != nil {
			log.Warnf("group-[%d] resync-rollback to preparing", g.Id)
			g.Promoting.State = models.ActionPreparing
			s.resyncSlotMappings(ctx, slots...)
			log.Warnf("group-[%d] resync-rollback to preparing, done", g.Id)
			return err
		}
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}

		fallthrough

	case models.ActionPrepared:

		if p := ctx.sentinel; len(p.Servers) != 0 {
			defer s.dirtySentinelCache()
			p.OutOfSync = true
			if err := s.storeUpdateSentinel(p); err != nil {
				return err
			}
			groupIds := map[int]bool{g.Id: true}
			sentinel := redis.NewSentinel(s.config.ProductName, s.config.ProductAuth)
			if err := sentinel.RemoveGroups(p.Servers, s.config.SentinelClientTimeout.Duration(), groupIds); err != nil {
				log.WarnErrorf(err, "group-[%d] remove sentinels failed", g.Id)
			}
			if s.ha.masters != nil {
				delete(s.ha.masters, gid)
			}
		}

		defer s.dirtyGroupCache(g.Id)

		var index = g.Promoting.Index
		var slice = make([]*models.GroupServer, 0, len(g.Servers))
		slice = append(slice, g.Servers[index])
		for i, x := range g.Servers {
			if i != index && i != 0 {
				slice = append(slice, x)
			}
		}
		slice = append(slice, g.Servers[0])

		for _, x := range slice {
			x.Action.Index = 0
			x.Action.State = models.ActionNothing
		}

		g.Servers = slice
		g.Promoting.Index = 0
		g.Promoting.State = models.ActionFinished
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}

		var master = slice[0].Addr
		if c, err := redis.NewClient(master, s.config.ProductAuth, 100 * time.Millisecond); err != nil {
			log.WarnErrorf(err, "create redis client to %s failed", master)
		} else {
			defer c.Close()
			if err := c.SetMaster("NO:ONE"); err != nil {
				log.WarnErrorf(err, "redis %s set master to NO:ONE failed", master)
			}
		}

		fallthrough

	case models.ActionFinished:

		log.Warnf("group-[%d] resync to finished", g.Id)

		slots := ctx.getSlotMappingsByGroupId(g.Id)

		if err := s.resyncSlotMappings(ctx, slots...); err != nil {
			log.Warnf("group-[%d] resync to finished failed", g.Id)
			return err
		}
		defer s.dirtyGroupCache(g.Id)

		g = &models.Group{
			Id:      g.Id,
			Servers: g.Servers,
		}
		return s.storeUpdateGroup(g)

	default:

		return errors.Errorf("group-[%d] action state is invalid", gid)

	}
}

func (s *Topom) trySwitchGroupMaster(gid int, master string, cache *redis.InfoCache) error {
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}

	var index = func() int {
		for i, x := range g.Servers {
			if x.Addr == master {
				return i
			}
		}
		for i, x := range g.Servers {
			rid1 := cache.GetRunId(master)
			rid2 := cache.GetRunId(x.Addr)
			if rid1 != "" && rid1 == rid2 {
				return i
			}
		}
		return -1
	}()
	if index == -1 {
		return errors.Errorf("group-[%d] doesn't have server %s with runid = '%s'", g.Id, master, cache.GetRunId(master))
	}
	if index == 0 {
		return nil
	}
	defer s.dirtyGroupCache(g.Id)

	log.Warnf("group-[%d] will switch master to server[%d] = %s", g.Id, index, g.Servers[index].Addr)

	g.Servers[0], g.Servers[index] = g.Servers[index], g.Servers[0]
	g.OutOfSync = true
	return s.storeUpdateGroup(g)
}

func (s *Topom) EnableReplicaGroups(gid int, addr string, value bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	index, err := ctx.getGroupIndex(g, addr)
	if err != nil {
		return err
	}

	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}
	defer s.dirtyGroupCache(g.Id)

	if len(g.Servers) != 1 && ctx.isGroupInUse(g.Id) {
		g.OutOfSync = true
	}
	g.Servers[index].ReplicaGroup = value

	return s.storeUpdateGroup(g)
}

func (s *Topom) EnableReplicaGroupsAll(value bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	for _, g := range ctx.group {
		if g.Promoting.State != models.ActionNothing {
			return errors.Errorf("group-[%d] is promoting", g.Id)
		}
		defer s.dirtyGroupCache(g.Id)

		var dirty bool
		for _, x := range g.Servers {
			if x.ReplicaGroup != value {
				x.ReplicaGroup = value
				dirty = true
			}
		}
		if !dirty {
			continue
		}
		if len(g.Servers) != 1 && ctx.isGroupInUse(g.Id) {
			g.OutOfSync = true
		}
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}
	}
	return nil
}

func (s *Topom) SyncCreateAction(addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, index, err := ctx.getGroupByServer(addr)
	if err != nil {
		return err
	}
	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}

	if g.Servers[index].Action.State == models.ActionPending {
		return errors.Errorf("server-[%s] action already exist", addr)
	}
	defer s.dirtyGroupCache(g.Id)

	g.Servers[index].Action.Index = ctx.maxSyncActionIndex() + 1
	g.Servers[index].Action.State = models.ActionPending
	return s.storeUpdateGroup(g)
}

func (s *Topom) SyncRemoveAction(addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, index, err := ctx.getGroupByServer(addr)
	if err != nil {
		return err
	}
	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}

	if g.Servers[index].Action.State == models.ActionNothing {
		return errors.Errorf("server-[%s] action doesn't exist", addr)
	}
	defer s.dirtyGroupCache(g.Id)

	g.Servers[index].Action.Index = 0
	g.Servers[index].Action.State = models.ActionNothing
	return s.storeUpdateGroup(g)
}

func (s *Topom) SyncActionPrepare() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return "", err
	}

	addr := ctx.minSyncActionIndex()
	if addr == "" {
		return "", nil
	}

	g, index, err := ctx.getGroupByServer(addr)
	if err != nil {
		return "", err
	}
	if g.Promoting.State != models.ActionNothing {
		return "", nil
	}

	if g.Servers[index].Action.State != models.ActionPending {
		return "", errors.Errorf("server-[%s] action state is invalid", addr)
	}
	defer s.dirtyGroupCache(g.Id)

	log.Warnf("server-[%s] action prepare", addr)

	g.Servers[index].Action.Index = 0
	g.Servers[index].Action.State = models.ActionSyncing
	return addr, s.storeUpdateGroup(g)
}

func (s *Topom) SyncActionComplete(addr string, failed bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, index, err := ctx.getGroupByServer(addr)
	if err != nil {
		return nil
	}
	if g.Promoting.State != models.ActionNothing {
		return nil
	}

	if g.Servers[index].Action.State != models.ActionSyncing {
		return nil
	}
	defer s.dirtyGroupCache(g.Id)

	log.Warnf("server-[%s] action failed = %t", addr, failed)

	var state string
	if !failed {
		state = "synced"
	} else {
		state = "synced_failed"
	}
	g.Servers[index].Action.State = state
	return s.storeUpdateGroup(g)
}

func (s *Topom) newSyncActionExecutor(addr string) (func() error, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}

	g, index, err := ctx.getGroupByServer(addr)
	if err != nil {
		return nil, nil
	}

	if g.Servers[index].Action.State != models.ActionSyncing {
		return nil, nil
	}

	var master = "NO:ONE"
	if index != 0 {
		master = g.Servers[0].Addr
	}
	return func() error {
		c, err := redis.NewClient(addr, s.config.ProductAuth, time.Minute*30)
		if err != nil {
			log.WarnErrorf(err, "create redis client to %s failed", addr)
			return err
		}
		defer c.Close()
		if err := c.SetMaster(master); err != nil {
			log.WarnErrorf(err, "redis %s set master to %s failed", addr, master)
			return err
		}
		return nil
	}, nil
}

func (s *Topom) ExpansionExecDataSync(plan *expansionPlan) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	//禁止重复执行
	if plan.MajorStep > ExpansionActionDataSync || (plan.MajorStep == ExpansionActionDataSync && plan.MinorStep > ExpansionStepNothing) {
		return errors.New("DataSync has been execed!")
	}
	//修改数据同步的状态
	//plan.Action = 1
	//plan.Step = 1
	updatePlanError(plan, "-");
	updatePlanMajorStep(plan, ExpansionActionDataSync)
	updatePlanMinorStep(plan, ExpansionStepRunning)

	//断开目标组主从关系，防止级联情况下同步数据出现异常
	g, err := ctx.getGroup(plan.DstGid)
	if err != nil {
		log.WarnErrorf(err, "get group-[%s] info failed", plan.DstGid)
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, "get group-[" + strconv.Itoa(plan.DstGid) + "] info failed!");
		return err
	}

	for _, server := range g.Servers {
		c, err := redis.NewClient(server.Addr, s.config.ProductAuth, time.Second * 5)
		if err != nil {
			log.WarnErrorf(err, "creat [%s] client failed", server.Addr)
			//plan.Step = 0
			updatePlanMinorStep(plan, ExpansionStepNothing)
			updatePlanError(plan, "creat " + server.Addr + " client failed, Server may be down!");
			return err
		}

		if _, err := c.Do("slaveof", "no", "one"); err != nil {
			log.WarnErrorf(err, "%s: slaveof no one failed ", server.Addr)
			//plan.Step = 0
			updatePlanMinorStep(plan, ExpansionStepNothing)
			updatePlanError(plan, server.Addr + ": slaveof no one failed!");
			return err
		}
		c.Close()
	}

	srcAddr := ctx.getGroupMaster(plan.SrcGid)
	srcClient, err := redis.NewClient(srcAddr, s.config.ProductAuth, time.Second * 5)
	if err != nil {
		log.WarnErrorf(err, "creat [%s] client failed", srcAddr)
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, "creat " + srcAddr + " client failed, Server may be down!");
		return errors.New("Client connection failed, Server may be down!")	
	}
	defer srcClient.Close()

	dstAddr := ctx.getGroupMaster(plan.DstGid)
	dstClient, err := redis.NewClient(dstAddr, s.config.ProductAuth, time.Second * 5)
	if err != nil {
		log.WarnErrorf(err, "creat [%s] client failed", dstAddr)
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, "creat " + dstAddr + " client failed, Server may be down!");
		return errors.New("Client connection failed, Server may be down!")	
	}
	defer dstClient.Close()

	// config set slotmigrate no
	plan.Status = "[" + srcAddr + "] config set slotmigrate no"
	if _, err := srcClient.Do("config", "set", "slotmigrate", "no"); err != nil {
		log.WarnErrorf(err, "set [%s] slotmigrate no failed", srcAddr)
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, srcAddr + ": config set slotmigrate no failed");
		return errors.New("Setting slotmigrate mode failed!")
	}
	plan.Status = "[" + dstAddr + "] config set slotmigrate no"
	if _, err := dstClient.Do("config", "set", "slotmigrate", "no"); err != nil {
		log.WarnErrorf(err, "set [%s] slotmigrate no failed", srcAddr)
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, dstAddr + ": config set slotmigrate no failed");
		return errors.New("Setting slotmigrate mode failed!")
	}

	// del slots key
	plan.Status = "[" + srcAddr + "] del slots key"
	pikaVersion, ok := s.stats.servers[srcAddr].Stats["pika_version"]
	if !ok {
		pikaVersion = ""
	}
	if err := DelSlotsKey(srcClient, pikaVersion); err != nil {
		log.WarnErrorf(err, "[%s] del slots key failed", srcAddr)
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, srcAddr + ": del slots key failed");
		return errors.New("")
	}

	// config set db-sync-speed 70 and slaveof 
	syncSpeed := strconv.Itoa(plan.SyncSpeed)
	binlogNums := strconv.Itoa(plan.BinlogNums)

	plan.Status = "[" + srcAddr + "] config set db-sync-speed " + syncSpeed
	if _, err := srcClient.Do("config", "set", "db-sync-speed", syncSpeed); err != nil {
		log.WarnErrorf(err, "set [%s] db-sync-speed %s failed", srcAddr, syncSpeed)
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, srcAddr + ": config set db-sync-speed " + syncSpeed + " failed");
		return errors.New("Setting db-sync-speed failed!")
	}

	plan.Status = "[" + dstAddr + "] config set db-sync-speed " + syncSpeed
	if _, err := dstClient.Do("config", "set", "db-sync-speed", syncSpeed); err != nil {
		log.WarnErrorf(err, "set [%s] db-sync-speed %s failed", dstAddr, syncSpeed)
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, dstAddr + ": config set db-sync-speed " + syncSpeed + " failed");
		return errors.New("Setting db-sync-speed failed!")
	}

	//config set expire-logs-nums 500
	plan.Status = "[" + srcAddr + "] config set expire-logs-nums " + binlogNums
	if _, err := srcClient.Do("config", "set", "expire-logs-nums", binlogNums); err != nil {
		log.WarnErrorf(err, "set [%s] expire-logs-nums %s failed", srcAddr, binlogNums)
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, srcAddr + ": config set expire-logs-nums " + binlogNums + " failed");
		return errors.New("Setting expire-logs-nums " + binlogNums + " failed!")
	}

	plan.Status = "[" + dstAddr + "] config set expire-logs-nums " + binlogNums
	if _, err := dstClient.Do("config", "set", "expire-logs-nums", binlogNums); err != nil {
		log.WarnErrorf(err, "set master-[%s] expire-logs-nums %s failed", dstAddr, binlogNums)
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, srcAddr + ": config set expire-logs-nums " + syncSpeed + " failed");
		return errors.New("Setting expire-logs-nums " + syncSpeed + " failed!")
	}

	masterAddr := strings.Split(srcAddr, ":")
	if len(masterAddr) < 2 {
		log.Warnf("invalid src_addr: %s!",srcAddr)
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, "invalid src_addr: " + srcAddr);
		return errors.New("invalid src_addr!")
	}
	plan.Status = "[" + dstAddr + "] slaveof " + masterAddr[0] + " " + masterAddr[1] + " 0 0"
	if _, err := dstClient.Do("slaveof", masterAddr[0], masterAddr[1], "0", "0"); err != nil {
		log.WarnErrorf(err, "[%s] slaveof [%s] [%s] failed", dstAddr, masterAddr[0], masterAddr[1])
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, dstAddr + ": slaveof " + masterAddr[0]+ " " + masterAddr[1] + " failed");
		return errors.New("Slaveof failed!")
	}

	//plan.Step = 2
	//plan.Status = "-"
	updatePlanMinorStep(plan, ExpansionStepFinshed)
	updatePlanStatus(plan, "-")
	return nil
}

func (s *Topom) ExpansionExecBackup(plan *expansionPlan, force bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	//需要先执行第一步
	if plan.MajorStep < ExpansionActionDataSync || (plan.MajorStep == ExpansionActionDataSync && plan.MinorStep < ExpansionStepFinshed) {
		return errors.New("DataSync does not exec!")
	}

	//禁止重复执行
	if plan.MajorStep > ExpansionActionBackup || (plan.MajorStep == ExpansionActionBackup && plan.MinorStep > ExpansionStepNothing) {
		return errors.New("DataCopy has been execed!")
	}

	if !force {
		//plan.Status = "-"
		//plan.Action = 2
		//plan.Step = 2
		updatePlanError(plan, "-")
		updatePlanStatus(plan, "-")
		updatePlanMajorStep(plan, ExpansionActionBackup)
		updatePlanMinorStep(plan, ExpansionStepFinshed)
		return nil
	}
	//修改数据同步的状态
	updatePlanError(plan, "-")
	//plan.Action = 2
	//plan.Step = 1
	updatePlanMajorStep(plan, ExpansionActionBackup)
	updatePlanMinorStep(plan, ExpansionStepRunning)

	g, err := ctx.getGroup(plan.DstGid)
	if err != nil {
		log.WarnErrorf(err, "ZK: get group-[%d] info faild!", plan.DstGid)
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, "ZK: get group-[" + strconv.Itoa(plan.DstGid) + "] info faild!")
		return err
	}

	if len(g.Servers) <= 1 {
		log.WarnErrorf(err, "group-[%d] have no slave", plan.DstGid)
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, "have no slave in group-[" + strconv.Itoa(plan.DstGid) + "]")
		return nil
	}

	masterAddr := ctx.getGroupMaster(plan.DstGid)

	for i:=1; i<len(g.Servers); i++ {
		slaveAddr := g.Servers[i].Addr
		c, err := redis.NewClient(slaveAddr, s.config.ProductAuth, time.Second * 5)
		if err != nil {
			log.WarnErrorf(err, "creat [%s] client failed", slaveAddr)
			//plan.Step = 0
			updatePlanMinorStep(plan, ExpansionStepNothing)
			updatePlanError(plan, "creat " + slaveAddr + " client failed, Server may be down!");
			return errors.New("Client connection failed, Server may be down!")	
		}
		defer c.Close()

		masterIpPort := strings.Split(masterAddr, ":")
		if len(masterIpPort) < 2 {
			log.Warnf("invalid master addr: %s!",masterAddr)
			//plan.Step = 0
			updatePlanMinorStep(plan, ExpansionStepNothing)
			updatePlanError(plan, "invalid master addr: " + masterAddr);
			return errors.New("invalid master addr!")
		}
		plan.Status = "[" + slaveAddr + "] slaveof " + masterIpPort[0] + " " + masterIpPort[1] + " 0 0"
		if _, err := c.Do("slaveof", masterIpPort[0], masterIpPort[1], 0, 0); err != nil {
			log.WarnErrorf(err, "[%s] slaveof [%s] [%s] 0 0 failed", slaveAddr, masterIpPort[0], masterIpPort[1])
			//plan.Step = 0
			updatePlanMinorStep(plan, ExpansionStepNothing)
			updatePlanError(plan, slaveAddr + ": slaveof " + masterIpPort[0]+ " " + masterIpPort[1] + " 0 0 failed");
			return errors.New("Slaveof failed!")
		}
	}

	updatePlanMinorStep(plan, ExpansionStepFinshed)
	updatePlanStatus(plan, "-")
	return nil
}

func (s *Topom) ExpansionExecSlotsMgrt(plan *expansionPlan) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	//需要先执行第一步
	if plan.MajorStep < ExpansionActionBackup || (plan.MajorStep == ExpansionActionBackup && plan.MinorStep < ExpansionStepFinshed) {
		return errors.New("DataSync does not exec!")
	}

	//禁止重启执行
	if plan.MajorStep > ExpansionActionSlotsMgrt || (plan.MajorStep == ExpansionActionSlotsMgrt && plan.MinorStep > ExpansionStepNothing) {
		return errors.New("SlotsMgrt has been execed!")
	}

	//plan.Action = 3
	//plan.Step = 1
	updatePlanError(plan, "-");
	updatePlanMajorStep(plan, ExpansionActionSlotsMgrt)
	updatePlanMinorStep(plan, ExpansionStepRunning)
	
	srcAddr := ctx.getGroupMaster(plan.SrcGid)
	srcClient, err := redis.NewClient(srcAddr, s.config.ProductAuth, time.Second * 5)
	if err != nil {
		log.WarnErrorf(err, "creat [%s] client failed", srcAddr)
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, "creat " + srcAddr + " client failed");
		return errors.New("Client connection failed, Server may be down!")	
	}
	defer srcClient.Close()

	dstAddr := ctx.getGroupMaster(plan.DstGid)
	dstClient, err := redis.NewClient(dstAddr, s.config.ProductAuth, time.Second * 5)
	if err != nil {
		log.WarnErrorf(err, "creat [%s] client failed", dstAddr)
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, "creat " + dstAddr + " client failed");
		return errors.New("Client connection failed, Server may be down!")	
	}
	defer dstClient.Close()

	if !isbinlogApproach(srcClient, dstClient) {
		log.WarnErrorf(err, "too much difference of binlog offset")
		updatePlanMajorStep(plan, ExpansionActionBackup)
		updatePlanMinorStep(plan, ExpansionStepFinshed)
		return errors.New("too much difference of binlog offset!")
	}

	masterIpPort := strings.Split(srcAddr, ":")
	if len(masterIpPort) < 2 {
		log.Warnf("invalid src_addr: %s!",srcAddr)
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, "invalid src_addr: " + srcAddr);
		return errors.New("invalid src_addr!")
	}
	plan.Status = "[" + dstAddr + "] slaveof " + masterIpPort[0] + " " + masterIpPort[1]
	if _, err := dstClient.Do("slaveof", masterIpPort[0], masterIpPort[1]); err != nil {
		log.WarnErrorf(err, "[%s] slaveof [%s] [%s] failed", dstAddr, masterIpPort[0], masterIpPort[1])
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, dstAddr + ": slaveof " + masterIpPort[0]+ " " + masterIpPort[1] + " failed");
		return errors.New("Slaveof failed!")
	}

	noSlotsAction := true
	defer func() {
		if noSlotsAction {
			srcClient.Do("config", "set", "slave-read-only", "no")
			srcClient.Do("config", "set", "slotmigrate", "no")
		}
	}()

	// config set slave-read-only yes
	plan.Status = "[" + srcAddr + "] config set slave-read-only yes"
	if _, err := srcClient.Do("config", "set", "slave-read-only", "yes"); err != nil {
		log.WarnErrorf(err, "set [%s] slave-read-only yes failed", srcAddr)
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, srcAddr + ": config set  slave-read-only yes failed");
		return errors.New("Setting slave-read-only mode failed!")
	}
	plan.Status = "[" + srcAddr + "] config set slotmigrate yes"
	if _, err := srcClient.Do("config", "set", "slotmigrate", "yes"); err != nil {
		log.WarnErrorf(err, "set [%s] slotmigrate yes failed", srcAddr)
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, srcAddr + ": config set slotmigrate yes failed");
		return errors.New("Setting slotmigrate mode failed!")
	}

	//master 与 slave 偏移量是否一致
	plan.Status = "wait binlog_offset same"
	binlogSame := false
	for i:=0; i<50; i++ {
		masterFilenum, masterOffset, err := srcClient.BinlogOffset()
		if err != nil {
			log.WarnErrorf(err, "get [%s] binlog offset failed", srcAddr)
			//plan.Step = 0
			updatePlanMinorStep(plan, ExpansionStepNothing)
			updatePlanError(plan, srcAddr + ": get  binlog offset failed");
			return errors.New("Failed to get binlog offset!")
		}
		slaveFilenum, slaveOffset, err := dstClient.BinlogOffset()
		if err != nil {
			log.WarnErrorf(err, "get [%s] binlog offset failed", dstAddr)
			//plan.Step = 0
			updatePlanMinorStep(plan, ExpansionStepNothing)
			updatePlanError(plan, dstAddr + ": get  binlog offset failed");
			return errors.New("Failed to get binlog offset!")
		}

		/*if masterFilenum != slaveFilenum  {
			log.WarnErrorf(err, "too much difference of binlog offset")
			updatePlanMajorStep(plan, 2)
			updatePlanMinorStep(plan, 2)
			return errors.New("too much difference of binlog offset!")
		}*/

		if (masterFilenum == slaveFilenum && masterOffset == slaveOffset) {
			binlogSame = true
			break;
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !binlogSame {
		log.WarnErrorf(err, "timeout: binlog offset not same in 5s!")
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, "timeout: binlog offset not same in 5s!");
		return errors.New("timeout: binlog offset not same in 5s!")
	}

	// slaveof no one
	plan.Status = "[" + dstAddr + "] slaveof no one"
	if _, err := dstClient.Do("slaveof", "no", "one"); err != nil {
		log.WarnErrorf(err, "[%s] slaveof no one failed", dstAddr)
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, dstAddr + ": slaveof no one failed");
		return errors.New("Slaveof no one failed!")
	}

	//迁移slots
	plan.Status = "migrate Slots-[" + plan.SlotsList + "] to Group-" + strconv.Itoa(plan.DstGid)
	//log.Warnf("start call ExpansionMgrtSlots")
	if err := s.ExpansionMgrtSlots(plan.SlotsArr, plan.DstGid, false); err != nil {
		log.WarnErrorf(err, "create slots mgrt action failed")
		//plan.Step = 0
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, "create slots mgrt action failed");
		return err
	}
	noSlotsAction = false
	//log.Warnf("end call ExpansionMgrtSlots")

	plan.Status = "wait slots action finsh"
	go func(plan *expansionPlan, addr string) {
		//log.Warnf("end ExpansionMgrtSlots to recover")
		timeout := true
		for i:=0; i<30; i++ {
			if s.isSlotsActionFinsh(plan.SlotsArr) {
				timeout = false
				break
			}
			time.Sleep(500 * time.Millisecond)
		}

		//plan.Step = 2
		updatePlanMinorStep(plan, ExpansionStepFinshed)
		if timeout {
			//plan.Status = "SlotMgrt timeout"
			//plan.Step = 0
			updatePlanMinorStep(plan, ExpansionStepNothing)
			updatePlanError(plan, "SlotMgrt timeout");
			log.Warnf("SlotMgrt timeout")
		} else {
			//plan.Status = "SlotsMgrt finshed"
			updatePlanStatus(plan, "SlotsMgrt finshed")
		}

		c, err := redis.NewClient(addr, s.config.ProductAuth, time.Second * 5)
		if err != nil {
			log.WarnErrorf(err, "creat [%s] client failed", addr)
			//plan.Step = 0
			updatePlanMinorStep(plan, ExpansionStepNothing)
			updatePlanError(plan, plan.Status + ": creat " + addr + " client failed");
			return
		}
		defer c.Close()

		//plan.Status = "[" + addr + "] config set slotmigrate no"
		if _, err := c.Do("config", "set", "slotmigrate", "no"); err != nil {
			log.WarnErrorf(err, "set [%s] slotmigrate no failed", addr)
			//plan.Step = 0
			updatePlanMinorStep(plan, ExpansionStepNothing)
			updatePlanError(plan, plan.Status + ": [" + addr + "] config set slotmigrate no failed");
			return
		}
		//plan.Status = "[" + addr + "] config set slave-read-only no"
		if _, err := c.Do("config", "set", "slave-read-only", "no"); err != nil {
			log.WarnErrorf(err, "set [%s] slave-read-only no failed", addr)
			//plan.Step = 0
			updatePlanMinorStep(plan, ExpansionStepNothing)
			updatePlanError(plan, plan.Status + ": [" + addr + "] config set slave-read-only no failed");
			return
		}
	}(plan, srcAddr)

	return nil
}

func (s *Topom) ExpansionExecDateClean(plan *expansionPlan) error {
	if plan.MajorStep < ExpansionActionSlotsMgrt || (plan.MajorStep == ExpansionActionSlotsMgrt && plan.MinorStep < ExpansionStepFinshed) {
		return errors.New("SlotsMgrt does not exec!") 
	}

	if plan.MajorStep == ExpansionActionDataClean && plan.MinorStep > ExpansionStepCompact {
		return errors.New("DateClean has finshed!") 
	}
	updatePlanError(plan, "-")
	if plan.MajorStep == ExpansionActionSlotsMgrt && plan.MinorStep == ExpansionStepFinshed {
		updatePlanError(plan, "-");
		updatePlanMajorStep(plan, ExpansionActionDataClean)
		updatePlanMinorStep(plan, ExpansionStepSlotsreload)
	}
	
	switch plan.MinorStep {

	case ExpansionStepSlotsreload:
		updatePlanStatus(plan, "start slotsreload")
		if err := s.DateCleanSlotsreload(plan.SrcGid); err != nil {
			log.WarnErrorf(err, "group-[%d] slotsreload failed", plan.SrcGid)
			updatePlanError(plan, "group-[" + strconv.Itoa(plan.SrcGid) + "] slotsreload failed");
			return err
		}
		if err := s.DateCleanSlotsreload(plan.DstGid); err != nil {
			log.WarnErrorf(err, "group-[%d] slotsreload failed", plan.DstGid)
			updatePlanError(plan, "group-[" + strconv.Itoa(plan.DstGid) + "] slotsreload failed");
			return err
		}
		updatePlanMinorStep(plan, ExpansionStepSlotsdel)
		break

	case ExpansionStepSlotsdel:
		if err := s.isSlotsreloadFinsh(plan); err != nil {
			return err
		} 
		updatePlanStatus(plan, "start slotsdel")
		if err := s.DateCleanSlotsdel(plan.SrcGid); err != nil {
			log.WarnErrorf(err, "group-[%d] slotsdel failed", plan.SrcGid)
			updatePlanError(plan, "group-[" + strconv.Itoa(plan.SrcGid) + "] slotsdel failed");
			return err
		}
		if err := s.DateCleanSlotsdel(plan.DstGid); err != nil {
			log.WarnErrorf(err, "group-[%d] slotsdel failed", plan.DstGid)
			updatePlanError(plan, "group-[" + strconv.Itoa(plan.DstGid) + "] slotsdel failed");
			return err
		}
		updatePlanMinorStep(plan, ExpansionStepDelSlotsKey)
		break
	
	case ExpansionStepDelSlotsKey:
		if err := s.isSlotsDelFinsh(plan); err != nil {
			return err
		} 
		updatePlanStatus(plan, "start Delslotskey")
		if err := s.DateCleanDelSlotsKey(plan.SrcGid); err != nil {
			log.WarnErrorf(err, "group-[%d] del slots key failed", plan.SrcGid)
			updatePlanError(plan, "group-[" + strconv.Itoa(plan.SrcGid) + "] del slots key failed");
			return err
		}
		if err := s.DateCleanDelSlotsKey(plan.DstGid); err != nil {
			log.WarnErrorf(err, "group-[%d] del slots key failed", plan.DstGid)
			updatePlanError(plan, "group-[" + strconv.Itoa(plan.DstGid) + "] del slots key failed");
			return err
		}
		updatePlanStatus(plan, "Delslotskey end")
		updatePlanMinorStep(plan, ExpansionStepCompact)
		break
	
	case ExpansionStepCompact:
		updatePlanStatus(plan, "start Compact")
		if err := s.DateCleanCompact(plan.SrcGid); err != nil {
			log.WarnErrorf(err, "group-[%d] compact failed", plan.SrcGid)
			updatePlanError(plan, "group-[" + strconv.Itoa(plan.SrcGid) + "] compact failed");
			return err
		}
		if err := s.DateCleanCompact(plan.DstGid); err != nil {
			log.WarnErrorf(err, "group-[%d] compact failed", plan.DstGid)
			updatePlanError(plan, "group-[" + strconv.Itoa(plan.DstGid) + "] compact failed");
			return err
		}
		updatePlanMinorStep(plan, ExpansionDataCleanFinshed)

	default:
		return errors.Errorf("DateClean has finshed!")
	}
	return nil
}

/*func (s *Topom) ExpansionExecDateClean(plan *expansionPlan) error {
	if plan.MajorStep < ExpansionActionSlotsMgrt || (plan.MajorStep == ExpansionActionSlotsMgrt && plan.MinorStep == ExpansionStepNothing) {
		return errors.New("SlotsMgrt does not exec!") 
	}

	if plan.MajorStep == ExpansionActionDataClean && plan.MinorStep > ExpansionStepNothing {
		return errors.New("DateClean has been exec!") 
	}
	
	//plan.Action = 4
	//plan.Step = 1
	updatePlanError(plan, "-");
	updatePlanMajorStep(plan, 4)
	updatePlanMinorStep(plan, 1)
	log.Warnf("group %d: start ExpansionDateCleanByGid", plan.SrcGid)
	go s.ExpansionDateCleanByGid(plan.SrcGid, plan)
	log.Warnf("group %d: start ExpansionDateCleanByGid", plan.DstGid)
	go s.ExpansionDateCleanByGid(plan.DstGid, plan)
	return nil
}*/

func isbinlogApproach(srcClient, dstClient *redis.Client) bool {
	if srcClient == nil || dstClient == nil {
		return false
	}
	masterFilenum, _, err := srcClient.BinlogOffset()
	if err != nil {
		return false
	}
	slaveFilenum, _, err := dstClient.BinlogOffset()
	if err != nil {
		return false
	}

	return masterFilenum == slaveFilenum
}

func (s *Topom) isSlotsreloadFinsh(plan *expansionPlan) error {
	err, ok := s.isSlotsreloadOff(plan.SrcGid)
	if err != nil {
		log.WarnErrorf(err, "group-[%d] wait slotsreload err", plan.SrcGid)
		updatePlanError(plan, "group-[" + strconv.Itoa(plan.SrcGid) + "] wait slotsreload err");
		return err
	} else if !ok {
		return errors.New("Slotsreload is running")
	}

	err, ok = s.isSlotsreloadOff(plan.DstGid)
	if err != nil {
		log.WarnErrorf(err, "group-[%d] wait slotsreload err", plan.DstGid)
		updatePlanError(plan, "group-[" + strconv.Itoa(plan.DstGid) + "] wait slotsreload err");
		return err
	}

	if !ok {
		return errors.New("Slotsreload is running")
	}

	return nil
}

func (s *Topom) isSlotsDelFinsh(plan *expansionPlan) error {
	err, ok := s.isSlotsdelOff(plan.SrcGid)
	if err != nil {
		log.WarnErrorf(err, "group-[%d] wait slotsdel err", plan.SrcGid)
		updatePlanError(plan, "group-[" + strconv.Itoa(plan.SrcGid) + "] wait slotsdel err");
		return err
	} else if !ok {
		return errors.New("Slotsdel is running")
	}

	err, ok = s.isSlotsdelOff(plan.DstGid)
	if err != nil {
		log.WarnErrorf(err, "group-[%d] wait slotsdel err", plan.DstGid)
		updatePlanError(plan, "group-[" + strconv.Itoa(plan.DstGid) + "] wait slotsdel err");
		return err
	}

	if !ok {
		return errors.New("Slotsdel is running")
	}

	return nil
}

func (s *Topom) ExpansionGroupDateClean(gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	//用来检测gid对应的master是否正常
	addr := ctx.getGroupMaster(gid)
	c, err := redis.NewClient(addr, s.config.ProductAuth, time.Second * 5)
	if err != nil {
		log.WarnErrorf(err, "creat [%s] client failed", addr)
		return err
	}
	defer c.Close()

	go s.ExpansionDateCleanByGid(gid, nil)

	return nil
}

func (s *Topom) ExpansionDateCleanByGid (gid int, plan *expansionPlan) {
	// slotsreload
	log.Warnf("slotsreload: gid: %d", gid)
	updatePlanStatus(plan, "group-[" + strconv.Itoa(gid) + "] slotsreload")
	//plan.Status = "group-[" + strconv.Itoa(gid) + "] slotsreload"
	if err := s.DateCleanSlotsreload(gid); err != nil {
		log.WarnErrorf(err, "group-[%d] slotsreload failed", gid)
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, "group-[" + strconv.Itoa(gid) + "] slotsreload failed");
		return 
	}

	// 等待slotsreload结束
	log.Warnf("wait slotsreload off : gid: %d", gid)
	for ;; {
		time.Sleep(30 * time.Second)
		err, ok := s.isSlotsreloadOff(gid)
		if err != nil {
			log.WarnErrorf(err, "group-[%d] wait slotsreload err", gid)
			updatePlanMinorStep(plan, ExpansionStepNothing)
			updatePlanError(plan, "group-[" + strconv.Itoa(gid) + "] wait slotsreload err");
			return
		} else if ok {
			break
		}
	}

	// slotsdel
	updatePlanStatus(plan, "group-[" + strconv.Itoa(gid) + "] slotsdel")
	log.Warnf("DateClean-Slotsdel : gid: %d", gid)
	s.DateCleanSlotsdel(gid)
	

	// 等待slotsdel执行结束：删除slots相关key
	log.Warnf("wait slotsdel off : gid: %d", gid)
	for ;; {
		time.Sleep(30 * time.Second)
		err, ok := s.isSlotsdelOff(gid)
		if err != nil {
			log.WarnErrorf(err, "group-[%d] wait slotsdel err", gid)
			updatePlanMinorStep(plan, ExpansionStepNothing)
			updatePlanError(plan, "group-[" + strconv.Itoa(gid) + "] wait slotsdel err");
			return
		} else if ok {
			break
		}
	}

	log.Warnf("DateCleanDelSlotsKey : gid: %d", gid)
	updatePlanStatus(plan, "group-[" + strconv.Itoa(gid) + "] del slots key")
	if err := s.DateCleanDelSlotsKey(gid); err != nil {
		log.WarnErrorf(err, "group-[%d] DateClean-DelSlotsKey failed", gid)
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, "group-[" + strconv.Itoa(gid) + "] DateClean-DelSlotsKey failed");
		return
	}

	//所有实例进行compact
	log.Warnf("compact : gid: %d", gid)
	updatePlanStatus(plan, "group-[" + strconv.Itoa(gid) + "] compact")
	if err := s.DateCleanCompact(gid); err != nil {
		log.WarnErrorf(err, "group-[%d] compact failed", gid)
		updatePlanMinorStep(plan, ExpansionStepNothing)
		updatePlanError(plan, "group-[" + strconv.Itoa(gid) + "] compact failed");
		return
	}

	if plan != nil && plan.MinorStep > ExpansionStepNothing {
		plan.MinorStep++
		if plan.MinorStep > 2 {
			//plan.Status = "DateClean finshed"
			updatePlanStatus(plan, "DateClean finshed")
		}
	}
}

func (s *Topom) DateCleanSlotsreload (gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	addr := ctx.getGroupMaster(gid)
	c, err := redis.NewClient(addr, s.config.ProductAuth, time.Second * 5)
	if err != nil {
		log.WarnErrorf(err, "creat [%s] client failed", addr)
		return err
	}
	defer c.Close()

	if _, err := c.Do("slotsreload"); err != nil {
		log.WarnErrorf(err, "%s: slotsreload failed ", addr)
		return err
	}
	return nil
}

func (s *Topom) DateCleanSlotsdel (gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	addr := ctx.getGroupMaster(gid)
	c, err := redis.NewClient(addr, s.config.ProductAuth, time.Second * 5)
	if err != nil {
		log.WarnErrorf(err, "creat [%s] client failed", addr)
		return err
	}
	defer c.Close()

	for _, m := range ctx.slots {
		if m.GroupId == gid || m.Action.TargetId == gid {
			continue
		}
		//有可能漏掉
		if _, err := c.Do("slotsdel", strconv.Itoa(m.Id)); err != nil {
			log.WarnErrorf(err, "%d: slotsdel failed ", m.Id)
			return err
		}
	}
	return nil
}

func (s *Topom) isSlotsreloadOff(gid int) (error, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err, false
	}

	addr := ctx.getGroupMaster(gid)
	isSlotsReloading, ok := s.stats.servers[addr].Stats["is_slots_reloading"]
	if !ok {
		return errors.New("'is_slots_reloading' status is not exsit!"), false
	}

	return nil, strings.Contains(isSlotsReloading, "No")
}

func (s *Topom) isSlotsdelOff(gid int) (error, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err, false
	}

	addr := ctx.getGroupMaster(gid)
	isSlotsDeleting, ok := s.stats.servers[addr].Stats["is_slots_deleting"]
	if !ok {
		return errors.New("'is_slots_reloading' status is not exsit!"), false
	}

	return nil, strings.Contains(isSlotsDeleting, "No")
}

/*func getSlotNo(status string) int {
	if strings.Contains(status, "[") {
		status_right := strings.Split(status, "[")[1]
		if strings.Contains(status_right, "]") {
			slot := strings.Split(status_right, "]")[0]
			slotno, err := strconv.Atoi(slot)
			if err == nil {
				return slotno
			}
		}
	}
	return -1
}*/

func (s *Topom) DateCleanDelSlotsKey(gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	addr := ctx.getGroupMaster(gid)
	c, err := redis.NewClient(addr, s.config.ProductAuth, time.Second * 5)
	if err != nil {
		log.WarnErrorf(err, "creat [%s] client failed", addr)
		return err	
	}
	defer c.Close()

	pikaVersion, ok := s.stats.servers[addr].Stats["pika_version"]
	if !ok {
		pikaVersion = ""
	}

	if err := DelSlotsKey(c, pikaVersion); err != nil {
		return err
	}
	return nil
}

func DelSlotsKey(c *redis.Client, pikaVersion string) error {
	if c == nil {
		return errors.New("invalid client")
	}

	prefix := "_internal:slotkey:4migrate:"
	if pikaVersion == "" {
		prefix = "_internal:slotkey:4migrate:" 
	} else {
		versionMajor, err := strconv.Atoi(pikaVersion[0:1])
		if err != nil {
			versionMajor = 0
		}
		
		if pikaVersion != "" && pikaVersion != "3.5-2.2.6" && versionMajor >= 3 {
			prefix = "_internal:4migrate:slotkey:"
		} else {
			prefix = "_internal:slotkey:4migrate:"
		}
	}

	for i:=0; i<1024; i++ {
		if err := c.Del(prefix+strconv.Itoa(i)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Topom) DateCleanCompact(gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}

	for _, server := range g.Servers {
		c, err := redis.NewClient(server.Addr, s.config.ProductAuth, time.Second * 5)
		if err != nil {
			log.WarnErrorf(err, "creat [%s] client failed", server.Addr)
			return err
		}

		if _, err := c.Do("compact"); err != nil {
			log.WarnErrorf(err, "%s: compact failed ", server.Addr)
			return err
		}
		c.Close()
	}
	return nil
}

func (s *Topom) IsInvalidSrcGroup(gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if ctx.group[gid] == nil {
		//return errors.New("Group is not exsit!")
		return errors.Errorf("Group-[%d] is not exsit!", gid)
	}

	for _, m := range ctx.slots {
		if m.Action.State != models.ActionNothing {
			return errors.New("have migrate action")
		} 
	}
	return nil
}

func (s *Topom) IsInvalidDstGroup(gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if ctx.group[gid] == nil {
		return errors.Errorf("Group-[%d] is not exsit!", gid)
	}

	for _, m := range ctx.slots {
		if m.GroupId == gid || m.Action.TargetId == gid {
			return errors.New("dst_group have slots")
		} 
	}
	return nil
}

func updatePlanStatus(plan *expansionPlan, status string) {
	if plan != nil {
		plan.mu.Lock()
		defer plan.mu.Unlock()
		plan.Status = status
	}
}

func updatePlanError(plan *expansionPlan, err string) {
	if plan != nil {
		plan.mu.Lock()
		defer plan.mu.Unlock()
		plan.Error = err
	}
}

func updatePlanMajorStep(plan *expansionPlan, step int) {
	if plan != nil {
		plan.mu.Lock()
		defer plan.mu.Unlock()
		plan.MajorStep = step
	}
}

func updatePlanMinorStep(plan *expansionPlan, step int) {
	if plan != nil {
		plan.mu.Lock()
		defer plan.mu.Unlock()
		plan.MinorStep = step
	}
}

func (s *Topom) UpgrateStart(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := os.Stat(path + "/codis-dashboard")
	if err != nil {
		return err
	}

	_, err = os.Stat(path + "/dashboard.toml")
	if err != nil {
		return err
	}

	log.Warnf("restart codis-dashboard.")
	exec.Command("rm", "./codis-dashboard").Run()
	exec.Command("cp", path + "/codis-dashboard", "./").Run()
	exec.Command("rm", "./dashboard.toml").Run()
	exec.Command("cp", path + "/dashboard.toml", "./").Run()
	
	cmd := exec.Command("./codis-dashboard", "-c", "dashboard.toml", "-s", "restart")
	cmd.Start()
	
	return nil
}