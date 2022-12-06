// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"fmt"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

type forwardMethod interface {
	GetId() int
	Forward(s *Slot, r *Request, hkey []byte) error
}

var (
	ErrSlotIsNotReady = errors.New("slot is not ready, may be offline")
	ErrRespIsRequired = errors.New("resp is required")
)

type forwardSync struct {
	forwardHelper
}

func (d *forwardSync) GetId() int {
	return models.ForwardSync
}

func (d *forwardSync) Forward(s *Slot, r *Request, hkey []byte) error {
	s.lock.RLock()
	bc, err := d.process(s, r, hkey)
	s.lock.RUnlock()
	if err != nil {
		//返回error时，process不会将执行r.Group.Add(1)
		//直接将error返回给上游，上游会处理没有加入队列的命令
		return err
	}

	//理论上bc是不可能为空的，后端连接为空时处理逻辑
	if bc == nil {
		//这种情况理论上不会发生，同步执行时只有d.forward2(s, r)返回为nil这种情况
		//但是d.forward2()中写死必须返回bc，因此理论上bc不可能为空
		//命令已经add过slot对应的计数器，因此这里要对slot计数器执行减一操作
		SetResponse(r, nil, fmt.Errorf("backend conn failure, backend is nil"))
		return nil
	}

	//如果连接不可用，直接返回错误，不加入bc队列
	if bc.IsConnectDown() {
		//bc 不为空则一定执行过r.Group.Add(1)
		//命令已经add过slot对应的计数器，因此这里要对slot计数器执行减一操作
		SetResponse(r, nil, fmt.Errorf("backend conn failure, connect is down"))
		return nil
	}

	bc.PushBack(r)
	return nil
}

func (d *forwardSync) process(s *Slot, r *Request, hkey []byte) (*BackendConn, error) {
	if s.backend.bc == nil {
		log.Debugf("slot-%04d is not ready: hash key = '%s'",
			s.id, hkey)
		return nil, ErrSlotIsNotReady
	}
	if s.migrate.bc != nil && len(hkey) != 0 {
		if err := d.slotsmgrt(s, hkey, r.Database, r.Seed16()); err != nil {
			log.Debugf("slot-%04d migrate from = %s to %s failed: hash key = '%s', database = %d, error = %s",
				s.id, s.migrate.bc.Addr(), s.backend.bc.Addr(), hkey, r.Database, err)
			return nil, err
		}
	}

	//必须持有slot对应的锁时，执行Add操作，
	//这样blockAndWait()持有slot锁后只需判断refs是否为0，就可以保证没有线程在操作slot
	r.Group = &s.refs
	r.Group.Add(1)

	return d.forward2(s, r), nil
}

type forwardSemiAsync struct {
	forwardHelper
}

func (d *forwardSemiAsync) GetId() int {
	return models.ForwardSemiAsync
}

func (d *forwardSemiAsync) Forward(s *Slot, r *Request, hkey []byte) error {
	var loop int
	for {
		s.lock.RLock()
		bc, retry, err := d.process(s, r, hkey)
		s.lock.RUnlock()

		switch {
		case err != nil:
			//返回error时，process不会将执行r.Group.Add(1)
			//直接将error返回给上游，上游会处理没有加入队列的命令
			return err
		case !retry:
			//不需要重试
			if bc != nil {
				//bc 不为空则一定执行过r.Group.Add(1)
				//如果连接不可用，直接返回错误，不加入bc队列
				if bc.IsConnectDown() {
					SetResponse(r, nil, fmt.Errorf("backend conn failure, connect is down"))
					return nil
				}
				bc.PushBack(r)
			}

			//bc == nil 只能是迁移失败，其他情况理论上不为nil
			//slot正在迁移，并且key没有迁移完成，此时还没有执行r.Group.Add(1)，
			//直接将error返回给上游，上游会处理没有加入队列的命令

			return nil
		}

		var delay time.Duration
		switch {
		case loop < 5:
			delay = 0
		case loop < 20:
			delay = time.Millisecond * time.Duration(loop)
		default:
			delay = time.Millisecond * 20
		}
		time.Sleep(delay)

		if r.IsBroken() {
			return ErrRequestIsBroken
		}
		loop += 1
	}
}

func (d *forwardSemiAsync) process(s *Slot, r *Request, hkey []byte) (_ *BackendConn, retry bool, _ error) {
	if s.backend.bc == nil {
		log.Debugf("slot-%04d is not ready: hash key = '%s'",
			s.id, hkey)
		return nil, false, ErrSlotIsNotReady
	}
	if s.migrate.bc != nil && len(hkey) != 0 {
		resp, moved, err := d.slotsmgrtExecWrapper(s, hkey, r.Database, r.Seed16(), r.Multi)
		switch {
		case err != nil:
			log.Debugf("slot-%04d migrate from = %s to %s failed: hash key = '%s', error = %s",
				s.id, s.migrate.bc.Addr(), s.backend.bc.Addr(), hkey, err)
			return nil, false, err
		case !moved:
			switch {
			case resp != nil:
				r.Resp = resp
				return nil, false, nil
			}
			return nil, true, nil
		}
	}
	r.Group = &s.refs
	r.Group.Add(1)
	return d.forward2(s, r), false, nil
}

type forwardHelper struct {
}

func (d *forwardHelper) slotsmgrt(s *Slot, hkey []byte, database int32, seed uint) error {
	m := &Request{}
	m.Multi = []*redis.Resp{
		redis.NewBulkBytes([]byte("SLOTSMGRTTAGONE")),
		redis.NewBulkBytes(s.backend.bc.host),
		redis.NewBulkBytes(s.backend.bc.port),
		redis.NewBulkBytes([]byte("3000")),
		redis.NewBulkBytes(hkey),
	}
	m.Batch = &sync.WaitGroup{}

	//设置命令标志位
	opstr, flag, _, _, err := getOpInfo(m.Multi)
	if err != nil {
		return err
	}
	m.OpStr = opstr
	m.OpFlag = flag

	s.migrate.bc.BackendConn(database, seed, m.OpFlag.IsQuick(), true).PushBack(m)

	m.Batch.Wait()

	if err := m.Err; err != nil {
		return err
	}
	switch resp := m.Resp; {
	case resp == nil:
		return ErrRespIsRequired
	case resp.IsError():
		return fmt.Errorf("bad slotsmgrt resp: %s", resp.Value)
	case resp.IsInt():
		log.Debugf("slot-%04d migrate from %s to %s: hash key = %s, database = %d, resp = %s",
			s.id, s.migrate.bc.Addr(), s.backend.bc.Addr(), hkey, database, resp.Value)
		return nil
	default:
		return fmt.Errorf("bad slotsmgrt resp: should be integer, but got %s", resp.Type)
	}
}

func (d *forwardHelper) slotsmgrtExecWrapper(s *Slot, hkey []byte, database int32, seed uint, multi []*redis.Resp) (_ *redis.Resp, moved bool, _ error) {
	m := &Request{}
	m.Multi = make([]*redis.Resp, 0, 2+len(multi))
	m.Multi = append(m.Multi,
		redis.NewBulkBytes([]byte("SLOTSMGRT-EXEC-WRAPPER")),
		redis.NewBulkBytes(hkey),
	)
	m.Multi = append(m.Multi, multi...)
	m.Batch = &sync.WaitGroup{}

	//设置命令标志位
	opstr, flag, _, _, err := getOpInfo(m.Multi)
	if err != nil {
		return nil, false, err
	}
	m.OpStr = opstr
	m.OpFlag = flag

	s.migrate.bc.BackendConn(database, seed, m.OpFlag.IsQuick(), true).PushBack(m)

	m.Batch.Wait()

	if err := m.Err; err != nil {
		return nil, false, err
	}
	switch resp := m.Resp; {
	case resp == nil:
		return nil, false, ErrRespIsRequired
	case resp.IsError():
		return nil, false, fmt.Errorf("bad slotsmgrt-exec-wrapper resp: %s", resp.Value)
	case resp.IsArray():
		if len(resp.Array) != 2 {
			return nil, false, fmt.Errorf("bad slotsmgrt-exec-wrapper resp: array.len = %d",
				len(resp.Array))
		}
		if !resp.Array[0].IsInt() || len(resp.Array[0].Value) != 1 {
			return nil, false, fmt.Errorf("bad slotsmgrt-exec-wrapper resp: type(array[0]) = %s, len(array[0].value) = %d",
				resp.Array[0].Type, len(resp.Array[0].Value))
		}
		switch resp.Array[0].Value[0] - '0' {
		case 0:
			return nil, true, nil
		case 1:
			return nil, false, nil
		case 2:
			return resp.Array[1], false, nil
		default:
			return nil, false, fmt.Errorf("bad slotsmgrt-exec-wrapper resp: [%s] %s",
				resp.Array[0].Value, resp.Array[1].Value)
		}
	default:
		return nil, false, fmt.Errorf("bad slotsmgrt-exec-wrapper resp: should be integer, but got %s", resp.Type)
	}
}

func (d *forwardHelper) forward2(s *Slot, r *Request) *BackendConn {
	var database = r.Database
	if s.migrate.bc == nil && !r.IsMasterOnly() && len(s.replicaGroups) != 0 {
		var seed = r.Seed16()
		for _, group := range s.replicaGroups {
			var i = seed
			for range group {
				i = (i + 1) % uint(len(group))
				if bc := group[i].BackendConn(database, seed, r.OpFlag.IsQuick(), false); bc != nil {
					return bc
				}
			}
		}
	}

	//相同slot命令转发到相同是后端连接上，防止hset xxx；expire xxx；
	return s.backend.bc.BackendConn(database, uint(s.id), r.OpFlag.IsQuick(), true)
}
