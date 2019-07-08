// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"io/ioutil"
	"os"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"github.com/CodisLabs/codis/pkg/utils/sync2"
)

func (s *Topom) ProcessSlotAction() error {
	for s.IsOnline() {
		var (
			marks = make(map[int]bool)
			plans = make(map[int]bool)
		)
		var accept = func(m *models.SlotMapping) bool {
			if marks[m.GroupId] || marks[m.Action.TargetId] {
				return false
			}
			if plans[m.Id] {
				return false
			}
			return true
		}
		var update = func(m *models.SlotMapping) bool {
			if m.GroupId != 0 {
				marks[m.GroupId] = true
			}
			marks[m.Action.TargetId] = true
			plans[m.Id] = true
			return true
		}
		var parallel = math2.MaxInt(1, s.config.MigrationParallelSlots)
		for parallel > len(plans) {
			_, ok, err := s.SlotActionPrepareFilter(accept, update)
			if err != nil {
				return err
			} else if !ok {
				break
			}
		}
		if len(plans) == 0 {
			return nil
		}
		var fut sync2.Future
		for sid, _ := range plans {
			fut.Add()
			go func(sid int) {
				log.Warnf("slot-[%d] process action", sid)
				var err = s.processSlotAction(sid)
				if err != nil {
					status := fmt.Sprintf("[ERROR] Slot[%04d]: %s", sid, err)
					s.action.progress.status.Store(status)
				} else {
					s.action.progress.status.Store("")
				}
				fut.Done(strconv.Itoa(sid), err)
			}(sid)
		}
		for _, v := range fut.Wait() {
			if v != nil {
				return v.(error)
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
	return nil
}

func (s *Topom) processSlotAction(sid int) error {
	var db int = 0
	for s.IsOnline() {
		if exec, err := s.newSlotActionExecutor(sid); err != nil {
			return err
		} else if exec == nil {
			time.Sleep(time.Second)
		} else {
			n, nextdb, err := exec(db)
			if err != nil {
				return err
			}
			log.Debugf("slot-[%d] action executor %d", sid, n)

			if n == 0 && nextdb == -1 {
				return s.SlotActionComplete(sid)
			}
			status := fmt.Sprintf("[OK] Slot[%04d]@DB[%d]=%d", sid, db, n)
			s.action.progress.status.Store(status)

			if us := s.GetSlotActionInterval(); us != 0 {
				time.Sleep(time.Microsecond * time.Duration(us))
			}
			db = nextdb
		}
	}
	return nil
}

func (s *Topom) ProcessSyncAction() error {
	addr, err := s.SyncActionPrepare()
	if err != nil || addr == "" {
		return err
	}
	log.Warnf("sync-[%s] process action", addr)

	exec, err := s.newSyncActionExecutor(addr)
	if err != nil || exec == nil {
		return err
	}
	return s.SyncActionComplete(addr, exec() != nil)
}

func (s *Topom) AutoPurgeLog(logfile string, expireLogDays int) error {
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