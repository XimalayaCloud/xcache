// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"sync"
	"strings"
	"strconv"

	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

const (
	ExpansionActionNothing  	= 0
	ExpansionActionDataSync 	= 1
	ExpansionActionBackup 		= 2
	ExpansionActionSlotsMgrt 	= 3
	ExpansionActionDataClean 	= 4
	ExpansionStepNothing		= 0
	ExpansionStepRunning		= 1
	ExpansionStepFinshed		= 2
	ExpansionStepSlotsreload	= 1
	ExpansionStepSlotsdel		= 2
	ExpansionStepDelSlotsKey	= 3
	ExpansionStepCompact		= 4
	ExpansionDataCleanFinshed	= 5
)

type expansionPlan struct {
	mu sync.Mutex
	Id int
	SrcGid int
	DstGid int
	SlotsList string
	SyncSpeed int
	BinlogNums int
	SlotsArr []int
	MajorStep int
	MinorStep int
	Status string
	Error string
}

var expansionLock sync.Mutex
var expansionPlanList = make([]*expansionPlan, 0)
var planDelimiter = "$";

func (s *Topom) ExpansionPullPlan() string {
	expansionLock.Lock()
	defer expansionLock.Unlock()

	resp := ""
	for i:=0; i<len(expansionPlanList); i++ {
		resp += strconv.Itoa(expansionPlanList[i].Id) + planDelimiter
		resp += strconv.Itoa(expansionPlanList[i].SrcGid) + planDelimiter
		resp += strconv.Itoa(expansionPlanList[i].DstGid) + planDelimiter
		resp += expansionPlanList[i].SlotsList + planDelimiter
		resp += strconv.Itoa(expansionPlanList[i].SyncSpeed) + planDelimiter
		resp += strconv.Itoa(expansionPlanList[i].BinlogNums) + planDelimiter
		resp += strconv.Itoa(expansionPlanList[i].MajorStep) + planDelimiter
		resp += strconv.Itoa(expansionPlanList[i].MinorStep) + planDelimiter
		resp += expansionPlanList[i].Status + planDelimiter
		resp += expansionPlanList[i].Error
		if i < len(expansionPlanList)-1 {
			resp += "\n"
		}
	}
	return resp
}

func (s *Topom) ExpansionAddPlan(planStr string) error {
	expansionLock.Lock()
	defer expansionLock.Unlock()

	plan := strings.Split(planStr, planDelimiter)

	if len(plan) != 5 {
		return errors.New("expansion plan is invalid.")
	} else {
		id := maxExpansionPlanId() + 1

		srcGid, err := strconv.Atoi(plan[0])
		if err != nil {
			return errors.New("expansion plan src gid is invalid.")
		} else {
			if err := s.IsInvalidSrcGroup(srcGid); err != nil {
				return err
			}
			if isConflictGid(srcGid) {
				return errors.New("src gid is confict")
			}
		}

		dstGid, err := strconv.Atoi(plan[1])
		if err != nil {
			return errors.New("expansion plan dst gid is invalid.")
		} else {
			if err := s.IsInvalidDstGroup(dstGid); err != nil {
				return err
			}
			if isConflictGid(dstGid) {
				return errors.New("dst gid is confict")
			}
		}

		slotsList := plan[2]
		slice, err := s.getSlotsList(slotsList, srcGid, dstGid);
		if err != nil {
			return err
		}

		syncSpeed, err := strconv.Atoi(plan[3])
		if err != nil {
			return errors.New("expansion sync_speed is invalid.")
		} else if syncSpeed < 0 || syncSpeed > 125 {
			return errors.New("expansion sync_speed out of range.")
		}

		binlogNums, err := strconv.Atoi(plan[4])
		if err != nil {
			return errors.New("expansion binlog_nums is invalid.")
		} else if binlogNums < 0 {
			return errors.New("expansion binlog_nums out of range.")
		}

		newPlan := new(expansionPlan)
		newPlan.Id = id
		newPlan.SrcGid = srcGid
		newPlan.DstGid = dstGid
		newPlan.SlotsList = slotsList
		newPlan.SlotsArr = slice
		newPlan.SyncSpeed = syncSpeed
		newPlan.BinlogNums = binlogNums
		newPlan.MajorStep = ExpansionActionNothing
		newPlan.MinorStep = ExpansionStepNothing
		newPlan.Status = "-"
		newPlan.Error = "-"
		expansionPlanList = append(expansionPlanList, newPlan)
		log.Warnf("Expansion: add a new expansion plan-[%d]: %s",id, planStr)
	}

	return nil
}

func (s *Topom) ExpansionDelPlan(planId int) error {
	expansionLock.Lock()
	defer expansionLock.Unlock()

	var slice = make([]*expansionPlan, 0, len(expansionPlanList))
	var isPlanExist = false
	for i:=0; i<len(expansionPlanList); i++ {
		if expansionPlanList[i].Id == planId {
			if expansionPlanList[i].MinorStep == ExpansionStepRunning || (expansionPlanList[i].MajorStep == ExpansionActionDataClean && expansionPlanList[i].MinorStep <= ExpansionStepCompact) {
				return errors.New("plan is running")
			}
			isPlanExist = true
			//plan = expansionPlanList[i]
		} else {
			slice = append(slice, expansionPlanList[i])
		}
	}
	expansionPlanList = slice
	
	if isPlanExist {
	    log.Warnf("Expansion: del plan-[%d] success",planId)
	} else {
	    return errors.New("plan not exist!")
	}

	return nil
}

func maxExpansionPlanId() int {
	maxId := 0
	for i:=0; i<len(expansionPlanList); i++ {
		if expansionPlanList[i].Id > maxId {
			maxId = expansionPlanList[i].Id
		}
	}
	return maxId
}

func isConflictGid(gid int) bool {
	for i:=0; i<len(expansionPlanList); i++ {
		if expansionPlanList[i].SrcGid == gid || expansionPlanList[i].DstGid == gid {
			return true
		}
	}
	return false
}

func (s *Topom) ExpansionDataSync(planId int) error {
	expansionLock.Lock()
	defer expansionLock.Unlock()

	plan := getExpansionPlanById(planId)
	if plan == nil {
		return errors.New("expansion plan is not exsit.")
	}

	if err := s.ExpansionExecDataSync(plan); err != nil {
		return err
	} 

	return nil
}

func (s *Topom) ExpansionBackup(planId, force int) error {
	expansionLock.Lock()
	defer expansionLock.Unlock()

	plan := getExpansionPlanById(planId)
	if plan == nil {
		return errors.New("expansion plan is not exsit.")
	}

	if err := s.ExpansionExecBackup(plan, force != 0); err != nil {
		return err
	}

	return nil
}

func (s *Topom) ExpansionSlotsMgrt(planId int) error {
	expansionLock.Lock()
	defer expansionLock.Unlock()

	plan := getExpansionPlanById(planId)
	if (plan == nil) {
		return errors.New("expansion plan is not exsit.")
	}

	if err := s.ExpansionExecSlotsMgrt(plan); err != nil {
		return err
	}

	return nil
}

func (s *Topom) ExpansionDateClean(planId int) error {
	expansionLock.Lock()
	defer expansionLock.Unlock()

	plan := getExpansionPlanById(planId)
	if (plan == nil) {
		return errors.New("expansion plan is not exsit.")
	}

	if err := s.ExpansionExecDateClean(plan); err != nil {
		return err
	}

	return nil
}

func getExpansionPlanById(planId int) *expansionPlan {
	var plan *expansionPlan = nil
	for i:=0; i<len(expansionPlanList); i++ {
		if (expansionPlanList[i].Id == planId) {
			plan = expansionPlanList[i]
			break;
		}
	}

	return plan
}