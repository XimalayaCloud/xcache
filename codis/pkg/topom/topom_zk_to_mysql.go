// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

func (s *Topom) RefillCache() error {
	if slots, err := s.refillCacheSlots(nil); err != nil {
		log.ErrorErrorf(err, "store: load slots failed")
		return errors.Errorf("store: load slots failed")
	} else {
		s.cache.slots = slots
	}
	if group, err := s.refillCacheGroup(nil); err != nil {
		log.ErrorErrorf(err, "store: load group failed")
		return errors.Errorf("store: load group failed")
	} else {
		s.cache.group = group
	}
	if proxy, err := s.refillCacheProxy(nil); err != nil {
		log.ErrorErrorf(err, "store: load proxy failed")
		return errors.Errorf("store: load proxy failed")
	} else {
		s.cache.proxy = proxy
	}
	if sentinel, err := s.refillCacheSentinel(nil); err != nil {
		log.ErrorErrorf(err, "store: load sentinel failed")
		return errors.Errorf("store: load sentinel failed")
	} else {
		s.cache.sentinel = sentinel
	}
	return nil
	return nil
}

func (s *Topom) ZkToMysql(client *models.Store) error {
	//group
	for _, g := range s.cache.group {
		if err := storeCreateGroup(g, client); err != nil {
			return err
		}
	}

	//proxy
	for _, p := range s.cache.proxy {
		if err := storeCreateProxy(p, client); err != nil {
			return err
		}
	}

	//slots
	for _, m := range s.cache.slots {
		if err := storeUpdateSlotMapping(m, client); err != nil {
			return err
		}
	}

	//sentinel
	if err := storeUpdateSentinel(s.cache.sentinel, client); err != nil {
		return err
	}

	return nil
}

func storeCreateGroup(g *models.Group, store *models.Store) error {
	log.Warnf("create group-[%d]:\n%s", g.Id, g.Encode())
	if err := store.UpdateGroup(g); err != nil {
		log.ErrorErrorf(err, "store: create group-[%d] failed", g.Id)
		return errors.Errorf("store: create group-[%d] failed", g.Id)
	}
	return nil
}

func storeCreateProxy(p *models.Proxy, store *models.Store) error {
	log.Warnf("create proxy-[%s]:\n%s", p.Token, p.Encode())
	if err := store.UpdateProxy(p); err != nil {
		log.ErrorErrorf(err, "store: create proxy-[%s] failed", p.Token)
		return errors.Errorf("store: create proxy-[%s] failed", p.Token)
	}
	return nil
}

func storeUpdateSlotMapping(m *models.SlotMapping, store *models.Store) error {
	log.Warnf("update slot-[%d]:\n%s", m.Id, m.Encode())
	if err := store.UpdateSlotMapping(m); err != nil {
		log.ErrorErrorf(err, "store: update slot-[%d] failed", m.Id)
		return errors.Errorf("store: update slot-[%d] failed", m.Id)
	}
	return nil
}

func storeUpdateSentinel(p *models.Sentinel, store *models.Store) error {
	log.Warnf("update sentinel:\n%s", p.Encode())
	if err := store.UpdateSentinel(p); err != nil {
		log.ErrorErrorf(err, "store: update sentinel failed")
		return errors.Errorf("store: update sentinel failed")
	}
	return nil
}


