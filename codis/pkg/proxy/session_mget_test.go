// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
)

func newMGetTestBackend(addr string) (*sharedBackendConn, *BackendConn) {
	bc := &BackendConn{
		addr:  addr,
		input: make(chan *Request, 16),
	}
	bc.state.Set(stateConnected)
	return &sharedBackendConn{
		addr:   addr,
		conns:  [][]*BackendConn{{bc}},
		single: []*BackendConn{bc},
	}, bc
}

func newMGetTestRequest(keys ...[]byte) *Request {
	multi := make([]*redis.Resp, 0, len(keys)+1)
	multi = append(multi, redis.NewBulkBytes([]byte("MGET")))
	for _, key := range keys {
		multi = append(multi, redis.NewBulkBytes(key))
	}
	return &Request{
		Multi:       multi,
		Batch:       &sync.WaitGroup{},
		OpStr:       "MGET",
		ReceiveTime: time.Now().UnixNano(),
	}
}

func newMGetTestProxy(enabled bool) *Proxy {
	config := NewDefaultConfig()
	config.MGetMergeEnable = enabled
	proxy := &Proxy{config: config}
	proxy.mgetMergeEnabled.Set(enabled)
	return proxy
}

func distinctMGetTestKeys(n int) [][]byte {
	keys := make([][]byte, 0, n)
	slots := make(map[uint32]struct{})
	for i := 0; len(keys) < n; i++ {
		key := []byte(fmt.Sprintf("mget-merge-key-%d", i))
		slot := Hash(key) % MaxSlotNum
		if _, ok := slots[slot]; ok {
			continue
		}
		slots[slot] = struct{}{}
		keys = append(keys, key)
	}
	return keys
}

func setMGetTestRoute(router *Router, key []byte, backend *sharedBackendConn) *Slot {
	id := int(Hash(key) % MaxSlotNum)
	slot := &router.slots[id]
	slot.id = id
	slot.method = &forwardSync{}
	slot.backend.bc = backend
	return slot
}

func waitMGetTestSlots(t *testing.T, slots ...*Slot) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		for _, slot := range slots {
			slot.refs.Wait()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for MGET slot references")
	}
}

func popMGetTestRequest(t *testing.T, conn *BackendConn) *Request {
	t.Helper()
	select {
	case request := <-conn.input:
		return request
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for backend request")
		return nil
	}
}

func TestMGetMergeConfigDefaultsDisabled(t *testing.T) {
	config := NewDefaultConfig()
	if config.MGetMergeEnable {
		t.Fatal("mget_merge_enable must default to false")
	}

	proxy := newMGetTestProxy(config.MGetMergeEnable)
	if got := string(proxy.ConfigGet("mget_merge_enable").Value); got != "false" {
		t.Fatalf("default mget_merge_enable = %q, want false", got)
	}

	if resp := proxy.ConfigSet("mget_merge_enable", "true"); !resp.IsString() {
		t.Fatalf("enable mget merge failed: %s", resp.Value)
	}
	if !proxy.mgetMergeEnabled.IsTrue() || !proxy.config.MGetMergeEnable {
		t.Fatal("mget merge switch was not enabled")
	}
	if got := string(proxy.ConfigGet("mget_merge_enable").Value); got != "true" {
		t.Fatalf("enabled mget_merge_enable = %q, want true", got)
	}

	if resp := proxy.ConfigSet("mget_merge_enable", "invalid"); !resp.IsError() {
		t.Fatal("invalid mget_merge_enable value must return an error")
	}
	if !proxy.mgetMergeEnabled.IsTrue() {
		t.Fatal("invalid config value changed the mget merge switch")
	}

	if resp := proxy.ConfigSet("mget_merge_enable", "false"); !resp.IsString() {
		t.Fatalf("disable mget merge failed: %s", resp.Value)
	}
	if proxy.mgetMergeEnabled.IsTrue() || proxy.config.MGetMergeEnable {
		t.Fatal("mget merge switch was not disabled")
	}
}

func TestMGetMergeDisabledUsesLegacyPath(t *testing.T) {
	keys := distinctMGetTestKeys(2)
	backend, conn := newMGetTestBackend("127.0.0.1:7001")
	router := &Router{}
	slots := []*Slot{
		setMGetTestRoute(router, keys[0], backend),
		setMGetTestRoute(router, keys[1], backend),
	}
	request := newMGetTestRequest(keys...)

	session := &Session{proxy: newMGetTestProxy(false)}
	if err := session.handleRequestMGet(request, router); err != nil {
		t.Fatalf("handle disabled MGET merge failed: %v", err)
	}
	if got := len(conn.input); got != len(keys) {
		t.Fatalf("disabled merge sent %d backend commands, want %d", got, len(keys))
	}

	for i := range keys {
		sub := popMGetTestRequest(t, conn)
		if got := len(sub.Multi) - 1; got != 1 {
			t.Fatalf("legacy MGET has %d keys, want 1", got)
		}
		conn.setResponse(sub, redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte(fmt.Sprintf("value-%d", i))),
		}), nil)
	}

	request.Batch.Wait()
	if err := request.Coalesce(); err != nil {
		t.Fatalf("coalesce disabled MGET merge failed: %v", err)
	}
	for i := range keys {
		if got := string(request.Resp.Array[i].Value); got != fmt.Sprintf("value-%d", i) {
			t.Fatalf("response[%d] = %q, want value-%d", i, got, i)
		}
	}
	waitMGetTestSlots(t, slots...)
}

func TestMGetMergeRuntimeDisableRestoresLegacyPath(t *testing.T) {
	keys := distinctMGetTestKeys(2)
	backend, conn := newMGetTestBackend("127.0.0.1:7001")
	router := &Router{}
	slots := []*Slot{
		setMGetTestRoute(router, keys[0], backend),
		setMGetTestRoute(router, keys[1], backend),
	}
	request := newMGetTestRequest(keys...)
	proxy := newMGetTestProxy(true)

	if resp := proxy.ConfigSet("mget_merge_enable", "false"); !resp.IsString() {
		t.Fatalf("disable mget merge failed: %s", resp.Value)
	}
	if proxy.mgetMergeEnabled.IsTrue() {
		t.Fatal("runtime mget merge switch stayed enabled")
	}

	session := &Session{proxy: proxy}
	if err := session.handleRequestMGet(request, router); err != nil {
		t.Fatalf("handle MGET after runtime disable failed: %v", err)
	}
	if got := len(conn.input); got != len(keys) {
		t.Fatalf("runtime-disabled merge sent %d backend commands, want legacy %d", got, len(keys))
	}

	for i := range keys {
		sub := popMGetTestRequest(t, conn)
		if got := len(sub.Multi) - 1; got != 1 {
			t.Fatalf("runtime-disabled MGET has %d keys, want 1", got)
		}
		if got := string(sub.Multi[1].Value); got != string(keys[i]) {
			t.Fatalf("legacy request[%d] key = %q, want %q", i, got, keys[i])
		}
		conn.setResponse(sub, redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte(fmt.Sprintf("value-%d", i))),
		}), nil)
	}

	request.Batch.Wait()
	if err := request.Coalesce(); err != nil {
		t.Fatalf("coalesce runtime-disabled MGET failed: %v", err)
	}
	for i := range keys {
		if got := string(request.Resp.Array[i].Value); got != fmt.Sprintf("value-%d", i) {
			t.Fatalf("response[%d] = %q, want value-%d", i, got, i)
		}
	}
	waitMGetTestSlots(t, slots...)
}

func TestMGetMergeEnabledUsesMergedPath(t *testing.T) {
	keys := distinctMGetTestKeys(2)
	backend, conn := newMGetTestBackend("127.0.0.1:7001")
	router := &Router{}
	slots := []*Slot{
		setMGetTestRoute(router, keys[0], backend),
		setMGetTestRoute(router, keys[1], backend),
	}
	request := newMGetTestRequest(keys...)

	session := &Session{proxy: newMGetTestProxy(true)}
	if err := session.handleRequestMGet(request, router); err != nil {
		t.Fatalf("handle enabled MGET merge failed: %v", err)
	}
	if got := len(conn.input); got != 1 {
		t.Fatalf("enabled merge sent %d backend commands, want 1", got)
	}

	sub := popMGetTestRequest(t, conn)
	if got := len(sub.Multi) - 1; got != len(keys) {
		t.Fatalf("merged MGET has %d keys, want %d", got, len(keys))
	}
	conn.setResponse(sub, redis.NewArray([]*redis.Resp{
		redis.NewBulkBytes([]byte("value-0")),
		redis.NewBulkBytes([]byte("value-1")),
	}), nil)

	request.Batch.Wait()
	if err := request.Coalesce(); err != nil {
		t.Fatalf("coalesce enabled MGET merge failed: %v", err)
	}
	waitMGetTestSlots(t, slots...)
}

func TestMGetMergedByBackendAndRestoreOrder(t *testing.T) {
	keys := distinctMGetTestKeys(4)
	// Force descending slot IDs in the client request. Internal route-planning
	// changes must never alter the client-visible MGET output order.
	sort.Slice(keys, func(i, j int) bool {
		return Hash(keys[i])%MaxSlotNum > Hash(keys[j])%MaxSlotNum
	})
	backendA1, connA1 := newMGetTestBackend("127.0.0.1:7001")
	backendA2, connA2 := newMGetTestBackend("127.0.0.1:7001")
	backendB, connB := newMGetTestBackend("127.0.0.1:7002")

	router := &Router{}
	slots := []*Slot{
		setMGetTestRoute(router, keys[0], backendA1),
		setMGetTestRoute(router, keys[1], backendB),
		setMGetTestRoute(router, keys[2], backendA2),
		setMGetTestRoute(router, keys[3], backendB),
	}
	request := newMGetTestRequest(keys...)

	merged, err := new(Session).handleRequestMGetMerged(request, router)
	if err != nil {
		t.Fatalf("merge MGET failed: %v", err)
	}
	if !merged {
		t.Fatal("expected MGET merged path")
	}
	if got := len(connA1.input) + len(connA2.input); got != 1 {
		t.Fatalf("same backend address received %d commands, want 1", got)
	}
	if got := len(connB.input); got != 1 {
		t.Fatalf("second backend received %d commands, want 1", got)
	}

	var (
		requestA *Request
		connA    *BackendConn
	)
	select {
	case requestA = <-connA1.input:
		connA = connA1
	case requestA = <-connA2.input:
		connA = connA2
	default:
		t.Fatal("first backend did not receive merged MGET")
	}
	requestB := <-connB.input

	if got := len(requestA.Multi) - 1; got != 2 {
		t.Fatalf("first backend MGET has %d keys, want 2", got)
	}
	if got := len(requestB.Multi) - 1; got != 2 {
		t.Fatalf("second backend MGET has %d keys, want 2", got)
	}

	connB.setResponse(requestB, redis.NewArray([]*redis.Resp{
		redis.NewBulkBytes([]byte("value-1")),
		redis.NewBulkBytes([]byte("value-3")),
	}), nil)
	connA.setResponse(requestA, redis.NewArray([]*redis.Resp{
		redis.NewBulkBytes([]byte("value-0")),
		redis.NewBulkBytes(nil),
	}), nil)

	request.Batch.Wait()
	if err := request.Coalesce(); err != nil {
		t.Fatalf("coalesce MGET failed: %v", err)
	}
	if got := len(request.Resp.Array); got != len(keys) {
		t.Fatalf("MGET response has %d values, want %d", got, len(keys))
	}
	if got := string(request.Resp.Array[0].Value); got != "value-0" {
		t.Fatalf("response[0] = %q, want value-0", got)
	}
	if got := string(request.Resp.Array[1].Value); got != "value-1" {
		t.Fatalf("response[1] = %q, want value-1", got)
	}
	if request.Resp.Array[2].Value != nil {
		t.Fatalf("response[2] = %q, want nil bulk", request.Resp.Array[2].Value)
	}
	if got := string(request.Resp.Array[3].Value); got != "value-3" {
		t.Fatalf("response[3] = %q, want value-3", got)
	}
	waitMGetTestSlots(t, slots...)
}

func TestMGetMergedFallsBackWhenAnySlotMigrates(t *testing.T) {
	keys := distinctMGetTestKeys(2)
	backend, conn := newMGetTestBackend("127.0.0.1:7001")
	migrate, _ := newMGetTestBackend("127.0.0.1:7000")

	router := &Router{}
	stableSlot := setMGetTestRoute(router, keys[0], backend)
	migratingSlot := setMGetTestRoute(router, keys[1], backend)
	migratingSlot.migrate.bc = migrate

	request := newMGetTestRequest(keys...)
	merged, err := new(Session).handleRequestMGetMerged(request, router)
	if err != nil {
		t.Fatalf("plan MGET fallback failed: %v", err)
	}
	if merged {
		t.Fatal("MGET with a migrating slot must use legacy path")
	}
	if got := len(conn.input); got != 0 {
		t.Fatalf("merged path enqueued %d commands before fallback", got)
	}
	waitMGetTestSlots(t, stableSlot, migratingSlot)
}

func TestMGetRoutePlanRetainsSameSlotOnce(t *testing.T) {
	keys := [][]byte{
		[]byte("mget:{same-slot}:1"),
		[]byte("mget:{same-slot}:2"),
	}
	backend, _ := newMGetTestBackend("127.0.0.1:7001")
	router := &Router{}
	slot := setMGetTestRoute(router, keys[0], backend)
	setMGetTestRoute(router, keys[1], backend)
	request := newMGetTestRequest(keys...)
	ctx := newMGetPlanContext()

	fallback, err := router.planMGetRoutes(request, ctx)
	if err != nil || fallback {
		t.Fatalf("route plan failed: fallback = %t, err = %v", fallback, err)
	}
	if got := len(ctx.routes); got != 1 {
		t.Fatalf("same slot produced %d route plans, want 1", got)
	}
	if got := len(ctx.order); got != 1 {
		t.Fatalf("same slot retained %d refs, want 1", got)
	}

	ctx.release()
	waitMGetTestSlots(t, slot)
}

func TestMGetRoutePlanFallsBackWhenSlotIsTopologyLocked(t *testing.T) {
	keys := distinctMGetTestKeys(2)
	backend, _ := newMGetTestBackend("127.0.0.1:7001")
	router := &Router{}
	slots := []*Slot{
		setMGetTestRoute(router, keys[0], backend),
		setMGetTestRoute(router, keys[1], backend),
	}
	request := newMGetTestRequest(keys...)
	slots[1].blockAndWait()
	defer slots[1].unblock()

	ctx := newMGetPlanContext()
	done := make(chan struct{})
	var fallback bool
	var err error
	go func() {
		fallback, err = router.planMGetRoutes(request, ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("MGET route planning waited behind a topology-held slot")
	}
	if err != nil {
		t.Fatalf("locked slot route plan failed: %v", err)
	}
	if !fallback {
		t.Fatal("locked slot route plan must fall back")
	}
	// Client-order planning may have retained an earlier stable slot before it
	// reaches the locked slot. The caller owns and releases that partial plan.
	ctx.release()
	waitMGetTestSlots(t, slots[0])
}

func TestSlotTopologyWaitsForMGetRoutePlanner(t *testing.T) {
	slot := &Slot{}
	if !slot.tryAcquireMGetRoutePlan() {
		t.Fatal("failed to acquire initial MGET route-planning token")
	}

	writerDone := make(chan struct{})
	go func() {
		slot.blockAndWait()
		close(writerDone)
	}()

	deadline := time.Now().Add(time.Second)
	for slot.mgetPlanState.Int64()&mgetPlanBlocked == 0 {
		if time.Now().After(deadline) {
			t.Fatal("topology writer did not block new MGET route planners")
		}
		runtime.Gosched()
	}
	if slot.tryAcquireMGetRoutePlan() {
		slot.releaseMGetRoutePlan()
		t.Fatal("MGET route planner entered after topology gate was closed")
	}

	select {
	case <-writerDone:
		t.Fatal("topology writer passed an active MGET route planner")
	default:
	}
	slot.releaseMGetRoutePlan()

	select {
	case <-writerDone:
	case <-time.After(time.Second):
		t.Fatal("topology writer did not continue after planner released its token")
	}
	if slot.tryAcquireMGetRoutePlan() {
		slot.releaseMGetRoutePlan()
		t.Fatal("MGET route planner entered while topology lock was retained")
	}

	slot.unblock()
	if !slot.tryAcquireMGetRoutePlan() {
		t.Fatal("MGET route-planning gate stayed closed after topology unlock")
	}
	slot.releaseMGetRoutePlan()
}

func TestMGetAdmissionPreventsCrossSlotFillDeadlock(t *testing.T) {
	low := &Slot{id: 1}
	high := &Slot{id: 19}

	// Model the original cycle: an MGET has retained slot 19, while FillSlots
	// holds slot 1 and then waits for slot 19's ref.
	if !high.tryAcquireMGetRoutePlan() {
		t.Fatal("failed to acquire high-slot route-planning token")
	}
	high.refs.Add(1)
	high.releaseMGetRoutePlan()

	writerDone := make(chan struct{})
	go func() {
		low.blockAndWait()
		high.blockAndWait()
		close(writerDone)
	}()

	deadline := time.Now().Add(time.Second)
	for low.mgetPlanState.Int64()&mgetPlanBlocked == 0 ||
		high.mgetPlanState.Int64()&mgetPlanBlocked == 0 {
		if time.Now().After(deadline) {
			t.Fatal("FillSlots did not close both MGET planning gates")
		}
		runtime.Gosched()
	}

	// The old planner blocked here on low.lock.RLock while still holding the
	// high ref. The admission gate must reject immediately, allowing the caller
	// to release all partial-plan refs.
	if low.tryAcquireMGetRoutePlan() {
		low.releaseMGetRoutePlan()
		t.Fatal("MGET planner entered a slot already held by FillSlots")
	}
	high.refs.Done()

	select {
	case <-writerDone:
	case <-time.After(time.Second):
		t.Fatal("FillSlots stayed blocked after the MGET released its high-slot ref")
	}
	high.unblock()
	low.unblock()
}

func TestMGetHandleUsesLegacyDuringMigration(t *testing.T) {
	keys := distinctMGetTestKeys(2)
	backend, conn := newMGetTestBackend("127.0.0.1:7001")
	migrate, migrateConn := newMGetTestBackend("127.0.0.1:7000")

	router := &Router{}
	stableSlot := setMGetTestRoute(router, keys[0], backend)
	migratingSlot := setMGetTestRoute(router, keys[1], backend)
	stableSlot.method = &forwardSemiAsync{}
	migratingSlot.method = &forwardSemiAsync{}
	migratingSlot.migrate.bc = migrate

	request := newMGetTestRequest(keys...)
	session := &Session{proxy: newMGetTestProxy(true)}
	handleDone := make(chan error, 1)
	go func() {
		handleDone <- session.handleRequestMGet(request, router)
	}()

	wrapper := popMGetTestRequest(t, migrateConn)
	if got := string(wrapper.Multi[0].Value); got != "SLOTSMGRT-EXEC-WRAPPER" {
		t.Fatalf("migration command = %q, want SLOTSMGRT-EXEC-WRAPPER", got)
	}
	if got := len(wrapper.Multi) - 3; got != 1 {
		t.Fatalf("migration wrapper contains %d MGET keys, want legacy single key", got)
	}
	migrateConn.setResponse(wrapper, redis.NewArray([]*redis.Resp{
		redis.NewInt([]byte("2")),
		redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte("value-1")),
		}),
	}), nil)

	select {
	case err := <-handleDone:
		if err != nil {
			t.Fatalf("handle legacy MGET failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for legacy MGET dispatch")
	}

	legacy := popMGetTestRequest(t, conn)
	if got := len(legacy.Multi) - 1; got != 1 {
		t.Fatalf("stable backend received MGET with %d keys, want legacy single key", got)
	}
	conn.setResponse(legacy, redis.NewArray([]*redis.Resp{
		redis.NewBulkBytes([]byte("value-0")),
	}), nil)

	request.Batch.Wait()
	if err := request.Coalesce(); err != nil {
		t.Fatalf("coalesce legacy MGET failed: %v", err)
	}
	if got := string(request.Resp.Array[0].Value); got != "value-0" {
		t.Fatalf("response[0] = %q, want value-0", got)
	}
	if got := string(request.Resp.Array[1].Value); got != "value-1" {
		t.Fatalf("response[1] = %q, want value-1", got)
	}
	waitMGetTestSlots(t, stableSlot, migratingSlot)
}

func TestMGetMergedRejectsUnexpectedResponseLength(t *testing.T) {
	keys := [][]byte{
		[]byte("mget:{same-slot}:1"),
		[]byte("mget:{same-slot}:2"),
	}
	backend, conn := newMGetTestBackend("127.0.0.1:7001")

	router := &Router{}
	slot := setMGetTestRoute(router, keys[0], backend)
	setMGetTestRoute(router, keys[1], backend)

	request := newMGetTestRequest(keys...)
	merged, err := new(Session).handleRequestMGetMerged(request, router)
	if err != nil {
		t.Fatalf("merge MGET failed: %v", err)
	}
	if !merged {
		t.Fatal("expected MGET merged path")
	}

	sub := <-conn.input
	if got := len(sub.Groups); got != 1 {
		t.Fatalf("same-slot MGET retained %d slot references, want 1", got)
	}
	conn.setResponse(sub, redis.NewArray([]*redis.Resp{
		redis.NewBulkBytes([]byte("only-one-value")),
	}), nil)

	request.Batch.Wait()
	if err := request.Coalesce(); err == nil {
		t.Fatal("expected response length error")
	}
	waitMGetTestSlots(t, slot)
}

func BenchmarkMGetRoutePlanning(b *testing.B) {
	for _, nkeys := range []int{2, 4, 8, 32, 128} {
		keys := distinctMGetTestKeys(nkeys)
		backend, _ := newMGetTestBackend("127.0.0.1:7001")
		router := &Router{}
		for _, key := range keys {
			setMGetTestRoute(router, key, backend)
		}
		request := newMGetTestRequest(keys...)

		b.Run(fmt.Sprintf("client-order/keys-%d", nkeys), func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					ctx := newMGetPlanContext()
					benchmarkPlanMGetRoutesClientOrder(request, router, ctx)
					ctx.release()
				}
			})
		})
		b.Run(fmt.Sprintf("admission-gate/keys-%d", nkeys), func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					ctx := newMGetPlanContext()
					fallback, err := router.planMGetRoutes(request, ctx)
					if err != nil || fallback {
						panic(fmt.Sprintf("route plan failed: fallback=%t err=%v", fallback, err))
					}
					ctx.release()
				}
			})
		})
	}
}

// benchmarkPlanMGetRoutesClientOrder models the 3.2.2-1.20 planner so the
// benchmark isolates the cost of enforcing slot order. It is intentionally
// test-only because client-order planning is not safe with multi-slot writers.
func benchmarkPlanMGetRoutesClientOrder(r *Request, router *Router, ctx *mgetPlanContext) {
	for i := 1; i < len(r.Multi); i++ {
		id := int(Hash(r.Multi[i].Value) % MaxSlotNum)
		if ctx.routes[id] != nil {
			continue
		}
		slot := &router.slots[id]
		slot.lock.RLock()
		bc := new(forwardHelper).forward2(slot, r)
		ctx.retain(&mgetRoutePlan{slot: slot, bc: bc})
		slot.lock.RUnlock()
	}
}
