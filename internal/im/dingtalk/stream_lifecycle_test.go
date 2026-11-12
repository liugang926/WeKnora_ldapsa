package dingtalk

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func ageLifecycle(t *testing.T, id string, age time.Duration) {
	t.Helper()
	s, e := lockStreamState(id)
	if e != nil {
		t.Fatal(e)
	}
	s.createdAt = time.Now().Add(-age)
	s.lastActivity = s.createdAt
	s.mu.Unlock()
}

func TestLifecycleLiveLongAndCancelledFinalize(t *testing.T) {
	var sends atomic.Int32
	a := receiptAdapter(t,
		func(_ *http.Request) (*http.Response,
			error,
		) {
			sends.Add(1)
			return receiptResponse(200),
				nil
		})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := receiptMessage("long-active")
	id, e := a.StartStream(ctx, m)
	if e != nil {
		t.Fatal(e)
	}
	ageLifecycle(t, id, 10*time.Minute)
	reapStreams(time.Now())
	if e = a.UpdateStreamContent(ctx, m, id, "partial"); e != nil {
		t.Fatal(e)
	}
	cancel()
	out := context.Background()
	if e = a.FinalizeStream(out, m, id, "final"); e != nil {
		t.Fatal(e)
	}
	reapStreams(time.Now())
	if e = a.UpdateStreamContent(out, m, id, "late partial"); e == nil {
		t.Fatal("finalized content overwritten")
	}
	if e = a.EndStream(out, m, id); e != nil {
		t.Fatal(e)
	}
	if sends.Load() != 2 {
		t.Fatalf("sends=%d", sends.Load())
	}
	// Completion retention starts at completion, not creation.
	reapStreams(time.Now())
	if e = a.EndStream(out, m, id); e != nil {
		t.Fatal(e)
	}
	if e = a.UpdateStreamContent(out, m, id, "late body"); e == nil {
		t.Fatal("late update accepted")
	}
	if e = a.FinalizeStream(out, m, id, "late final"); e == nil {
		t.Fatal("late final accepted")
	}
	if e = a.SendStreamChunk(out, m, id, "late chunk"); e == nil {
		t.Fatal("late chunk accepted")
	}
	s, e := lockStreamState(id)
	if e != nil {
		t.Fatal(e)
	}
	if s.content.Len() != 0 || s.sessionWebhook != "" || s.outTrackID != "" {
		t.Error("terminal retained body")
	}
	s.completedAt = time.Now().Add(-streamOrphanTTL - time.Second)
	s.mu.Unlock()
	reapStreams(time.Now())
	if e = a.EndStream(out, m, id); e == nil {
		t.Fatal("missing state claimed success")
	}
	if sends.Load() != 2 {
		t.Fatal("duplicate final")
	}
}

func TestLifecycleOrphanAndAbsoluteBound(t *testing.T) {
	a := receiptAdapter(t, func(_ *http.Request) (*http.Response, error) { return receiptResponse(200), nil })
	for _, kind := range []string{"background", "cancelled", "absolute"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			cancel := func() {}
			if kind != "background" {
				ctx, cancel = context.WithCancel(ctx)
			}
			defer cancel()
			m := receiptMessage(kind)
			id, e := a.StartStream(ctx, m)
			if e != nil {
				t.Fatal(e)
			}
			age := streamOrphanTTL + time.Second
			if kind == "absolute" {
				age = streamMaxLifetime + time.Second
			} else {
				cancel()
			}
			ageLifecycle(t, id, age)
			reapStreams(time.Now())
			if e = a.EndStream(context.Background(), m, id); e == nil {
				t.Fatal("expired state claimed delivery")
			}
		})
	}
}

func TestLifecycleEndingCannotBeReaped(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	a := receiptAdapter(t, func(_ *http.Request) (*http.Response, error) {
		if calls.Add(1) == 2 {
			close(entered)
			<-release
		}
		return receiptResponse(200), nil
	})
	m := receiptMessage("ending")
	ctx := context.Background()
	id, e := a.StartStream(ctx, m)
	if e != nil {
		t.Fatal(e)
	}
	if err := a.FinalizeStream(ctx, m, id, "final"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- a.EndStream(ctx, m, id) }()
	<-entered
	ageLifecycle(t, id, streamMaxLifetime+time.Hour)
	reapStreams(time.Now())
	if e = a.UpdateStreamContent(ctx, m, id, "late"); e == nil {
		t.Error("ending write accepted")
	}
	close(release)
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	if e = a.EndStream(ctx, m, id); e != nil {
		t.Fatal(e)
	}
	if calls.Load() != 2 {
		t.Fatal("duplicate send")
	}
}

func TestLifecycleInitializingReaper(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var n atomic.Int32
	a := receiptAdapter(t, func(_ *http.Request) (*http.Response, error) {
		if n.Add(1) == 1 {
			close(entered)
			<-release
		}
		return receiptResponse(200), nil
	})
	m := receiptMessage("initializing")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan string, 1)
	go func() { id, _ := a.StartStream(ctx, m); result <- id }()
	<-entered
	for i := 0; i < 20; i++ {
		reapStreams(time.Now().Add(streamMaxLifetime + time.Hour))
	}
	close(release)
	id := <-result
	if e := a.FinalizeStream(ctx, m, id, "final"); e != nil {
		t.Fatal(e)
	}
	if e := a.EndStream(ctx, m, id); e != nil {
		t.Fatal(e)
	}
	if n.Load() != 2 {
		t.Fatal("initialization lost stream")
	}
}

func TestLifecycleCardUpdateCannotOvertakeEnd(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	a := receiptAdapter(t, func(_ *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return receiptResponse(200), nil
	})
	state := &streamState{outTrackID: "card", createdAt: time.Now(), lastActivity: time.Now()}
	streamsMu.Lock()
	dStreams["card-order"] = state
	streamsMu.Unlock()
	m := receiptMessage("card-order")
	ctx := context.Background()
	updated := make(chan error, 1)
	ended := make(chan error, 1)
	go func() { updated <- a.UpdateStreamContent(ctx, m, "card-order", "answer") }()
	<-entered
	go func() { ended <- a.EndStream(ctx, m, "card-order") }()
	// Wait for End's state transition, not its blocked network operation.
	deadline := time.Now().Add(time.Second)
	for {
		state.mu.Lock()
		ending := state.ended
		state.mu.Unlock()
		if ending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("End did not start")
		}
		time.Sleep(time.Millisecond)
	}
	if calls.Load() != 1 {
		t.Error("final send overtook active update")
	}
	close(release)
	if e := <-updated; e != nil {
		t.Fatal(e)
	}
	if e := <-ended; e != nil {
		t.Fatal(e)
	}
	if calls.Load() != 2 {
		t.Fatal("missing final card send")
	}
	if e := a.UpdateStreamContent(ctx, m, "card-order", "late"); e == nil {
		t.Fatal("late card update accepted")
	}
}
