package im

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type auditBlockingAdapter struct {
	Adapter
	entered  chan string
	releases map[string]chan struct{}
}

func (a *auditBlockingAdapter) DownloadFile(ctx context.Context, m *IncomingMessage) (io.ReadCloser, string, error) {
	a.entered <- m.MessageID
	select {
	case <-a.releases[m.MessageID]:
	case <-ctx.Done():
	}
	return nil, "", errors.New("controlled download completion")
}
func (a *auditBlockingAdapter) SendReply(context.Context, *IncomingMessage, *ReplyMessage) error {
	return nil
}

func TestAuditSameUserRunningRequestRemainsStoppable(t *testing.T) {
	s := &Service{}
	a := &auditBlockingAdapter{entered: make(chan string, 2), releases: map[string]chan struct{}{"a": make(chan struct{}), "b": make(chan struct{})}}
	finished := make(chan string, 2)
	q := newQAQueue(2, 10, 3, 0, func(r *qaRequest) { s.executeQARequest(r); finished <- r.msg.MessageID }, nil)
	stop := make(chan struct{})
	q.Start(stop)
	defer func() { close(stop); q.Stop() }()
	newReq := func(id string) *qaRequest {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		return &qaRequest{ctx: ctx, cancel: cancel, userKey: "same-channel:user:chat", msg: &IncomingMessage{MessageID: id, MessageType: MessageTypeFile}, adapter: a}
	}
	if _, err := q.Enqueue(newReq("a")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-a.entered:
	case <-time.After(time.Second):
		t.Fatal("first worker did not start")
	}
	if _, err := q.Enqueue(newReq("b")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-a.entered:
	case <-time.After(time.Second):
		t.Fatal("same-user second request did not execute concurrently")
	}
	if _, ok := s.inflight.Load("same-channel:user:chat"); !ok {
		t.Fatal("no initial second mapping")
	}
	close(a.releases["a"])
	select {
	case id := <-finished:
		if id != "a" {
			t.Fatal(id)
		}
	case <-time.After(time.Second):
		t.Fatal("first did not finish")
	}
	_, stoppable := s.inflight.Load("same-channel:user:chat")
	close(a.releases["b"])
	<-finished
	if !stoppable {
		t.Fatal("running second request lost its cancellation mapping when first finished")
	}
}
func TestAuditConcurrentDuplicateAndQueueRelease(t *testing.T) {
	s := &Service{}
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !s.isDuplicate(context.Background(), "one-callback") {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatalf("accepted=%d", accepted.Load())
	}
	q := newQAQueue(1, 2, 1, 0, func(*qaRequest) {}, nil)
	makeReq := func() *qaRequest {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		return &qaRequest{ctx: ctx, cancel: cancel, userKey: "u"}
	}
	if _, err := q.Enqueue(makeReq()); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(makeReq()); err == nil {
		t.Fatal("per-user limit not enforced")
	}
	if !q.Remove("u") {
		t.Fatal("remove failed")
	}
	if _, err := q.Enqueue(makeReq()); err != nil {
		t.Fatalf("counter leaked: %v", err)
	}
	if q.Metrics().Depth != 1 {
		t.Fatal("wrong depth")
	}
	q.Stop()
}
