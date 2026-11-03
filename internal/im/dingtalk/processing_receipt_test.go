package dingtalk

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Tencent/WeKnora/internal/im"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type receiptTransport func(*http.Request) (*http.Response, error)

func (f receiptTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func receiptAdapter(t *testing.T, handle func(*http.Request) (*http.Response, error)) *Adapter {
	t.Helper()
	old := httpClient
	httpClient = &http.Client{Transport: receiptTransport(handle)}
	t.Cleanup(func() {
		for _, state := range dStreams {
			if state.receiptDone != nil {
				select {
				case <-state.receiptDone:
				case <-time.After(3 * time.Second):
					t.Error("receipt sender did not exit")
				}
			}
		}
		httpClient = old
		streamsMu.Lock()
		dStreams = map[string]*streamState{}
		streamsMu.Unlock()
	})
	return &Adapter{clientID: "test", token: "token", tokenExpAt: time.Now().Add(time.Hour)}
}
func receiptResponse(code int) *http.Response {
	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}
}
func receiptContent(r *http.Request) string {
	var body struct {
		MsgParam string `json:"msgParam"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	var msg struct {
		Text string `json:"text"`
	}
	json.Unmarshal([]byte(body.MsgParam), &msg)
	return msg.Text
}
func receiptMessage(id string) *im.IncomingMessage {
	return &im.IncomingMessage{UserID: "same-user", MessageID: id, ChatType: im.ChatTypeDirect}
}
func TestProcessingReceiptOnceBeforeFinal(t *testing.T) {
	var mu sync.Mutex
	var sent []string
	a := receiptAdapter(t, func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		sent = append(sent, receiptContent(r))
		mu.Unlock()
		return receiptResponse(200), nil
	})
	m := receiptMessage("once")
	ctx := context.Background()
	id, err := a.StartStream(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 || sent[0] != processingReceiptText {
		t.Fatal(sent)
	}
	if _, err = a.StartStream(ctx, m); err != nil {
		t.Fatal(err)
	}
	a.UpdateStreamContent(ctx, m, id, "partial")
	a.FinalizeStream(ctx, m, id, "final")
	if err = a.EndStream(ctx, m, id); err != nil {
		t.Fatal(err)
	}
	a.EndStream(ctx, m, id)
	a.StartStream(ctx, m)
	if len(sent) != 2 || sent[1] != "final" {
		t.Fatal(sent)
	}
}
func TestProcessingReceiptFailureAndTimeoutPreserveFinal(t *testing.T) {
	for _, mode := range []string{"failure", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			a := receiptAdapter(t, func(r *http.Request) (*http.Response, error) {
				if calls.Add(1) == 1 {
					if mode == "timeout" {
						<-r.Context().Done()
						return nil, r.Context().Err()
					}
					return receiptResponse(500), nil
				}
				return receiptResponse(200), nil
			})
			ctx := context.Background()
			m := receiptMessage(mode)
			start := time.Now()
			id, err := a.StartStream(ctx, m)
			if err != nil {
				t.Fatal(err)
			}
			if time.Since(start) > 3*time.Second {
				t.Fatal("ack exceeded budget")
			}
			a.FinalizeStream(ctx, m, id, "final")
			if err = a.EndStream(ctx, m, id); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 2 {
				t.Fatal(calls.Load())
			}
		})
	}
}
func TestProcessingReceiptCancelledBeforeStart(t *testing.T) {
	calls := 0
	a := receiptAdapter(t, func(r *http.Request) (*http.Response, error) { calls++; return receiptResponse(200), nil })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.StartStream(ctx, receiptMessage("cancel")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal(calls)
	}
}
func TestProcessingReceiptConcurrentRequests(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	a := receiptAdapter(t, func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return receiptResponse(200), nil
	})
	var wg sync.WaitGroup
	for _, id := range []string{"a", "a", "b", "b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if _, err := a.StartStream(context.Background(), receiptMessage(id)); err != nil {
				t.Error(err)
			}
		}(id)
	}
	wg.Wait()
	if calls != 2 {
		t.Fatal(calls)
	}
}
func TestProcessingReceiptFinalFailurePropagates(t *testing.T) {
	calls := 0
	a := receiptAdapter(t, func(r *http.Request) (*http.Response, error) {
		calls++
		if calls > 1 {
			return receiptResponse(500), nil
		}
		return receiptResponse(200), nil
	})
	m := receiptMessage("finalfail")
	id, _ := a.StartStream(context.Background(), m)
	a.FinalizeStream(context.Background(), m, id, "final")
	if a.EndStream(context.Background(), m, id) == nil {
		t.Fatal("final failure swallowed")
	}
}
func TestProcessingReceiptCardAndFallback(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "card", true: "fallback"}[fail], func(t *testing.T) {
			plain := 0
			a := receiptAdapter(t, func(r *http.Request) (*http.Response, error) {
				if strings.Contains(r.URL.Path, "card/") {
					if fail {
						return receiptResponse(500), nil
					}
					return receiptResponse(200), nil
				}
				plain++
				return receiptResponse(200), nil
			})
			a.cardTemplateID = "template"
			m := receiptMessage("card")
			id, err := a.StartStream(context.Background(), m)
			if err != nil {
				t.Fatal(err)
			}
			a.FinalizeStream(context.Background(), m, id, "answer")
			err = a.EndStream(context.Background(), m, id)
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if fail {
				want = 2
			}
			if plain != want {
				t.Fatalf("plain=%d want %d", plain, want)
			}
		})
	}
}
func TestProcessingReceiptTokenLockBudget(t *testing.T) {
	a := receiptAdapter(t, func(r *http.Request) (*http.Response, error) { return receiptResponse(200), nil })
	a.tokenMu.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			a.tokenMu.Unlock()
		}
	}()
	start := time.Now()
	id, err := a.StartStream(context.Background(), receiptMessage("lock"))
	if err != nil {
		t.Fatal(err)
	}
	if id == "" || time.Since(start) > 3*time.Second {
		t.Fatal("token lock exceeded receipt budget")
	}
	a.tokenMu.Unlock()
	unlocked = true
	// Let the cancelled sender observe the released lock before test cleanup.
	time.Sleep(30 * time.Millisecond)
}

func TestProcessingReceiptCancellationDuringSend(t *testing.T) {
	started := make(chan struct{})
	a := receiptAdapter(t, func(r *http.Request) (*http.Response, error) {
		close(started)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := a.StartStream(ctx, receiptMessage("cancel-during")); result <- err }()
	<-started
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled start did not return")
	}
}
func TestProcessingReceiptRepeatedEndPreservesFailure(t *testing.T) {
	calls := 0
	a := receiptAdapter(t, func(r *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return receiptResponse(200), nil
		}
		return receiptResponse(500), nil
	})
	m := receiptMessage("repeat-error")
	ctx := context.Background()
	id, _ := a.StartStream(ctx, m)
	a.FinalizeStream(ctx, m, id, "answer")
	if a.EndStream(ctx, m, id) == nil || a.EndStream(ctx, m, id) == nil {
		t.Fatal("end error lost on repeat")
	}
	if calls != 2 {
		t.Fatal("duplicate delivery", calls)
	}
}

func TestProcessingReceiptEmptyMessageIDsAreIndependent(t *testing.T) {
	calls := 0
	a := receiptAdapter(t, func(r *http.Request) (*http.Response, error) { calls++; return receiptResponse(200), nil })
	ctx := context.Background()
	m := receiptMessage("")
	first, _ := a.StartStream(ctx, m)
	a.FinalizeStream(ctx, m, first, "first")
	a.EndStream(ctx, m, first)
	second, _ := a.StartStream(ctx, m)
	a.FinalizeStream(ctx, m, second, "second")
	a.EndStream(ctx, m, second)
	if first == second || calls != 4 {
		t.Fatalf("empty IDs collided: same=%v calls=%d", first == second, calls)
	}
	streamsMu.Lock()
	defer streamsMu.Unlock()
	for _, id := range []string{first, second} {
		state := dStreams[id]
		state.mu.Lock()
		length := state.content.Len()
		state.mu.Unlock()
		if length != 0 {
			t.Fatal("completed stream retained answer")
		}
	}
}
