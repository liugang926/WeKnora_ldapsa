package im

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestInflightOwnershipRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	}()
	s := &Service{redis: r}
	ca, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	cb, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	a, ea := s.beginInflight(ca, "u", cancelA)
	b, eb := s.beginInflight(cb, "u", cancelB)
	s.bindInflight(b, "u", "sessionB", "messageB")
	s.bindInflight(a, "u", "sessionA", "messageA")
	x, y, ok := s.loadInflightMapping(b, "u")
	if !ok || x != "sessionB" || y != "messageB" {
		t.Fatalf("old binding overwrote B: %q %q", x, y)
	}
	if eb.sessionID != "sessionB" {
		t.Fatal("B local identity overwritten")
	}
	s.finishInflight(a, "u", ea)
	if v, _ := s.inflight.Load("u"); v != eb {
		t.Fatal("A removed B")
	}
	if _, _, ok = s.loadInflightMapping(b, "u"); !ok {
		t.Fatal("A removed Redis B")
	}
	raw, _ := s.inflight.LoadAndDelete("u")
	raw.(*inflightEntry).cancel()
	if cb.Err() == nil || ca.Err() != nil {
		t.Fatal("wrong request cancelled")
	}
	s.finishInflight(b, "u", eb)
	if mr.Exists(RedisKeyInflight + "u") {
		t.Fatal("cleanup leaked")
	}
	if err := mr.Set(RedisKeyInflight+"u", "legacySession:legacyMessage"); err != nil {
		t.Fatal(err)
	}
	x, y, ok = s.loadInflightMapping(a, "u")
	if !ok || x != "legacySession" || y != "legacyMessage" {
		t.Fatal("legacy mapping unsupported")
	}
	s.finishInflight(a, "u", ea)
	if !mr.Exists(RedisKeyInflight + "u") {
		t.Fatal("old cleanup deleted unowned legacy")
	}
}

func TestInflightReverseFinishAndMissingOwner(t *testing.T) {
	s := &Service{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, ea := s.beginInflight(ctx, "u", cancel)
	b, eb := s.beginInflight(ctx, "u", cancel)
	s.finishInflight(b, "u", eb)
	s.bindInflight(a, "u", "a", "a")
	if _, ok := s.inflight.Load("u"); ok {
		t.Fatal("old binding resurrected selection")
	}
	s.finishInflight(a, "u", ea)
	s.bindInflight(ctx, "u", "none", "none")
}

func TestInflightActualStopSelectsCurrent(t *testing.T) {
	s := &Service{}
	s.qaQueue = newQAQueue(1, 5, 3, 0, func(*qaRequest) {}, nil)
	defer s.qaQueue.Stop()
	ca, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	cb, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	key := makeUserKey("channel", "user", "chat", "")
	a, ea := s.beginInflight(ca, key, cancelA)
	b, eb := s.beginInflight(cb, key, cancelB)
	s.finishInflight(a, key, ea)
	err := s.handleCommand(context.Background(),
		newStopCommand(),
		nil,
		&IncomingMessage{
			UserID: "user",
			ChatID: "chat",
		},
		&auditBlockingAdapter{},
		&IMChannel{
			ID:         "channel",
			OutputMode: "full",
		},
		&ChannelSession{},
		nil)
	if err != nil || cb.Err() == nil || ca.Err() != nil {
		t.Fatalf("stop failed: %v A=%v B=%v", err, ca.Err(), cb.Err())
	}
	s.finishInflight(b, key, eb)
}

func TestInflightRedisUnavailableAndCancelled(t *testing.T) {
	mr := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	s := &Service{redis: r}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	owned, e := s.beginInflight(ctx, "u", cancel)
	s.bindInflight(owned, "u", "s", "m")
	s.finishInflight(owned, "u", e)
	if _, ok := s.inflight.Load("u"); ok {
		t.Fatal("local cleanup leaked on Redis failure")
	}
}
