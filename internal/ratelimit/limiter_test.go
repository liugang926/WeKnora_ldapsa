package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestLocalLimiterAllowsWithinBudget(t *testing.T) {
	l := New(nil, "test:", time.Minute, "inst")
	ctx := context.Background()
	const max = 3
	for i := 0; i < max; i++ {
		if !l.Allow(ctx, "k1", max) {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}
	if l.Allow(ctx, "k1", max) {
		t.Fatal("request over budget should be denied")
	}
}

func TestRedisLimiterCountsRepeatedMillisecondHits(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	limiter := New(client, "test:", time.Minute, "same-instance")
	fixed := time.UnixMilli(1_700_000_000_000)
	limiter.now = func() time.Time { return fixed }
	for i := 0; i < 3; i++ {
		if !limiter.Allow(context.Background(), "same-ip", 3) {
			t.Fatalf("same-millisecond request %d should be allowed", i+1)
		}
	}
	if limiter.Allow(context.Background(), "same-ip", 3) {
		t.Fatal("fourth same-millisecond request must be denied")
	}
}

func TestLocalLimiterIndependentKeys(t *testing.T) {
	l := New(nil, "test:", time.Minute, "inst")
	ctx := context.Background()
	if !l.Allow(ctx, "a", 1) {
		t.Fatal("first key should be allowed")
	}
	if !l.Allow(ctx, "b", 1) {
		t.Fatal("second key should be allowed")
	}
}

func TestAllowSkipsWhenMaxZero(t *testing.T) {
	l := New(nil, "test:", time.Minute, "inst")
	for i := 0; i < 100; i++ {
		if !l.Allow(context.Background(), "k", 0) {
			t.Fatal("max<=0 should always allow")
		}
	}
}
