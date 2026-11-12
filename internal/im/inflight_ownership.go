package im

import (
	"context"
	"crypto/sha256"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type inflightOwnerKey struct{}

const inflightTTL = 10 * time.Minute

// Registration order on Redis selects its current request. No distributed
// wall-clock ordering is assumed. Later binding/cleanup can never steal it.
func (s *Service) beginInflight(ctx context.Context,
	key string,
	cancel context.CancelFunc) (context.Context,
	*inflightEntry,
) {
	hash := sha256.Sum256([]byte(key))
	lock := &s.inflightRegistration[int(hash[0])%len(s.inflightRegistration)]
	lock.Lock()
	defer lock.Unlock()
	e := &inflightEntry{cancel: cancel, token: uuid.NewString()}
	s.inflight.Store(key, e)
	// Local starts must not race their Redis registration. Serialize this small
	// registration section with message binding and cleanup for this entry.
	e.mu.Lock()
	if s.redis != nil {
		op, done := context.WithTimeout(ctx, 2*time.Second)
		err := s.redis.Set(op, RedisKeyInflight+key, "v1|"+e.token+"||", inflightTTL).Err()
		done()
		if err != nil {
			logger.Warn(ctx, "[IM] Inflight registration unavailable")
		}
	}
	e.mu.Unlock()
	return context.WithValue(ctx, inflightOwnerKey{}, e), e
}

var inflightBindScript = redis.NewScript(`
local v=redis.call('GET',KEYS[1])
local prefix='v1|'..ARGV[1]..'|'
if v and string.sub(v,1,string.len(prefix))==prefix then
 redis.call('SET',KEYS[1],prefix..ARGV[2]..'|'..ARGV[3],'PX',ARGV[4])
 return 1
end
return 0`)

var inflightDeleteScript = redis.NewScript(`
local v=redis.call('GET',KEYS[1])
local prefix='v1|'..ARGV[1]..'|'
if v and string.sub(v,1,string.len(prefix))==prefix then return redis.call('DEL',KEYS[1]) end
return 0`)

func (s *Service) bindInflight(ctx context.Context, key, session, message string) {
	e, _ := ctx.Value(inflightOwnerKey{}).(*inflightEntry)
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sessionID, e.assistantMessageID = session, message
	if s.redis != nil && ctx.Err() == nil {
		op, done := context.WithTimeout(ctx, 2*time.Second)
		defer done()
		if err := inflightBindScript.Run(
			op,
			s.redis,
			[]string{
				RedisKeyInflight +
					key,
			},
			e.token,
			session,
			message,
			inflightTTL.Milliseconds()).Err(); err != nil {
			logger.Warn(ctx, "[IM] Inflight binding unavailable")
		}
	}
}

func (s *Service) finishInflight(ctx context.Context, key string, e *inflightEntry) {
	s.inflight.CompareAndDelete(key, e)
	if s.redis != nil {
		op, done := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer done()
		if err := inflightDeleteScript.Run(op, s.redis, []string{RedisKeyInflight + key}, e.token).Err(); err != nil {
			logger.Warn(ctx, "[IM] Inflight cleanup unavailable")
		}
	}
}

func (s *Service) loadInflightMapping(ctx context.Context, key string) (session, message string, ok bool) {
	if s.redis == nil {
		return "", "", false
	}
	val, err := s.redis.Get(ctx, RedisKeyInflight+key).Result()
	if err != nil {
		return "", "", false
	}
	if strings.HasPrefix(val, "v1|") {
		p := strings.Split(val, "|")
		if len(p) == 4 && p[2] != "" && p[3] != "" {
			return p[2], p[3], true
		}
		return "", "", false
	}
	p := strings.SplitN(val, ":", 2)
	if len(p) != 2 || p[0] == "" || p[1] == "" {
		return "", "", false
	}
	return p[0], p[1], true
}
