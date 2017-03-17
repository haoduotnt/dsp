package bindings

import (
	"errors"
	"fmt"
	"github.com/clixxa/dsp/services"
	"gopkg.in/redis.v5"
	"sync/atomic"
	"time"
)

var CantStoreErr = errors.New("redis returned not ok")

type RecallRedis struct {
	*redis.Client
	calls uint64
}

func (r *RecallRedis) Store(keyStr string, val string) error {
	atomic.AddUint64(&r.calls, 1)
	res := r.SetNX(keyStr, val, 10*time.Minute)
	if err := res.Err(); err != nil {
		return err
	}
	if res.Val() {
		return nil
	}
	return CantStoreErr
}

func (r *RecallRedis) Load(keyStr string) (string, error) {
	atomic.AddUint64(&r.calls, 1)
	cmd := r.Get(keyStr)
	if err := cmd.Err(); err != nil {
		return "", err
	}
	return cmd.Result()
}

func (r *RecallRedis) String() string {
	v := atomic.SwapUint64(&r.calls, 0)
	return fmt.Sprintf(`redis client called %d times since last dump`, v)
}

func NewRedis(conn string) (*RecallRedis, error) {
	red := &redis.Options{Addr: conn}
	r := &RecallRedis{Client: redis.NewClient(red)}
	if err := r.Ping().Err(); err != nil {
		return nil, err
	}
	return r, nil
}

func NewRedisCache(conn string) (services.CacheSystem, error) {
	return NewRedis(conn)
}
