package bindings

import (
	"errors"
	"fmt"
	"gopkg.in/redis.v5"
	"hash/crc32"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type CacheSystem interface {
	Store(string, string) error
	Load(string) (string, error)
	String() string
}

type RandomCache struct {
	CacheSystem
}

func (r *RandomCache) String() string {
	return r.CacheSystem.String()
}

func (r *RandomCache) FindID(val string) (int, error) {
	attempt := 0
	for {
		rec := int(rand.Int63())
		if err := r.Store(strconv.Itoa(rec), val); err != nil && attempt > 5 {
			return 0, err
		} else if err == nil {
			return rec, nil
		}
		attempt += 1
	}
}

type ShardSystem struct {
	Children   []CacheSystem
	Fallback   CacheSystem
	totalCount uint64
}

func (s *ShardSystem) Store(keyStr string, val string) error {
	atomic.AddUint64(&s.totalCount, 1)
	return s.Pick(keyStr).Store(keyStr, val)
}

func (s *ShardSystem) Pick(keyStr string) CacheSystem {
	key, err := strconv.Atoi(keyStr)
	if err != nil {
		key = int(crc32.ChecksumIEEE([]byte(keyStr)))
	}
	p := key % len(s.Children)
	ch := s.Children[p]
	fmt.Printf("keyStr %s (key %d) picked %d (%s)\n", keyStr, key, p, ch)
	return ch
}

func (s *ShardSystem) Load(keyStr string) (string, error) {
	atomic.AddUint64(&s.totalCount, 1)
	res, err := s.Pick(keyStr).Load(keyStr)
	if err != nil && s.Fallback != nil {
		res, err = s.Fallback.Load(keyStr)
	}
	return res, err
}

func (s *ShardSystem) String() string {
	count := atomic.SwapUint64(&s.totalCount, 0)
	if count == 0 {
		return ""
	}
	str := []string{fmt.Sprintf("shard system counts (total %d)..", count)}
	for i, child := range s.Children {
		str = append(str, fmt.Sprintf(`child %d: %s`, i, child.String()))
	}
	return strings.Join(str, "\n")
}

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

type CountingCache struct {
	Callback func(int, interface{}) (string, error)
	n        int
}

func (s *CountingCache) Store(keyStr string, val string) (err error) {
	if s.Callback != nil {
		_, err = s.Callback(s.n, []interface{}{keyStr, val})
	}
	s.n++
	return
}

func (s *CountingCache) Load(keyStr string) (string, error) {
	s.n++
	if s.Callback != nil {
		return "", nil
	}
	return s.Callback(s.n-1, keyStr)
}

func (s *CountingCache) String() string {
	return fmt.Sprintf(`counting cache at %d`, s.n)
}
