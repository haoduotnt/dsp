package bindings

import (
	"errors"
	"gopkg.in/redis.v5"
	"math/rand"
	"strconv"
	"time"
)

type CacheSystem interface {
	Store(int, string) error
	Load(string) (string, error)
}

type RandomCache struct {
	CacheSystem
}

func (r *RandomCache) FindID(val string) (int, error) {
	attempt := 0
	for {
		rec := int(rand.Int63())
		if err := r.Store(rec, val); err != nil && attempt > 5 {
			return 0, err
		} else if err == nil {
			return rec, nil
		}
		attempt += 1
	}
}

type ShardSystem struct {
	Children []CacheSystem
	Fallback CacheSystem
}

func (s *ShardSystem) Store(key int, val string) error {
	pick := s.Children[key%len(s.Children)]
	return pick.Store(key, val)
}

func (s *ShardSystem) Load(keyStr string) (string, error) {
	key, err := strconv.Atoi(keyStr)
	if err != nil {
		return "", err
	}
	pick := s.Children[key%len(s.Children)]

	res, err := pick.Load(keyStr)
	if err != nil && s.Fallback != nil {
		res, err = s.Fallback.Load(keyStr)
	}
	return res, err
}

var CantStoreErr = errors.New("redis returned not ok")

type RecallRedis struct {
	*redis.Client
}

func (r *RecallRedis) Store(key int, val string) error {
	res := r.SetNX(strconv.Itoa(key), val, 10*time.Minute)
	if err := res.Err(); err != nil {
		return err
	}
	if res.Val() {
		return nil
	}
	return CantStoreErr
}

func (r *RecallRedis) Load(keyStr string) (string, error) {
	cmd := r.Get(keyStr)
	if err := cmd.Err(); err != nil {
		return "", err
	}
	return cmd.Result()
}

type CountingCache struct {
	Callback func(int, interface{}) (string, error)
	n        int
}

func (s *CountingCache) Store(key int, val string) error {
	_, err := s.Callback(s.n, []interface{}{key, val})
	s.n++
	return err
}

func (s *CountingCache) Load(keyStr string) (string, error) {
	key, err := strconv.Atoi(keyStr)
	if err != nil {
		return "", err
	}
	s.n++
	return s.Callback(s.n-1, key)
}
