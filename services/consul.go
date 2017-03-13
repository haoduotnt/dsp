package services

import (
	"errors"
	"github.com/hashicorp/consul/api"
)

type ConsulConfigs struct {
	Client    *api.Client
	KV        *api.KV
	RedisUrls string
}

var KeyMissing = errors.New("Key Missing")

func (c *ConsulConfigs) Cycle() error {
	if c.Client == nil {
		client, err := api.NewClient(api.DefaultConfig())
		if err != nil {
			return ErrAllowed{err}
		}
		c.Client = client
		c.KV = client.KV()
	}

	pair, _, err := c.KV.Get("ms/redis/urls", nil)
	if err != nil {
		return ErrAllowed{err}
	}
	if pair == nil {
		return ErrAllowed{KeyMissing}
	}
	c.RedisUrls = string(pair.Value)
	return nil
}
