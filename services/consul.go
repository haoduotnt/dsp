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

func (c *ConsulConfigs) Cycle(quit func(error) bool) {
	if c.Client == nil {
		client, err := api.NewClient(api.DefaultConfig())
		if err != nil {
			quit(ErrDatabaseMissing{"consul", err})
			return
		}
		c.Client = client
		c.KV = client.KV()
	}

	pair, _, err := c.KV.Get("ms/redis/urls", nil)
	if quit(ErrDatabaseMissing{"consul", err}) {
		return
	}
	if pair == nil {
		quit(ErrParsing{"consul redis urls", KeyMissing})
		return
	}
	c.RedisUrls = string(pair.Value)
	return
}
