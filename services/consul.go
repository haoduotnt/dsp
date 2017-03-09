package services

import (
	"github.com/hashicorp/consul/api"
)

type ConsulConfigs struct {
	Client    *api.Client
	KV        *api.KV
	RedisUrls string
}

func (c *ConsulConfigs) Cycle() error {
	if c.Client == nil {
		client, err := api.NewClient(api.DefaultConfig())
		if err != nil {
			return err
		}
		c.Client = client
		c.KV = client.KV()
	}

	pair, _, err := c.KV.Get("ms/redis/urls", nil)
	if err != nil {
		return err
	}
	c.RedisUrls = string(pair.Value)
	return nil
}
