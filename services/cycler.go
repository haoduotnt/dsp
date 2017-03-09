package services

import (
	"github.com/clixxa/dsp/bindings"
	"time"
)

type ErrAllowed struct {
	UnderlyingErr error
}

func (e ErrAllowed) Error() string {
	return "ignored: " + e.UnderlyingErr.Error()
}

type CycleService struct {
	BindingDeps bindings.BindingDeps
	Children    []interface {
		Cycle() error
	}
	Proxy func() error
}

func (c *CycleService) Launch(errs chan error) error {
	if err := c.cycleAll(); err != nil {
		return err
	}
	go func() {
		for range time.NewTicker(time.Minute).C {
			if err := c.cycleAll(); err != nil {
				errs <- err
			}
		}
	}()
	return nil
}

func (c *CycleService) cycleAll() error {
	for _, ch := range c.Children {
		if err := ch.Cycle(); err != nil {
			c.BindingDeps.Logger.Printf("failed to cycle child %#v, err: %s", ch, err.Error())
			if _, ok := err.(ErrAllowed); !ok {
				return err
			}
		}
	}
	return nil
}

func (c *CycleService) Cycle() error {
	if c.Proxy != nil {
		return c.Proxy()
	}
	return nil
}
