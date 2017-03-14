package services

import (
	"fmt"
	"time"
)

type CycleService struct {
	Messages    chan string
	ErrorFilter func(error) bool
	Children    []interface {
		Cycle(func(error) bool)
	}
	Proxy func(func(error) bool)
}

func (c *CycleService) Launch(errs chan error) error {
	c.Messages <- "launching cycler"
	if err := c.cycleAll(c.ErrorFilter); c.ErrorFilter(err) {
		return ErrLaunching{err}
	}
	go func() {
		for range time.NewTicker(time.Minute).C {
			if err := c.cycleAll(c.ErrorFilter); c.ErrorFilter(err) {
				errs <- err
			}
		}
	}()
	return nil
}

func (c *CycleService) cycleAll(quit func(error) bool) error {
	var retErr error
	nq := func(e error) bool {
		if quit(e) {
			retErr = e
			return true
		}
		return false
	}
	for _, ch := range c.Children {
		if retErr != nil {
			break
		}
		c.Messages <- "cycler launching " + fmt.Sprintf(`%T`, ch)
		ch.Cycle(nq)
	}
	return retErr
}

func (c *CycleService) Cycle(quit func(error) bool) {
	if c.Proxy != nil {
		c.Proxy(quit)
	}
}
