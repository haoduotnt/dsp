package services

import (
	"github.com/clixxa/dsp/bindings"
)

type LaunchService struct {
	BindingDeps bindings.BindingDeps
	Errors      chan error
	Children    []interface {
		Launch(chan error) error
	}
}

func (l *LaunchService) Launch() error {
	for _, ch := range l.Children {
		if err := ch.Launch(l.Errors); err != nil {
			return err
		}
		l.BindingDeps.Logger.Println("launched", ch)
	}
	return <-l.Errors
}
