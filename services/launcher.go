package services

import (
	"fmt"
)

type LaunchService struct {
	Messages chan string
	Errors   chan error
	Children []interface {
		Launch(chan error) error
	}
}

func (l *LaunchService) Launch() error {
	l.Errors = make(chan error, 10)
	go func() {
		for err := range l.Errors {
			l.Messages <- "CYCLE ERR " + err.Error()
		}
	}()
	for _, ch := range l.Children {
		l.Messages <- fmt.Sprintf(`starting to launch %T`, ch)
		if err := ch.Launch(l.Errors); err != nil {
			l.Messages <- fmt.Sprintf(`launch err: %s`, err)
			return err
		}
	}
	select {}
	return nil
}
