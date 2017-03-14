package services

import (
	"log"
	"time"
)

type Printer struct {
	Messages chan string
	PrintTo  *log.Logger
}

func (p *Printer) Launch(errs chan error) error {
	p.Messages <- "launching printer"
	go func() {
		timeChanged := true
		go func() {
			for range time.NewTicker(time.Second).C {
				timeChanged = true
			}
		}()
		for str := range p.Messages {
			if timeChanged {
				p.PrintTo.Println(time.Now().Format(time.Stamp))
				timeChanged = false
			}
			p.PrintTo.Println(str)
		}
	}()
	return nil
}

func (p *Printer) Flush() {
	close(p.Messages)
	for str := range p.Messages {
		p.PrintTo.Println(str)
	}
}
