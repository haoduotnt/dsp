package services

import (
	"github.com/clixxa/dsp/bindings"
)

type Printer struct {
	Deps     bindings.BindingDeps
	Messages chan string
}

func (p *Printer) Launch(errs chan error) error {
	p.Deps.Logger.Println("starting printer")
	go func() {
		for str := range p.Messages {
			p.Deps.Logger.Println(str)
		}
	}()
	return nil
}
