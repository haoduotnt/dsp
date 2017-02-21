package services

import (
	"github.com/clixxa/dsp/bindings"
	"net/http"
)

type RouterService struct {
	BindingDeps bindings.BindingDeps
	Mux         *http.ServeMux
}

func (r *RouterService) Add(s string, h http.Handler) {
	r.BindingDeps.Logger.Println("adding", h, "at", s)
}

func (r *RouterService) Cycle() error {
	return nil
}

func (r *RouterService) Launch(chan error) error {
	return nil
}
