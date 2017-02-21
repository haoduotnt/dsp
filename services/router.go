package services

import (
	"github.com/clixxa/dsp/bindings"
	"net/http"
)

type RouterService struct {
	BindingDeps bindings.BindingDeps
	Mux         *http.ServeMux
}

func (r *RouterService) Launch(errs chan error) error {
	go func() {
		errs <- http.ListenAndServe(":8080", r.Mux)
	}()
	return nil
}
