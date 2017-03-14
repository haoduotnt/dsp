package services

import (
	"net/http"
)

type RouterService struct {
	Messages chan string
	Mux      *http.ServeMux
}

func (r *RouterService) Launch(errs chan error) error {
	r.Messages <- "launching router"
	go func() {
		errs <- http.ListenAndServe(":8080", r.Mux)
	}()
	return nil
}
