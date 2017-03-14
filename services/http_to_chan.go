package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

type DecoderProxy interface {
	json.Unmarshaler
}

type Querier interface {
	ParseQuery(url.Values)
}

type HttpToChannel struct {
	ObjectFactory func() (DecoderProxy, func() error)
	Messages      chan string
}

func (h *HttpToChannel) ServeHTTP(r *http.Request, w http.ResponseWriter) {
	b := bytes.NewBuffer(nil)
	if _, err := b.ReadFrom(r.Body); err != nil {
		w.WriteHeader(500)
		h.Messages <- "failed to read because " + err.Error()
		return
	}
	r.Body.Close()
	h.Messages <- fmt.Sprintf(`recieved %s`, b.String())
	o, ready := h.ObjectFactory()
	if err := o.UnmarshalJSON(b.Bytes()); err != nil {
		w.WriteHeader(500)
		h.Messages <- "failed to decode because " + err.Error()
		return
	}

	if q, ok := o.(Querier); ok {
		v, err := url.ParseRequestURI(r.RequestURI)
		if err != nil {
			w.WriteHeader(500)
			h.Messages <- "failed to decode because " + err.Error()
			return
		}
		q.ParseQuery(v.Query())
	}

	if err := ready(); err != nil {
		w.WriteHeader(500)
		h.Messages <- "setup failed because " + err.Error()
		return
	}
	w.WriteHeader(200)
}
