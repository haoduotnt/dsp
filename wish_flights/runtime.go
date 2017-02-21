package wish_flights

import (
	"github.com/clixxa/dsp/bindings"
	"net/http"
	"strings"
	"sync/atomic"
)

// Uses environment variables and real database connections to create Runtimes
type WishEntrypoint struct {
	winFlight   atomic.Value
	BindingDeps bindings.BindingDeps

	AllTest  bool
	DeferSql bool
}

func (e *WishEntrypoint) Cycle() error {
	// create template win flight
	wf := &WinFlight{}
	if old, found := e.winFlight.Load().(*WinFlight); found {
		e.BindingDeps.Debug.Println("using old runtime")
		wf.Runtime = old.Runtime
	} else {
		wf.Runtime.Logger = e.BindingDeps.Logger
		wf.Runtime.Logger.Println("brand new runtime")
		wf.Runtime.Debug = e.BindingDeps.Debug

		wf.Runtime.Storage.Recall = bindings.Recalls{Env: e.BindingDeps, DoWork: !e.DeferSql}.Fetch
		wf.Runtime.Storage.Purchases = bindings.Purchases{Env: e.BindingDeps, DoWork: !e.DeferSql}.Save
	}

	e.winFlight.Store(wf)
	return nil
}

func (e *WishEntrypoint) Boot() {
	if err := e.Cycle(); err != nil {
		panic("couldn't do initial cycle " + err.Error())
	}
}

func (e *WishEntrypoint) Block() error {
	return http.ListenAndServe(":8080", e)
}

func (e *WishEntrypoint) WinFlight() *WinFlight {
	sf := e.winFlight.Load().(*WinFlight)
	flight := &WinFlight{}
	flight.Runtime = sf.Runtime
	return flight
}

func (e *WishEntrypoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.RequestURI, `/win`) {
		request := e.WinFlight()
		request.HttpRequest = r
		request.HttpResponse = w
		request.Launch()
	}
}
