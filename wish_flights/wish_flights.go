package wish_flights

import (
	"encoding/json"
	"fmt"
	"github.com/clixxa/dsp/bindings"
	"github.com/clixxa/dsp/dsp_flights"
	"log"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"
	"sync/atomic"
	"time"
)

// Uses environment variables and real database connections to create Runtimes
type WishEntrypoint struct {
	winFlight   atomic.Value
	BindingDeps bindings.BindingDeps

	AllTest bool
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

		wf.Runtime.Storage.Recall = bindings.Recalls{Env: e.BindingDeps}.Fetch
		wf.Runtime.Storage.Purchases = bindings.Purchases{Env: e.BindingDeps}.Save
	}

	e.winFlight.Store(wf)
	return nil
}

func (e *WishEntrypoint) WinFlight() *WinFlight {
	sf := e.winFlight.Load().(*WinFlight)
	flight := &WinFlight{}
	flight.Runtime = sf.Runtime
	return flight
}

func (e *WishEntrypoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	request := e.WinFlight()
	request.HttpRequest = r
	request.HttpResponse = w
	request.Launch()
}

type WinFlight struct {
	Runtime struct {
		Storage struct {
			Purchases func([17]interface{}, *error)
			Recall    func(json.Unmarshaler, *error, string)
		}
		Logger *log.Logger
		Debug  *log.Logger
	} `json:"-"`

	HttpRequest  *http.Request       `json:"-"`
	HttpResponse http.ResponseWriter `json:"-"`

	RevTXHome int    `json:"-"`
	PaidPrice int    `json:"-"`
	RecallID  string `json:"-"`
	SaleID    int    `json:"-"`

	FolderID   int                 `json:"folder"`
	CreativeID int                 `json:"creative"`
	Margin     int                 `json:"margin"`
	Request    dsp_flights.Request `json:"req"`

	StartTime time.Time
	Error     error `json:"-"`
}

func (wf *WinFlight) String() string {
	e := ""
	if wf.Error != nil {
		e = wf.Error.Error()
	}
	return fmt.Sprintf(`winflight id%d err%s`, wf.RecallID, e)
}

func (wf *WinFlight) Launch() {
	defer func() {
		if err := recover(); err != nil {
			wf.Runtime.Logger.Println("uncaught panic, stack trace following", err)
			s := debug.Stack()
			wf.Runtime.Logger.Println(string(s))
		}
	}()
	ReadWinNotice(wf)
	ProcessWin(wf)
	WriteWinResponse(wf)
}

func (wf *WinFlight) Columns() [17]interface{} {
	return [17]interface{}{wf.SaleID, !wf.Request.Test, wf.RevTXHome, wf.RevTXHome, wf.PaidPrice, wf.PaidPrice, 0, wf.FolderID, wf.CreativeID, wf.Request.CountryID, wf.Request.VerticalID, wf.Request.BrandID, wf.Request.NetworkID, wf.Request.SubNetworkID, wf.Request.NetworkTypeID, wf.Request.GenderID, wf.Request.DeviceTypeID}
}

type wfProxy WinFlight

func (wf *WinFlight) UnmarshalJSON(d []byte) error {
	return json.Unmarshal(d, (*wfProxy)(wf))
}

func ReadWinNotice(flight *WinFlight) {
	flight.StartTime = time.Now()
	flight.Runtime.Logger.Println(`starting ProcessWin`, flight.String())

	if u, e := url.ParseRequestURI(flight.HttpRequest.RequestURI); e != nil {
		flight.Runtime.Logger.Println(`win url not valid`, e.Error())
	} else {
		flight.RecallID = u.Query().Get("key")
		flight.Runtime.Logger.Printf(`got recallid %s`, flight.RecallID)

		p := u.Query().Get("price")
		if price, e := strconv.ParseInt(p, 10, 64); e != nil {
			flight.Runtime.Logger.Println(`win url not valid`, e.Error())
		} else {
			flight.PaidPrice = int(price)
			flight.Runtime.Logger.Printf(`got price %d`, flight.PaidPrice)
		}

		imp := u.Query().Get("imp")
		if impid, e := strconv.ParseInt(imp, 10, 64); e != nil {
			flight.Runtime.Logger.Println(`win url not valid`, e.Error())
		} else {
			flight.SaleID = int(impid)
			flight.Runtime.Logger.Printf(`got impid %d`, flight.SaleID)
		}
	}
}

// Perform any post-flight logging, etc
func ProcessWin(flight *WinFlight) {
	if flight.Error != nil {
		flight.Runtime.Logger.Println(`not processing win because err: %s`, flight.Error.Error())
		return
	}

	flight.Runtime.Logger.Printf(`getting bid info for %d`, flight.RecallID)
	flight.Runtime.Storage.Recall(flight, &flight.Error, flight.RecallID)
	flight.RevTXHome = flight.PaidPrice + flight.Margin

	flight.Runtime.Logger.Printf(`adding margin of %d to paid price of %d`, flight.Margin, flight.PaidPrice)
	flight.Runtime.Logger.Printf(`win: revssp%d revtx%d`, flight.PaidPrice, flight.RevTXHome)
	flight.Runtime.Logger.Println(`inserting purchase record`)
	flight.Runtime.Storage.Purchases(flight.Columns(), &flight.Error)
}

func WriteWinResponse(flight *WinFlight) {
	if flight.Error != nil {
		flight.Runtime.Logger.Printf(`!! got an error handling win notice !! %s !!`, flight.Error.Error())
		flight.Runtime.Debug.Printf(`!! got an error handling win notice !! %s !!`, flight.Error.Error())
		flight.Runtime.Logger.Printf(`winflight %#v`, flight)
		flight.HttpResponse.WriteHeader(http.StatusInternalServerError)
	} else {
		flight.HttpResponse.WriteHeader(http.StatusOK)
	}
	flight.Runtime.Logger.Println(`dsp /win took`, time.Since(flight.StartTime))
}
