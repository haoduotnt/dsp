package wish_flights

import (
	"encoding/json"
	"fmt"
	"github.com/clixxa/dsp/bindings"
	"github.com/clixxa/dsp/dsp_flights"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// Uses environment variables and real database connections to create Runtimes
type WishEntrypoint struct {
	BindingDeps       bindings.BindingDeps
	LockedBindingDeps *bindings.BindingDeps
	AllTest           bool
	Wins              chan *WinFlight
	Errors            chan error
	Messages          chan string
	ConfigLock        sync.RWMutex
}

func (e *WishEntrypoint) NewFlight() (*WinFlight, func()) {
	wf := &WinFlight{}
	return wf, func() { e.Wins <- wf }
}

func (e *WishEntrypoint) Launch(errs chan error) error {
	// create template win flight
	e.Wins = make(chan *WinFlight)
	e.Errors = errs
	go e.Consume()
	return nil
}

func (e *WishEntrypoint) Cycle() error {
	e.ConfigLock.Lock()
	defer e.ConfigLock.Unlock()
	e.LockedBindingDeps = &e.BindingDeps
	return nil
}

func (e *WishEntrypoint) Consume() {
	for {
		buff := make([]*WinFlight, 100)
		for n := range buff {
			buff[n] = <-e.Wins
		}
		e.ConsumeBatch(buff)
	}
}

func (e *WishEntrypoint) ConsumeBatch(buff []*WinFlight) {
	e.ConfigLock.RLock()
	defer e.ConfigLock.RLocker()

	purchases := bindings.Purchases{Env: *e.LockedBindingDeps}
	recalls := bindings.Recalls{Env: *e.LockedBindingDeps}

	start := time.Now()
	for _, wf := range buff {
		var err error

		// parse the incoming params
		if price, err := strconv.ParseInt(wf.PriceRaw, 10, 64); e != nil {
			e.Messages <- err.Error()
			continue
		} else {
			wf.PaidPrice = int(price)
		}

		if impid, err := strconv.ParseInt(wf.ImpRaw, 10, 64); e != nil {
			e.Messages <- err.Error()
			continue
		} else {
			wf.SaleID = int(impid)
		}

		// get the recalls
		e.Messages <- fmt.Sprintf(`getting bid info for %d`, wf.RecallID)
		recalls.Fetch(wf, &err, wf.RecallID)
		if err != nil {
			e.Messages <- err.Error()
			continue
		}

		// apply business logic
		wf.RevTXHome = wf.PaidPrice + wf.Margin
		e.Messages <- fmt.Sprintf(`adding margin of %d to paid price of %d`, wf.Margin, wf.PaidPrice)
		e.Messages <- fmt.Sprintf(`win: revssp%d revtx%d`, wf.PaidPrice, wf.RevTXHome)

		// store into purchases table
		e.Messages <- `inserting purchase record`
		purchases.Save(wf.Columns(), &err)
		if err != nil {
			e.Messages <- err.Error()
			continue
		}
	}

	e.Messages <- fmt.Sprintf(`win batch took %s`, time.Since(start))
}

type WinFlight struct {
	FolderID   int                 `json:"folder"`
	CreativeID int                 `json:"creative"`
	Request    dsp_flights.Request `json:"req"`
	Margin     int                 `json:"margin"`

	RevTXHome int    `json:"-"`
	PaidPrice int    `json:"-"`
	RecallID  string `json:"-"`
	SaleID    int    `json:"-"`

	PriceRaw, ImpRaw string
}

type wfProxy WinFlight

func (wf *WinFlight) String() string {
	return fmt.Sprintf(`winflight id%d`, wf.RecallID)
}

func (wf *WinFlight) Columns() [17]interface{} {
	return [17]interface{}{wf.SaleID, !wf.Request.RawRequest.Test, wf.RevTXHome, wf.RevTXHome, wf.PaidPrice, wf.PaidPrice, 0, wf.FolderID, wf.CreativeID, wf.Request.CountryID, wf.Request.VerticalID, wf.Request.BrandID, wf.Request.NetworkID, wf.Request.SubNetworkID, wf.Request.NetworkTypeID, wf.Request.GenderID, wf.Request.DeviceTypeID}
}

func (wf *WinFlight) UnmarshalJSON(d []byte) error {
	return json.Unmarshal(d, (*wfProxy)(wf))
}

func (wf *WinFlight) ParseQuery(u url.Values) {
	wf.RecallID = u.Get("key")
	wf.PriceRaw = u.Get("price")
	wf.ImpRaw = u.Get("imp")
}
