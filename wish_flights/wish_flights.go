package wish_flights

import (
	"encoding/json"
	"fmt"
	"github.com/clixxa/dsp/bindings"
	"github.com/clixxa/dsp/rtb_types"
	"github.com/clixxa/dsp/services"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// Uses environment variables and real database connections to create Runtimes
type WishEntrypoint struct {
	BindingDeps       services.BindingDeps
	LockedBindingDeps *services.BindingDeps
	AllTest           bool
	Wins              chan *WinFlight
	Errors            chan error
	Messages          chan string
	ConfigLock        sync.RWMutex
	Quit              func(error) bool
}

func (e *WishEntrypoint) NewFlight() (services.DecoderProxy, func() error) {
	wf := &WinFlight{}
	return wf, func() error {
		e.Wins <- wf
		return nil
	}
}

func (e *WishEntrypoint) Launch(errs chan error) error {
	// create template win flight
	e.Wins = make(chan *WinFlight)
	e.Errors = errs
	e.Messages <- "launching wish"
	go e.Consume()
	return nil
}

func (e *WishEntrypoint) Cycle(quit func(error) bool) {
	e.ConfigLock.Lock()
	defer e.ConfigLock.Unlock()
	e.Quit = quit
	e.LockedBindingDeps = &e.BindingDeps
}

func (e *WishEntrypoint) Consume() {
	for {
		e.Messages <- "waiting to consume a batch"
		buff := make([]*WinFlight, 100)
		done := false

		var to <-chan time.Time
		for n := range buff {
			if n == 0 {
				buff[n] = <-e.Wins
				to = time.After(time.Second * 10)
				continue
			}
			if done {
				break
			}
			select {
			case buff[n] = <-e.Wins:
			case <-to:
				done = true
				buff = buff[:n]
				break
			}
		}
		if len(buff) > 0 {
			e.ConsumeBatch(buff)
		}
	}
}

func (e *WishEntrypoint) ConsumeBatch(buff []*WinFlight) {
	e.ConfigLock.RLock()
	defer e.ConfigLock.RUnlock()

	quit := e.Quit

	purchases := bindings.Purchases{Env: *e.LockedBindingDeps}
	recalls := bindings.Recalls{Env: *e.LockedBindingDeps}

	start := time.Now()
	rows := [][17]interface{}{}
	for _, wf := range buff {

		// parse the incoming params
		if price, err := strconv.ParseInt(wf.PriceRaw, 10, 64); quit(&services.ErrParsing{"price", err}) {
			e.Messages <- err.Error()
			continue
		} else {
			wf.PaidPrice = int(price)
		}

		if impid, err := strconv.ParseInt(wf.ImpRaw, 10, 64); quit(&services.ErrParsing{"imp", err}) {
			continue
		} else {
			wf.SaleID = int(impid)
		}

		// get the recalls
		e.Messages <- fmt.Sprintf(`getting bid info for %d`, wf.RecallID)
		if quit(&services.ErrDatabaseMissing{"recallid", recalls.Fetch(wf, wf.RecallID)}) {
			return
		}

		// apply business logic
		wf.RevTXHome = wf.PaidPrice + wf.Margin
		e.Messages <- fmt.Sprintf(`adding margin of %d to paid price of %d`, wf.Margin, wf.PaidPrice)
		e.Messages <- fmt.Sprintf(`win: revssp%d revtx%d`, wf.PaidPrice, wf.RevTXHome)

		// store into purchases table
		rows = append(rows, wf.Columns())
	}

	e.Messages <- `inserting purchase records`
	purchases.Save(rows, quit)
	e.Messages <- fmt.Sprintf(`win batch did %d successfully in %s`, len(buff), time.Since(start))
}

type WinFlight struct {
	RevTXHome, PaidPrice, SaleID int
	RecallID, PriceRaw, ImpRaw   string
	rtb_types.BidSnapshot
}

type wfProxy WinFlight

func (wf *WinFlight) String() string {
	return fmt.Sprintf(`winflight id%d`, wf.RecallID)
}

func (wf *WinFlight) Columns() [17]interface{} {
	return [17]interface{}{wf.SaleID, !wf.BidSnapshot.Raw.Test, wf.RevTXHome, wf.RevTXHome, wf.PaidPrice, wf.PaidPrice, 0, wf.FolderID, wf.CreativeID, wf.BidSnapshot.Dims.CountryID, wf.BidSnapshot.Dims.VerticalID, wf.BidSnapshot.Dims.BrandID, wf.BidSnapshot.Dims.NetworkID, wf.BidSnapshot.Dims.SubNetworkID, wf.BidSnapshot.Dims.NetworkTypeID, wf.BidSnapshot.Dims.GenderID, wf.BidSnapshot.Dims.DeviceTypeID}
}

func (wf *WinFlight) UnmarshalJSON(d []byte) error {
	if len(d) == 0 {
		return nil
	}
	return json.Unmarshal(d, &wf.BidSnapshot)
}

func (wf *WinFlight) ParseQuery(u url.Values) {
	wf.RecallID = u.Get("key")
	wf.PriceRaw = u.Get("price")
	wf.ImpRaw = u.Get("imp")
}
