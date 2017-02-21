package dsp_flights

import (
	"github.com/clixxa/dsp/bindings"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
)

// Uses environment variables and real database connections to create Runtimes
type BidEntrypoint struct {
	demandFlight atomic.Value

	BindingDeps bindings.BindingDeps
	Logic       BiddingLogic
	AllTest     bool
}

func (e *BidEntrypoint) Cycle() error {

	// create template demand flight
	df := &DemandFlight{}
	if old, found := e.demandFlight.Load().(*DemandFlight); found {
		e.BindingDeps.Debug.Println("using old runtime")
		df.Runtime = old.Runtime
	} else {
		df.Runtime.Logger = e.BindingDeps.Logger
		df.Runtime.Logger.Println("brand new runtime")
		df.Runtime.Debug = e.BindingDeps.Debug
		df.Runtime.Storage.Recalls = bindings.Recalls{Env: e.BindingDeps}.Save
		s := strings.Split(e.BindingDeps.DefaultKey, ":")
		key, iv := s[0], s[1]
		df.Runtime.DefaultB64 = &bindings.B64{Key: []byte(key), IV: []byte(iv)}
		df.Runtime.Logic = e.Logic
		df.Runtime.TestOnly = e.AllTest

		if err := (bindings.StatsDB{}).Marshal(e.BindingDeps.StatsDB); err != nil {
			e.BindingDeps.Debug.Println("err:", err.Error())
			return err
		}
	}

	if err := df.Runtime.Storage.Folders.Unmarshal(1, e.BindingDeps); err != nil {
		e.BindingDeps.Debug.Println("err:", err.Error())
		return err
	}
	if err := df.Runtime.Storage.Creatives.Unmarshal(1, e.BindingDeps); err != nil {
		e.BindingDeps.Debug.Println("err:", err.Error())
		return err
	}

	if err := df.Runtime.Storage.Users.Unmarshal(1, e.BindingDeps); err != nil {
		e.BindingDeps.Debug.Println("err:", err.Error())
		return err
	}
	if err := df.Runtime.Storage.Pseudonyms.Unmarshal(1, e.BindingDeps); err != nil {
		e.BindingDeps.Debug.Println("err:", err.Error())
		return err
	}

	e.demandFlight.Store(df)
	return nil
}

func (e *BidEntrypoint) DemandFlight() *DemandFlight {
	sf := e.demandFlight.Load().(*DemandFlight)
	flight := &DemandFlight{}
	flight.Runtime = sf.Runtime
	return flight
}

func (e *BidEntrypoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	request := e.DemandFlight()
	request.HttpRequest = r
	request.HttpResponse = w
	request.Launch()
}

type BiddingLogic interface {
	SelectFolderAndCreative(flight *DemandFlight, folders []ElegibleFolder, totalCpc int)
	CalculateRevshare(flight *DemandFlight) float64
	GenerateClickID(*DemandFlight) string
}

type SimpleLogic struct {
}

func (s SimpleLogic) SelectFolderAndCreative(flight *DemandFlight, folders []ElegibleFolder, totalCpc int) {
	eg := folders[flight.Request.Random255%len(folders)]
	foldIds := make([]string, len(folders))
	for n, folder := range folders {
		foldIds[n] = strconv.Itoa(folder.FolderID)
	}
	flight.Runtime.Logger.Println(`folders`, strings.Join(foldIds, ","), `to choose from, picked`, eg.FolderID)
	flight.FolderID = eg.FolderID
	flight.FullPrice = eg.BidAmount
	folder := flight.Runtime.Storage.Folders.ByID(eg.FolderID)
	flight.CreativeID = folder.Creative[flight.Request.Random255%len(folder.Creative)]
}

func (s SimpleLogic) CalculateRevshare(flight *DemandFlight) float64 { return 98.0 }

func (s SimpleLogic) GenerateClickID(*DemandFlight) string { return "" }
