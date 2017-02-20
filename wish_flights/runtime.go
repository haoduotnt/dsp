package wish_flights

import (
	"database/sql"
	"github.com/clixxa/dsp/bindings"
	"gopkg.in/redis.v5"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
)

// Uses environment variables and real database connections to create Runtimes
type Production struct {
	demandFlight atomic.Value
	winFlight    atomic.Value

	BindingDeps bindings.BindingDeps

	Logic  BiddingLogic

	AllTest  bool
	DeferSql bool
}

func (p *Production) ConfigDSN() *bindings.DSN {
	return &bindings.DSN{
		"mysql",
		os.Getenv("TCONFIGDBHOST"),
		os.Getenv("TCONFIGDBPORT"),
		os.Getenv("TCONFIGDB"),
		os.Getenv("TCONFIGDBUSERNAME"),
		os.Getenv("TCONFIGDBPASSWORD"),
	}
}

func (p *Production) StatsDSN() *bindings.DSN {
	return &bindings.DSN{
		"postgres",
		os.Getenv("TSTATSDBHOST"),
		os.Getenv("TSTATSDBPORT"),
		os.Getenv("TSTATSDB"),
		os.Getenv("TSTATSDBUSERNAME"),
		os.Getenv("TSTATSDBPASSWORD"),
	}
}

func (p *Production) RedisDSN() string {
	return os.Getenv("TRECALLURL")
}

func (e *Production) Cycle() error {
	if e.BindingDeps.Debug == nil {
		e.BindingDeps.Debug = log.New(os.Stderr, "", log.Lshortfile|log.Ltime)
	}

	if e.BindingDeps.Logger == nil {
		e.BindingDeps.Logger = log.New(os.Stdout, "", log.Lshortfile|log.Ltime)
		e.BindingDeps.Debug.Println("created new Logger to stdout")
	}

	e.BindingDeps.Logger.Printf("logic %#v", e.Logic)

	if e.BindingDeps.DefaultKey == "" {
		e.BindingDeps.DefaultKey = os.Getenv("TDEFAULTKEY")
	}

	if e.BindingDeps.Redis == nil {
		e.BindingDeps.Redis = redis.NewClient(&redis.Options{Addr: e.RedisDSN()})
		if err := e.BindingDeps.Redis.Ping().Err(); err != nil {
			return err
		}
	}

	if e.BindingDeps.ConfigDB == nil {
		e.BindingDeps.Debug.Println("connecting to real config")
		dsn := e.ConfigDSN()
		db, err := sql.Open(dsn.Driver, dsn.Dump())
		if err != nil {
			e.BindingDeps.Debug.Println("err:", err.Error())
			return err
		}
		if err := db.Ping(); err != nil {
			e.BindingDeps.Debug.Println("err:", err.Error())
			return err
		}
		e.BindingDeps.ConfigDB = db
	}

	if e.BindingDeps.StatsDB == nil {
		e.BindingDeps.Debug.Println("connecting to real stats")
		dsn := e.StatsDSN()
		db, err := sql.Open(dsn.Driver, dsn.Dump())
		if err != nil {
			e.BindingDeps.Debug.Println("err:", err.Error())
			return err
		}
		if err := db.Ping(); err != nil {
			e.BindingDeps.Debug.Println("err:", err.Error())
			return err
		}
		e.BindingDeps.StatsDB = db
	}

	// create template demand flight
	df := &DemandFlight{}
	if old, found := e.demandFlight.Load().(*DemandFlight); found {
		e.BindingDeps.Debug.Println("using old runtime")
		df.Runtime = old.Runtime
	} else {
		df.Runtime.Logger = e.BindingDeps.Logger
		df.Runtime.Logger.Println("brand new runtime")
		df.Runtime.Debug = e.BindingDeps.Debug
		df.Runtime.Storage.Recalls = bindings.Recalls{Env: e.BindingDeps, DoWork: !e.DeferSql}.Save
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

func (e *Production) Boot() {
	if err := e.Cycle(); err != nil {
		panic("couldn't do initial cycle " + err.Error())
	}
}

func (e *Production) Block() error {
	return http.ListenAndServe(":8080", e)
}

func (e *Production) DemandFlight() *DemandFlight {
	sf := e.demandFlight.Load().(*DemandFlight)
	flight := &DemandFlight{}
	flight.Runtime = sf.Runtime
	return flight
}

func (e *Production) WinFlight() *WinFlight {
	sf := e.winFlight.Load().(*WinFlight)
	flight := &WinFlight{}
	flight.Runtime = sf.Runtime
	return flight
}

func (e *Production) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.RequestURI, `/win`) {
		request := e.WinFlight()
		request.HttpRequest = r
		request.HttpResponse = w
		request.Launch()
	} else {
		request := e.DemandFlight()
		request.HttpRequest = r
		request.HttpResponse = w
		request.Launch()
	}
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
