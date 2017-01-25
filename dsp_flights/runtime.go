package dsp_flights

import (
	"database/sql"
	"gopkg.in/redis.v5"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// Uses environment variables and real database connections to create Runtimes
type Production struct {
	demandFlight atomic.Value
	winFlight    atomic.Value

	Redis    *redis.Client
	Logger   *log.Logger
	Debug    *log.Logger
	ConfigDB *sql.DB
	StatsDB  *sql.DB

	RevshareFunc func(*DemandFlight) float64
	ClickIDFunc  func(*DemandFlight) string

	AllTest  bool
	DeferSql bool

	DefaultKey string
}

func (p *Production) ConfigDSN() *DSN {
	return &DSN{
		"mysql",
		os.Getenv("TCONFIGDBHOST"),
		os.Getenv("TCONFIGDBPORT"),
		os.Getenv("TCONFIGDB"),
		os.Getenv("TCONFIGDBUSERNAME"),
		os.Getenv("TCONFIGDBPASSWORD"),
	}
}

func (p *Production) StatsDSN() *DSN {
	return &DSN{
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
	if e.Debug == nil {
		e.Debug = log.New(os.Stderr, "", log.Lshortfile|log.Ltime)
	}

	if e.Logger == nil {
		e.Logger = log.New(os.Stdout, "", log.Lshortfile|log.Ltime)
		e.Debug.Println("created new Logger to stdout")
	}

	if e.DefaultKey == "" {
		e.DefaultKey = os.Getenv("TDEFAULTKEY")
	}

	if e.Redis == nil {
		e.Redis = redis.NewClient(&redis.Options{Addr: e.RedisDSN()})
		if err := e.Redis.Ping().Err(); err != nil {
			return err
		}
	}

	if e.ConfigDB == nil {
		e.Debug.Println("connecting to real config")
		dsn := e.ConfigDSN()
		db, err := sql.Open(dsn.Driver, dsn.Dump())
		if err != nil {
			e.Debug.Println("err:", err.Error())
			return err
		}
		if err := db.Ping(); err != nil {
			e.Debug.Println("err:", err.Error())
			return err
		}
		e.ConfigDB = db
	}

	if e.StatsDB == nil {
		e.Debug.Println("connecting to real stats")
		dsn := e.StatsDSN()
		db, err := sql.Open(dsn.Driver, dsn.Dump())
		if err != nil {
			e.Debug.Println("err:", err.Error())
			return err
		}
		if err := db.Ping(); err != nil {
			e.Debug.Println("err:", err.Error())
			return err
		}
		e.StatsDB = db
	}

	// create template demand flight
	df := &DemandFlight{}
	if old, found := e.demandFlight.Load().(*DemandFlight); found {
		e.Debug.Println("using old runtime")
		df.Runtime = old.Runtime
	} else {
		df.Runtime.Logger = e.Logger
		df.Runtime.Logger.Println("brand new runtime")
		df.Runtime.Debug = e.Debug
		df.Runtime.Storage.Recalls = Recalls{Env: e, DoWork: !e.DeferSql}.Save
		s := strings.Split(e.DefaultKey, ":")
		key, iv := s[0], s[1]
		df.Runtime.DefaultB64 = &B64{Key: []byte(key), IV: []byte(iv)}
		df.Runtime.RevshareFunc = e.RevshareFunc
		df.Runtime.TestOnly = e.AllTest

		if err := (StatsDB{}).Marshal(e.StatsDB); err != nil {
			e.Debug.Println("err:", err.Error())
			return err
		}
	}

	if err := df.Runtime.Storage.Folders.Unmarshal(1, e); err != nil {
		e.Debug.Println("err:", err.Error())
		return err
	}
	if err := df.Runtime.Storage.Creatives.Unmarshal(1, e); err != nil {
		e.Debug.Println("err:", err.Error())
		return err
	}

	if err := df.Runtime.Storage.Users.Unmarshal(1, e); err != nil {
		e.Debug.Println("err:", err.Error())
		return err
	}
	if err := df.Runtime.Storage.Pseudonyms.Unmarshal(1, e); err != nil {
		e.Debug.Println("err:", err.Error())
		return err
	}

	e.demandFlight.Store(df)

	// create template win flight
	wf := &WinFlight{}
	if old, found := e.winFlight.Load().(*WinFlight); found {
		e.Debug.Println("using old runtime")
		wf.Runtime = old.Runtime
	} else {
		wf.Runtime.Logger = e.Logger
		wf.Runtime.Logger.Println("brand new runtime")
		wf.Runtime.Debug = e.Debug

		wf.Runtime.Storage.Recall = Recalls{Env: e, DoWork: !e.DeferSql}.Fetch
		wf.Runtime.Storage.Purchases = Purchases{Env: e, DoWork: !e.DeferSql}.Save
	}

	e.winFlight.Store(wf)
	return nil
}

func (e *Production) Boot() {
	if err := e.Cycle(); err != nil {
		panic("couldn't do initial cycle " + err.Error())
	}
	go func() {
		for range time.NewTicker(time.Minute).C {
			if err := e.Cycle(); err != nil {
				e.Logger.Println("error cycling", err.Error())
			}
		}
	}()
}

func (e *Production) Block() error {
	e.Boot()
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
