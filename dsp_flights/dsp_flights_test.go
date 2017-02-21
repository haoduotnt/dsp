package dsp_flights

import (
	"encoding/json"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/clixxa/dsp/bindings"
	"gopkg.in/redis.v5"
	"testing"
)

func TestStageFindClient(t *testing.T) {
	l, fin := bindings.BufferedLogger(t)
	flight := &DemandFlight{}
	flight.Runtime.Logger = l
	flight.Runtime.Logger.Println("testing StoreFlight, before:", flight)
	flight.Runtime.DefaultB64 = &bindings.B64{Key: []byte("gekk"), IV: []byte("whatwhat")}

	store := &flight.Runtime.Storage
	store.Recalls = func(df json.Marshaler, a *error, b *int) {
		t.Log("recall save", df)
	}
	flight.Runtime.Logic = SimpleLogic{}

	crid := store.Creatives.Add(&bindings.Creative{})
	own := store.Users.Add(&bindings.User{Age: 10})

	bfid := store.Folders.Add(&bindings.Folder{OwnerID: own, Brand: 6, Creative: []int{crid}, CPC: 350})
	store.Folders.Add(&bindings.Folder{Country: 3, Children: []int{bfid}, CPC: 500})
	store.Folders.Add(&bindings.Folder{Country: 4, CPC: 500})
	store.Folders.Add(&bindings.Folder{Country: 3, Brand: 6, CPC: 50})
	badfolder := store.Folders.Add(&bindings.Folder{OwnerID: own, Country: 3, CPC: 50})
	store.Folders.Add(&bindings.Folder{Country: 3, CPC: 700, Children: []int{badfolder}})
	randpick := store.Folders.Add(&bindings.Folder{OwnerID: own, Country: 3, Brand: 6, CPC: 500, Creative: []int{crid}})
	_ = randpick
	store.Folders.Add(&bindings.Folder{Country: 3, Brand: 6, CPC: 250})

	flight.Request.Impressions = []Impression{Impression{}}
	flight.Request.Device.Geo.CountryID = 3
	flight.Request.Site.BrandID = 6

	res := map[int]int{}
	for i := 0; i < 255; i++ {
		flight.Request.Random255 = i
		flight.Response.SeatBids = nil
		flight.FolderID = 0
		flight.CreativeID = 0
		flight.FullPrice = 0

		flight.Runtime.Logger.Println("testing FindClient, before:", flight)
		FindClient(flight)
		flight.Runtime.Logger.Println("after:", flight)
		fin()
		if _, found := res[flight.FolderID]; !found {
			res[flight.FolderID] = 0
		}
		res[flight.FolderID] += 1
	}
	t.Log(res)
	if d := res[bfid] - res[randpick]; d < -5 || d > 5 {
		t.Error("unequal distribution")
	}
}

func TestLoadAll(t *testing.T) {
	db, sqlm, _ := sqlmock.New()

	sqlm.ExpectExec("purchases").WillReturnError(fmt.Errorf(`expectedErr`))

	sqlm.ExpectQuery("folders").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(5))

	sqlm.ExpectQuery("folders").WithArgs(5).
		WillReturnRows(sqlmock.NewRows([]string{"budget", "bid", "creative_id", "owner", "status"}).
			AddRow(100, 50, 30, 5, "live"))
	sqlm.ExpectQuery("parent_folder").WithArgs(5).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("7"))
	sqlm.ExpectQuery("parent_folder").WithArgs(5).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("8"))
	sqlm.ExpectQuery("dimentions").WithArgs(5).WillReturnRows(sqlmock.NewRows([]string{"a", "b"}))

	sqlm.ExpectQuery("creatives").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(5))
	sqlm.ExpectQuery("creatives").WithArgs(5).
		WillReturnRows(sqlmock.NewRows([]string{"url"}).AddRow("test.com"))

	sqlm.ExpectQuery("users").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(5))
	sqlm.ExpectQuery("ip_histories").WithArgs(5).
		WillReturnRows(sqlmock.NewRows([]string{"ip"}).AddRow("1.1.1.1"))
	sqlm.ExpectQuery("user_settings").WithArgs(5).
		WillReturnRows(sqlmock.NewRows([]string{"setting", "value"}).AddRow(6, "what"))

	sqlm.ExpectQuery("SELECT (.+) FROM countries").
		WillReturnRows(sqlmock.NewRows([]string{"id", "iso_2alpha"}))

	sqlm.ExpectQuery("SELECT (.+) FROM networks").
		WillReturnRows(sqlmock.NewRows([]string{"id", "iso_2alpha"}))

	sqlm.ExpectQuery("SELECT (.+) FROM subnetworks").
		WillReturnRows(sqlmock.NewRows([]string{"id", "iso_2alpha"}))

	sqlm.ExpectQuery("SELECT (.+) FROM subnetworks").
		WillReturnRows(sqlmock.NewRows([]string{"id", "iso_2alpha"}))

	sqlm.ExpectQuery("SELECT (.+) FROM brands").
		WillReturnRows(sqlmock.NewRows([]string{"id", "iso_2alpha"}))

	sqlm.ExpectQuery("SELECT (.+) FROM verticals").
		WillReturnRows(sqlmock.NewRows([]string{"id", "iso_2alpha"}))

	sqlm.MatchExpectationsInOrder(false)

	out, dump := bindings.BufferedLogger(t)
	if err := (&BidEntrypoint{BindingDeps: bindings.BindingDeps{ConfigDB: db, StatsDB: db, Logger: out, Debug: out, DefaultKey: ":", Redis: redis.NewClient(&redis.Options{})}}).Cycle(); err != nil {
		t.Log("failed to cycle, dumping")
		dump()
		t.Log("err", err.Error())
	} else {
		dump()
	}
	if err := sqlm.ExpectationsWereMet(); err != nil {
		t.Error("err", err.Error())
	}
}
