package dsp_flights

import (
	"encoding/json"
	"github.com/clixxa/dsp/bindings"
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
