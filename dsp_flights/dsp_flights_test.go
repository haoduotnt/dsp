package dsp_flights

import (
	"testing"
)

func TestStageFindClient(t *testing.T) {
	l, fin := testLogger(t)
	flight := &DemandFlight{}
	flight.Runtime.Logger = l
	flight.Runtime.Logger.Println("testing StoreFlight, before:", flight)
	flight.Runtime.DefaultB64 = &B64{Key: []byte("gekk"), IV: []byte("whatwhat")}

	store := &flight.Runtime.Storage
	store.Recalls = func(df *DemandFlight) {
		t.Log("recall save", df)
	}
	flight.Runtime.RevshareFunc = func(*DemandFlight) float64 { return 95.0 }
	flight.Runtime.ClickIDFunc = func(*DemandFlight) string { return "" }

	crid := store.Creatives.Add(&Creative{})
	own := store.Users.Add(&User{Age: 10})

	bfid := store.Folders.Add(&Folder{OwnerID: own, Brand: 6, Creative: []int{crid}, CPC: 350})
	store.Folders.Add(&Folder{Country: 3, Children: []int{bfid}, CPC: 500})
	store.Folders.Add(&Folder{Country: 4, CPC: 500})
	store.Folders.Add(&Folder{Country: 3, Brand: 6, CPC: 50})
	badfolder := store.Folders.Add(&Folder{OwnerID: own, Country: 3, CPC: 50})
	store.Folders.Add(&Folder{Country: 3, CPC: 700, Children: []int{badfolder}})
	randpick := store.Folders.Add(&Folder{OwnerID: own, Country: 3, Brand: 6, CPC: 500, Creative: []int{crid}})
	store.Folders.Add(&Folder{Country: 3, Brand: 6, CPC: 250})

	flight.Request.Impressions = []Impression{Impression{}}
	flight.Request.Device.Geo.CountryID = 3
	flight.Request.Site.BrandID = 6

	tests := [][]int{[]int{bfid, 255}, []int{bfid, 100}, []int{bfid, 90}, []int{randpick, 0}, []int{randpick, 50}}
	for _, n := range tests {
		rightFolder := n[0]
		flight.Request.Random255 = n[1]
		flight.Response.SeatBids = nil
		flight.FolderID = 0
		flight.CreativeID = 0
		flight.FullPrice = 0

		flight.Runtime.Logger.Println("testing FindClient, before:", flight)
		FindClient(flight)
		flight.Runtime.Logger.Println("after:", flight)
		fin()
		t.Logf(`wanted folder %d, got %d`, rightFolder, flight.FolderID)
		if flight.FolderID != rightFolder {
			t.Log(flight.Runtime.Storage.Folders.ByID(flight.FolderID))
			t.Error("folder mismatch")
		}
		t.Logf(`wanted creative %d, got %d`, crid, flight.CreativeID)
		if flight.CreativeID != crid {
			t.Error("creative mismatch")
		}
		t.Logf(`dsp bid with, got %f`, flight.Response.SeatBids[0].Bids[0].Price)
		amt := store.Folders.ByID(rightFolder).CPC
		t.Logf(`wanted bid amount %d, got %d`, amt, flight.FullPrice)
		if flight.FullPrice != amt {
			t.Error("amount mismatch")
		}
		if flight.Error != nil {
			t.Error(flight.Error)
		}
	}
}
