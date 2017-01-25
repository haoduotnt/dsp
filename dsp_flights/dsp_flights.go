package dsp_flights

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type DemandFlight struct {
	Runtime struct {
		DefaultB64 *B64
		Storage    struct {
			Folders    Folders
			Creatives  Creatives
			Pseudonyms Pseudonyms
			Users      Users

			Recalls func(*DemandFlight)
		}
		Logger       *log.Logger
		Debug        *log.Logger
		TestOnly     bool
		RevshareFunc func(*DemandFlight) float64
		ClickIDFunc  func(*DemandFlight) string
	} `json:"-"`

	HttpRequest  *http.Request       `json:"-"`
	HttpResponse http.ResponseWriter `json:"-"`

	Response Response `json:"-"`

	StartTime time.Time

	FolderID   int     `json:"folder"`
	CreativeID int     `json:"creative"`
	Request    Request `json:"req"`
	Margin     int     `json:"margin"`

	RecallID  int    `json:"-"`
	FullPrice int    `json:"-"`
	WinUrl    string `json:"-"`

	Error error `json:"-"`
}

func (df *DemandFlight) String() string {
	e := "nil"
	if df.Error != nil {
		e = df.Error.Error()
	}
	return fmt.Sprintf(`demandflight e%s`, e)
}

func (df *DemandFlight) Launch() {
	ReadBidRequest(df)
	FindClient(df)
	WriteBidResponse(df)
}

func ReadBidRequest(flight *DemandFlight) {
	if e := json.NewDecoder(flight.HttpRequest.Body).Decode(&flight.Request); e != nil {
		flight.Error = e
		flight.Runtime.Logger.Println(`failed to decode body`, e.Error())
	}
	flight.WinUrl = `http://` + flight.HttpRequest.Host + `/win?price=${AUCTION_PRICE}&key=${AUCTION_BID_ID}&imp=${AUCTION_IMP_ID}`
	flight.StartTime = time.Now()
}

// Fill out the elegible bid
func FindClient(flight *DemandFlight) {
	flight.Runtime.Logger.Println(`starting FindClient`, flight.String())
	if flight.Error != nil {
		return
	}

	bid := Bid{}

	FolderMatches := func(folder *Folder) string {
		if !flight.Request.Test {
			if folder.Country > 0 && flight.Request.Device.Geo.CountryID != folder.Country {
				return "Country"
			}
		}
		if folder.Brand > 0 && flight.Request.Site.BrandID != folder.Brand {
			return "Brand"
		}
		if folder.Network > 0 && flight.Request.Site.NetworkID != folder.Network {
			return "Network"
		}
		if folder.NetworkType > 0 && flight.Request.Site.NetworkTypeID != folder.NetworkType {
			return "NetworkType"
		}
		if folder.SubNetwork > 0 && flight.Request.Site.SubNetworkID != folder.SubNetwork {
			return "SubNetwork"
		}
		if folder.Gender > 0 && flight.Request.User.GenderID != folder.Gender {
			return "Gender"
		}
		if folder.DeviceType > 0 && flight.Request.Device.DeviceTypeID != folder.DeviceType {
			return "DeviceType"
		}
		if folder.Vertical > 0 && flight.Request.Site.VerticalID != folder.Vertical {
			return "Vertical"
		}

		if folder.CPC > 0 && folder.CPC < flight.Request.Impressions[0].BidFloor {
			return "CPC"
		}
		return ""
	}

	Visit := func(folder *Folder) bool {
		if s := FolderMatches(folder); s != "" {
			flight.Runtime.Logger.Printf("folder %d doesn't match cause %s..", folder.ID, s)
			return false
		}

		flight.Runtime.Logger.Printf("folder %d matches..", folder.ID)

		if len(folder.Creative) > 0 {
			cpc := folder.CPC
			if folder.ParentID != nil && cpc == 0 {
				cpc = flight.Runtime.Storage.Folders.ByID(*folder.ParentID).CPC
			}

			diff := int(math.Pow((float64(cpc)/(float64(flight.FullPrice+cpc))), 2) * 255)
			flight.Runtime.Logger.Printf("folder %d diffs %d", folder.ID, diff)
			if diff >= flight.Request.Random255 {
				flight.FolderID = folder.ID
				flight.FullPrice = cpc
				flight.CreativeID = folder.Creative[flight.Request.Random255%len(folder.Creative)]
				flight.Runtime.Logger.Printf("folder %d selected at %d..", folder.ID, cpc)
			} else {
				flight.Runtime.Logger.Printf("folder %d not rand selected..", folder.ID)
			}
		}

		return true
	}

	for _, folder := range flight.Runtime.Storage.Folders {
		if folder.ParentID == nil {
			if !Visit(folder) {
				continue
			}
			for _, r := range folder.Children {
				if !Visit(flight.Runtime.Storage.Folders.ByID(r)) {
					continue
				}
			}
		}
	}

	if flight.FolderID == 0 {
		flight.Runtime.Logger.Println(`no folder found`)
		return
	}

	revShare := flight.Runtime.RevshareFunc(flight)
	if revShare > 100 {
		revShare = 100
	}
	flight.Runtime.Logger.Printf("rev calculated at %f", revShare)
	bid.Price = float64(flight.FullPrice) * revShare / 100
	flight.Margin = flight.FullPrice - int(bid.Price)

	net, found := flight.Runtime.Storage.Pseudonyms.NetworkIDS[flight.Request.Site.NetworkID]
	if !found {
		flight.Runtime.Logger.Printf(`net not found %d`, flight.Request.Site.NetworkID)
		net = ""
	}
	snet, found := flight.Runtime.Storage.Pseudonyms.SubnetworkIDS[flight.Request.Site.SubNetworkID]
	if !found {
		flight.Runtime.Logger.Printf(`snet not found %d`, flight.Request.Site.SubNetworkID)
		snet = ""
	}
	brand, found := flight.Runtime.Storage.Pseudonyms.BrandIDS[flight.Request.Site.BrandID]
	if !found {
		flight.Runtime.Logger.Printf(`brand not found %d`, flight.Request.Site.BrandID)
		brand = ""
	}
	vert, found := flight.Runtime.Storage.Pseudonyms.VerticalIDS[flight.Request.Site.VerticalID]
	if !found {
		flight.Runtime.Logger.Printf(`vert not found %d`, flight.Request.Site.VerticalID)
		vert = ""
	}

	ct := flight.Runtime.ClickIDFunc(flight)

	flight.Runtime.Logger.Println(`saving reference to KVS`)

	flight.Runtime.Storage.Recalls(flight)
	bid.ID = strconv.Itoa(flight.RecallID)

	bid.WinUrl = flight.WinUrl

	clickid := flight.Runtime.DefaultB64.Encrypt([]byte(fmt.Sprintf(`%d`, flight.RecallID)))

	cr := flight.Runtime.Storage.Creatives.ByID(flight.CreativeID)
	url := cr.RedirectUrl
	url = strings.Replace(url, `{realnetwork}`, "", 1)
	url = strings.Replace(url, `{realsubnetwork}`, "", 1)
	url = strings.Replace(url, `{ct}`, ct, 1)
	url = strings.Replace(url, `{clickid}`, fmt.Sprintf(`%s`, clickid), 1)

	url = strings.Replace(url, `{network}`, fmt.Sprintf(`%s`, net), 1)
	url = strings.Replace(url, `{subnetwork}`, fmt.Sprintf(`%s`, snet), 1)
	url = strings.Replace(url, `{brand}`, fmt.Sprintf(`%s`, brand), 1)
	url = strings.Replace(url, `{vertical}`, fmt.Sprintf(`%s`, vert), 1)

	bid.URL = url

	if flight.Error != nil {
		flight.Runtime.Logger.Println(`error occured in FindClient: %s`, flight.Error.Error())
		return
	}

	flight.Response.SeatBids = append(flight.Response.SeatBids, SeatBid{Bids: []Bid{bid}})
	flight.Runtime.Logger.Println("finished FindClient", flight.String())
}

func WriteBidResponse(flight *DemandFlight) {
	var res []byte
	if flight.Runtime.TestOnly && len(flight.Response.SeatBids) > 0 && !flight.Request.Test {
		flight.Runtime.Logger.Println(`test traffic only and traffic is non-test, removing bid`, flight.Response.SeatBids)
		flight.Response.SeatBids = nil
	}

	if len(flight.Response.SeatBids) > 0 {
		if j, e := json.Marshal(flight.Response); e != nil && flight.Error == nil {
			flight.Error = e
		} else {
			res = j
		}
	}

	if flight.Error != nil {
		flight.Runtime.Logger.Println("err encoding %s, returning 500", flight.Error.Error())
		flight.HttpResponse.WriteHeader(http.StatusInternalServerError)
	} else if res != nil {
		flight.HttpResponse.Write(res)
		flight.HttpResponse.WriteHeader(http.StatusOK)
	} else {
		flight.HttpResponse.WriteHeader(http.StatusNoContent)
	}
	flight.Runtime.Logger.Println(`dsp /bid took`, time.Since(flight.StartTime))
}

type WinFlight struct {
	Runtime struct {
		Storage struct {
			Purchases func(*WinFlight)
			Recall    func(*WinFlight)
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

	FolderID   int     `json:"folder"`
	CreativeID int     `json:"creative"`
	Margin     int     `json:"margin"`
	Request    Request `json:"req"`

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
	ReadWinNotice(wf)
	ProcessWin(wf)
	WriteWinResponse(wf)
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
	flight.Runtime.Storage.Recall(flight)
	flight.RevTXHome = flight.PaidPrice + flight.Margin

	flight.Runtime.Logger.Printf(`adding margin of %d to paid price of %d`, flight.Margin, flight.PaidPrice)
	flight.Runtime.Logger.Printf(`win: revssp%d revtx%d`, flight.PaidPrice, flight.RevTXHome)
	flight.Runtime.Logger.Println(`inserting purchase record`)
	flight.Runtime.Storage.Purchases(flight)
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
