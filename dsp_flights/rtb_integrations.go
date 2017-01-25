package dsp_flights

import ()

type Impression struct {
	ID       string `json:"id"`
	BidFloor int    `json:"bidfloor"`
	Redirect struct {
		BannedAttributes []string `json:"battr"`
	} `json:"redirect"`
}

type Request struct {
	Random255   int          `json:"rand"`
	Test        bool         `json:"test"`
	Impressions []Impression `json:"imp"`
	Site        struct {
		VerticalID    int `json:"vertical"`
		BrandID       int `json:"brand"`
		NetworkID     int `json:"network"`
		SubNetworkID  int `json:"subnetwork"`
		NetworkTypeID int `json:"networktype"`
	} `json:"site"`
	Device struct {
		DeviceTypeID int `json:"devicetype"`
		Geo          struct {
			CountryID int `json:"country"`
		} `json:"geo"`
	} `json:"device"`
	User struct {
		GenderID     int `json:"gender"`
		RemoteAddrID int `json:"remoteaddr"`
	} `json:"user"`
}

type Bid struct {
	ID     string  `json:"id"`
	Price  float64 `json:"price"`
	URL    string  `json:"rurl"`
	WinUrl string  `json:"nurl"`
}

type SeatBid struct {
	Bids []Bid `json:"bid"`
}

type Response struct {
	SeatBids []SeatBid `json:"seatbid"`
}

type WinNotice struct {
	PaidPrice    int    `json:"paidprice"`
	OfferedPrice int    `json:"offerprice"`
	RevPubHome   int    `json:"pubprice"`
	WinUrl       string `json:"nurl"`
	Bid          Bid    `json:"bid"`
	ImpID        int    `json:"impid"`
}
