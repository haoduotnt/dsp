package rtb_types

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
		Vertical    string `json:"vertical"`
		Brand       string `json:"brand"`
		Network     string `json:"network"`
		SubNetwork  string `json:"subnetwork"`
		NetworkType string `json:"networktype"`
	} `json:"site"`
	Device struct {
		UserAgent  string `json:"ua"`
		DeviceType string `json:"devicetype"`
		Geo        struct {
			Country string `json:"country"`
		} `json:"geo"`
	} `json:"device"`
	User struct {
		Gender     string `json:"gender"`
		RemoteAddr string `json:"remoteaddr"`
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
