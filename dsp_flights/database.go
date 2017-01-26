package dsp_flights

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

func tojson(i interface{}) string {
	s, _ := json.Marshal(i)
	return string(s)
}

func wide(depth int) string {
	return strings.Repeat("\t", depth)
}

type DSN struct {
	Driver   string
	Host     string
	Port     string
	Database string
	Username string
	Password string
}

func (d *DSN) Dump() string {
	tmpl := `unfilled %s, %s, %s, %s, %s`
	if d.Driver == "mysql" {
		tmpl = `%s:%s@tcp(%s:%s)/%s?autocommit=true`
	} else {
		tmpl = `postgres://%s:%s@%s:%s/%s?sslmode=disable`
	}
	return fmt.Sprintf(tmpl, d.Username, d.Password, d.Host, d.Port, d.Database)
}

func (d *DSN) String() string {
	if len(d.Password) > 3 {
		return fmt.Sprintf(`host %s:%s, user %s, pw %s, db %s`, d.Host, d.Port, d.Username, d.Password[:2], d.Database)
	}
	return fmt.Sprintf(`host %s:%s, user %s, pw is too short to display, db %s`, d.Host, d.Port, d.Username, d.Database)
}

const sqlUserIPs = `SELECT ip FROM ip_histories WHERE user_id = ?`
const sqlUser = `SELECT setting_id, value FROM user_settings WHERE user_id = ?`
const sqlDimention = `SELECT dimentions_id, dimentions_type FROM dimentions WHERE folder_id = ?`
const sqlDimension = `SELECT dimensions_id, dimensions_type FROM dimensions WHERE folder_id = ?`
const sqlFolder = `SELECT budget, bid, creative_id, user_id FROM folders LEFT JOIN creative_folder ON folder_id = id WHERE id = ?`
const sqlCreative = `SELECT destination_url FROM creatives cr WHERE cr.id = ?`
const sqlCountries = `SELECT id, iso_2alpha FROM countries`
const sqlNetworks = `SELECT id, pseudonym FROM networks`
const sqlSubNetworks = `SELECT id, pseudonym FROM subnetworks`
const sqlSubNetworkLabels = `SELECT id, label FROM subnetworks`
const sqlBrands = `SELECT id, label FROM brands`
const sqlBrandSlugs = `SELECT id, slug FROM brands`
const sqlVerticals = `SELECT id, label FROM verticals`
const sqlSubnetworkToNetwork = `SELECT id, network_id FROM subnetworks`
const sqlNetworkToNetworkType = `SELECT network_id, network_type_id FROM network_network_type`

type Pseudonyms struct {
	Countries          map[string]int
	CountryIDS         map[int]string
	Networks           map[string]int
	NetworkIDS         map[int]string
	Subnetworks        map[string]int
	SubnetworkIDS      map[int]string
	SubnetworkLabels   map[string]int
	SubnetworkLabelIDS map[int]string
	Brands             map[string]int
	BrandIDS           map[int]string
	BrandSlugs         map[string]int
	BrandSlugIDS       map[int]string
	Verticals          map[string]int
	VerticalIDS        map[int]string

	SubnetworkToNetwork  map[int]int
	NetworkToNetworkType map[int]int

	DeviceTypes map[string]int
}

func (c *Pseudonyms) Unmarshal(depth int, env *Production) error {
	c.Namespace(env, sqlCountries, &c.Countries, &c.CountryIDS)
	c.Namespace(env, sqlNetworks, &c.Networks, &c.NetworkIDS)
	c.Namespace(env, sqlSubNetworks, &c.Subnetworks, &c.SubnetworkIDS)
	c.Namespace(env, sqlSubNetworkLabels, &c.SubnetworkLabels, &c.SubnetworkLabelIDS)
	c.Namespace(env, sqlBrands, &c.Brands, &c.BrandIDS)
	c.Namespace(env, sqlBrandSlugs, &c.BrandSlugs, &c.BrandSlugIDS)
	c.Namespace(env, sqlVerticals, &c.Verticals, &c.VerticalIDS)

	c.Map(env, sqlNetworkToNetworkType, &c.NetworkToNetworkType)
	c.Map(env, sqlSubnetworkToNetwork, &c.SubnetworkToNetwork)

	c.DeviceTypes = map[string]int{"desktop": 1, "mobile": 2, "tablet": 3, "unknown": 4}

	env.Debug.Printf("LOADED %s %T %s", wide(depth), c, tojson(c))
	return nil
}

func (c *Pseudonyms) Map(env *Production, sql string, dest *map[int]int) error {
	rows, err := env.ConfigDB.Query(sql)
	if err != nil {
		env.Debug.Println("err", err)
		return err
	}
	*dest = make(map[int]int)
	for rows.Next() {
		var left_side int
		var right_side int
		if err := rows.Scan(&left_side, &right_side); err != nil {
			env.Debug.Println("err", err)
			return err
		}
		(*dest)[left_side] = right_side
	}
	return nil
}

func (c *Pseudonyms) Namespace(env *Production, sql string, dest *map[string]int, dest2 *map[int]string) error {
	rows, err := env.ConfigDB.Query(sql)
	if err != nil {
		env.Debug.Println("err", err)
		return err
	}
	*dest = make(map[string]int)
	*dest2 = make(map[int]string)
	for rows.Next() {
		var realName string
		var id int
		if err := rows.Scan(&id, &realName); err != nil {
			env.Debug.Println("err", err)
			return err
		}
		(*dest)[realName] = id
		(*dest2)[id] = realName
	}
	return nil
}

type Users []*User

func (f *Users) ByID(id int) *User {
	for _, u := range *f {
		if u.ID == id {
			return u
		}
	}
	return nil
}

func (c *Users) Add(ch *User) int {
	m := 1
	for _, och := range *c {
		if och.ID >= m {
			m = och.ID + 1
		}
	}
	ch.ID = m
	*c = append(*c, ch)
	return ch.ID
}

func (f *Users) Unmarshal(depth int, env *Production) error {
	var rows *sql.Rows
	var err error
	rows, err = env.ConfigDB.Query(`SELECT id FROM users`)
	if err != nil {
		env.Debug.Println("err", err)
		return err
	}
	var id int
	*f = (*f)[:0]
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			env.Debug.Println("err", err)
			return err
		}
		*f = append(*f, &User{ID: id})
	}
	for _, f := range *f {
		if err := f.Unmarshal(depth+1, env); err != nil {
			env.Debug.Println("err", err)
			return err
		}
	}
	env.Debug.Printf("LOADED %s %T %s", wide(depth), f, tojson(f))
	return nil
}

type User struct {
	ID  int
	IPs []string
	Age int
	Key string
	B64 *B64
}

func (u *User) Unmarshal(depth int, env *Production) error {
	rows, err := env.ConfigDB.Query(sqlUserIPs, u.ID)
	if err != nil {
		env.Debug.Println("err", err)
		return err
	}
	var ip string
	for rows.Next() {
		if err := rows.Scan(&ip); err != nil {
			env.Debug.Println("err", err)
			return err
		}
		u.IPs = append(u.IPs, ip)
	}

	{
		rows, err := env.ConfigDB.Query(sqlUser, u.ID)
		if err != nil {
			env.Debug.Println("err", err)
			return err
		}
		var value string
		var setting int
		for rows.Next() {
			if err := rows.Scan(&setting, &value); err != nil {
				env.Debug.Println("err", err)
				return err
			}
			switch setting {
			case 5:
				u.Age, _ = strconv.Atoi(value)
			case 6:
				u.Key = value
			}
		}
	}

	s := strings.Split(env.DefaultKey, ":")
	key, iv := s[0], s[1]
	if u.Key != "" {
		key = u.Key
	}
	u.B64 = &B64{Key: []byte(key), IV: []byte(iv)}

	env.Debug.Printf("LOADED %s %T %s", wide(depth), u, tojson(u))
	return nil
}

func AllIDs(table string, env *Production) ([]int, error) {
	rows, err := env.ConfigDB.Query(`SELECT id FROM ` + table)
	if err != nil {
		env.Debug.Println("err", err)
		return nil, err
	}
	var id int
	var ids []int
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			env.Debug.Println("err", err)
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

type Dimensions struct {
	FolderID   int
	Dimensions []*Dimension
	mode       int
}

func (d *Dimensions) Unmarshal(depth int, env *Production) error {
	sql := sqlDimension
	if d.mode == 1 {
		sql = sqlDimention
	}
	rows, err := env.ConfigDB.Query(sql, d.FolderID)
	if err != nil {
		if d.mode == 0 {
			d.mode = 1
			env.Debug.Println("dimension didn't work, trying dimention")
			return d.Unmarshal(depth, env)
		}
		env.Debug.Println("err", err)
		return err
	}
	for rows.Next() {
		dim := &Dimension{}
		if err := rows.Scan(&dim.Value, &dim.Type); err != nil {
			env.Debug.Println("err", err)
			return err
		}
		d.Dimensions = append(d.Dimensions, dim)
	}

	env.Debug.Printf("LOADED %s %T %s", wide(depth), d, tojson(d))
	return nil
}

type Dimension struct {
	Type  string
	Value int
}

func (d *Dimension) Transfer(f *Folder) error {
	parts := strings.Split(d.Type, `\`)
	switch parts[len(parts)-1] {
	case `Vertical`:
		f.Vertical = d.Value
		return nil
	case `Country`:
		f.Country = d.Value
		return nil
	case `Brand`:
		f.Brand = d.Value
		return nil
	case `Network`:
		f.Network = d.Value
		return nil
	case `SubNetwork`:
		f.SubNetwork = d.Value
		return nil
	case `NetworkType`:
		f.NetworkType = d.Value
		return nil
	case `Gender`:
		f.Gender = d.Value
		return nil
	case `DeviceType`:
		f.DeviceType = d.Value
		return nil
	default:
		return fmt.Errorf(`unknown type: %s`, d.Type)
	}
}

type Folder struct {
	ID       int
	ParentID *int
	Children []int
	Creative []int
	CPC      int
	Budget   int
	OwnerID  int

	Vertical    int
	Country     int
	Brand       int
	Network     int
	SubNetwork  int
	NetworkType int
	Gender      int
	DeviceType  int

	mode int
}

func (f *Folder) Unmarshal(depth int, env *Production) error {
	// var child_id, creative_id int
	var creative_id sql.NullInt64
	row := env.ConfigDB.QueryRow(sqlFolder, f.ID)

	var budget, bid sql.NullInt64
	if err := row.Scan(&budget, &bid, &creative_id, &f.OwnerID); err != nil {
		if f.mode == 0 {
			f.mode = 1
			env.Debug.Println("users didn't work, trying user")
			return f.Unmarshal(depth, env)
		}
		env.Debug.Println("err", err)
		return err
	}

	if budget.Valid {
		f.Budget = int(budget.Int64)
	}

	if bid.Valid {
		f.CPC = int(bid.Int64)
	}

	if creative_id.Valid {
		f.Creative = append(f.Creative, int(creative_id.Int64))
	}

	{
		rows, err := env.ConfigDB.Query(`SELECT child_folder_id FROM parent_folder WHERE parent_folder_id = ?`, f.ID)
		if err != nil {
			return err
		}
		var id int
		for rows.Next() {
			if err := rows.Scan(&id); err != nil {
				env.Debug.Println("err", err)
				return err
			}
			f.Children = append(f.Children, id)
		}
	}

	{
		rows, err := env.ConfigDB.Query(`SELECT parent_folder_id FROM parent_folder WHERE child_folder_id = ?`, f.ID)
		if err != nil {
			return err
		}
		var id sql.NullInt64
		for rows.Next() {
			if err := rows.Scan(&id); err != nil {
				env.Debug.Println("err", err)
				return err
			}
			if id.Valid {
				i := int(id.Int64)
				f.ParentID = &i
			}
		}
	}

	// dimensions
	d := &Dimensions{FolderID: f.ID}
	if err := d.Unmarshal(depth+1, env); err != nil {
		env.Debug.Println("err", err)
		return err
	}
	for _, dim := range d.Dimensions {
		if err := dim.Transfer(f); err != nil {
			env.Debug.Println("err", err)
			return err
		}
	}

	env.Debug.Printf("LOADED %s %T %s", wide(depth), f, tojson(f))
	return nil
}

func (f *Folder) String() string {
	dims := fmt.Sprintf(`ve %d, co %d, br %d, ne %d, su %d, nt %d, ge %d, de %d`, f.Vertical, f.Country, f.Brand, f.Network, f.SubNetwork, f.NetworkType, f.Gender, f.DeviceType)
	return fmt.Sprintf(`folder %d (child %d, cpc %d, #cr %d, dims %s)`, f.ID, len(f.Children), f.CPC, len(f.Creative), dims)
}

type Folders []*Folder

func (f *Folders) ByID(id int) *Folder {
	for _, u := range *f {
		if u.ID == id {
			return u
		}
	}
	return nil
}

func (c *Folders) Add(ch *Folder) int {
	m := 1
	for _, och := range *c {
		if och.ID >= m {
			m = och.ID + 1
		}
	}
	ch.ID = m
	*c = append(*c, ch)
	return ch.ID
}

func (f *Folders) Unmarshal(depth int, env *Production) error {
	if ids, err := AllIDs("folders", env); err != nil {
		return err
	} else {
		*f = (*f)[:0]
		for _, id := range ids {
			ch := &Folder{ID: id}
			if err := ch.Unmarshal(depth+1, env); err != nil {
				env.Debug.Println("err", err)
				return err
			}
			*f = append(*f, ch)
		}
	}

	env.Debug.Printf("LOADED %s %T %s", wide(depth), f, tojson(f))
	return nil
}

func (f *Folders) String() string {
	if f == nil {
		return "x0[]"
	}
	str := []string{}
	for _, fo := range *f {
		str = append(str, fo.String())
	}
	return fmt.Sprintf("x%d[%s]", len(*f), strings.Join(str, `,`))
}

type Creatives []*Creative

func (f *Creatives) ByID(id int) *Creative {
	for _, u := range *f {
		if u.ID == id {
			return u
		}
	}
	return nil
}

func (c *Creatives) Add(ch *Creative) int {
	m := 1
	for _, och := range *c {
		if och.ID >= m {
			m = och.ID + 1
		}
	}
	ch.ID = m
	*c = append(*c, ch)
	return ch.ID
}

func (f *Creatives) Unmarshal(depth int, env *Production) error {
	if ids, err := AllIDs("creatives", env); err != nil {
		return err
	} else {
		*f = (*f)[:0]
		for _, id := range ids {
			ch := &Creative{ID: id}
			if err := ch.Unmarshal(depth+1, env); err != nil {
				env.Debug.Println("err", err)
				return err
			}
			*f = append(*f, ch)
		}
	}

	env.Debug.Printf("LOADED %s %T %s", wide(depth), f, tojson(f))
	return nil
}

type Creative struct {
	ID          int
	RedirectUrl string
}

func (c *Creative) Unmarshal(depth int, env *Production) error {
	row := env.ConfigDB.QueryRow(sqlCreative, c.ID)
	if err := row.Scan(&c.RedirectUrl); err != nil {
		env.Debug.Println("err", err)
		return err
	}
	return nil
}

func (c *Creative) String() string {
	return fmt.Sprintf(`creative %d (%s)`, c.ID, c.RedirectUrl)
}

type StatsDB struct{}

func (StatsDB) allowFailure(sql string, db *sql.DB) {
	createRes, err := db.Exec(sql)
	if err != nil {
		log.Println("expected an err, recieved:", err, ", assuming query has run already")
	} else {
		log.Println("no error returned, must mean this step had effect")
		log.Println(createRes.LastInsertId())
		log.Println(createRes.RowsAffected())
	}
}

func (s StatsDB) Marshal(db *sql.DB) error {
	log.Println("creating purchases table")
	s.allowFailure(sqlCreatePurchases, db)
	return nil
}

type Recalls struct {
	Env    *Production
	DoWork bool
}

func (s Recalls) Save(f *DemandFlight) {
	js, _ := json.Marshal(f)
	if !s.DoWork {
		return
	}

	for attempt := 0; attempt < 5; attempt += 1 {
		f.RecallID = int(rand.Int63())
		res := s.Env.Redis.SetNX(strconv.Itoa(f.RecallID), js, 10*time.Minute)
		if err := res.Err(); err != nil {
			f.Error = err
			f.Runtime.Logger.Println(`err saving recall`, err.Error())
			return
		}
		if res.Val() {
			f.Runtime.Logger.Println(`saved with id`, f.RecallID)
			return
		}
	}

	f.Error = fmt.Errorf(`couldnt find available id for recall`)
	f.Runtime.Logger.Println(f.Error.Error())
}

func (s Recalls) Fetch(f *WinFlight) {
	if !s.DoWork {
		return
	}

	cmd := s.Env.Redis.Get(f.RecallID)
	if err := cmd.Err(); err != nil {
		f.Error = err
		f.Runtime.Logger.Println(`err fetching recall`, err.Error())
		return
	}

	var target string
	if raw, err := cmd.Result(); err == nil {
		target = raw
	} else {
		f.Runtime.Logger.Println(`couldn't find in redis, looking in postgres`, err.Error())
		f.Error = err
		return
	}

	if e := json.Unmarshal([]byte(target), f); e != nil {
		f.Error = e
		f.Runtime.Logger.Println(`err saving recall`, e.Error())
		return
	}
}

type Purchases struct {
	Env    *Production
	DoWork bool
}

func (s Purchases) Save(f *WinFlight) {
	args := []interface{}{f.SaleID, !f.Request.Test, f.RevTXHome, f.RevTXHome, f.PaidPrice, f.PaidPrice, 0, f.FolderID, f.CreativeID, f.Request.Device.Geo.CountryID, f.Request.Site.VerticalID, f.Request.Site.BrandID, f.Request.Site.NetworkID, f.Request.Site.SubNetworkID, f.Request.Site.NetworkTypeID, f.Request.User.GenderID, f.Request.Device.DeviceTypeID}
	f.Runtime.Debug.Printf(`would query %s with..`, sqlInsertPurchases)
	f.Runtime.Logger.Println("saving purchases", args)
	if !s.DoWork {
		return
	}

	if _, e := s.Env.StatsDB.Exec(sqlInsertPurchases, args...); e != nil {
		f.Error = e
		f.Runtime.Logger.Println(`err saving purchases`, e.Error())
		return
	}
}

const sqlInsertPurchases = `INSERT INTO purchases (sale_id, billable, rev_tx, rev_tx_home, rev_ssp, rev_ssp_home, ssp_id, folder_id, creative_id, country_id, vertical_id, brand_id, network_id, subnetwork_id, networktype_id, gender_id, devicetype_id)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
`

const sqlCreatePurchases = `CREATE TABLE purchases (
	created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	sale_id int NOT NULL,
	billable bool NOT NULL,
	rev_tx_home int NOT NULL,
	rev_tx int NOT NULL,
	rev_ssp int NOT NULL,
	rev_ssp_home int NOT NULL,
	ssp_id int NOT NULL,

  	folder_id int NOT NULL,
  	creative_id int NOT NULL,

	country_id int NOT NULL,
	vertical_id int NOT NULL,
	brand_id int NOT NULL,
	network_id int NOT NULL,
	subnetwork_id int NOT NULL,
	networktype_id int NOT NULL,
	gender_id int NOT NULL,
	devicetype_id int NOT NULL
);`
