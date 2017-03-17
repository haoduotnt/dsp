package services

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

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

type BindingDeps struct {
	StatsDB    *sql.DB
	ConfigDB   *sql.DB
	Debug      *log.Logger
	Logger     *log.Logger
	DefaultKey string
	Redis      *RandomCache
}

type ProductionDepsService struct {
	BindingDeps  BindingDeps
	RedisStr     string
	Consul       *ConsulConfigs
	Messages     chan string
	RedisFactory func(string) (CacheSystem, error)
}

func (p *ProductionDepsService) ConfigDSN() *DSN {
	return &DSN{
		"mysql",
		os.Getenv("TCONFIGDBHOST"),
		os.Getenv("TCONFIGDBPORT"),
		os.Getenv("TCONFIGDB"),
		os.Getenv("TCONFIGDBUSERNAME"),
		os.Getenv("TCONFIGDBPASSWORD"),
	}
}

func (p *ProductionDepsService) StatsDSN() *DSN {
	return &DSN{
		"postgres",
		os.Getenv("TSTATSDBHOST"),
		os.Getenv("TSTATSDBPORT"),
		os.Getenv("TSTATSDB"),
		os.Getenv("TSTATSDBUSERNAME"),
		os.Getenv("TSTATSDBPASSWORD"),
	}
}

func (p *ProductionDepsService) RedisDSN() string {
	if p.Consul.RedisUrls != "" {
		return p.Consul.RedisUrls
	}
	return os.Getenv("TRECALLURL")
}

func (p *ProductionDepsService) Cycle(quit func(error) bool) {
	if p.BindingDeps.Debug == nil {
		p.BindingDeps.Debug = log.New(os.Stderr, "", log.Lshortfile|log.Ltime)
	}

	if p.BindingDeps.Logger == nil {
		p.BindingDeps.Logger = log.New(os.Stdout, "", 0)
		p.BindingDeps.Debug.Println("created new Logger to stdout")
	}

	if p.BindingDeps.DefaultKey == "" {
		p.BindingDeps.DefaultKey = os.Getenv("TDEFAULTKEY")
	}

	if p.BindingDeps.Redis != nil {
		go func(oldredis *RandomCache) {
			time.Sleep(4 * time.Second)
			s := p.BindingDeps.Redis.String()
			if s != "" {
				p.Messages <- s
			}
		}(p.BindingDeps.Redis)
	}

	if str := p.RedisDSN(); str != p.RedisStr {
		p.RedisStr = str
		sh := &ShardSystem{Fallback: p.BindingDeps.Redis}
		rc2 := &RandomCache{sh}
		for _, url := range strings.Split(str, ",") {
			if r, err := p.RedisFactory(url); quit(ErrDatabaseMissing{"redis", err}) {
				return
			} else {
				sh.Children = append(sh.Children, r)
			}
		}
		p.BindingDeps.Redis = rc2
	}

	if p.BindingDeps.ConfigDB == nil {
		dsn := p.ConfigDSN()
		db, err := sql.Open(dsn.Driver, dsn.Dump())
		if err != nil {
			if quit(ErrDatabaseMissing{"config db", err}) {
				return
			}
			db = nil
		} else {
			if err := db.Ping(); err != nil {
				if quit(ErrDatabaseMissing{"config db ping", err}) {
					return
				}
				db = nil
			}
		}
		p.BindingDeps.ConfigDB = db
	}

	if p.BindingDeps.StatsDB == nil {
		dsn := p.StatsDSN()
		db, err := sql.Open(dsn.Driver, dsn.Dump())
		if err != nil {
			if quit(ErrDatabaseMissing{"stats db", err}) {
				return
			}
			db = nil
		} else {
			if err := db.Ping(); err != nil {
				if quit(ErrDatabaseMissing{"stats db ping", err}) {
					return
				}
				db = nil
			}
		}
		p.BindingDeps.StatsDB = db
	}
}
