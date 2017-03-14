package services

import (
	"database/sql"
	"github.com/clixxa/dsp/bindings"
	"gopkg.in/redis.v5"
	"log"
	"os"
	"strings"
	"time"
)

type ProductionDepsService struct {
	BindingDeps bindings.BindingDeps
	RedisStr    string
	Consul      *ConsulConfigs
	Messages    chan string
}

func (p *ProductionDepsService) ConfigDSN() *bindings.DSN {
	return &bindings.DSN{
		"mysql",
		os.Getenv("TCONFIGDBHOST"),
		os.Getenv("TCONFIGDBPORT"),
		os.Getenv("TCONFIGDB"),
		os.Getenv("TCONFIGDBUSERNAME"),
		os.Getenv("TCONFIGDBPASSWORD"),
	}
}

func (p *ProductionDepsService) StatsDSN() *bindings.DSN {
	return &bindings.DSN{
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
		go func(oldredis *bindings.RandomCache) {
			time.Sleep(4 * time.Second)
			s := p.BindingDeps.Redis.String()
			if s != "" {
				p.Messages <- s
			}
		}(p.BindingDeps.Redis)
	}

	if str := p.RedisDSN(); str != p.RedisStr {
		p.RedisStr = str
		sh := &bindings.ShardSystem{Fallback: p.BindingDeps.Redis}
		rc2 := &bindings.RandomCache{sh}
		for _, url := range strings.Split(str, ",") {
			red := &redis.Options{Addr: url}
			r := &bindings.RecallRedis{Client: redis.NewClient(red)}
			sh.Children = append(sh.Children, r)
			if err := r.Ping().Err(); err != nil {
				if quit(ErrDatabaseMissing{"redis " + url, err}) {
					return
				} else {
					rc2 = nil
				}
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
