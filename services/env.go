package services

import (
	"database/sql"
	"github.com/clixxa/dsp/bindings"
	"gopkg.in/redis.v5"
	"log"
	"os"
)

type ProductionDepsService struct {
	BindingDeps bindings.BindingDeps
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
	return os.Getenv("TRECALLURL")
}

func (p *ProductionDepsService) Cycle() error {
	if p.BindingDeps.Debug == nil {
		p.BindingDeps.Debug = log.New(os.Stderr, "", log.Lshortfile|log.Ltime)
	}

	if p.BindingDeps.Logger == nil {
		p.BindingDeps.Logger = log.New(os.Stdout, "", log.Lshortfile|log.Ltime)
		p.BindingDeps.Debug.Println("created new Logger to stdout")
	}

	if p.BindingDeps.DefaultKey == "" {
		p.BindingDeps.DefaultKey = os.Getenv("TDEFAULTKEY")
	}

	if p.BindingDeps.Redis == nil {
		p.BindingDeps.Redis = redis.NewClient(&redis.Options{Addr: p.RedisDSN()})
		if err := p.BindingDeps.Redis.Ping().Err(); err != nil {
			return err
		}
	}

	if p.BindingDeps.ConfigDB == nil {
		p.BindingDeps.Debug.Println("connecting to real config")
		dsn := p.ConfigDSN()
		db, err := sql.Open(dsn.Driver, dsn.Dump())
		if err != nil {
			p.BindingDeps.Debug.Println("err:", err.Error())
			return err
		}
		if err := db.Ping(); err != nil {
			p.BindingDeps.Debug.Println("err:", err.Error())
			return err
		}
		p.BindingDeps.ConfigDB = db
	}

	if p.BindingDeps.StatsDB == nil {
		p.BindingDeps.Debug.Println("connecting to real stats")
		dsn := p.StatsDSN()
		db, err := sql.Open(dsn.Driver, dsn.Dump())
		if err != nil {
			p.BindingDeps.Debug.Println("err:", err.Error())
			return err
		}
		if err := db.Ping(); err != nil {
			p.BindingDeps.Debug.Println("err:", err.Error())
			return err
		}
		p.BindingDeps.StatsDB = db
	}
	return nil
}
