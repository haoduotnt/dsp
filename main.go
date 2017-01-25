package main

import (
	"fmt"
	"github.com/clixxa/dsp/dsp_flights"
	"os"
	"sync"
)

type Main struct {
	TestOnly bool
	WG       sync.WaitGroup
}

func (m *Main) Launch() {
	clickid := func(*dsp_flights.DemandFlight) string { return "" }
	revshare := func(*dsp_flights.DemandFlight) float64 { return 98.0 }
	sprod := &dsp_flights.Production{AllTest: m.TestOnly, RevshareFunc: revshare, ClickIDFunc: clickid}

	fmt.Println("running dsp_flights")
	m.WG.Add(1)
	go func() {
		sprod.Block()
		m.WG.Done()
	}()

	m.WG.Wait()
}

func NewMain() *Main {
	m := &Main{}
	for _, flag := range os.Args[1:] {
		fmt.Printf(`arg %s`, flag)
		switch flag {
		case "test":
			m.TestOnly = true
		}
	}
	return m
}

func main() {
	NewMain().Launch()
}
