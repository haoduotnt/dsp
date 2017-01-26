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
	sprod := &dsp_flights.Production{AllTest: m.TestOnly, Logic: dsp_flights.SimpleLogic{}}

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
