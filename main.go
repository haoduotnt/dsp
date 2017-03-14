package main

import (
	"fmt"
	"github.com/clixxa/dsp/dsp_flights"
	"github.com/clixxa/dsp/services"
	"github.com/clixxa/dsp/wish_flights"
	"net/http"
	"os"
)

type Main struct {
	TestOnly bool
}

func (m *Main) Launch() {
	consul := &services.ConsulConfigs{}

	messages := make(chan string, 100)
	printer := &services.Printer{Messages: messages}

	deps := &services.ProductionDepsService{Messages: messages, Consul: consul}

	dspRuntime := &dsp_flights.BidEntrypoint{AllTest: m.TestOnly, Logic: dsp_flights.SimpleLogic{}}
	winRuntime := &wish_flights.WishEntrypoint{Messages: messages}

	router := &services.RouterService{Messages: messages}
	router.Mux = http.NewServeMux()

	ef := &services.ErrorFilter{Tolerances: services.ConnectionErrors & services.ParsingErrors, Messages: messages}
	router.Mux.Handle("/", dspRuntime)

	winChan := &services.HttpToChan{Messages: messages, ObjectFactory: winRuntime.NewFlight}
	router.Mux.Handle("/win", winChan)

	launch := &services.LaunchService{Messages: messages}

	wireUp := &services.CycleService{Proxy: func(func(error) bool) {
		dspRuntime.BindingDeps = deps.BindingDeps
		winRuntime.BindingDeps = deps.BindingDeps
		printer.PrintTo = deps.BindingDeps.Logger
	}}

	cycler := &services.CycleService{ErrorFilter: ef.Quit, Messages: messages}
	cycler.Children = append(cycler.Children, consul, deps, wireUp, dspRuntime, winRuntime)
	launch.Children = append(launch.Children, cycler, printer, router, winRuntime)

	fmt.Println("starting launcher")
	fmt.Println("launch returned", launch.Launch())
	printer.Flush()
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
