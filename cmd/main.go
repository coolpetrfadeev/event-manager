package main

import (
	"event-manager/internal/app"
	"event-manager/pkg/config"
	"github.com/labstack/gommon/log"
	"os"
)

func main() {
	cfg := &config.Configuration{}
	err := cfg.Read()
	if err != nil {
		log.Error(err)
		os.Exit(2)
	}
	app.Run(cfg)
}
