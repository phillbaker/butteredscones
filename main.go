package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	var configFile string
	flag.StringVar(&configFile, "config", "", "configuration file path")
	flag.Parse()

	if configFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	config, err := LoadConfiguration(configFile)
	if err != nil {
		fmt.Printf("error opening configuration file: %s\n", err.Error())
		os.Exit(1)
	}

	tlsConfig, err := config.TLSConfig()
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		os.Exit(1)
	}

	// TODO: Wrap this in our own class so we can do better error handling and
	// reconnect logic
	client := NewLumberjackClient(&LumberjackClientOptions{
		Network:           "tcp",
		Address:           config.Network.Server,
		TLSConfig:         tlsConfig,
		ConnectionTimeout: time.Duration(config.Network.Timeout) * time.Second,
		WriteTimeout:      time.Duration(config.Network.Timeout) * time.Second,
		ReadTimeout:       time.Duration(config.Network.Timeout) * time.Second,
	})

	fmt.Printf("%#v\n", client)
}
