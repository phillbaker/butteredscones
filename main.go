package main

import (
	"flag"
	"fmt"
	"github.com/boltdb/bolt"
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

	// tlsConfig, err := config.TLSConfig()
	// if err != nil {
	// 	fmt.Printf("%s\n", err.Error())
	// 	os.Exit(1)
	// }

	// client := NewLumberjackClient(&LumberjackClientOptions{
	// 	Network:           "tcp",
	// 	Address:           config.Network.Server,
	// 	TLSConfig:         tlsConfig,
	// 	ConnectionTimeout: time.Duration(config.Network.Timeout) * time.Second,
	// 	WriteTimeout:      time.Duration(config.Network.Timeout) * time.Second,
	// 	ReadTimeout:       time.Duration(config.Network.Timeout) * time.Second,
	// })

	db, err := bolt.Open(config.State, 0600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		fmt.Printf("error opening state database: %s\n", err.Error())
		os.Exit(1)
	}
	snapshotter := &BoltSnapshotter{DB: db}

	client := &StdoutClient{}
	spooler := &Spooler{Size: 1024, Timeout: 1 * time.Second}

	supervisor := &Supervisor{
		Files:       config.Files,
		Client:      client,
		Snapshotter: snapshotter,
		Spooler:     spooler,
	}
	supervisor.Serve()
}
