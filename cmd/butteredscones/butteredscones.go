package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alindeman/butteredscones"
	"github.com/alindeman/butteredscones/client"
	"github.com/alindeman/butteredscones/lumberjack"
	"github.com/boltdb/bolt"
	"github.com/technoweenie/grohl"
)

func main() {
	grohl.AddContext("app", "buttered-scones")

	var configFile string
	flag.StringVar(&configFile, "config", "", "configuration file path")
	flag.Parse()

	if configFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	config, err := butteredscones.LoadConfiguration(configFile)
	if err != nil {
		fmt.Printf("error opening configuration file: %s\n", err.Error())
		os.Exit(1)
	}

	clients := make([]client.Client, 0, len(config.Network.Servers))
	for _, server := range config.Network.Servers {
		tlsConfig, err := config.BuildTLSConfig()
		if err != nil {
			fmt.Printf("%s\n", err.Error())
			os.Exit(1)
		}
		tlsConfig.ServerName = server.Name

		options := &lumberjack.ClientOptions{
			Network:           "tcp",
			Address:           server.Addr,
			TLSConfig:         tlsConfig,
			ConnectionTimeout: time.Duration(config.Network.Timeout) * time.Second,
			SendTimeout:       time.Duration(config.Network.Timeout) * time.Second,
		}
		client := lumberjack.NewClient(options)
		clients = append(clients, client)
	}

	// clients := []Client{&StdoutClient{}}

	db, err := bolt.Open(config.State, 0600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		fmt.Printf("error opening state database: %s\n", err.Error())
		os.Exit(1)
	}
	snapshotter := &butteredscones.BoltSnapshotter{DB: db}

	if config.Statistics.Addr != "" {
		stats_server := &butteredscones.StatisticsServer{
			Statistics: butteredscones.GlobalStatistics,
			Addr:       config.Statistics.Addr,
		}

		go func() {
			err := stats_server.ListenAndServe()
			grohl.Report(err, grohl.Data{"msg": "stats server failed to start"})
		}()
	}

	// Default spool size
	spoolSize := config.Network.SpoolSize
	if spoolSize == 0 {
		spoolSize = 1024
	}

	supervisor := butteredscones.NewSupervisor(config.Files, clients, snapshotter)
	supervisor.SpoolSize = spoolSize
	supervisor.GlobRefresh = 15 * time.Second

	supervisor.Start()

	signalCh := make(chan os.Signal, 1)
	go signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)

	signal := <-signalCh
	fmt.Printf("Received %s, shutting down cleanly ...\n", signal)
	supervisor.Stop()
	fmt.Printf("Done shutting down\n")
}
