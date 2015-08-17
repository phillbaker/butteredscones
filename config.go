package butteredscones

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"os"
)

type Configuration struct {
	State      string                  `json:"state"`
	Network    NetworkConfiguration    `json:"network"`
	Statistics StatisticsConfiguration `json:"statistics"`
	Inputs     []InputConfiguration    `json:"inputs"`
	MaxLength  int                     `json:"max_length"`
}

type NetworkConfiguration struct {
	Servers     []ServerConfiguration `json:"servers"`
	Certificate string                `json:"certificate"`
	Key         string                `json:"key"`
	CA          string                `json:"ca"`
	Timeout     int                   `json:"timeout"`
	SpoolSize   int                   `json:"spool_size"`
}

type ServerConfiguration struct {
	Addr string `json:"addr"`
	Name string `json:"name"`
}

type StatisticsConfiguration struct {
	Addr string `json:"addr"`
}

type InputConfiguration struct {
	Type   string             `json:"type"`
	Fields map[string]string `json:"fields"`

	// Fields for File inputs
	Paths  []string          `json:"paths"`
}

func LoadConfiguration(configFile string) (*Configuration, error) {
	file, err := os.Open(configFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	configuration := new(Configuration)

	err = decoder.Decode(configuration)
	if err != nil {
		return nil, err
	}
	return configuration, nil
}

func (c *Configuration) BuildTLSConfig() (*tls.Config, error) {
	if c.Network.Certificate == "" || c.Network.Key == "" {
		return nil, fmt.Errorf("certificate and key not specified")
	}

	cert, err := tls.LoadX509KeyPair(c.Network.Certificate, c.Network.Key)
	if err != nil {
		return nil, err
	}

	tlsConfig := new(tls.Config)
	tlsConfig.Certificates = []tls.Certificate{cert}

	if c.Network.CA != "" {
		tlsConfig.RootCAs = x509.NewCertPool()

		data, err := ioutil.ReadFile(c.Network.CA)
		if err != nil {
			return nil, err
		}

		block, _ := pem.Decode(data)
		if block == nil {
			return nil, fmt.Errorf("CA file %q did not contain PEM encoded data", c.Network.CA)
		}
		if block.Type != "CERTIFICATE" {
			return nil, fmt.Errorf("CA file %q did not contain certificate data", c.Network.CA)
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, err
		}

		tlsConfig.RootCAs.AddCert(cert)
	}

	return tlsConfig, nil
}
