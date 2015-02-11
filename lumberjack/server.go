package lumberjack

import (
	"bytes"
	"compress/zlib"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/alindeman/butteredscones/client"
)

type Server struct {
	options  *serverOptions
	listener net.Listener
}

type serverOptions struct {
	Network string
	Address string

	TLSConfig *tls.Config

	WriteTimeout time.Duration
	ReadTimeout  time.Duration
}

func newLumberjackServer(options *serverOptions) (*Server, error) {
	var listener net.Listener

	listener, err := net.Listen(options.Network, options.Address)
	if err != nil {
		return nil, err
	}

	return &Server{
		options:  options,
		listener: listener,
	}, nil
}

func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

func (s *Server) ServeInto(dataCh chan<- client.Data) error {
	for {
		var client net.Conn

		client, err := s.listener.Accept()
		if err != nil {
			return err
		}

		if s.options.TLSConfig != nil {
			client = tls.Server(client, s.options.TLSConfig)
		}

		go func() {
			err := s.serveClient(client, dataCh)
			if err != nil {
				// TODO: grohl logging
				log.Print(err)
			}
		}()
	}
}

func (s *Server) serveClient(conn net.Conn, dataCh chan<- client.Data) error {
	defer conn.Close()
	controlBuf := make([]byte, 8) // up to 8 bytes (uint32 size) for storing control bytes

	conn.SetReadDeadline(time.Now().Add(s.options.ReadTimeout))

	// Window size
	var windowSize uint32
	if _, err := conn.Read(controlBuf[0:2]); err != nil {
		return err
	}
	if bytes.Compare(controlBuf[0:2], []byte("1W")) != 0 {
		return fmt.Errorf("Expected 1W, got %v", controlBuf[0:2])
	}
	if err := binary.Read(conn, binary.BigEndian, &windowSize); err != nil {
		return err
	}

	// Compressed size
	var compressedSize uint32
	if _, err := conn.Read(controlBuf[0:2]); err != nil {
		return err
	}
	if bytes.Compare(controlBuf[0:2], []byte("1C")) != 0 {
		return fmt.Errorf("Expected 1C, got %v", controlBuf[0:2])
	}
	if err := binary.Read(conn, binary.BigEndian, &compressedSize); err != nil {
		return err
	}

	// Compressed payload
	// TODO: It is possible to rework this without allocating a huge buffer upfront
	compressedBuf := make([]byte, int(compressedSize))
	if _, err := conn.Read(compressedBuf); err != nil {
		return err
	}
	uncompressor, err := zlib.NewReader(bytes.NewBuffer(compressedBuf))
	if err != nil {
		return err
	}
	defer uncompressor.Close()

	lines := make([]client.Data, 0, int(windowSize))
	for i := 0; i < int(windowSize); i++ {
		if _, err := uncompressor.Read(controlBuf[0:2]); err != nil {
			return err
		}
		if bytes.Compare(controlBuf[0:2], []byte("1D")) != 0 {
			return fmt.Errorf("Expected 1D, got %v", controlBuf[0:2])
		}

		// Sequence
		var sequence uint32
		if err := binary.Read(uncompressor, binary.BigEndian, &sequence); err != nil {
			return err
		}

		// Payload key length
		var dataLength uint32
		if err = binary.Read(uncompressor, binary.BigEndian, &dataLength); err != nil {
			return err
		}

		data := make(client.Data, int(dataLength))
		for j := 0; j < int(dataLength); j++ {
			var length uint32

			if err = binary.Read(uncompressor, binary.BigEndian, &length); err != nil {
				return err
			}
			k := make([]byte, int(length))
			if _, err = uncompressor.Read(k); err != nil {
				return err
			}

			if err = binary.Read(uncompressor, binary.BigEndian, &length); err != nil {
				return err
			}
			v := make([]byte, int(length))
			if _, err = uncompressor.Read(v); err != nil {
				return err
			}

			data[string(k)] = string(v)
		}

		lines = append(lines, data)
	}

	conn.SetWriteDeadline(time.Now().Add(s.options.WriteTimeout))
	conn.Write([]byte("ackack")) // TODO: What exactly is ack supposed to be here?

	for _, data := range lines {
		dataCh <- data
	}
	return nil
}

func (s *Server) Close() error {
	return s.listener.Close()
}
