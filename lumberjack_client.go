package main

import (
	"bytes"
	"compress/zlib"
	"crypto/tls"
	"encoding/binary"
	"net"
	"strings"
	"time"
)

type LumberjackClient struct {
	options *LumberjackClientOptions

	conn     net.Conn
	sequence uint32
}

type LumberjackClientOptions struct {
	Network           string
	Address           string
	ConnectionTimeout time.Duration
	SendTimeout       time.Duration
	TLSConfig         *tls.Config
}

func NewLumberjackClient(options *LumberjackClientOptions) *LumberjackClient {
	return &LumberjackClient{
		options: options,
	}
}

func (c *LumberjackClient) ensureConnected() error {
	if c.conn == nil {
		var conn net.Conn

		conn, err := net.DialTimeout(c.options.Network, c.options.Address, c.options.ConnectionTimeout)
		if err != nil {
			return err
		}

		if c.options.TLSConfig != nil {
			parts := strings.Split(c.options.Address, ":")
			c.options.TLSConfig.ServerName = parts[0]

			tlsConn := tls.Client(conn, c.options.TLSConfig)
			tlsConn.SetDeadline(time.Now().Add(c.options.SendTimeout))
			if err := tlsConn.Handshake(); err != nil {
				conn.Close()
				return err
			}
			conn = tlsConn
		}

		c.conn = conn
	}

	return nil
}

func (c *LumberjackClient) Disconnect() error {
	var err error
	if c.conn != nil {
		err = c.conn.Close()
		c.conn = nil
	}

	c.sequence = 0
	return err
}

func (c *LumberjackClient) Name() string {
	return c.options.Address
}

func (c *LumberjackClient) Send(lines []Data) error {
	err := c.ensureConnected()
	if err != nil {
		return err
	}

	// Serialize (w/ compression)
	linesBuf := c.serialize(lines)
	linesBytes := linesBuf.Bytes()

	headerBuf := new(bytes.Buffer)

	// Window size
	headerBuf.WriteString("1W")
	binary.Write(headerBuf, binary.BigEndian, uint32(len(lines)))

	// Compressed size
	headerBuf.WriteString("1C")
	binary.Write(headerBuf, binary.BigEndian, uint32(len(linesBytes)))

	// Write header to socket
	c.conn.SetDeadline(time.Now().Add(c.options.SendTimeout))
	_, err = c.conn.Write(headerBuf.Bytes())
	if err != nil {
		c.Disconnect()
		return err
	}

	// Write compressed lines to socket
	_, err = c.conn.Write(linesBytes)
	if err != nil {
		c.Disconnect()
		return err
	}

	// Wait for ACK (6 bytes)
	// This is pretty weird, but is mirroring what logstash-forwarder does
	ack := make([]byte, 6)
	ackBytes := 0
	for ackBytes < 6 {
		n, err := c.conn.Read(ack[ackBytes:len(ack)])
		if n > 0 {
			ackBytes += n
		} else if err != nil {
			c.Disconnect()
			return err
		}
	}

	return nil
}

func (c *LumberjackClient) serialize(lines []Data) *bytes.Buffer {
	buf := new(bytes.Buffer)
	compressor := zlib.NewWriter(buf)

	for _, data := range lines {
		c.sequence += 1

		compressor.Write([]byte("1D"))
		binary.Write(compressor, binary.BigEndian, uint32(c.sequence))
		binary.Write(compressor, binary.BigEndian, uint32(len(data)))
		for k, v := range data {
			binary.Write(compressor, binary.BigEndian, uint32(len(k)))
			compressor.Write([]byte(k))
			binary.Write(compressor, binary.BigEndian, uint32(len(v)))
			compressor.Write([]byte(v))
		}
	}

	compressor.Close()
	return buf
}
