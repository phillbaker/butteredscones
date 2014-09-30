package lumberjack

import (
	"bytes"
	"compress/zlib"
	"crypto/tls"
	"encoding/binary"
	"log"
	"net"
	"time"
)

type Client struct {
	options *ClientOptions

	conn     net.Conn
	sequence uint32
}

type ClientOptions struct {
	Network           string
	Address           string
	ConnectionTimeout time.Duration

	TLSConfig *tls.Config

	WriteTimeout time.Duration
	ReadTimeout  time.Duration
}

type Data map[string]string
type Payload []Data

func Dial(options *ClientOptions) (*Client, error) {
	var conn net.Conn

	conn, err := net.DialTimeout(options.Network, options.Address, options.ConnectionTimeout)
	if err != nil {
		return nil, err
	}

	if options.TLSConfig != nil {
		conn = tls.Client(conn, options.TLSConfig)
	}

	return &Client{
		options: options,
		conn:    conn,
	}, nil
}

func (c *Client) Send(payload Payload) (n int, err error) {
	// Serialize (w/ compression)
	payloadBuf := c.serialize(payload)
	payloadBytes := payloadBuf.Bytes()

	buf := new(bytes.Buffer)

	// Window size
	buf.WriteString("1W")
	binary.Write(buf, binary.BigEndian, uint32(len(payload)))

	// Compressed size
	buf.WriteString("1C")
	log.Printf("payloadBytes: %d\n", len(payloadBytes))
	binary.Write(buf, binary.BigEndian, uint32(len(payloadBytes)))

	// Actual payload
	buf.Write(payloadBytes)

	c.conn.SetWriteDeadline(time.Now().Add(c.options.WriteTimeout))
	n, err = c.conn.Write(buf.Bytes())
	if err != nil {
		//  TODO: Reconnect socket
		return n, err
	}

	// Wait for ACK (6 bytes)
	// This is pretty weird, but is mirroring what logstash-forwarder does
	c.conn.SetReadDeadline(time.Now().Add(c.options.ReadTimeout))

	ack := make([]byte, 6)
	ackBytes := 0
	for ackBytes < 6 {
		n, err := c.conn.Read(ack[ackBytes:len(ack)])
		if n > 0 {
			ackBytes += n
		} else if err != nil {
			// TODO: Reconnect socket
			return n, err
		}
	}

	return n, err
}

func (c *Client) serialize(payload Payload) *bytes.Buffer {
	buf := new(bytes.Buffer)
	compressor := zlib.NewWriter(buf)

	for _, data := range payload {
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
