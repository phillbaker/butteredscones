package lumberjack

import (
	"net"
	"testing"
	"time"
)

func makeTestServer() net.Listener {
	server, err := net.Listen("tcp", "127.0.0.1:0") // :0 is random port
	if err != nil {
		panic(err)
	}

	return server
}

func TestClientSmokeTest(t *testing.T) {
	serverOptions := &ServerOptions{
		Network: "tcp",
		Address: "127.0.0.1:0", // random port

		WriteTimeout: 2 * time.Second,
		ReadTimeout:  2 * time.Second,
	}
	server, err := Listen(serverOptions)
	if err != nil {
		t.Error(err)
	}

	payloads := make(chan Payload, 1)
	go server.ServeInto(payloads)
	defer server.Close()

	options := &ClientOptions{
		Network:           "tcp",
		Address:           server.Addr().String(),
		ConnectionTimeout: 2 * time.Second,
		WriteTimeout:      2 * time.Second,
		ReadTimeout:       2 * time.Second,
	}

	client, err := Dial(options)
	if err != nil {
		t.Error(err)
	}

	payload := Payload{
		Data{"line": "foo bar baz", "offset": "25"},
	}
	_, err = client.Send(payload)
	if err != nil {
		t.Error(err)
	}

	select {
	case receivedPayload := <-payloads:
		if len(receivedPayload) != len(payload) {
			t.Fatalf("Got payload of size %d, expected %d", len(receivedPayload), len(payload))
		}
		if receivedPayload[0]["line"] != payload[0]["line"] {
			t.Fatalf("Got line of %s, expected %s", receivedPayload[0]["line"], payload[0]["line"])
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Timeout waiting for payload to arrive")
	}
}
