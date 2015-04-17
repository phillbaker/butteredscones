package lumberjack

import (
	"testing"
	"time"

	"github.com/digitalocean/butteredscones/client"
)

func TestClientSmokeTest(t *testing.T) {
	server, err := newLumberjackServer(&serverOptions{
		Network: "tcp",
		Address: "127.0.0.1:0", // random port

		WriteTimeout: 2 * time.Second,
		ReadTimeout:  2 * time.Second,
	})

	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	dataCh := make(chan client.Data, 1)
	go server.ServeInto(dataCh)

	c := NewClient(&ClientOptions{
		Network:           "tcp",
		Address:           server.Addr().String(),
		ConnectionTimeout: 2 * time.Second,
		SendTimeout:       2 * time.Second,
	})

	lines := []client.Data{
		client.Data{"line": "foo bar baz", "offset": "25"},
	}
	err = c.Send(lines)
	if err != nil {
		t.Error(err)
	}

	select {
	case receivedLine := <-dataCh:
		if receivedLine["line"] != lines[0]["line"] {
			t.Fatalf("Got line of %s, expected %s", receivedLine["line"], lines[0]["line"])
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Timeout waiting for lines to arrive")
	}
}

func TestClientReconnectSmokeTest(t *testing.T) {
	server, err := newLumberjackServer(&serverOptions{
		Network: "tcp",
		Address: "127.0.0.1:0", // random port

		WriteTimeout: 2 * time.Second,
		ReadTimeout:  2 * time.Second,
	})

	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	// Without the server accepting connections, we should run into a connection
	// timeout
	c := NewClient(&ClientOptions{
		Network:           "tcp",
		Address:           server.Addr().String(),
		ConnectionTimeout: 1 * time.Second,
		SendTimeout:       1 * time.Second,
	})

	lines := []client.Data{
		client.Data{"line": "foo bar baz", "offset": "25"},
	}
	err = c.Send(lines)
	if err == nil {
		t.Fatalf("Expected Send to timeout, but did not")
	}

	// Now, setup the server properly, things should go through
	dataCh := make(chan client.Data, 1)
	go server.ServeInto(dataCh)

	err = c.Send(lines)
	if err != nil {
		t.Error(err)
	}

	select {
	case <-dataCh:
		// success
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Timeout waiting for lines to arrive")
	}
}
