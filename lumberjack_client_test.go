package main

import (
	"testing"
	"time"
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

	dataCh := make(chan Data, 1)
	go server.ServeInto(dataCh)

	client := NewLumberjackClient(&LumberjackClientOptions{
		Network:           "tcp",
		Address:           server.Addr().String(),
		ConnectionTimeout: 2 * time.Second,
		WriteTimeout:      2 * time.Second,
		ReadTimeout:       2 * time.Second,
	})

	lines := []Data{
		Data{"line": "foo bar baz", "offset": "25"},
	}
	err = client.Send(lines)
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
