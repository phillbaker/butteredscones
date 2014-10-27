package main

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestSupervisorSmokeTest(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "buttered-scones")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write([]byte("line1\n"))
	if err != nil {
		t.Fatal(err)
	}

	client := &TestClient{}
	snapshotter := &MemorySnapshotter{}
	supervisor := &Supervisor{
		Files: []FileConfiguration{
			FileConfiguration{
				Paths:  []string{tmpFile.Name()},
				Fields: map[string]string{"field1": "value1"},
			},
		},
		Client:       client,
		Snapshotter:  snapshotter,
		SpoolSize:    1024,
		SpoolTimeout: 50 * time.Millisecond,
	}

	done := make(chan interface{})
	go supervisor.Serve(done)

	// Spool timeout, plus some buffer
	<-time.After(75 * time.Millisecond)

	if len(client.DataSent) != 1 {
		t.Fatalf("Expected %d message, but got %d", 1, len(client.DataSent))
	}
	if client.DataSent[0]["line"] != "line1" {
		t.Fatalf("Expected line = %q, but got %q", "line1", client.DataSent[0]["line"])
	}
	if client.DataSent[0]["field1"] != "value1" {
		t.Fatalf("Expected field1 = %q, but got %q", "value1", client.DataSent[0]["field1"])
	}
}

func TestSupervisorReopensAfterEOF(t *testing.T) {
}
