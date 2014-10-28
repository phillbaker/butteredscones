package main

import (
	"fmt"
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
	defer tmpFile.Close()
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
		GlobRefresh:  20 * time.Second,
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

	// Check that file was snapshotted
	highWaterMark, err := snapshotter.HighWaterMark(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	if highWaterMark.Position != 6 {
		t.Fatalf("Expected highWaterMark.Position = %d, but got %d", 6, highWaterMark.Position)
	}
}

// Supervisor should continually reopen files after hitting EOF to check for
// more data
func TestSupervisorReopensAfterEOF(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "buttered-scones")
	if err != nil {
		t.Fatal(err)
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

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
		GlobRefresh:  50 * time.Millisecond,
	}

	done := make(chan interface{})
	go supervisor.Serve(done)

	// Spool timeout, plus some buffer
	<-time.After(75 * time.Millisecond)

	// Now, after the file has been closed because it hit EOF, write some data
	// to it
	_, err = tmpFile.Write([]byte("line1\n"))
	if err != nil {
		t.Fatal(err)
	}

	// EOF timeout, plus some buffer
	<-time.After(supervisorEOFRetryMinimum * 5)

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

func TestSupervisorRetryServerFailure(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "buttered-scones")
	if err != nil {
		t.Fatal(err)
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write([]byte("line1\n"))
	if err != nil {
		t.Fatal(err)
	}

	// Initially, simulate a client error
	client := &TestClient{}
	client.Error = fmt.Errorf("something went wrong!")

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
		GlobRefresh:  20 * time.Second,
	}

	done := make(chan interface{})
	go supervisor.Serve(done)

	<-time.After(supervisorClientRetryMinimum)

	// OK, things magically resolved!
	client.Error = nil
	<-time.After(supervisorClientRetryMinimum * 3)

	// Make sure the message was retried
	if len(client.DataSent) != 1 {
		t.Fatalf("Expected %d message, but got %d", 1, len(client.DataSent))
	}
	if client.DataSent[0]["line"] != "line1" {
		t.Fatalf("Expected line = %q, but got %q", "line1", client.DataSent[0]["line"])
	}
	if client.DataSent[0]["field1"] != "value1" {
		t.Fatalf("Expected field1 = %q, but got %q", "value1", client.DataSent[0]["field1"])
	}

	// Check that file was snapshotted
	highWaterMark, err := snapshotter.HighWaterMark(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	if highWaterMark.Position != 6 {
		t.Fatalf("Expected highWaterMark.Position = %d, but got %d", 6, highWaterMark.Position)
	}
}
