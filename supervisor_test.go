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
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write([]byte("line1\n"))
	if err != nil {
		t.Fatal(err)
	}

	files := []FileConfiguration{
		FileConfiguration{Paths: []string{tmpFile.Name()}, Fields: map[string]string{"field1": "value1"}},
	}
	testClient := &TestClient{}
	snapshotter := &MemorySnapshotter{}

	supervisor := NewSupervisor(files, []Client{testClient}, snapshotter)
	supervisor.Start()
	defer supervisor.Stop()

	<-time.After(250 * time.Millisecond)
	if testClient.DataSent == nil {
		t.Fatalf("no data sent on test client before timeout")
	}

	data := testClient.DataSent[0]
	if data["line"] != "line1" {
		t.Fatalf("expected [\"line\"] to be %q, but got %q", "line1", data["line"])
	}

	hwm, err := snapshotter.HighWaterMark(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	if hwm.Position != 6 {
		t.Fatalf("expected high water mark position to be %d, but got %d", 6, hwm.Position)
	}
}
