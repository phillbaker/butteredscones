package client

import (
	"fmt"
)

type Data map[string]string

type Client interface {
	// A human-readable unique name for the client, for use in statistics. A
	// reasonable name for a remote client would be the hostname:port, for
	// instance.
	Name() string

	// Send forwards a payload of `Data` instances to a remote system
	Send(lines []Data) error
}

// TestClient is an in-memory client that allows inspecting the data that was
// 'sent' thorugh it. It is useful in test cases.
type TestClient struct {
	DataSent []Data

	// Set Error to return an error to clients when they call Send. It is useful
	// for testing how they react to errors.
	Error error
}

func (c *TestClient) Name() string {
	return fmt.Sprintf("TestClient[%p]", c)
}

func (c *TestClient) Send(lines []Data) error {
	if c.DataSent == nil {
		c.DataSent = make([]Data, 0)
	}

	if c.Error != nil {
		return c.Error
	} else {
		c.DataSent = append(c.DataSent, lines...)
		return nil
	}
}

// StdoutClient writes messages to stardard out. It was useful for development.
type StdoutClient struct {
}

func (c *StdoutClient) Name() string {
	return fmt.Sprintf("StdoutClient[%p]", c)
}

func (c *StdoutClient) Send(lines []Data) error {
	for _, data := range lines {
		fmt.Printf("%#v\n", data)
	}

	return nil
}
