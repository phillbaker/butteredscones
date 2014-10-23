package main

type Data map[string]string

type Client interface {
	// Send forwards a payload of `Data` instances to a remote system
	Send(lines []Data) error
}

// TestClient is an in-memory client that allows inspecting the data that was
// 'sent' thorugh it. It is useful in test cases.
type TestClient struct {
	DataSent []Data
}

func (c *TestClient) Send(lines []Data) error {
	if c.DataSent == nil {
		c.DataSent = make([]Data, 0)
	}

	c.DataSent = append(c.DataSent, lines...)
	return nil
}
