package signals

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

type Client struct {
	conn *nats.Conn
}

func Connect(url string) (*Client, error) {
	nc, err := nats.Connect(url, nats.Timeout(5*time.Second), nats.Name("pulsequeue"))
	if err != nil {
		return nil, err
	}
	return &Client{conn: nc}, nil
}

func (c *Client) Close() {
	if c != nil && c.conn != nil {
		c.conn.Drain()
		c.conn.Close()
	}
}

func (c *Client) Ping(ctx context.Context) error {
	if c == nil || c.conn == nil || !c.conn.IsConnected() {
		return errors.New("nats disconnected")
	}
	done := make(chan error, 1)
	go func() {
		done <- c.conn.Flush()
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) PublishJobAvailable(queue string) error {
	return c.conn.Publish(JobAvailableSubject(queue), []byte(queue))
}

func (c *Client) SubscribeJobAvailable(queue string, handler func()) (*nats.Subscription, error) {
	return c.conn.QueueSubscribe(JobAvailableSubject(queue), "pulsequeue-workers-"+queue, func(*nats.Msg) {
		handler()
	})
}

func JobAvailableSubject(queue string) string {
	if queue == "" {
		queue = "default"
	}
	return fmt.Sprintf("pulsequeue.jobs.available.%s", queue)
}
