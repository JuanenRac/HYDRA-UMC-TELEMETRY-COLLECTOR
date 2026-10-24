// HYDRA-UMC-TELEMETRY-COLLECTOR - mqttin/client.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// A small MQTT 3.1.1 subscriber, standard library only: it connects, subscribes to some topic filters and hands every
// message it receives to a handler. It never publishes (the collector only listens), asks for QoS 1 at most, and
// reconnects with a growing pause when the connection is lost. Nothing here interprets a payload: adapter.go does.
package mqttin

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// MaxPacketBytes bounds one incoming packet: a state message is a few hundred bytes.
	MaxPacketBytes = 256 << 10
	// DefaultKeepAlive is the keep-alive interval offered to the broker.
	DefaultKeepAlive = 30 * time.Second

	packetConnect   = 1
	packetConnack   = 2
	packetPublish   = 3
	packetPuback    = 4
	packetSubscribe = 8
	packetSuback    = 9
	packetPingreq   = 12
	packetPingresp  = 13
	packetDisconect = 14
)

// Options configures a Client.
type Options struct {
	// URL is tcp://host:port or tls://host:port (mqtt:// and mqtts:// are accepted as the same).
	URL      string
	ClientID string
	Username string
	Password string
	// CAFile is a PEM file of the certificate authority that signed the broker's certificate (tls:// only; empty uses the system roots).
	CAFile string
	// Topics are the topic filters to subscribe to.
	Topics    []string
	KeepAlive time.Duration
	// MaxBackoff bounds the pause between reconnect attempts; zero means 30 seconds.
	MaxBackoff time.Duration
}

// Message is one received publication.
type Message struct {
	Topic    string
	Payload  []byte
	Retained bool
}

// Client is a reconnecting subscriber.
type Client struct {
	opts      Options
	handler   func(Message)
	connected atomic.Bool
	received  atomic.Uint64
	reconnect atomic.Uint64
	lastErr   atomic.Value // string
	mu        sync.Mutex
}

// New builds a Client; Run connects it.
func New(opts Options, handler func(Message)) (*Client, error) {
	if _, _, _, err := parseURL(opts.URL); err != nil {
		return nil, err
	}
	if len(opts.Topics) == 0 {
		return nil, errors.New("mqttin: at least one topic filter is needed")
	}
	if opts.ClientID == "" {
		opts.ClientID = fmt.Sprintf("hydra-umc-telemetry-collector-%d", os.Getpid())
	}
	if opts.KeepAlive <= 0 {
		opts.KeepAlive = DefaultKeepAlive
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = 30 * time.Second
	}
	return &Client{opts: opts, handler: handler}, nil
}

// Status is a snapshot of the counters of a Client.
type Status struct {
	Connected  bool   `json:"connected"`
	Received   uint64 `json:"received"`
	Reconnects uint64 `json:"reconnects"`
	LastError  string `json:"lastError,omitempty"`
}

// Status reports the state of the connection.
func (c *Client) Status() Status {
	last, _ := c.lastErr.Load().(string)
	return Status{Connected: c.connected.Load(), Received: c.received.Load(), Reconnects: c.reconnect.Load(), LastError: last}
}

func parseURL(raw string) (host string, useTLS bool, serverName string, err error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "", false, "", fmt.Errorf("mqttin: %q is not a broker address like tcp://127.0.0.1:1883", raw)
	}
	switch strings.ToLower(u.Scheme) {
	case "tcp", "mqtt":
	case "tls", "ssl", "mqtts":
		useTLS = true
	default:
		return "", false, "", fmt.Errorf("mqttin: unsupported scheme %q (use tcp:// or tls://)", u.Scheme)
	}
	host = u.Host
	port := u.Port()
	if port == "" {
		if useTLS {
			host = net.JoinHostPort(u.Hostname(), "8883")
		} else {
			host = net.JoinHostPort(u.Hostname(), "1883")
		}
	}
	return host, useTLS, u.Hostname(), nil
}

// Run connects and keeps the subscription alive until ctx ends.
func (c *Client) Run(ctx context.Context) {
	pause := time.Second
	for ctx.Err() == nil {
		started := time.Now()
		err := c.session(ctx)
		c.connected.Store(false)
		if err != nil && ctx.Err() == nil {
			c.lastErr.Store(err.Error())
		}
		if ctx.Err() != nil {
			return
		}
		c.reconnect.Add(1)
		// a session that lasted a while earns a short pause again
		if time.Since(started) > 2*c.opts.MaxBackoff {
			pause = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(pause):
		}
		pause *= 2
		if pause > c.opts.MaxBackoff {
			pause = c.opts.MaxBackoff
		}
	}
}

func (c *Client) dial(ctx context.Context) (net.Conn, error) {
	host, useTLS, serverName, _ := parseURL(c.opts.URL)
	d := net.Dialer{Timeout: 10 * time.Second}
	raw, err := d.DialContext(ctx, "tcp", host)
	if err != nil {
		return nil, err
	}
	if !useTLS {
		return raw, nil
	}
	cfg := &tls.Config{ServerName: serverName, MinVersion: tls.VersionTLS12}
	if c.opts.CAFile != "" {
		pem, err := os.ReadFile(c.opts.CAFile)
		if err != nil {
			_ = raw.Close()
			return nil, fmt.Errorf("mqttin: cannot read the CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			_ = raw.Close()
			return nil, errors.New("mqttin: the CA file holds no certificate")
		}
		cfg.RootCAs = pool
	}
	conn := tls.Client(raw, cfg)
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := conn.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

func (c *Client) session(ctx context.Context) error {
	conn, err := c.dial(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.SetDeadline(time.Now()) // unblocks the read
		case <-stop:
		}
	}()

	r := bufio.NewReader(conn)
	if err := c.writeLocked(conn, connectPacket(c.opts)); err != nil {
		return err
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	kind, _, body, err := readPacket(r)
	if err != nil {
		return err
	}
	if kind != packetConnack || len(body) != 2 {
		return errors.New("mqttin: the broker did not answer the connection with a CONNACK")
	}
	if body[1] != 0 {
		return fmt.Errorf("mqttin: the broker refused the connection (code %d)", body[1])
	}
	if err := c.writeLocked(conn, subscribePacket(1, c.opts.Topics)); err != nil {
		return err
	}
	c.connected.Store(true)
	c.lastErr.Store("")

	// keep-alive
	go func() {
		t := time.NewTicker(c.opts.KeepAlive / 2)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if err := c.writeLocked(conn, []byte{packetPingreq << 4, 0}); err != nil {
					_ = conn.Close()
					return
				}
			}
		}
	}()

	for {
		_ = conn.SetReadDeadline(time.Now().Add(c.opts.KeepAlive * 3 / 2))
		kind, flags, body, err := readPacket(r)
		if err != nil {
			return err
		}
		switch kind {
		case packetSuback:
			for _, code := range body[min(2, len(body)):] {
				if code == 0x80 {
					return errors.New("mqttin: the broker refused a subscription (is the topic allowed for this user?)")
				}
			}
		case packetPublish:
			msg, id, qos, err := parsePublish(flags, body)
			if err != nil {
				return err
			}
			c.received.Add(1)
			c.handler(msg)
			if qos == 1 {
				if err := c.writeLocked(conn, []byte{packetPuback << 4, 2, byte(id >> 8), byte(id)}); err != nil {
					return err
				}
			}
		case packetPingresp, packetPuback:
		default:
			// nothing else is expected from a broker to a subscriber that publishes nothing
		}
	}
}

func (c *Client) writeLocked(w io.Writer, packet []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := w.Write(packet)
	return err
}

func appendString(b []byte, s string) []byte {
	b = append(b, byte(len(s)>>8), byte(len(s)))
	return append(b, s...)
}

func encodeLength(n int) []byte {
	var out []byte
	for {
		digit := byte(n % 128)
		n /= 128
		if n > 0 {
			digit |= 0x80
		}
		out = append(out, digit)
		if n == 0 {
			return out
		}
	}
}

func connectPacket(o Options) []byte {
	flags := byte(0x02) // clean session
	if o.Username != "" {
		flags |= 0x80
		if o.Password != "" {
			flags |= 0x40
		}
	}
	var body []byte
	body = appendString(body, "MQTT")
	body = append(body, 4, flags, byte(int(o.KeepAlive/time.Second)>>8), byte(int(o.KeepAlive/time.Second)))
	body = appendString(body, o.ClientID)
	if o.Username != "" {
		body = appendString(body, o.Username)
		if o.Password != "" {
			body = appendString(body, o.Password)
		}
	}
	return append(append([]byte{packetConnect << 4}, encodeLength(len(body))...), body...)
}

func subscribePacket(id uint16, topics []string) []byte {
	body := []byte{byte(id >> 8), byte(id)}
	for _, t := range topics {
		body = appendString(body, t)
		body = append(body, 1) // QoS 1 at most
	}
	return append(append([]byte{packetSubscribe<<4 | 2}, encodeLength(len(body))...), body...)
}

func readPacket(r *bufio.Reader) (kind byte, flags byte, body []byte, err error) {
	first, err := r.ReadByte()
	if err != nil {
		return 0, 0, nil, err
	}
	length, shift := 0, 0
	for i := 0; ; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return 0, 0, nil, err
		}
		length |= int(b&0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
		if i >= 3 {
			return 0, 0, nil, errors.New("mqttin: malformed packet length")
		}
	}
	if length > MaxPacketBytes {
		return 0, 0, nil, fmt.Errorf("mqttin: a packet of %d bytes is above the %d limit", length, MaxPacketBytes)
	}
	body = make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, 0, nil, err
	}
	return first >> 4, first & 0x0F, body, nil
}

func parsePublish(flags byte, body []byte) (msg Message, id uint16, qos byte, err error) {
	if len(body) < 2 {
		return Message{}, 0, 0, errors.New("mqttin: short PUBLISH")
	}
	topicLen := int(body[0])<<8 | int(body[1])
	if len(body) < 2+topicLen {
		return Message{}, 0, 0, errors.New("mqttin: short PUBLISH topic")
	}
	msg.Topic = string(body[2 : 2+topicLen])
	rest := body[2+topicLen:]
	qos = (flags >> 1) & 3
	msg.Retained = flags&1 == 1
	if qos == 3 {
		return Message{}, 0, 0, errors.New("mqttin: PUBLISH with QoS 3")
	}
	if qos > 0 {
		if len(rest) < 2 {
			return Message{}, 0, 0, errors.New("mqttin: PUBLISH without a packet identifier")
		}
		id = uint16(rest[0])<<8 | uint16(rest[1])
		rest = rest[2:]
	}
	msg.Payload = rest
	return msg, id, qos, nil
}
