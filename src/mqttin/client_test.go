// HYDRA-UMC-TELEMETRY-COLLECTOR - mqttin/client_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package mqttin

import (
	"bufio"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeBroker accepts connections, checks the CONNECT, answers the SUBSCRIBE and then publishes what a test hands it.
type fakeBroker struct {
	ln       net.Listener
	mu       sync.Mutex
	connects []string // user names presented
	filters  [][]string
	refuse   byte
	sessions chan net.Conn
	acks     chan uint16
}

func newBroker(t *testing.T) *fakeBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &fakeBroker{ln: ln, sessions: make(chan net.Conn, 4), acks: make(chan uint16, 16)}
	go b.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return b
}

func (b *fakeBroker) url() string { return "tcp://" + b.ln.Addr().String() }

func (b *fakeBroker) serve() {
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			return
		}
		go b.handle(conn)
	}
}

func (b *fakeBroker) handle(conn net.Conn) {
	r := bufio.NewReader(conn)
	kind, _, body, err := readPacket(r)
	if err != nil || kind != packetConnect {
		_ = conn.Close()
		return
	}
	// protocol name, level, flags, keepalive, client id, [user], [password]
	pos := 2 + int(body[0])<<8 + int(body[1])
	flags := body[pos+1]
	pos += 4
	id := int(body[pos])<<8 | int(body[pos+1])
	pos += 2 + id
	user := ""
	if flags&0x80 != 0 {
		n := int(body[pos])<<8 | int(body[pos+1])
		user = string(body[pos+2 : pos+2+n])
	}
	b.mu.Lock()
	b.connects = append(b.connects, user)
	refuse := b.refuse
	b.mu.Unlock()
	_, _ = conn.Write([]byte{packetConnack << 4, 2, 0, refuse})
	if refuse != 0 {
		_ = conn.Close()
		return
	}
	for {
		kind, _, body, err := readPacket(r)
		if err != nil {
			return
		}
		switch kind {
		case packetSubscribe:
			var filters []string
			pos := 2
			for pos < len(body) {
				n := int(body[pos])<<8 | int(body[pos+1])
				filters = append(filters, string(body[pos+2:pos+2+n]))
				pos += 2 + n + 1
			}
			b.mu.Lock()
			b.filters = append(b.filters, filters)
			b.mu.Unlock()
			ack := []byte{packetSuback << 4, byte(2 + len(filters)), body[0], body[1]}
			for range filters {
				ack = append(ack, 1)
			}
			_, _ = conn.Write(ack)
			b.sessions <- conn
		case packetPuback:
			b.acks <- uint16(body[0])<<8 | uint16(body[1])
		case packetPingreq:
			_, _ = conn.Write([]byte{packetPingresp << 4, 0})
		}
	}
}

func publish(topic, payload string, qos byte, id uint16, retained bool) []byte {
	var body []byte
	body = appendString(body, topic)
	if qos > 0 {
		body = append(body, byte(id>>8), byte(id))
	}
	body = append(body, payload...)
	first := byte(packetPublish<<4) | qos<<1
	if retained {
		first |= 1
	}
	return append(append([]byte{first}, encodeLength(len(body))...), body...)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestTheClientSubscribesAndHandsOverWhatTheBrokerPublishes(t *testing.T) {
	b := newBroker(t)
	var mu sync.Mutex
	var got []Message
	c, err := New(Options{URL: b.url(), Username: "collector", Password: "secret", Topics: []string{StateTopic, AvailabilityTopic}},
		func(m Message) { mu.Lock(); got = append(got, m); mu.Unlock() })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	conn := <-b.sessions
	b.mu.Lock()
	if b.connects[0] != "collector" || len(b.filters[0]) != 2 || b.filters[0][0] != StateTopic {
		t.Fatalf("connect %v filters %v", b.connects, b.filters)
	}
	b.mu.Unlock()

	_, _ = conn.Write(publish("hydra/bridges/cnc/state", `{"x":1}`, 0, 0, true))
	_, _ = conn.Write(publish("hydra/bridges/cnc/availability", "online", 1, 77, false))
	waitFor(t, "two messages", func() bool { mu.Lock(); defer mu.Unlock(); return len(got) == 2 })
	if got[0].Topic != "hydra/bridges/cnc/state" || !got[0].Retained || string(got[0].Payload) != `{"x":1}` {
		t.Fatalf("first = %+v", got[0])
	}
	if id := <-b.acks; id != 77 {
		t.Fatalf("a QoS 1 message is acknowledged with its identifier, got %d", id)
	}
	st := c.Status()
	if !st.Connected || st.Received != 2 {
		t.Fatalf("status = %+v", st)
	}
}

func TestTheClientReconnectsAfterTheConnectionIsLost(t *testing.T) {
	b := newBroker(t)
	c, err := New(Options{URL: b.url(), Topics: []string{StateTopic}, MaxBackoff: 50 * time.Millisecond}, func(Message) {})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	first := <-b.sessions
	_ = first.Close()
	select {
	case <-b.sessions:
	case <-time.After(8 * time.Second):
		t.Fatal("the client did not come back")
	}
	waitFor(t, "a reconnect to be counted", func() bool { return c.Status().Reconnects >= 1 })
}

func TestAConnectionRefusedByTheBrokerIsReported(t *testing.T) {
	b := newBroker(t)
	b.mu.Lock()
	b.refuse = 5 // not authorised
	b.mu.Unlock()
	c, err := New(Options{URL: b.url(), Topics: []string{StateTopic}}, func(Message) {})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	waitFor(t, "the refusal", func() bool { return strings.Contains(c.Status().LastError, "refused the connection (code 5)") })
	if c.Status().Connected {
		t.Fatal("a refused client is not connected")
	}
}

func TestOptionsAreChecked(t *testing.T) {
	for _, url := range []string{"", "127.0.0.1:1883", "http://x:1", "tcp://"} {
		if _, err := New(Options{URL: url, Topics: []string{"a"}}, func(Message) {}); err == nil {
			t.Errorf("%q should be refused", url)
		}
	}
	if _, err := New(Options{URL: "tcp://127.0.0.1:1883"}, func(Message) {}); err == nil {
		t.Error("no topics should be refused")
	}
	if host, tls, _, err := parseURL("tls://broker"); err != nil || !tls || host != "broker:8883" {
		t.Errorf("tls default port: %q %v %v", host, tls, err)
	}
	if host, tls, _, err := parseURL("tcp://broker"); err != nil || tls || host != "broker:1883" {
		t.Errorf("tcp default port: %q %v %v", host, tls, err)
	}
}

func TestPacketLengthsAreEncodedAndBounded(t *testing.T) {
	for _, n := range []int{0, 127, 128, 16383, 16384, 2097151} {
		enc := encodeLength(n)
		r := bufio.NewReader(strings.NewReader(string([]byte{0x30}) + string(enc) + strings.Repeat("x", min(n, 1000))))
		if n > MaxPacketBytes {
			if _, _, _, err := readPacket(r); err == nil {
				t.Errorf("length %d should be refused", n)
			}
		}
	}
	if len(encodeLength(128)) != 2 || len(encodeLength(16384)) != 3 {
		t.Error("length encoding")
	}
}
