package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ralscha/sse-eventbus-go"
)

func TestRESPCommandAndArray(t *testing.T) {
	var command bytes.Buffer
	if err := writeCommand(&command, "PUBLISH", "channel", "hello"); err != nil {
		t.Fatal(err)
	}
	if command.String() != "*3\r\n$7\r\nPUBLISH\r\n$7\r\nchannel\r\n$5\r\nhello\r\n" {
		t.Fatalf("command=%q", command.String())
	}
	value, err := readRESP(bufio.NewReader(bytes.NewBufferString("*3\r\n$7\r\nmessage\r\n$7\r\nchannel\r\n$5\r\nhello\r\n")))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(value, []any{"message", "channel", "hello"}) {
		t.Fatalf("value=%#v", value)
	}
}

func TestRESPRejectsMalformedOrOversizedValues(t *testing.T) {
	tests := map[string]string{
		"line ending":    "+OK\n",
		"bulk ending":    "$3\r\nabcXX",
		"bulk too large": "$2097153\r\n",
		"negative array": "*-2\r\n",
		"deep nesting":   strings.Repeat("*1\r\n", maxRESPDepth+2) + "+OK\r\n",
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := readRESP(bufio.NewReader(strings.NewReader(input))); err == nil {
				t.Fatal("malformed RESP value was accepted")
			}
		})
	}
}

type fakeValkey struct {
	listener    net.Listener
	published   chan string
	subscribed  chan struct{}
	subscribeMu sync.Mutex
	subscribers []net.Conn
	closeOnce   sync.Once
}

func newFakeValkey(t *testing.T) *fakeValkey {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &fakeValkey{listener: listener, published: make(chan string, 1), subscribed: make(chan struct{})}
	go server.accept()
	t.Cleanup(server.close)
	return server
}

func (s *fakeValkey) accept() {
	for {
		connection, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handle(connection)
	}
}

func (s *fakeValkey) handle(connection net.Conn) {
	reader := bufio.NewReader(connection)
	value, err := readRESP(reader)
	if err != nil {
		_ = connection.Close()
		return
	}
	parts, _ := value.([]any)
	if len(parts) == 0 {
		_ = connection.Close()
		return
	}
	switch asString(parts[0]) {
	case "SUBSCRIBE":
		s.subscribeMu.Lock()
		s.subscribers = append(s.subscribers, connection)
		if len(s.subscribers) == 1 {
			close(s.subscribed)
		}
		s.subscribeMu.Unlock()
		_ = writeCommand(connection, "subscribe", asString(parts[1]), "1")
	case "PUBLISH":
		s.published <- asString(parts[2])
		_, _ = connection.Write([]byte(":1\r\n"))
		_ = connection.Close()
	default:
		_ = connection.Close()
	}
}

func (s *fakeValkey) broadcast(t *testing.T, channel, payload string) {
	t.Helper()
	s.subscribeMu.Lock()
	defer s.subscribeMu.Unlock()
	for _, subscriber := range s.subscribers {
		if err := writeCommand(subscriber, "message", channel, payload); err != nil {
			t.Fatal(err)
		}
	}
}

func (s *fakeValkey) close() {
	s.closeOnce.Do(func() {
		_ = s.listener.Close()
		s.subscribeMu.Lock()
		defer s.subscribeMu.Unlock()
		for _, subscriber := range s.subscribers {
			_ = subscriber.Close()
		}
	})
}

func TestValkeyTransportPublishesAndReceivesRemoteEvents(t *testing.T) {
	server := newFakeValkey(t)
	transport := newValkeyTransport(server.listener.Addr().String(), "events")
	t.Cleanup(transport.Close)
	received := make(chan sseeventbus.Event, 1)
	if err := transport.SetRemoteEventConsumer(func(event sseeventbus.Event) { received <- event }); err != nil {
		t.Fatal(err)
	}
	select {
	case <-server.subscribed:
	case <-time.After(2 * time.Second):
		t.Fatal("transport did not subscribe")
	}

	original := sseeventbus.NewNamedEventWithData("chat", `{"text":"hello","node":"Node A"}`)
	if err := transport.PublishRemote(context.Background(), original); err != nil {
		t.Fatal(err)
	}
	var envelope remoteEnvelope
	select {
	case payload := <-server.published:
		if err := json.Unmarshal([]byte(payload), &envelope); err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("transport did not publish")
	}
	if envelope.OriginNodeID != transport.nodeID || envelope.Event.Name != "chat" {
		t.Fatalf("published envelope=%#v", envelope)
	}

	// Valkey normally echoes publications to every subscriber. The local echo
	// must be suppressed by the transport.
	localPayload, _ := json.Marshal(envelope)
	server.broadcast(t, "events", string(localPayload))
	select {
	case event := <-received:
		t.Fatalf("received local echo: %#v", event)
	case <-time.After(50 * time.Millisecond):
	}

	envelope.OriginNodeID = "another-node"
	remotePayload, _ := json.Marshal(envelope)
	server.broadcast(t, "events", string(remotePayload))
	select {
	case event := <-received:
		if event.Name != "chat" || event.Data != original.Data {
			t.Fatalf("remote event=%#v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("remote event was not delivered")
	}
}

func TestValkeyPublishHonorsContextDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = connection.Close() }()
		_, _ = readRESP(bufio.NewReader(connection))
		_, _ = io.Copy(io.Discard, connection)
	}()

	transport := newValkeyTransport(listener.Addr().String(), "events")
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err = transport.PublishRemote(ctx, sseeventbus.NewEvent("payload"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("PublishRemote = %v, want context deadline", err)
	}
}

type captureConnection struct{ messages chan sseeventbus.Message }

func (c *captureConnection) Send(message sseeventbus.Message) error {
	c.messages <- message
	return nil
}
func (*captureConnection) Close() error { return nil }

func TestSendPublishesStructuredPayload(t *testing.T) {
	bus, err := sseeventbus.New(sseeventbus.WithSynchronousDelivery())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close(context.Background()) })
	connection := &captureConnection{messages: make(chan sseeventbus.Message, 1)}
	if err := bus.Register("client", connection, sseeventbus.SubscribeTo("chat")); err != nil {
		t.Fatal(err)
	}

	response := httptest.NewRecorder()
	newHandler(bus, "Node A").ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(" hello ")))
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %q", response.Code, response.Body.String())
	}
	if response.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatal("cross-origin production client is not allowed")
	}
	message := <-connection.messages
	var payload chatPayload
	if err := json.Unmarshal([]byte(message.Data), &payload); err != nil {
		t.Fatal(err)
	}
	if payload != (chatPayload{Text: "hello", Node: "Node A"}) {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestSendRejectsEmptyMessage(t *testing.T) {
	bus, err := sseeventbus.New(sseeventbus.WithSynchronousDelivery())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close(context.Background()) })
	response := httptest.NewRecorder()
	newHandler(bus, "Node A").ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(" \n ")))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}
