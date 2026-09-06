package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ralscha/sse-eventbus-go"
)

const (
	valkeyIOTimeout  = 3 * time.Second
	maxRESPBulkSize  = 2 << 20
	maxRESPArraySize = 1024
	maxRESPDepth     = 16
)

type wireEvent struct {
	ClientIDs        []string `json:"clientIds,omitempty"`
	ExcludeClientIDs []string `json:"excludeClientIds,omitempty"`
	Name             string   `json:"name"`
	Data             string   `json:"data,omitempty"`
	HasData          bool     `json:"hasData"`
	Retry            int64    `json:"retry,omitempty"`
	ID               string   `json:"id,omitempty"`
	Comment          string   `json:"comment,omitempty"`
}
type remoteEnvelope struct {
	OriginNodeID string    `json:"originNodeId"`
	Event        wireEvent `json:"event"`
}

type valkeyTransport struct {
	address, channel, nodeID string
	mu                       sync.RWMutex
	consumer                 func(sseeventbus.Event)
	configured               bool
	ctx                      context.Context
	cancel                   context.CancelFunc
	wg                       sync.WaitGroup
}

func newValkeyTransport(address, channel string) *valkeyTransport {
	ctx, cancel := context.WithCancel(context.Background())
	return &valkeyTransport{address: address, channel: channel, nodeID: randomID(), ctx: ctx, cancel: cancel}
}

func (t *valkeyTransport) SetRemoteEventConsumer(consumer func(sseeventbus.Event)) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if consumer == nil {
		return errors.New("remote event consumer is nil")
	}
	if t.configured {
		return errors.New("remote event consumer already configured")
	}
	t.consumer = consumer
	t.configured = true
	t.wg.Add(1)
	go t.subscribe()
	return nil
}

func (t *valkeyTransport) PublishRemote(ctx context.Context, event sseeventbus.Event) error {
	if ctx == nil {
		ctx = context.Background()
	}
	data, hasData := event.Data.(string)
	if event.Data != nil && !hasData {
		encoded, err := json.Marshal(event.Data)
		if err != nil {
			return err
		}
		data = string(encoded)
		hasData = true
	}
	payload, err := json.Marshal(remoteEnvelope{
		OriginNodeID: t.nodeID,
		Event: wireEvent{
			ClientIDs: event.ClientIDs, ExcludeClientIDs: event.ExcludeClientIDs,
			Name: event.Name, Data: data, HasData: hasData, Retry: int64(event.Retry),
			ID: event.ID, Comment: event.Comment,
		},
	})
	if err != nil {
		return fmt.Errorf("encode remote event: %w", err)
	}
	dialer := net.Dialer{Timeout: valkeyIOTimeout}
	connection, err := dialer.DialContext(ctx, "tcp", t.address)
	if err != nil {
		return fmt.Errorf("connect to Valkey: %w", err)
	}
	defer func() { _ = connection.Close() }()
	deadline := time.Now().Add(valkeyIOTimeout)
	contextDeadline, hasContextDeadline := ctx.Deadline()
	if hasContextDeadline && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := connection.SetDeadline(deadline); err != nil {
		return fmt.Errorf("set Valkey deadline: %w", err)
	}
	stopCancellation := context.AfterFunc(ctx, func() { _ = connection.SetDeadline(time.Now()) })
	defer stopCancellation()
	if err := writeCommand(connection, "PUBLISH", t.channel, string(payload)); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("publish to Valkey: %w", err)
	}
	response, err := readRESP(bufio.NewReader(connection))
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if hasContextDeadline && !time.Now().Before(contextDeadline) {
			return context.DeadlineExceeded
		}
		return fmt.Errorf("read Valkey publish response: %w", err)
	}
	if _, ok := response.(int64); !ok {
		return fmt.Errorf("unexpected Valkey publish response %T", response)
	}
	return nil
}

func (t *valkeyTransport) subscribe() {
	defer t.wg.Done()
	for {
		if t.ctx.Err() != nil {
			return
		}
		if err := t.subscribeOnce(); err != nil && t.ctx.Err() == nil {
			log.Printf("Valkey subscriber: %v", err)
		}
		select {
		case <-t.ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (t *valkeyTransport) subscribeOnce() error {
	dialer := net.Dialer{Timeout: valkeyIOTimeout}
	connection, err := dialer.DialContext(t.ctx, "tcp", t.address)
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()
	if err := writeCommand(connection, "SUBSCRIBE", t.channel); err != nil {
		return err
	}
	reader := bufio.NewReader(connection)
	for {
		if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			return err
		}
		value, err := readRESP(reader)
		if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
			if t.ctx.Err() != nil {
				return nil
			}
			continue
		}
		if err != nil {
			return err
		}
		parts, ok := value.([]any)
		if !ok || len(parts) != 3 || asString(parts[0]) != "message" || asString(parts[1]) != t.channel {
			continue
		}
		payload, ok := parts[2].(string)
		if !ok {
			log.Printf("decode remote event: payload is %T", parts[2])
			continue
		}
		var envelope remoteEnvelope
		if err := json.Unmarshal([]byte(payload), &envelope); err != nil {
			log.Printf("decode remote event: %v", err)
			continue
		}
		if envelope.OriginNodeID == "" || envelope.OriginNodeID == t.nodeID {
			continue
		}
		event := sseeventbus.Event{ClientIDs: envelope.Event.ClientIDs, ExcludeClientIDs: envelope.Event.ExcludeClientIDs, Name: envelope.Event.Name, Retry: time.Duration(envelope.Event.Retry), ID: envelope.Event.ID, Comment: envelope.Event.Comment}
		if envelope.Event.HasData {
			event.Data = envelope.Event.Data
		}
		t.mu.RLock()
		consumer := t.consumer
		t.mu.RUnlock()
		if consumer != nil {
			consumer(event)
		}
	}
}

func (t *valkeyTransport) Close() { t.cancel(); t.wg.Wait() }

func writeCommand(writer io.Writer, parts ...string) error {
	if _, err := fmt.Fprintf(writer, "*%d\r\n", len(parts)); err != nil {
		return err
	}
	for _, part := range parts {
		if _, err := fmt.Fprintf(writer, "$%d\r\n%s\r\n", len(part), part); err != nil {
			return err
		}
	}
	return nil
}

func readRESP(reader *bufio.Reader) (any, error) {
	return readRESPDepth(reader, 0)
}

func readRESPDepth(reader *bufio.Reader, depth int) (any, error) {
	if depth > maxRESPDepth {
		return nil, errors.New("RESP nesting is too deep")
	}
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	line := func() (string, error) {
		value, err := reader.ReadString('\n')
		if err != nil {
			return "", err
		}
		if !strings.HasSuffix(value, "\r\n") {
			return "", errors.New("RESP line does not end in CRLF")
		}
		return value[:len(value)-2], nil
	}
	switch prefix {
	case '+':
		return line()
	case '-':
		value, readErr := line()
		if readErr != nil {
			return nil, readErr
		}
		return nil, errors.New(value)
	case ':':
		value, readErr := line()
		if readErr != nil {
			return nil, readErr
		}
		return strconv.ParseInt(value, 10, 64)
	case '$':
		lengthText, readErr := line()
		if readErr != nil {
			return nil, readErr
		}
		length, parseErr := strconv.Atoi(lengthText)
		if parseErr != nil {
			return nil, parseErr
		}
		if length == -1 {
			return nil, nil
		}
		if length < -1 || length > maxRESPBulkSize {
			return nil, fmt.Errorf("invalid RESP bulk length %d", length)
		}
		buffer := make([]byte, length+2)
		if _, readErr = io.ReadFull(reader, buffer); readErr != nil {
			return nil, readErr
		}
		if buffer[length] != '\r' || buffer[length+1] != '\n' {
			return nil, errors.New("RESP bulk string does not end in CRLF")
		}
		return string(buffer[:length]), nil
	case '*':
		countText, readErr := line()
		if readErr != nil {
			return nil, readErr
		}
		count, parseErr := strconv.Atoi(countText)
		if parseErr != nil {
			return nil, parseErr
		}
		if count == -1 {
			return nil, nil
		}
		if count < -1 || count > maxRESPArraySize {
			return nil, fmt.Errorf("invalid RESP array length %d", count)
		}
		values := make([]any, count)
		for i := range values {
			values[i], readErr = readRESPDepth(reader, depth+1)
			if readErr != nil {
				return nil, readErr
			}
		}
		return values, nil
	default:
		return nil, fmt.Errorf("unsupported RESP prefix %q", prefix)
	}
}

func asString(value any) string { text, _ := value.(string); return text }
func randomID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		panic(err)
	}
	return hex.EncodeToString(value)
}
