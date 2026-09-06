package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ralscha/sse-eventbus-go"
	"github.com/ralscha/sse-eventbus-go/httpadapter"
)

func main() {
	port := flag.Int("port", 8080, "HTTP port")
	nodeName := flag.String("node", "Node A", "name included in chat messages")
	valkeyAddress := flag.String("valkey", "localhost:6379", "Valkey address")
	flag.Parse()
	transport := newValkeyTransport(*valkeyAddress, "sse-eventbus")
	bus, err := sseeventbus.New(
		sseeventbus.WithDistributedTransport(transport),
		sseeventbus.WithHeartbeat(30*time.Second, "keep-alive"),
		sseeventbus.WithClientExpiration(10*time.Minute, time.Minute),
	)
	if err != nil {
		log.Fatal(err)
	}
	mux := newHandler(bus, *nodeName)
	server := &http.Server{Addr: fmt.Sprintf(":%d", *port), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveErrors := make(chan error, 1)
	go func() {
		log.Printf("%s listening on http://localhost:%d (Valkey %s)", *nodeName, *port, *valkeyAddress)
		serveErrors <- server.ListenAndServe()
	}()
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-serveErrors:
		stop()
	}

	closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
	if err := bus.Close(closeCtx); err != nil {
		log.Printf("close event bus: %v", err)
	}
	cancelClose()
	transport.Close()

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("shut down HTTP server: %v", err)
	}
	cancelShutdown()
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		log.Fatal(serveErr)
	}
}

type chatPayload struct {
	Text string `json:"text"`
	Node string `json:"node"`
}

func newHandler(bus *sseeventbus.Bus, nodeName string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /register/{clientId}", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.PathValue("clientId"))
		if id == "" || len(id) > 128 {
			http.Error(w, "invalid client ID", http.StatusBadRequest)
			return
		}
		if err := httpadapter.Serve(w, r, bus, id,
			httpadapter.WithTimeout(0),
			httpadapter.WithWriteTimeout(10*time.Second),
			httpadapter.WithRegistration(sseeventbus.ReplaceSubscriptions("chat")),
		); err != nil && !errors.Is(err, sseeventbus.ErrClosed) && !errors.Is(err, context.Canceled) {
			log.Printf("SSE client %q: %v", id, err)
		}
	})
	mux.HandleFunc("POST /send", func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = r.Body.Close() }()
		body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		if len(body) > maxRequestBody {
			http.Error(w, "request body is too large", http.StatusRequestEntityTooLarge)
			return
		}
		text := strings.TrimSpace(string(body))
		if text == "" {
			http.Error(w, "message is required", http.StatusBadRequest)
			return
		}
		payload := chatPayload{Text: text, Node: nodeName}
		if err := bus.Publish(r.Context(), sseeventbus.NewNamedEventWithData("chat", payload)); err != nil {
			http.Error(w, "publish message", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	if _, err := os.Stat("src/main/resources/static"); err == nil {
		mux.Handle("/", http.FileServer(http.Dir("src/main/resources/static")))
	}
	return allowCORS(mux)
}

const maxRequestBody = 1 << 20

func allowCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
