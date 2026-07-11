package main

import (
	"context"
	"encoding/json"
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
	bus, err := sseeventbus.New(sseeventbus.WithDistributedTransport(transport))
	if err != nil {
		log.Fatal(err)
	}
	mux := newHandler(bus, *nodeName)
	server := &http.Server{Addr: fmt.Sprintf(":%d", *port), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		log.Printf("%s listening on http://localhost:%d (Valkey %s)", *nodeName, *port, *valkeyAddress)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	_ = bus.Close(shutdownCtx)
	transport.Close()
}

func newHandler(bus *sseeventbus.Bus, nodeName string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /register/{clientId}", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.PathValue("clientId"))
		w.Header().Set("Cache-Control", "no-store")
		if err := httpadapter.Serve(w, r, bus, id, httpadapter.WithTimeout(0), httpadapter.WithRegistration(sseeventbus.SubscribeTo("chat"))); err != nil && !errors.Is(err, sseeventbus.ErrClosed) {
			log.Printf("SSE client %q: %v", id, err)
		}
	})
	mux.HandleFunc("POST /send", func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = r.Body.Close() }()
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "read body", 400)
			return
		}
		payload, _ := json.Marshal(struct {
			Text string `json:"text"`
			Node string `json:"node"`
		}{Text: string(body), Node: nodeName})
		if err := bus.Publish(r.Context(), sseeventbus.NewNamedEventWithData("chat", string(payload))); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	if _, err := os.Stat("src/main/resources/static"); err == nil {
		mux.Handle("/", http.FileServer(http.Dir("src/main/resources/static")))
	}
	return mux
}
