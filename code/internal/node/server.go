// Package node exposes the client-facing HTTP interface. Every node runs
// its own HTTP server; a request can land on any peer and will be routed
// to the responsible node transparently.
package node

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"dito/internal/consensus"
)

type Server struct {
	node *Node
	srv  *http.Server
}

func NewServer(n *Node, addr string) *Server {
	mux := http.NewServeMux()
	s := &Server{node: n}

	mux.HandleFunc("POST /put/{id}", s.handlePut)
	mux.HandleFunc("POST /update/{id}", s.handleUpdate)
	mux.HandleFunc("GET /get/{id}", s.handleGet)
	mux.HandleFunc("GET /debug/membership", s.handleMembership)
	mux.HandleFunc("GET /debug/dump", s.handleDump)

	s.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

func (s *Server) Start() error {
	go func() {
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// Fatal-ish: logged via Printf in main via wrapper if desired.
		}
	}()
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.node.HandlePut(r.Context(), id, body); err != nil {
		writeOpErr(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) handleUpdate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.node.HandleUpdate(r.Context(), id, body); err != nil {
		writeOpErr(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	value, found, _, err := s.node.HandleGet(r.Context(), id)
	if err != nil {
		writeOpErr(w, err)
		return
	}
	if !found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(value)
}

// handleMembership exposes the SWIM membership-table size for this node so a
// remote benchmark can validate the O(R)-per-node claim (tracked count should
// stay bounded by ~2.7R regardless of cluster size N). Read-only, no state
// mutation. Output is a single line: "tracked=<n> groups=<g> changes=<c>".
func (s *Server) handleMembership(w http.ResponseWriter, r *http.Request) {
	tracked := s.node.Gossip().TrackedCount()
	changes := s.node.Gossip().CompositionChanges()
	groups := len(s.node.Manager().Groups())
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "tracked=%d groups=%d changes=%d\n", tracked, groups, changes)
}

// handleDump streams this node's locally-committed key→value pairs — one line per
// resident Paxos group that holds a value, "<key>\t<base64(value)>\n". Read-only,
// no consensus: it reports exactly what THIS replica has applied, so the
// all-replica audit can cross-reference every node and flag any key whose
// committed value differs across replicas (split-brain at rest).
func (s *Server) handleDump(w http.ResponseWriter, r *http.Request) {
	dump := s.node.Manager().LocalDump()
	w.Header().Set("Content-Type", "text/plain")
	bw := bufio.NewWriter(w)
	defer bw.Flush()
	for k, v := range dump {
		fmt.Fprintf(bw, "%s\t%s\n", k, base64.StdEncoding.EncodeToString(v))
	}
}

// writeOpErr maps state-machine outcomes to their HTTP statuses by sentinel
// identity, not by message text: dispatch returns the consensus sentinels
// directly on the local path and reconstructs exactly these two from a
// forwarded ErrMsg, so errors.Is holds across forwarding. Matching on
// substrings instead would turn unrelated transport errors that happen to
// contain "not found" into definitive 404s — a liveness failure disguised
// as a semantic answer.
func writeOpErr(w http.ResponseWriter, err error) {
	msg := err.Error()
	switch {
	case errors.Is(err, consensus.ErrAlreadyExists):
		http.Error(w, msg, http.StatusConflict)
	case errors.Is(err, consensus.ErrNotFound):
		http.Error(w, msg, http.StatusNotFound)
	case errors.Is(err, consensus.ErrUnconfirmedConfig):
		// The responsible node could only resolve a guessed (unconfirmed)
		// config and refused rather than risk a stale answer. This is the
		// CP choice: unavailable, not inconsistent. 503 so the client retries
		// (and re-routing converges on the canonical owner).
		http.Error(w, msg, http.StatusServiceUnavailable)
	default:
		http.Error(w, msg, http.StatusInternalServerError)
	}
}
