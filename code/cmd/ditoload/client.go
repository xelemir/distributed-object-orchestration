package main

// client.go — HTTP operations against a dito node plus the failure taxonomy.
//
// Every operation outcome falls into exactly one class. Only class A is a bug;
// the rest are either correct behaviour or capacity signals. Each scenario
// declares which classes it expects (see scenario.go).
//
//	A  Correctness violation   non-linearizable / ghost value / stale read.
//	                           Detected post-hoc (porcupine, oracle, stale
//	                           probe) — NOT a per-op class. Must always be 0.
//	B  Semantic rejection      409 on PUT-existing, 404 on UPDATE/GET-absent.
//	                           Correct protocol behaviour → counted as success.
//	C  Liveness failure        connection reset/refused, EOF, "no forward
//	                           target", "NotInGroup" exhausted: the node we
//	                           talked to (or its quorum) was down mid-request.
//	                           Expected under churn; counts against availability.
//	D  Congestion timeout      context deadline exceeded from overload /
//	                           proposer dueling. A capacity signal, not a bug.
//	E  Unexpected error        non-churn 5xx, malformed response. Investigate.
//
// Phantom rule: a write that ends in C, D, or E MAY have committed (the first
// forward hop can drive Paxos to completion before the response path
// collapses). Such writes are recorded with writeOutput{unknown:true} so the
// porcupine model explores both branches; they are never class-A by themselves.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
)

type failClass int

const (
	classOK         failClass = iota // success (incl. semantic rejection)
	classSemantic                    // B — expected protocol rejection
	classLiveness                    // C — node/quorum down mid-request
	classCongestion                  // D — overload timeout
	classUnexpected                  // E — investigate
)

func (c failClass) String() string {
	switch c {
	case classOK:
		return "ok"
	case classSemantic:
		return "semantic"
	case classLiveness:
		return "liveness"
	case classCongestion:
		return "congestion"
	default:
		return "unexpected"
	}
}

// opOutcome is the structured result of one HTTP operation.
type opOutcome struct {
	status  int    // HTTP status, 0 on transport error
	value   []byte // GET body (reads only)
	exists  bool   // GET found the key
	class   failClass
	phantom bool // write may have committed despite a non-OK outcome
	err     error
}

// doPut issues POST /put/{key}. 201=created, 409=already-exists (semantic).
func doPut(ctx context.Context, cl *http.Client, base, key string, val []byte) opOutcome {
	return doWrite(ctx, cl, http.MethodPost, base+"/put/"+key, val,
		http.StatusCreated, http.StatusConflict)
}

// doUpdate issues POST /update/{key}. 200=ok, 404=not-found (semantic).
func doUpdate(ctx context.Context, cl *http.Client, base, key string, val []byte) opOutcome {
	return doWrite(ctx, cl, http.MethodPost, base+"/update/"+key, val,
		http.StatusOK, http.StatusNotFound)
}

func doWrite(ctx context.Context, cl *http.Client, method, url string, val []byte, okCode, semCode int) opOutcome {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(val))
	if err != nil {
		return opOutcome{class: classUnexpected, err: err}
	}
	resp, err := cl.Do(req)
	if err != nil {
		return classifyTransport(err, true) // writes: transport error is a phantom
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case okCode:
		_, _ = io.Copy(io.Discard, resp.Body)
		return opOutcome{status: resp.StatusCode, class: classOK}
	case semCode:
		// 409/404 — write definitively did NOT change state. Not a phantom.
		_, _ = io.Copy(io.Discard, resp.Body)
		return opOutcome{status: resp.StatusCode, class: classSemantic}
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return classifyStatus(resp.StatusCode, string(body), true)
	}
}

// doGet issues GET /get/{key}, returning the body so the read value can be
// matched in the porcupine history. 200=value, 404=not-exists.
func doGet(ctx context.Context, cl *http.Client, base, key string) opOutcome {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/get/"+key, nil)
	if err != nil {
		return opOutcome{class: classUnexpected, err: err}
	}
	resp, err := cl.Do(req)
	if err != nil {
		return classifyTransport(err, false) // reads cannot be phantom
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		body, _ := io.ReadAll(resp.Body)
		return opOutcome{status: resp.StatusCode, value: body, exists: true, class: classOK}
	case http.StatusNotFound:
		_, _ = io.Copy(io.Discard, resp.Body)
		return opOutcome{status: resp.StatusCode, exists: false, class: classOK}
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return classifyStatus(resp.StatusCode, string(body), false)
	}
}

// classifyTransport maps a Go HTTP transport error to class C/D/E. `isWrite`
// marks the outcome phantom for non-semantic write failures.
func classifyTransport(err error, isWrite bool) opOutcome {
	out := opOutcome{err: err, phantom: isWrite}
	if errors.Is(err, context.DeadlineExceeded) {
		out.class = classCongestion // D
		return out
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "deadline") || strings.Contains(msg, "timeout") || strings.Contains(msg, "timed out"):
		out.class = classCongestion // D
	case strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "eof"),
		strings.Contains(msg, "no route to host"),
		strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "connection closed"):
		out.class = classLiveness // C — the ingress node was down
	default:
		out.class = classUnexpected // E
	}
	return out
}

// classifyStatus maps a non-OK/non-semantic HTTP status to class C/D/E. A 5xx
// whose body names a churn condition (forwarding/quorum drift) is liveness (C),
// not a bug; anything else unexpected is E.
func classifyStatus(status int, body string, isWrite bool) opOutcome {
	out := opOutcome{
		status:  status,
		phantom: isWrite,
		err:     errors.New("http " + http.StatusText(status) + ": " + body),
	}
	if isChurnMsg(body) {
		out.class = classLiveness // C — quorum/forwarding drift during churn
	} else {
		out.class = classUnexpected // E
	}
	return out
}

// isChurnMsg reports whether an error message describes an expected churn /
// routing condition (class C) rather than a genuine fault.
func isChurnMsg(msg string) bool {
	msg = strings.ToLower(msg)
	return strings.Contains(msg, "no forward target") ||
		strings.Contains(msg, "notingroup") ||
		strings.Contains(msg, "not in group") ||
		strings.Contains(msg, "no quorum") ||
		strings.Contains(msg, "forward hint") ||
		strings.Contains(msg, "no paxos group")
}
