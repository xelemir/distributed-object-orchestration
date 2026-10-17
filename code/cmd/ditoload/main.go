// ditoload is the distributed load generator and correctness checker for dito
// HTTP APIs. It drives one of several workload scenarios against a live cluster
// and, crucially, verifies LINEARIZABILITY of every per-key history with the
// porcupine checker — so a run reports not just throughput/latency but whether
// the system ever returned data no valid linearisation can explain.
//
// Each scenario runs `-runs` times (default 10); KPIs are reported as
// mean ± stddev. Run `ditoload -scenario list` to see all scenarios and the
// failure classes each one expects.
//
// The failure taxonomy (see client.go) is the heart of "what unsuccessful
// means": only class A (non-linearizable / ghost / stale) is an actual bug.
// B (semantic 409/404) is success; C (node died mid-request) is expected under
// churn; D (congestion timeout) is a capacity signal, not incorrectness.
//
// Usage:
//
//	ditoload -scenario mixed -targets http://10.0.0.1:9001,http://10.0.0.2:9001
//	ditoload -scenario churn -runs 10 -dur 90s -targets ...
//	ditoload -membership -targets ...        # probe /debug/membership and exit
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// keyNonce salts every key namespace so distinct ditoload processes never
// collide on a persistent cluster (see runOnce). Set once in main().
var keyNonce string

func main() {
	var (
		targetsFlag = flag.String("targets", "http://127.0.0.1:9001",
			"comma-separated HTTP base URLs of dito nodes")
		scenarioFlag = flag.String("scenario", "mixed",
			"workload scenario: fill|read|mixed|hammer|stale|churn|split (or 'list')")
		runs       = flag.Int("runs", 10, "measurement runs to average over")
		workers    = flag.Int("workers", 0, "worker goroutines (0 = scenario default)")
		keySpace   = flag.Int("keys", 0, "distinct object keys (0 = scenario default)")
		dur        = flag.Duration("dur", 0, "per-run duration (0 = scenario default)")
		opTO       = flag.Duration("op-timeout", 10*time.Second, "per-operation HTTP timeout")
		valueLen   = flag.Int("value-bytes", 256, "PUT/UPDATE value size in bytes")
		perKeyCap  = flag.Int("per-key-cap", 2000, "max ops recorded per key for porcupine (bounds check cost)")
		settle     = flag.Duration("settle", 3*time.Second, "pause between runs")
		membership = flag.Bool("membership", false, "probe /debug/membership on all targets and exit")
		keyTag     = flag.String("key-tag", "", "key-namespace salt (default: unique per process; set to isolate/reuse a namespace across invocations)")
		auditTgts  = flag.String("audit-targets", "",
			"comma-separated HTTP base URLs of ALL nodes (not just ingress) for the\n"+
				"  full-replica audit run by fullAudit scenarios (mixedfull, rechurn);\n"+
				"  every node is polled once via /debug/dump and unreachable ones skipped")
		auditSettle = flag.Duration("audit-settle", 0,
			"pause after the workload before the full-replica audit, so churn-driven\n"+
				"  migrations drain and the audit sees the settled (not mid-migration) state")
	)
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Ltime)

	// Unique per-process key namespace unless the caller pins one with -key-tag.
	if keyNonce = *keyTag; keyNonce == "" {
		keyNonce = strconv.FormatInt(time.Now().UnixNano(), 36)
	}

	targets := parseTargets(*targetsFlag)
	if len(targets) == 0 {
		log.Fatal("-targets must contain at least one URL")
	}

	if *membership {
		probeMembership(targets, *opTO)
		return
	}
	if *scenarioFlag == "list" {
		printScenarioList()
		return
	}

	sc, ok := scenarios[*scenarioFlag]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n\n", *scenarioFlag)
		printScenarioList()
		os.Exit(2)
	}

	// Resolve params: flags override scenario defaults when non-zero.
	p := sc.def
	if *workers > 0 {
		p.workers = *workers
	}
	if *keySpace > 0 {
		p.keySpace = *keySpace
	}
	if *dur > 0 {
		p.duration = *dur
	}

	clients := make([]*http.Client, len(targets))
	for i := range clients {
		clients[i] = &http.Client{
			Timeout:   *opTO + time.Second,
			Transport: &http.Transport{MaxIdleConnsPerHost: 256, MaxConnsPerHost: 0},
		}
	}
	e := &env{
		targets:      targets,
		clients:      clients,
		valueLen:     *valueLen,
		opTimeout:    *opTO,
		perKeyCap:    *perKeyCap,
		auditTargets: parseTargets(*auditTgts),
		auditSettle:  *auditSettle,
	}

	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sig; log.Println("interrupt — stopping after current run"); cancel() }()

	log.Printf("ditoload scenario=%s runs=%d workers=%d keys=%d dur=%s targets=%d",
		sc.name, *runs, p.workers, p.keySpace, p.duration, len(targets))
	log.Printf("expected unsuccessful: %s", sc.expects)

	agg := newAggregator()
	for run := 0; run < *runs; run++ {
		if ctx.Err() != nil {
			log.Printf("aborted before run %d", run+1)
			break
		}
		log.Printf("%s", dividerLine())
		log.Printf("run %d/%d", run+1, *runs)
		k := runOnce(ctx, e, sc, p, run)
		agg.addRun(k)
		if run < *runs-1 {
			time.Sleep(*settle)
		}
	}

	pass := agg.report(sc.name, len(agg.values[firstKey(agg)]))
	if !pass {
		os.Exit(1)
	}
}

// runOnce executes one measurement run and returns its KPIs.
func runOnce(ctx context.Context, e *env, sc scenarioDef, p params, run int) runKPIs {
	hist := newOpHistory(e.perKeyCap)
	oracle := newValueOracle()
	ctr := &counters{}
	lat := &latSamples{}
	h := &harness{env: e, hist: hist, oracle: oracle, ctr: ctr, lat: lat}

	// keyNonce isolates this process's key namespace. Without it, two ditoload
	// invocations against the SAME persistent cluster reuse prefixes like
	// "mixed-r0-": the second process then reads keys the first left behind —
	// values its own (empty) oracle/porcupine history never recorded — and
	// reports them as ghost/linearizability violations. A per-process salt makes
	// every invocation's namespace disjoint (critical for the worker sweep,
	// which runs ditoload many times without wiping the cluster).
	prefix := fmt.Sprintf("%s-%s-r%d-", sc.name, keyNonce, run)
	elapsed := sc.run(ctx, h, prefix, p)

	lin := checkLinearizability(hist)
	for _, s := range lin.samples {
		log.Printf("CLASS-A linearizability violation:\n%s", s)
	}

	total := ctr.total.Load()
	succ := ctr.succ.Load()
	denom := float64(maxU64(total, 1))

	var k runKPIs
	k.add("throughput", float64(succ)/elapsed.Seconds(), "ops/s")
	k.add("success rate", 100*float64(succ)/denom, "%")
	k.addDurPct("latency", lat.calc())
	// Failure-class breakdown (% of total ops).
	k.add("B semantic", 100*float64(ctr.semantic.Load())/denom, "%")
	k.add("C liveness", 100*float64(ctr.liveness.Load())/denom, "%")
	k.add("D congestion", 100*float64(ctr.congestion.Load())/denom, "%")
	k.add("E unexpected", float64(ctr.unexpected.Load()), "")
	// Class A — correctness; must be 0.
	k.addFatal("A lin violations", float64(lin.violations))
	k.addFatal("A ghost values", float64(ctr.ghost.Load()))
	k.addFatal("A stale reads", float64(ctr.stale.Load()))
	if sc.name == "split" {
		// Membership-divergence detectors (split scenario only).
		k.addFatal("A split-brain double-create", float64(ctr.splitCreate.Load()))
		k.addFatal("A split-brain divergent-read", float64(ctr.splitRead.Load()))
	}
	// Full all-node replica audit (mixedfull / rechurn): poll every node's
	// committed state and cross-reference each key across replicas.
	if sc.fullAudit && len(e.auditTargets) > 0 {
		if e.auditSettle > 0 {
			log.Printf("settling %s before audit (let churn-driven migrations drain)...", e.auditSettle)
			select {
			case <-time.After(e.auditSettle):
			case <-ctx.Done():
			}
		}
		log.Printf("running full all-node replica audit over %d nodes...", len(e.auditTargets))
		a := runFullAudit(ctx, e, h)
		log.Printf("audit: %d/%d nodes reached, %d keys, %.2f replicas/key, divergence(quorum)=%d any-divergence=%d ghost-at-rest=%d lost-confirmed=%d",
			a.nodesReached, len(e.auditTargets), a.keysAudited, a.replicasPerKey, a.replicaDivergence, a.anyDivergence, a.ghostAtRest, a.lostConfirmed)
		// Forensics: name the keys behind each class-A finding so a split-brain
		// can be diagnosed (two CREATEs vs a write race) without re-querying.
		for _, s := range a.divSamples {
			log.Printf("CLASS-A replica divergence:\n  %s", s)
		}
		for _, s := range a.lostSamples {
			log.Printf("CLASS-A lost confirmed write: %s", s)
		}
		for _, s := range a.ghostSamples {
			log.Printf("CLASS-A ghost-at-rest: %s", s)
		}
		// The two failure modes, reported as separate class-A KPIs so a rechurn
		// sweep can attribute violations: replica divergence = DHT/membership
		// inconsistency (two quorums); lost confirmed = failure budget exceeded
		// (all replicas gone before re-replication).
		k.addFatal("A replica divergence", float64(a.replicaDivergence))
		k.addFatal("A lost confirmed write", float64(a.lostConfirmed))
		k.addFatal("A ghost-at-rest", float64(a.ghostAtRest))
		k.add("audit any-divergence (incl stragglers)", float64(a.anyDivergence), "")
		k.add("audit nodes reached", float64(a.nodesReached), "")
		k.add("audit keys", float64(a.keysAudited), "")
		k.add("audit replicas/key", a.replicasPerKey, "")
		if a.keysNoReplica > 0 {
			k.add("audit keys w/o replica", float64(a.keysNoReplica), "")
		}
	}
	// Diagnostics.
	k.add("keys checked", float64(lin.keysChecked), "")
	if lin.timedOut > 0 {
		k.add("lin inconclusive keys", float64(lin.timedOut), "")
	}
	if c := hist.cappedKeys(); c > 0 {
		k.add("cap-truncated keys", float64(c), "")
	}
	// Scenario-specific KPIs.
	if sc.extraKPIs != nil {
		sc.extraKPIs(h, p)
	}
	k.order = append(k.order, h.extra...)

	log.Printf("run done: %.0f ops/s  succ=%d/%d  A(lin/ghost/stale)=%d/%d/%d",
		float64(succ)/elapsed.Seconds(), succ, total,
		lin.violations, ctr.ghost.Load(), ctr.stale.Load())
	return k
}

// ─── Membership probe (scale sweep) ─────────────────────────────────────────

// probeMembership GETs /debug/membership on every target and prints the
// tracked-peer-count distribution, validating the O(R)-per-node claim: tracked
// count should stay bounded regardless of cluster size N.
func probeMembership(targets []string, to time.Duration) {
	cl := &http.Client{Timeout: to}
	var tracked []int
	var groups []int
	for _, base := range targets {
		ctx, cancel := context.WithTimeout(context.Background(), to)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/debug/membership", nil)
		resp, err := cl.Do(req)
		if err != nil {
			cancel()
			log.Printf("  %s: %v", base, err)
			continue
		}
		buf := make([]byte, 256)
		n, _ := resp.Body.Read(buf)
		resp.Body.Close()
		cancel()
		t, g := parseMembership(string(buf[:n]))
		tracked = append(tracked, t)
		groups = append(groups, g)
	}
	if len(tracked) == 0 {
		log.Fatal("no membership data collected")
	}
	sort.Ints(tracked)
	sum, mx := 0, tracked[len(tracked)-1]
	for _, t := range tracked {
		sum += t
	}
	fmt.Printf("=== membership (n=%d nodes probed) ===\n", len(tracked))
	fmt.Printf("  tracked peers: avg=%.2f  max=%d  min=%d  median=%d\n",
		float64(sum)/float64(len(tracked)), mx, tracked[0], tracked[len(tracked)/2])
	gsum := 0
	for _, g := range groups {
		gsum += g
	}
	fmt.Printf("  groups/node  : avg=%.2f\n", float64(gsum)/float64(len(groups)))
}

func parseMembership(s string) (tracked, groups int) {
	for _, f := range strings.Fields(s) {
		k, v, ok := strings.Cut(f, "=")
		if !ok {
			continue
		}
		n, _ := strconv.Atoi(v)
		switch k {
		case "tracked":
			tracked = n
		case "groups":
			groups = n
		}
	}
	return
}

// ─── Helpers ────────────────────────────────────────────────────────────────

func printScenarioList() {
	fmt.Println("scenarios (each runs -runs times, KPIs averaged):")
	names := make([]string, 0, len(scenarios))
	for n := range scenarios {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		sc := scenarios[n]
		fmt.Printf("  %-8s  %s\n", sc.name, sc.expects)
	}
}

func firstKey(a *aggregator) string {
	if len(a.order) == 0 {
		return ""
	}
	return a.order[0]
}

func parseTargets(raw string) []string {
	var out []string
	for _, s := range strings.Split(raw, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
