package main

// report.go — latency percentiles, per-run KPI capture, and cross-run
// aggregation. Every scenario runs `-runs` times; each run emits an ordered
// list of named KPIs and we report mean ± sample-stddev across runs so the
// thesis can quote a stable figure with its run-to-run variance.

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// ─── Latency samples ────────────────────────────────────────────────────────

type latSamples struct {
	mu      sync.Mutex
	samples []time.Duration
}

func (s *latSamples) record(d time.Duration) {
	s.mu.Lock()
	s.samples = append(s.samples, d)
	s.mu.Unlock()
}

type percentiles struct{ p50, p90, p99, p999 time.Duration }

func (s *latSamples) calc() percentiles {
	s.mu.Lock()
	cp := make([]time.Duration, len(s.samples))
	copy(cp, s.samples)
	s.mu.Unlock()
	if len(cp) == 0 {
		return percentiles{}
	}
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	n := len(cp)
	at := func(p float64) time.Duration { return cp[int(float64(n-1)*p+0.5)] }
	return percentiles{at(0.50), at(0.90), at(0.99), at(0.999)}
}

func fmtDur(d time.Duration) string {
	switch {
	case d == 0:
		return "0"
	case d < time.Millisecond:
		return fmt.Sprintf("%.0fµs", float64(d)/float64(time.Microsecond))
	default:
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	}
}

// ─── KPI capture ────────────────────────────────────────────────────────────

// A kpi is one named, numeric measurement from a single run. `unit` controls
// formatting in the aggregate table; `fatal` marks a correctness metric (class
// A) that must be 0 — any non-zero mean is flagged as a FAILED benchmark.
type kpi struct {
	name  string
	val   float64
	unit  string // "ops/s", "ms", "%", "" (count)
	fatal bool
}

// runKPIs is the ordered KPI list produced by one run of a scenario.
type runKPIs struct {
	order []kpi
}

func (r *runKPIs) add(name string, val float64, unit string) {
	r.order = append(r.order, kpi{name: name, val: val, unit: unit})
}

// addFatal records a class-A correctness metric that must be 0.
func (r *runKPIs) addFatal(name string, val float64) {
	r.order = append(r.order, kpi{name: name, val: val, unit: "", fatal: true})
}

func (r *runKPIs) addDurPct(prefix string, p percentiles) {
	r.add(prefix+" p50", float64(p.p50)/float64(time.Millisecond), "ms")
	r.add(prefix+" p90", float64(p.p90)/float64(time.Millisecond), "ms")
	r.add(prefix+" p99", float64(p.p99)/float64(time.Millisecond), "ms")
	r.add(prefix+" p99.9", float64(p.p999)/float64(time.Millisecond), "ms")
}

// ─── Cross-run aggregation ──────────────────────────────────────────────────

type aggregator struct {
	order  []string // KPI names, first-seen order
	unit   map[string]string
	fatal  map[string]bool
	values map[string][]float64
}

func newAggregator() *aggregator {
	return &aggregator{
		unit:   map[string]string{},
		fatal:  map[string]bool{},
		values: map[string][]float64{},
	}
}

func (a *aggregator) addRun(r runKPIs) {
	for _, k := range r.order {
		if _, seen := a.values[k.name]; !seen {
			a.order = append(a.order, k.name)
		}
		a.unit[k.name] = k.unit
		a.fatal[k.name] = a.fatal[k.name] || k.fatal
		a.values[k.name] = append(a.values[k.name], k.val)
	}
}

func meanStddev(xs []float64) (mean, stddev float64) {
	if len(xs) == 0 {
		return 0, 0
	}
	var sum float64
	for _, x := range xs {
		sum += x
	}
	mean = sum / float64(len(xs))
	if len(xs) < 2 {
		return mean, 0
	}
	var ss float64
	for _, x := range xs {
		d := x - mean
		ss += d * d
	}
	return mean, math.Sqrt(ss / float64(len(xs)-1)) // sample stddev
}

// report prints the aggregate KPI table and returns false if any fatal
// (class-A correctness) KPI has a non-zero mean across runs.
func (a *aggregator) report(scenario string, runs int) bool {
	fmt.Printf("\n══════════════════════════════════════════════════════════════\n")
	fmt.Printf("  scenario: %s   (mean ± stddev over %d runs)\n", scenario, runs)
	fmt.Printf("══════════════════════════════════════════════════════════════\n")
	pass := true
	width := 0
	for _, name := range a.order {
		if len(name) > width {
			width = len(name)
		}
	}
	for _, name := range a.order {
		mean, sd := meanStddev(a.values[name])
		unit := a.unit[name]
		flag := ""
		if a.fatal[name] && mean != 0 {
			flag = "   ← CLASS-A VIOLATION"
			pass = false
		}
		fmt.Printf("  %-*s : %s ± %s %s%s\n", width, name,
			fmtNum(mean, unit), fmtNum(sd, ""), unit, flag)
	}
	verdict := "PASS — no correctness violations"
	if !pass {
		verdict = "FAIL — correctness violation detected (see class-A KPIs)"
	}
	fmt.Printf("──────────────────────────────────────────────────────────────\n")
	fmt.Printf("  verdict: %s\n", verdict)
	fmt.Printf("══════════════════════════════════════════════════════════════\n")
	return pass
}

func fmtNum(v float64, unit string) string {
	switch unit {
	case "ops/s":
		return fmt.Sprintf("%.0f", v)
	case "ms":
		return fmt.Sprintf("%.2f", v)
	case "%":
		return fmt.Sprintf("%.3f", v)
	default:
		if v == math.Trunc(v) {
			return fmt.Sprintf("%.0f", v)
		}
		return fmt.Sprintf("%.2f", v)
	}
}

func dividerLine() string { return strings.Repeat("─", 62) }
