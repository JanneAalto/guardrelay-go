package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)

type phaseSample struct {
	value      float64
	receivedAt time.Time
}

type smartPiRelay struct {
	cfg             *Config
	now             func() time.Time
	mu              sync.Mutex
	phases          [3]phaseSample
	energy          [3]float64
	energyReady     bool
	energyPublishMu sync.Mutex
	checkpointMu    sync.Mutex
	savedEnergy     [3]float64
	checkpointSaved bool
}

func newSmartPiRelay(cfg *Config) *smartPiRelay {
	return &smartPiRelay{cfg: cfg, now: time.Now}
}

func (r *smartPiRelay) recordPower(ctx context.Context, phase int, value float64, receivedAt time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	previous := r.phases[phase]
	r.phases[phase] = phaseSample{value: value, receivedAt: receivedAt}
	elapsed := receivedAt.Sub(previous.receivedAt)
	// Acknowledge the initial (restored or zero) snapshot before accumulation.
	if !r.energyReady || previous.receivedAt.IsZero() || elapsed <= 0 || elapsed >= r.cfg.PhaseFreshness {
		return nil
	}
	// Divide time first so a finite kWh increment does not overflow in watts-seconds.
	increment := math.Max(previous.value, 0) * (elapsed.Seconds() / 3600000)
	next := r.energy
	next[phase] += increment
	total := next[0] + next[1] + next[2]
	if math.IsNaN(total) || math.IsInf(total, 0) {
		return fmt.Errorf("SmartPi imported energy for phase %d would overflow; counters unchanged", phase+1)
	}
	r.energy = next
	return nil
}

func (r *smartPiRelay) resetPhases() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.phases = [3]phaseSample{}
}

func (r *smartPiRelay) totalPower(ctx context.Context) (float64, bool) {
	now := r.now()
	sum, ok := r.freshPowerSum(ctx, now)
	if !ok {
		return 0, false
	}
	if math.IsNaN(sum) || math.IsInf(sum, 0) {
		log.Printf("SmartPi power total is not finite; suppressing aggregate")
		return 0, false
	}
	return sum, true
}

func (r *smartPiRelay) freshPowerSum(ctx context.Context, now time.Time) (float64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ctx.Err() != nil {
		return 0, false
	}
	var sum float64
	for _, sample := range r.phases {
		age := now.Sub(sample.receivedAt)
		if sample.receivedAt.IsZero() || age < 0 || age >= r.cfg.PhaseFreshness {
			return 0, false
		}
		sum += sample.value
	}
	return sum, true
}
