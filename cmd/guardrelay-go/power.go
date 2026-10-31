package main

import (
	"context"
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
	cfg    *Config
	now    func() time.Time
	mu     sync.Mutex
	phases [3]phaseSample
}

func newSmartPiRelay(cfg *Config) *smartPiRelay {
	return &smartPiRelay{cfg: cfg, now: time.Now}
}

func (r *smartPiRelay) recordPower(ctx context.Context, phase int, value float64, receivedAt time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ctx.Err() == nil {
		r.phases[phase] = phaseSample{value: value, receivedAt: receivedAt}
	}
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
