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
	cfg                 *Config
	now                 func() time.Time
	mu                  sync.Mutex
	phases              [3]phaseSample
	measurements        map[string]time.Time
	energy              [3]float64
	exportedEnergy      [3]float64
	energyReady         bool
	energyPublishMu     sync.Mutex
	healthMu            sync.Mutex
	healthKnown         bool
	healthProblem       bool
	checkpointMu        sync.Mutex
	savedEnergy         [3]float64
	savedExportedEnergy [3]float64
	checkpointSaved     bool
}

func newSmartPiRelay(cfg *Config) *smartPiRelay {
	return &smartPiRelay{cfg: cfg, now: time.Now, measurements: make(map[string]time.Time)}
}

func (r *smartPiRelay) recordMeasurement(ctx context.Context, input string, receivedAt time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ctx.Err() != nil {
		return false
	}
	r.measurements[input] = receivedAt
	return true
}

func (r *smartPiRelay) measurementProblem(now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, definition := range sensorDefinitions {
		if definition.input == "" {
			continue
		}
		receivedAt := r.measurements[definition.input]
		age := now.Sub(receivedAt)
		if receivedAt.IsZero() || age < 0 || age >= r.cfg.PhaseFreshness {
			return true
		}
	}
	return false
}

func (r *smartPiRelay) publishMeasurementProblem(ctx context.Context, client mqttPublisher) error {
	r.healthMu.Lock()
	defer r.healthMu.Unlock()
	problem := r.measurementProblem(r.now())
	if r.healthKnown && problem == r.healthProblem {
		return nil
	}
	payload := "OFF"
	if problem {
		payload = "ON"
	}
	if err := publish(ctx, client, r.cfg.MQTTBaseTopic+"/state/"+measurementProblemKey,
		[]byte(payload), true); err != nil {
		return err
	}
	r.healthKnown, r.healthProblem = true, problem
	return nil
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
	hours := elapsed.Seconds() / 3600000
	nextImported, nextExported := r.energy, r.exportedEnergy
	nextImported[phase] += math.Max(previous.value, 0) * hours
	nextExported[phase] += math.Max(-previous.value, 0) * hours
	if !finiteEnergyTotal(nextImported) || !finiteEnergyTotal(nextExported) {
		return fmt.Errorf("SmartPi energy for phase %d would overflow; counters unchanged", phase+1)
	}
	r.energy, r.exportedEnergy = nextImported, nextExported
	return nil
}

func finiteEnergyTotal(values [3]float64) bool {
	var total float64
	for _, value := range values {
		total += value
	}
	return !math.IsNaN(total) && !math.IsInf(total, 0)
}

func (r *smartPiRelay) resetPhases() {
	r.mu.Lock()
	r.phases = [3]phaseSample{}
	clear(r.measurements)
	r.mu.Unlock()
	r.healthMu.Lock()
	r.healthKnown = false
	r.healthMu.Unlock()
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
