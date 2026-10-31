package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"time"
)

const (
	energyCheckpointInterval = 10 * time.Minute
	maxEnergyCheckpointSize  = 1 << 20
)

type energyCheckpoint struct {
	Version       int        `json:"version"`
	DeviceID      string     `json:"device_id"`
	SmartPiPrefix string     `json:"smartpi_prefix"`
	BaseTopic     string     `json:"base_topic"`
	Phases        []*float64 `json:"imported_kwh"`
}

func (r *smartPiRelay) restoreCheckpoint() error {
	if r.cfg.EnergyStateFile == "" {
		return nil
	}
	r.checkpointMu.Lock()
	defer r.checkpointMu.Unlock()
	f, err := os.Open(r.cfg.EnergyStateFile)
	if errors.Is(err, os.ErrNotExist) {
		log.Printf("Energy checkpoint %s is missing; starting at zero", r.cfg.EnergyStateFile)
		return nil
	}
	if err != nil {
		return fmt.Errorf("open energy checkpoint: %w", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(f, maxEnergyCheckpointSize+1))
	if err := errors.Join(readErr, f.Close()); err != nil {
		return fmt.Errorf("read energy checkpoint: %w", err)
	}
	if len(data) > maxEnergyCheckpointSize {
		return fmt.Errorf("energy checkpoint exceeds %d bytes", maxEnergyCheckpointSize)
	}
	var state energyCheckpoint
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return fmt.Errorf("decode energy checkpoint: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("energy checkpoint contains trailing data")
	}
	if state.Version != 1 || state.DeviceID != r.cfg.HADeviceID ||
		state.SmartPiPrefix != r.cfg.SmartPiPrefix || state.BaseTopic != r.cfg.MQTTBaseTopic ||
		len(state.Phases) != 3 {
		return fmt.Errorf("energy checkpoint version, meter identity, or phase count does not match configuration")
	}
	var values [3]float64
	for i, value := range state.Phases {
		if value == nil {
			return fmt.Errorf("energy checkpoint contains null imported kWh")
		}
		values[i] = *value
	}
	if err := validateCheckpointEnergy(values); err != nil {
		return err
	}
	r.mu.Lock()
	r.energy = values
	r.mu.Unlock()
	r.savedEnergy, r.checkpointSaved = values, true
	log.Printf("Restored imported energy from %s", r.cfg.EnergyStateFile)
	return nil
}

func (r *smartPiRelay) saveCheckpoint() error {
	if r.cfg.EnergyStateFile == "" {
		return nil
	}
	r.checkpointMu.Lock()
	defer r.checkpointMu.Unlock()
	r.mu.Lock()
	values := r.energy
	r.mu.Unlock()
	if r.checkpointSaved && values == r.savedEnergy {
		return nil
	}
	if err := validateCheckpointEnergy(values); err != nil {
		return err
	}
	state := energyCheckpoint{
		Version: 1, DeviceID: r.cfg.HADeviceID, SmartPiPrefix: r.cfg.SmartPiPrefix,
		BaseTopic: r.cfg.MQTTBaseTopic, Phases: []*float64{&values[0], &values[1], &values[2]},
	}
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode energy checkpoint: %w", err)
	}
	path := r.cfg.EnergyStateFile
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create energy checkpoint directory: %w", err)
	}
	f, err := os.CreateTemp(dir, ".energy-*.tmp")
	if err != nil {
		return fmt.Errorf("create energy checkpoint: %w", err)
	}
	renamed := false
	defer func() {
		if !renamed {
			if err := os.Remove(f.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
				log.Printf("Remove incomplete energy checkpoint: %v", err)
			}
		}
	}()
	if err := f.Chmod(0600); err != nil {
		return fmt.Errorf("set energy checkpoint permissions: %w", errors.Join(err, f.Close()))
	}
	_, writeErr := f.Write(append(data, '\n'))
	syncErr := f.Sync()
	if err := errors.Join(writeErr, syncErr, f.Close()); err != nil {
		return fmt.Errorf("write energy checkpoint: %w", err)
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return fmt.Errorf("replace energy checkpoint: %w", err)
	}
	renamed = true
	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open energy checkpoint directory: %w", err)
	}
	if err := errors.Join(directory.Sync(), directory.Close()); err != nil {
		return fmt.Errorf("sync energy checkpoint directory: %w", err)
	}
	r.savedEnergy, r.checkpointSaved = values, true
	return nil
}

func validateCheckpointEnergy(values [3]float64) error {
	var total float64
	for _, value := range values {
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("energy checkpoint contains invalid imported kWh")
		}
		total += value
	}
	if math.IsInf(total, 0) {
		return fmt.Errorf("energy checkpoint total overflows")
	}
	return nil
}

func (r *smartPiRelay) checkpointEvery(ctx context.Context) error {
	if r.cfg.EnergyStateFile == "" {
		<-ctx.Done()
		return nil
	}
	ticker := time.NewTicker(energyCheckpointInterval)
	defer ticker.Stop()
	return r.checkpointOnTicks(ctx, ticker.C)
}

func (r *smartPiRelay) checkpointOnTicks(ctx context.Context, ticks <-chan time.Time) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticks:
			if err := r.saveCheckpoint(); err != nil {
				return fmt.Errorf("periodic energy checkpoint failed: %w", err)
			}
		}
	}
}
