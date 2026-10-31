package main

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

func checkpointRelay(t *testing.T) *smartPiRelay {
	t.Helper()
	cfg := testConfig()
	cfg.EnergyStateFile = filepath.Join(t.TempDir(), "state", "energy.json")
	return newSmartPiRelay(cfg)
}

func readCheckpoint(t *testing.T, path string) energyCheckpoint {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var state energyCheckpoint
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	return state
}

func TestCheckpointRoundTripAndUnchangedWrites(t *testing.T) {
	r := checkpointRelay(t)
	if err := r.restoreCheckpoint(); err != nil || r.energy != [3]float64{} {
		t.Fatalf("missing checkpoint should start at zero: %v", err)
	}
	r.energy = [3]float64{1.2345678901234567, 2, 3}
	r.exportedEnergy = [3]float64{4, 5, 6}
	if err := r.saveCheckpoint(); err != nil {
		t.Fatal(err)
	}
	path := r.cfg.EnergyStateFile
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("checkpoint permissions: %v", err)
	}
	info, err = os.Stat(filepath.Dir(path))
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatalf("directory permissions: %v", err)
	}
	old := time.Unix(1000, 0)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	if err := r.saveCheckpoint(); err != nil {
		t.Fatal(err)
	}
	info, err = os.Stat(path)
	if err != nil || !info.ModTime().Equal(old) {
		t.Fatalf("unchanged counters rewrote checkpoint: %v", err)
	}
	restarted := newSmartPiRelay(r.cfg)
	if err := restarted.restoreCheckpoint(); err != nil {
		t.Fatal(err)
	}
	if restarted.energy != r.energy || restarted.exportedEnergy != r.exportedEnergy ||
		restarted.phases != [3]phaseSample{} || restarted.energyReady {
		t.Fatal("restore lost precision or restored baselines/readiness")
	}
	restarted.now = func() time.Time { return old }
	publisher := &sensorPublisher{}
	if err := restarted.publishEnergy(context.Background(), publisher); err != nil {
		t.Fatal(err)
	}
	restarted.handleSmartPi(&paho.Publish{
		Topic: r.cfg.SmartPiPrefix + "/P1", Payload: []byte("1000"),
	}, context.Background(), publisher)
	if restarted.energy != r.energy {
		t.Fatal("first live reading backfilled energy after restoration")
	}
}

func TestCheckpointMigratesImportedOnlyVersionOne(t *testing.T) {
	r := checkpointRelay(t)
	state := map[string]any{
		"version": 1, "device_id": r.cfg.HADeviceID, "smartpi_prefix": r.cfg.SmartPiPrefix,
		"base_topic": r.cfg.MQTTBaseTopic, "imported_kwh": []float64{1, 2, 3},
	}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(r.cfg.EnergyStateFile), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(r.cfg.EnergyStateFile, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := r.restoreCheckpoint(); err != nil {
		t.Fatal(err)
	}
	if r.energy != [3]float64{1, 2, 3} || r.exportedEnergy != [3]float64{} {
		t.Fatalf("version 1 counters were not migrated: imported=%v exported=%v", r.energy, r.exportedEnergy)
	}
	if err := r.saveCheckpoint(); err != nil {
		t.Fatal(err)
	}
	migrated := readCheckpoint(t, r.cfg.EnergyStateFile)
	if migrated.Version != 2 || len(migrated.ExportedPhases) != 3 ||
		*migrated.ExportedPhases[0] != 0 || *migrated.ExportedPhases[1] != 0 ||
		*migrated.ExportedPhases[2] != 0 {
		t.Fatalf("version 1 checkpoint was not saved in the current format: %+v", migrated)
	}
}

func TestCheckpointRejectsInvalidExportedCounters(t *testing.T) {
	for _, exported := range []any{
		[]float64{-1, 0, 0},
		[]float64{math.MaxFloat64, math.MaxFloat64, 0},
		[]any{nil, 0.0, 0.0},
	} {
		r := checkpointRelay(t)
		state := map[string]any{
			"version": 2, "device_id": r.cfg.HADeviceID, "smartpi_prefix": r.cfg.SmartPiPrefix,
			"base_topic": r.cfg.MQTTBaseTopic, "imported_kwh": []float64{1, 2, 3},
			"exported_kwh": exported,
		}
		data, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(r.cfg.EnergyStateFile), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(r.cfg.EnergyStateFile, data, 0600); err != nil {
			t.Fatal(err)
		}
		r.energy, r.exportedEnergy = [3]float64{9, 9, 9}, [3]float64{8, 8, 8}
		if err := r.restoreCheckpoint(); err == nil {
			t.Fatalf("invalid exported checkpoint was accepted: %s", data)
		}
		if r.energy != [3]float64{9, 9, 9} || r.exportedEnergy != [3]float64{8, 8, 8} {
			t.Fatal("invalid exported checkpoint reset counters")
		}
	}
}

func TestCheckpointRejectsInvalidStateWithoutResetting(t *testing.T) {
	for _, change := range []func(map[string]any){
		func(s map[string]any) { s["version"] = 2 },
		func(s map[string]any) { delete(s, "version") },
		func(s map[string]any) { s["device_id"] = "different" },
		func(s map[string]any) { s["smartpi_prefix"] = "other/smartpi" },
		func(s map[string]any) { s["base_topic"] = "other/relay" },
		func(s map[string]any) { s["imported_kwh"] = []float64{1, 2} },
		func(s map[string]any) { s["imported_kwh"] = []float64{1, 2, 3, 4} },
		func(s map[string]any) { s["imported_kwh"] = []float64{-1, 2, 3} },
		func(s map[string]any) { s["imported_kwh"] = []any{nil, 2, 3} },
		func(s map[string]any) { s["imported_kwh"] = []float64{math.MaxFloat64, math.MaxFloat64, 0} },
		func(s map[string]any) { s["version"] = 2 },
		func(s map[string]any) { s["unexpected"] = true },
	} {
		r := checkpointRelay(t)
		state := map[string]any{
			"version": 1, "device_id": r.cfg.HADeviceID, "smartpi_prefix": r.cfg.SmartPiPrefix,
			"base_topic": r.cfg.MQTTBaseTopic, "imported_kwh": []float64{1, 2, 3},
		}
		change(state)
		data, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(r.cfg.EnergyStateFile), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(r.cfg.EnergyStateFile, data, 0600); err != nil {
			t.Fatal(err)
		}
		r.energy = [3]float64{9, 9, 9}
		if err := r.restoreCheckpoint(); err == nil || r.energy != [3]float64{9, 9, 9} {
			t.Fatalf("invalid checkpoint silently reset counters: %s, %v", data, err)
		}
	}
	for _, data := range []string{"", "{", "null", strings.Repeat(" ", maxEnergyCheckpointSize+1),
		`{"version":1} {}`, `{"imported_kwh":[NaN,0,0]}`} {
		r := checkpointRelay(t)
		if err := os.MkdirAll(filepath.Dir(r.cfg.EnergyStateFile), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(r.cfg.EnergyStateFile, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if err := r.restoreCheckpoint(); err == nil {
			t.Fatalf("accepted invalid JSON checkpoint %q", data)
		}
	}
}

func TestCheckpointFailedSavePreservesFileAndCleansTemporaryFile(t *testing.T) {
	r := checkpointRelay(t)
	r.energy[0] = 1
	if err := r.saveCheckpoint(); err != nil {
		t.Fatal(err)
	}
	r.energy[0] = math.Inf(1)
	if err := r.saveCheckpoint(); err == nil {
		t.Fatal("nonfinite counter was saved")
	}
	if got := readCheckpoint(t, r.cfg.EnergyStateFile); *got.Phases[0] != 1 {
		t.Fatal("failed save replaced the valid checkpoint")
	}
	r.energy[0] = 2
	r.cfg.EnergyStateFile = filepath.Join(filepath.Dir(r.cfg.EnergyStateFile), "directory")
	if err := os.Mkdir(r.cfg.EnergyStateFile, 0700); err != nil {
		t.Fatal(err)
	}
	if err := r.saveCheckpoint(); err == nil {
		t.Fatal("rename failure was hidden")
	}
	files, err := os.ReadDir(filepath.Dir(r.cfg.EnergyStateFile))
	if err != nil || len(files) != 2 {
		t.Fatalf("failed save left temporary files: %v, %v", files, err)
	}
}

func TestCheckpointPeriodicTimerAndFailure(t *testing.T) {
	if energyCheckpointInterval != 10*time.Minute {
		t.Fatal("checkpoint interval is not 10 minutes")
	}
	r := checkpointRelay(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ticks := make(chan time.Time)
	result := make(chan error, 1)
	go func() { result <- r.checkpointOnTicks(ctx, ticks) }()
	if _, err := os.Stat(r.cfg.EnergyStateFile); !os.IsNotExist(err) {
		t.Fatal("timer saved before a tick")
	}
	for i := 0; i < 2; i++ {
		select {
		case ticks <- time.Now():
		case <-time.After(2 * time.Second):
			t.Fatal("checkpoint timer did not accept tick")
		}
	}
	if got := readCheckpoint(t, r.cfg.EnergyStateFile); *got.Phases[0] != 0 {
		t.Fatal("periodic checkpoint did not save current counters")
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}

	r = checkpointRelay(t)
	if err := os.MkdirAll(r.cfg.EnergyStateFile, 0700); err != nil {
		t.Fatal(err)
	}
	ticks = make(chan time.Time, 1)
	ticks <- time.Now()
	result = make(chan error, 1)
	go func() { result <- r.checkpointOnTicks(context.Background(), ticks) }()
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "periodic energy checkpoint failed") {
			t.Fatalf("periodic failure was not surfaced: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("periodic checkpoint error did not stop worker")
	}
}

func TestCheckpointConcurrentUpdates(t *testing.T) {
	r := checkpointRelay(t)
	if err := r.saveCheckpoint(); err != nil {
		t.Fatal(err)
	}
	readCtx, stopReader := context.WithCancel(context.Background())
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for readCtx.Err() == nil {
			data, err := os.ReadFile(r.cfg.EnergyStateFile)
			if err != nil {
				t.Error(err)
				return
			}
			var state energyCheckpoint
			if err := json.Unmarshal(data, &state); err != nil ||
				len(state.Phases) != 3 || len(state.ExportedPhases) != 3 {
				t.Errorf("reader observed an incomplete checkpoint: %v", err)
				return
			}
		}
	}()
	defer func() { stopReader(); <-readerDone }()
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Go(func() {
			for j := 0; j < 10; j++ {
				r.mu.Lock()
				r.energy[0]++
				r.mu.Unlock()
				if err := r.saveCheckpoint(); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	workers.Wait()
	if got := readCheckpoint(t, r.cfg.EnergyStateFile); *got.Phases[0] != 40 {
		t.Fatal("concurrent save rolled back counters")
	}
}

func TestCheckpointSupportsLongValidIdentity(t *testing.T) {
	r := checkpointRelay(t)
	r.cfg.SmartPiPrefix = strings.Repeat("&", 65000)
	r.cfg.MQTTBaseTopic = strings.Repeat("<", 65000)
	if err := r.cfg.validate(); err != nil {
		t.Fatal(err)
	}
	r.energy[0] = 1
	if err := r.saveCheckpoint(); err != nil {
		t.Fatal(err)
	}
	restarted := newSmartPiRelay(r.cfg)
	if err := restarted.restoreCheckpoint(); err != nil ||
		restarted.energy != r.energy || restarted.exportedEnergy != r.exportedEnergy {
		t.Fatalf("valid identity cannot round-trip through checkpoint: %v", err)
	}
}

func TestRunRejectsCorruptCheckpoint(t *testing.T) {
	r := checkpointRelay(t)
	if err := os.MkdirAll(filepath.Dir(r.cfg.EnergyStateFile), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(r.cfg.EnergyStateFile, []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := run(ctx, r.cfg); err == nil || !strings.Contains(err.Error(), "decode energy checkpoint") {
		t.Fatalf("corrupt checkpoint did not prevent startup: %v", err)
	}
	if data, err := os.ReadFile(r.cfg.EnergyStateFile); err != nil || string(data) != "broken" {
		t.Fatal("startup replaced the corrupt checkpoint")
	}
}
