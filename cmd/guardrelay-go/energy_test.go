package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

func assertEnergy(t *testing.T, got, want float64) {
	t.Helper()
	if math.IsNaN(got) || math.IsInf(got, 0) || math.Abs(got-want) > 1e-15 {
		t.Fatalf("energy = %.17g, want %.17g kWh", got, want)
	}
}

func TestEnergyIntegratesPreviousPositivePhasePower(t *testing.T) {
	h := newPowerHarness()
	h.send("P1", "1000", false)
	assertEnergy(t, h.relay.energy[0], 0)
	h.now = h.now.Add(2 * time.Second)
	h.send("P1", "2000", false)
	assertEnergy(t, h.relay.energy[0], 2.0/3600)
	h.now = h.now.Add(time.Second)
	h.send("P1", "0", false)
	assertEnergy(t, h.relay.energy[0], 4.0/3600)
	h.now = h.now.Add(time.Second)
	h.send("P1", "-1000", false)
	h.now = h.now.Add(time.Second)
	h.send("P1", "1000", false)
	assertEnergy(t, h.relay.energy[0], 4.0/3600)
	h.now = h.now.Add(time.Second)
	h.send("P1", "1000", false)
	assertEnergy(t, h.relay.energy[0], 5.0/3600)
}

func TestEnergyIntegratesPreviousNegativePhasePowerAsExport(t *testing.T) {
	h := newPowerHarness()
	h.send("P1", "-1000", false)
	assertEnergy(t, h.relay.exportedEnergy[0], 0)
	h.now = h.now.Add(2 * time.Second)
	h.send("P1", "-2000", false)
	assertEnergy(t, h.relay.exportedEnergy[0], 2.0/3600)
	assertEnergy(t, h.relay.energy[0], 0)
	h.now = h.now.Add(time.Second)
	h.send("P1", "0", false)
	assertEnergy(t, h.relay.exportedEnergy[0], 4.0/3600)
	h.now = h.now.Add(time.Second)
	h.send("P1", "1000", false)
	h.now = h.now.Add(time.Second)
	h.send("P1", "-1000", false)
	assertEnergy(t, h.relay.energy[0], 1.0/3600)
	assertEnergy(t, h.relay.exportedEnergy[0], 4.0/3600)
}

func TestEnergyUsesSumOfImportedPhasesNotNetPower(t *testing.T) {
	h := newPowerHarness()
	h.send("P1", "1000", false)
	h.send("P2", "-500", false)
	h.send("P3", "2000", false)
	h.now = h.now.Add(2 * time.Second)
	h.send("P1", "0", false)
	h.send("P2", "0", false)
	h.send("P3", "0", false)
	assertEnergy(t, h.relay.energy[0], 2.0/3600)
	assertEnergy(t, h.relay.energy[1], 0)
	assertEnergy(t, h.relay.energy[2], 4.0/3600)
	assertEnergy(t, h.relay.exportedEnergy[1], 1.0/3600)
	var importedTotal, exportedTotal float64
	foundImportedTotal, foundExportedTotal := false, false
	for _, packet := range h.publisher.packets {
		if !strings.HasSuffix(packet.Topic, "/energy_total") &&
			!strings.HasSuffix(packet.Topic, "/energy_export_total") {
			continue
		}
		value, err := strconv.ParseFloat(string(packet.Payload), 64)
		if err != nil {
			t.Fatalf("invalid total energy state: %+v", packet)
		}
		if strings.HasSuffix(packet.Topic, "/energy_export_total") {
			exportedTotal, foundExportedTotal = value, true
		} else {
			importedTotal, foundImportedTotal = value, true
		}
	}
	if !foundImportedTotal || !foundExportedTotal {
		t.Fatal("imported or exported energy total was not published")
	}
	assertEnergy(t, importedTotal, 6.0/3600)
	assertEnergy(t, exportedTotal, 1.0/3600)
}

func TestEnergyIntervalBoundariesAndRebaselining(t *testing.T) {
	for _, tt := range []struct {
		name       string
		gap, limit time.Duration
		accumulate bool
	}{
		{"zero", 0, 3 * time.Second, false},
		{"backwards", -time.Second, 3 * time.Second, false},
		{"fresh", 3*time.Second - time.Nanosecond, 3 * time.Second, true},
		{"boundary", 3 * time.Second, 3 * time.Second, false},
		{"stale", 4 * time.Second, 3 * time.Second, false},
		{"custom", 5 * time.Second, 10 * time.Second, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newPowerHarness()
			h.relay.cfg.PhaseFreshness = tt.limit
			h.send("P1", "1000", false)
			h.now = h.now.Add(tt.gap)
			h.send("P1", "1000", false)
			want := 0.0
			if tt.accumulate {
				want = tt.gap.Seconds() / 3600
			}
			assertEnergy(t, h.relay.energy[0], want)
			h.now = h.now.Add(time.Second)
			h.send("P1", "1000", false)
			assertEnergy(t, h.relay.energy[0], want+1.0/3600)
		})
	}
}

func TestEnergyRetainedReplayCannotAccumulateOrChangeBaseline(t *testing.T) {
	h := newPowerHarness()
	h.send("P1", "1000", true)
	if h.relay.energyReady || len(h.publisher.packets) != 1 {
		t.Fatal("retained power initialized energy or emitted counters")
	}
	h.send("P1", "1000", false)
	baseline := h.relay.phases[0]
	count := len(h.publisher.packets)
	h.now = h.now.Add(time.Second)
	h.send("P1", "2000", true)
	if h.relay.phases[0] != baseline || len(h.publisher.packets) != count+1 {
		t.Fatal("retained replay altered baseline or emitted an energy snapshot")
	}
	h.now = h.now.Add(time.Second)
	h.send("P1", "1000", false)
	assertEnergy(t, h.relay.energy[0], 2.0/3600)
}

func TestEnergyReconnectPreservesCountersButNotIntegrationBaselines(t *testing.T) {
	h := newPowerHarness()
	h.send("P1", "1000", false)
	h.now = h.now.Add(2 * time.Second)
	h.send("P1", "1000", false)
	ctx, cancel := context.WithCancel(context.Background())
	l := newMQTTLifecycle(ctx, h.relay.cfg)
	l.relay = h.relay
	defer func() { cancel(); await(t, l.done) }()
	first := newFakeMQTT()
	l.connectionUp(first)
	await(t, first.online)
	assertEnergy(t, h.relay.energy[0], 2.0/3600)
	l.connectionDown()
	h.now = h.now.Add(time.Hour)
	h.send("P1", "1000", false)
	assertEnergy(t, h.relay.energy[0], 2.0/3600)
	h.now = h.now.Add(2 * time.Second)
	h.send("P1", "1000", false)
	assertEnergy(t, h.relay.energy[0], 4.0/3600)
	second := newFakeMQTT()
	l.connectionUp(second)
	await(t, second.online)
	ops, _ := second.snapshot()
	var republished bool
	for _, op := range ops {
		topic, payload, _ := strings.Cut(op, "=")
		if topic == h.relay.cfg.MQTTBaseTopic+"/state/energy_l1" {
			value, err := strconv.ParseFloat(payload, 64)
			if err != nil {
				t.Fatal(err)
			}
			assertEnergy(t, value, 4.0/3600)
			republished = true
		}
	}
	if !republished || h.relay.phases != [3]phaseSample{} {
		t.Fatal("reconnect failed to publish current counters or invalidate baselines")
	}
}

func TestEnergyStartupAndRestartPublishZeroBeforeAccumulation(t *testing.T) {
	cfg := testConfig()
	cfg.MQTTBaseTopic = "custom/relay"
	for run := 0; run < 2; run++ {
		relay := newSmartPiRelay(cfg)
		publisher := newFakeMQTT()
		if err := initializeMQTT(context.Background(), cfg, publisher, relay); err != nil {
			t.Fatal(err)
		}
		ops, _ := publisher.snapshot()
		for i, key := range []string{
			"energy_l1", "energy_l2", "energy_l3", "energy_total",
			"energy_export_l1", "energy_export_l2", "energy_export_l3", "energy_export_total",
		} {
			if ops[i+1] != cfg.MQTTBaseTopic+"/state/"+key+"=0" {
				t.Fatalf("run %d did not publish startup zero: %v", run, ops[:6])
			}
		}
		if relay.energy != [3]float64{} || relay.exportedEnergy != [3]float64{} || !relay.energyReady {
			t.Fatal("new process restored old counters or failed to initialize energy")
		}
		now := time.Unix(1000, 0)
		if err := relay.recordPower(context.Background(), 0, 1000, now); err != nil {
			t.Fatal(err)
		}
		if err := relay.recordPower(context.Background(), 0, 1000, now.Add(2*time.Second)); err != nil {
			t.Fatal(err)
		}
		assertEnergy(t, relay.energy[0], 2.0/3600)
	}
}

func TestEnergySnapshotsAreRetainedFiniteAndConsistent(t *testing.T) {
	h := newPowerHarness()
	h.relay.cfg.MQTTBaseTopic = "custom/relay"
	var previous [8]float64
	for cycle := 0; cycle < 10; cycle++ {
		for phase := 1; phase <= 3; phase++ {
			h.now = h.now.Add(time.Millisecond)
			h.send(fmt.Sprintf("P%d", phase), fmt.Sprint(phase*1000), false)
			packets := h.publisher.packets[len(h.publisher.packets)-8:]
			var values [8]float64
			for i, packet := range packets {
				key := []string{
					"energy_l1", "energy_l2", "energy_l3", "energy_total",
					"energy_export_l1", "energy_export_l2", "energy_export_l3", "energy_export_total",
				}[i]
				value, err := strconv.ParseFloat(string(packet.Payload), 64)
				if err != nil || math.IsNaN(value) || math.IsInf(value, 0) ||
					packet.Topic != "custom/relay/state/"+key || !packet.Retain || packet.QoS != 1 ||
					value < previous[i] {
					t.Fatalf("invalid/nonmonotonic energy snapshot: %+v", packet)
				}
				values[i] = value
			}
			if values[3] != values[0]+values[1]+values[2] ||
				values[7] != values[4]+values[5]+values[6] {
				t.Fatalf("energy totals differ from phase counters: %v", values)
			}
			previous = values
		}
	}
}

func TestEnergyDoesNotAccumulateUntilStartupZeroIsAcknowledged(t *testing.T) {
	h := newPowerHarness()
	publisher := newFakeMQTT()
	publisher.failPrefix = h.relay.cfg.MQTTBaseTopic + "/state/energy_l2"
	if err := h.relay.publishEnergy(context.Background(), publisher); err == nil {
		t.Fatal("energy publication failure ignored")
	}
	now := h.now
	h.relay.recordPower(context.Background(), 0, 1000, now)
	h.relay.recordPower(context.Background(), 0, 1000, now.Add(time.Second))
	if h.relay.energyReady || h.relay.energy != [3]float64{} ||
		h.relay.exportedEnergy != [3]float64{} {
		t.Fatal("accumulated before startup zero snapshot was acknowledged")
	}
	publisher.failPrefix = ""
	if err := h.relay.publishEnergy(context.Background(), publisher); err != nil {
		t.Fatal(err)
	}
	h.relay.recordPower(context.Background(), 0, 1000, now.Add(2*time.Second))
	assertEnergy(t, h.relay.energy[0], 1.0/3600)
}

func TestEnergyLargeFiniteIncrementAndCounterOverflow(t *testing.T) {
	h := newPowerHarness()
	h.send("P1", "1e308", false)
	h.now = h.now.Add(2 * time.Second)
	h.send("P1", "1e308", false)
	want := 1e308 * (2.0 / 3600000)
	if math.IsInf(h.relay.energy[0], 0) || math.Abs(h.relay.energy[0]/want-1) > 1e-14 {
		t.Fatal("a finite increment overflowed during intermediate multiplication")
	}
	for _, exported := range []bool{false, true} {
		for _, phase := range []int{0, 1} {
			t.Run(fmt.Sprintf("exported=%v/phase=%d", exported, phase+1), func(t *testing.T) {
				h := newPowerHarness()
				h.relay.energyReady = true
				power := 1e308
				wantImported, wantExported := [3]float64{}, [3]float64{}
				if exported {
					h.relay.exportedEnergy[0] = math.MaxFloat64
					wantExported[0] = math.MaxFloat64
					power = -power
				} else {
					h.relay.energy[0] = math.MaxFloat64
					wantImported[0] = math.MaxFloat64
				}
				h.relay.recordPower(context.Background(), phase, power, h.now)
				err := h.relay.recordPower(context.Background(), phase, power, h.now.Add(2*time.Second))
				if err == nil || h.relay.energy != wantImported || h.relay.exportedEnergy != wantExported {
					t.Fatal("phase or summed counter overflow was not rejected atomically")
				}
			})
		}
	}
}

func TestEnergyCalculationAndPublicationErrorsAreLogged(t *testing.T) {
	h := newPowerHarness()
	h.relay.energyReady = true
	h.relay.energy[0] = math.MaxFloat64
	output := captureSensorLogs(t)
	h.send("P1", "1e308", false)
	h.now = h.now.Add(2 * time.Second)
	h.send("P1", "1e308", false)
	if !strings.Contains(output.String(), "would overflow") {
		t.Fatal("energy calculation error was not logged")
	}
	output.Reset()
	publisher := newFakeMQTT()
	publisher.failPrefix = h.relay.cfg.MQTTBaseTopic + "/state/energy_"
	h.relay.handleSmartPi(&paho.Publish{Topic: "smartpi/P1", Payload: []byte("1000")},
		context.Background(), publisher)
	if !strings.Contains(output.String(), "energy_l1 rejected") {
		t.Fatal("energy publication rejection was not logged")
	}
}

type gatedEnergyPublisher struct {
	mu      sync.Mutex
	packets []*paho.Publish
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *gatedEnergyPublisher) Publish(ctx context.Context, packet *paho.Publish) (*paho.PublishResponse, error) {
	p.once.Do(func() {
		close(p.started)
		select {
		case <-p.release:
		case <-ctx.Done():
		}
	})
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.packets = append(p.packets, packet)
	p.mu.Unlock()
	return &paho.PublishResponse{}, nil
}

func TestEnergySnapshotPublicationIsSerializedWithoutBlockingReset(t *testing.T) {
	h := newPowerHarness()
	h.send("P1", "1000", false)
	publisher := &gatedEnergyPublisher{started: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	unblock := func() { release.Do(func() { close(publisher.release) }) }
	defer unblock()
	errs := make(chan error, 2)
	go func() { errs <- h.relay.publishEnergy(context.Background(), publisher) }()
	await(t, publisher.started)
	h.relay.recordPower(context.Background(), 0, 1000, h.now.Add(2*time.Second))
	go func() { errs <- h.relay.publishEnergy(context.Background(), publisher) }()
	resetDone := make(chan struct{})
	go func() { h.relay.resetPhases(); close(resetDone) }()
	await(t, resetDone)
	unblock()
	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("energy publication did not finish")
		}
	}
	if len(publisher.packets) != 16 {
		t.Fatalf("incomplete snapshots: %d packets", len(publisher.packets))
	}
	for i, packet := range publisher.packets {
		value, err := strconv.ParseFloat(string(packet.Payload), 64)
		if err != nil {
			t.Fatal(err)
		}
		want := 0.0
		if i == 8 || i == 11 {
			want = 2.0 / 3600
		}
		assertEnergy(t, value, want)
	}
}

func TestEnergyCountersDoNotCreateStateFiles(t *testing.T) {
	directory := t.TempDir()
	t.Chdir(directory)
	h := newPowerHarness()
	h.send("P1", "1000", false)
	h.now = h.now.Add(2 * time.Second)
	h.send("P1", "1000", false)
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("energy calculation created local state files: %v, %v", entries, err)
	}
}
