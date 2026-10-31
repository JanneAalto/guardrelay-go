package main

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
)

var wireEnergyKeys = []string{
	"energy_l1", "energy_l2", "energy_l3", "energy_total",
	"energy_export_l1", "energy_export_l2", "energy_export_l3", "energy_export_total",
}

type wireRelayRun struct {
	cancel   context.CancelFunc
	result   chan error
	finished bool
}

func startWireRelay(t *testing.T, cfg *Config) *wireRelayRun {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &wireRelayRun{cancel: cancel, result: make(chan error, 1)}
	go func() { r.result <- run(ctx, cfg) }()
	t.Cleanup(func() {
		cancel()
		if !r.finished {
			select {
			case err := <-r.result:
				if err != nil {
					t.Logf("relay cleanup: %v", err)
				}
			case <-time.After(20 * time.Second):
				t.Error("relay did not stop during cleanup")
			}
		}
	})
	return r
}

type wireBroker struct {
	t             *testing.T
	cfg           *Config
	conn          net.Conn
	states        map[string][]string
	discovery     map[string]SensorConfig
	deleted       map[string]bool
	availability  []string
	energy        [8]float64
	snapshots     [][8]float64
	nextEnergy    int
	subscribed    bool
	rejectCleanup bool
}

func acceptWireBroker(t *testing.T, listener *net.TCPListener, cfg *Config) *wireBroker {
	t.Helper()
	if err := listener.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatal(err)
	}
	conn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return &wireBroker{
		t: t, cfg: cfg, conn: conn,
		states: make(map[string][]string), discovery: make(map[string]SensorConfig),
		deleted: make(map[string]bool),
	}
}

func (b *wireBroker) write(packet interface {
	WriteTo(io.Writer) (int64, error)
}) {
	b.t.Helper()
	if _, err := packet.WriteTo(b.conn); err != nil {
		b.t.Fatal(err)
	}
}

func (b *wireBroker) read() *packets.ControlPacket {
	b.t.Helper()
	if err := b.conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		b.t.Fatal(err)
	}
	packet, err := packets.ReadPacket(b.conn)
	if err != nil {
		b.t.Fatal(err)
	}
	switch p := packet.Content.(type) {
	case *packets.Connect:
		if p.ProtocolVersion != 5 || p.ClientID != b.cfg.MQTTClientID ||
			!p.WillFlag || !p.WillRetain || p.WillQOS != 1 ||
			p.WillTopic != b.cfg.MQTTBaseTopic+"/status" || string(p.WillMessage) != "offline" {
			b.t.Fatalf("incorrect CONNECT/will: %+v", p)
		}
		b.write(packets.NewControlPacket(packets.CONNACK))
	case *packets.Subscribe:
		if b.subscribed || len(b.snapshots) != 1 || len(b.availability) != 1 ||
			len(p.Subscriptions) != 1 || p.Subscriptions[0].Topic != b.cfg.SmartPiPrefix+"/#" ||
			p.Subscriptions[0].QoS != 1 {
			b.t.Fatalf("subscription preceded startup energy or has incorrect shape: %+v", p)
		}
		b.subscribed = true
		response := packets.NewControlPacket(packets.SUBACK)
		response.Content.(*packets.Suback).PacketID = p.PacketID
		response.Content.(*packets.Suback).Reasons = []byte{1}
		b.write(response)
	case *packets.Publish:
		if p.QoS != 1 || p.PacketID == 0 {
			b.t.Fatalf("relay output does not use acknowledged QoS 1: %+v", p)
		}
		b.observe(p)
		response := packets.NewControlPacket(packets.PUBACK)
		ack := response.Content.(*packets.Puback)
		ack.PacketID = p.PacketID
		if b.rejectCleanup && len(p.Payload) == 0 {
			ack.ReasonCode = 0x87
		}
		b.write(response)
	case *packets.Pingreq:
		b.write(packets.NewControlPacket(packets.PINGRESP))
	case *packets.Disconnect:
		if p.ReasonCode != 0x04 {
			b.t.Fatal("DISCONNECT did not request offline will publication")
		}
	default:
		b.t.Fatalf("unexpected relay packet: %T", p)
	}
	return packet
}

func (b *wireBroker) observe(p *packets.Publish) {
	b.t.Helper()
	switch {
	case p.Topic == b.cfg.MQTTBaseTopic+"/status":
		state := string(p.Payload)
		if !p.Retain || state != "online" && state != "offline" {
			b.t.Fatalf("invalid availability: %+v", p)
		}
		if len(b.availability) == 0 && state != "offline" {
			b.t.Fatal("initial availability was not offline")
		}
		if state == "online" {
			if !b.subscribed || len(b.discovery) != 26 || len(b.deleted) != 5 {
				b.t.Fatal("online preceded subscription or complete discovery migration")
			}
		}
		b.availability = append(b.availability, state)
	case strings.HasPrefix(p.Topic, b.cfg.MQTTBaseTopic+"/state/"):
		key := strings.TrimPrefix(p.Topic, b.cfg.MQTTBaseTopic+"/state/")
		valid := key == "power_total" || key == measurementProblemKey
		for _, measurement := range measurementCases {
			valid = valid || key == measurement.key
		}
		for _, energyKey := range wireEnergyKeys {
			valid = valid || key == energyKey
		}
		if !valid {
			b.t.Fatalf("unexpected state key: %s", key)
		}
		b.states[key] = append(b.states[key], string(p.Payload))
		if key == measurementProblemKey {
			if !p.Retain || string(p.Payload) != "ON" && string(p.Payload) != "OFF" {
				b.t.Fatalf("invalid retained measurement freshness state: %+v", p)
			}
		} else if strings.HasPrefix(key, "energy_") {
			index := b.nextEnergy
			if !p.Retain || index >= len(wireEnergyKeys) || key != wireEnergyKeys[index] {
				b.t.Fatalf("incorrect retained energy snapshot ordering: %s", key)
			}
			value, err := strconv.ParseFloat(string(p.Payload), 64)
			if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < b.energy[index] {
				b.t.Fatalf("invalid/nonmonotonic energy value: %q", p.Payload)
			}
			b.energy[index] = value
			b.nextEnergy = (index + 1) % len(wireEnergyKeys)
			if b.nextEnergy == 0 {
				if b.energy[3] != b.energy[0]+b.energy[1]+b.energy[2] ||
					b.energy[7] != b.energy[4]+b.energy[5]+b.energy[6] {
					b.t.Fatalf("energy total differs from phase counters: %v", b.energy)
				}
				b.snapshots = append(b.snapshots, b.energy)
			}
		} else if p.Retain {
			b.t.Fatalf("instantaneous state is unexpectedly retained: %s", key)
		}
	case strings.HasPrefix(p.Topic, b.cfg.HADiscoveryPref+"/sensor/") ||
		strings.HasPrefix(p.Topic, b.cfg.HADiscoveryPref+"/binary_sensor/"):
		component := "sensor"
		if strings.HasPrefix(p.Topic, b.cfg.HADiscoveryPref+"/binary_sensor/") {
			component = "binary_sensor"
		}
		if !p.Retain {
			b.t.Fatal("discovery configuration is not retained")
		}
		if len(p.Payload) == 0 {
			valid := false
			for _, key := range []string{"current_total", "voltage_total", "phase_angle_l1", "phase_angle_l2", "phase_angle_l3"} {
				if p.Topic == b.cfg.HADiscoveryPref+"/sensor/"+b.cfg.HADeviceID+"_"+key+"/config" {
					valid = true
				}
			}
			if component != "sensor" || !valid || b.deleted[p.Topic] {
				b.t.Fatalf("incorrect/duplicate discovery deletion: %s", p.Topic)
			}
			b.deleted[p.Topic] = true
			return
		}
		var sc SensorConfig
		if err := json.Unmarshal(p.Payload, &sc); err != nil {
			b.t.Fatal(err)
		}
		key := strings.TrimPrefix(sc.StateTopic, b.cfg.MQTTBaseTopic+"/state/")
		unit, class, stateClass := "", "", "measurement"
		if key == measurementProblemKey {
			class, stateClass = "problem", ""
		}
		for _, measurement := range measurementCases {
			if key == measurement.key {
				unit, class = measurement.unit, measurement.deviceClass
			}
		}
		if key == "power_total" {
			unit, class = "W", "power"
		}
		for _, energyKey := range wireEnergyKeys {
			if key == energyKey {
				unit, class, stateClass = "kWh", "energy", "total_increasing"
			}
		}
		if class == "" || sc.StateTopic != b.cfg.MQTTBaseTopic+"/state/"+key ||
			sc.UniqueID != b.cfg.HADeviceID+"_"+key ||
			p.Topic != b.cfg.HADiscoveryPref+"/"+component+"/"+sc.UniqueID+"/config" ||
			sc.UnitOfMeasurement != unit || sc.DeviceClass != class || sc.StateClass != stateClass ||
			sc.AvailabilityTopic != b.cfg.MQTTBaseTopic+"/status" ||
			sc.PayloadAvailable != "online" || sc.PayloadNotAvailable != "offline" ||
			len(sc.Device.Identifiers) != 1 || sc.Device.Identifiers[0] != b.cfg.HADeviceID ||
			sc.Device.Name != b.cfg.HADeviceName || sc.Device.Manufacturer != "Charge Amps" ||
			sc.Device.Model != "Amp Guard" || sc.Name == "" {
			b.t.Fatalf("incorrect discovery contract: %s=%s", p.Topic, p.Payload)
		}
		if _, exists := b.discovery[key]; exists {
			b.t.Fatalf("duplicate discovery sensor: %s", key)
		}
		b.discovery[key] = sc
	default:
		b.t.Fatalf("unexpected publication topic: %s", p.Topic)
	}
}

func (b *wireBroker) ready(wantEnergy [8]float64) {
	b.t.Helper()
	for len(b.availability) < 2 {
		b.read()
	}
	if b.availability[1] != "online" || len(b.snapshots) != 1 || b.energy != wantEnergy ||
		len(b.states[measurementProblemKey]) != 1 || b.states[measurementProblemKey][0] != "ON" {
		b.t.Fatalf("incorrect ready/reset snapshot: availability=%v energy=%v, want %v",
			b.availability, b.energy, wantEnergy)
	}
}

func (b *wireBroker) awaitMeasurementHealth(want string) {
	b.t.Helper()
	for len(b.states[measurementProblemKey]) == 0 ||
		b.states[measurementProblemKey][len(b.states[measurementProblemKey])-1] != want {
		b.read()
	}
	if got := b.states[measurementProblemKey][len(b.states[measurementProblemKey])-1]; got != want {
		b.t.Fatalf("measurement health = %s, want %s", got, want)
	}
}

func (b *wireBroker) measurement(input, payload, key, state string, retained bool) {
	b.t.Helper()
	beforeStates, beforeSnapshots := len(b.states[key]), len(b.snapshots)
	b.write(&packets.Publish{
		Topic: b.cfg.SmartPiPrefix + "/" + input, Payload: []byte(payload),
		Retain: retained, Properties: &packets.Properties{},
	})
	wantSnapshot := strings.HasPrefix(input, "P") && !retained
	for len(b.states[key]) == beforeStates || wantSnapshot && len(b.snapshots) == beforeSnapshots {
		b.read()
	}
	if got := b.states[key]; len(got) != beforeStates+1 || got[len(got)-1] != state {
		b.t.Fatalf("misrouted measurement %s: %v, want %s", input, got, state)
	}
	if !wantSnapshot && len(b.snapshots) != beforeSnapshots {
		b.t.Fatal("retained or non-power measurement emitted an energy snapshot")
	}
}

func (b *wireBroker) shutdown(r *wireRelayRun) {
	b.t.Helper()
	r.cancel()
	for {
		if _, ok := b.read().Content.(*packets.Disconnect); ok {
			break
		}
	}
	if b.availability[len(b.availability)-1] != "offline" {
		b.t.Fatal("graceful shutdown did not acknowledge retained offline")
	}
	select {
	case err := <-r.result:
		r.finished = true
		if err != nil {
			b.t.Fatalf("unexpected relay exit: %v", err)
		}
	case <-time.After(20 * time.Second):
		b.t.Fatal("relay did not finish after graceful DISCONNECT")
	}
}

func wireListener(t *testing.T) (*net.TCPListener, *Config) {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	cfg := testConfig()
	cfg.MQTTURL = "mqtt://" + listener.Addr().String()
	cfg.SmartPiPrefix, cfg.MQTTBaseTopic = "contract/smartpi", "contract/relay"
	cfg.HADiscoveryPref, cfg.HADeviceID = "contract/discovery", "contract_meter"
	// Numerical interval boundaries are covered with a deterministic clock;
	// allow scheduling headroom in this real-transport test.
	cfg.PhaseFreshness = 30 * time.Second
	return listener, cfg
}

func TestRunWireMeasurementsReconnectAndRestart(t *testing.T) {
	stateDir := t.TempDir()
	t.Chdir(stateDir)
	listener, cfg := wireListener(t)
	r := startWireRelay(t, cfg)
	first := acceptWireBroker(t, listener, cfg)
	first.ready([8]float64{})
	for _, measurement := range measurementCases {
		first.measurement(measurement.input, measurement.payload, measurement.key, measurement.state, false)
	}
	first.awaitMeasurementHealth("OFF")
	for _, measurement := range measurementCases {
		if states := first.states[measurement.key]; len(states) != 1 || states[0] != measurement.state {
			t.Fatalf("supplied measurement was overwritten: %s=%v", measurement.key, states)
		}
	}
	if states := first.states["power_total"]; len(states) != 1 || states[0] != "509.3200" {
		t.Fatalf("supplied sequence total: %v", states)
	}
	for _, measurement := range measurementCases {
		if strings.HasPrefix(measurement.input, "P") {
			first.measurement(measurement.input, measurement.payload, measurement.key, measurement.state, false)
		}
	}
	for phase := 0; phase < 3; phase++ {
		if first.energy[phase] <= 0 {
			t.Fatalf("live phase did not accumulate imported energy: %v", first.energy)
		}
	}
	beforeReconnect := first.energy
	if err := first.conn.Close(); err != nil {
		t.Fatal(err)
	}
	second := acceptWireBroker(t, listener, cfg)
	second.ready(beforeReconnect)
	for phase, key := range []string{"power_l1", "power_l2", "power_l3"} {
		second.measurement("P"+strconv.Itoa(phase+1), "9999", key, "9999.0000", true)
	}
	if len(second.snapshots) != 1 || len(second.states["power_total"]) != 0 {
		t.Fatal("retained replay emitted energy or established aggregate freshness")
	}
	phaseCount := 0
	for _, measurement := range measurementCases {
		if strings.HasPrefix(measurement.input, "P") {
			phaseCount++
			second.measurement(measurement.input, measurement.payload, measurement.key, measurement.state, false)
			if second.energy != beforeReconnect {
				t.Fatal("first post-reconnect samples backfilled energy")
			}
			if phaseCount < 3 && len(second.states["power_total"]) != 0 {
				t.Fatal("reconnect retained stale phase samples")
			}
		}
	}
	if got := second.states[measurementProblemKey][len(second.states[measurementProblemKey])-1]; got != "ON" {
		t.Fatalf("reconnect cleared measurement staleness without fresh samples: %s", got)
	}
	if states := second.states["power_total"]; len(states) != 1 || states[0] != "509.3200" {
		t.Fatalf("live phases did not restore the correct aggregate: %v", states)
	}
	second.shutdown(r)

	restarted := startWireRelay(t, cfg)
	third := acceptWireBroker(t, listener, cfg)
	third.ready([8]float64{})
	for _, key := range wireEnergyKeys {
		if states := third.states[key]; len(states) != 1 || states[0] != "0" {
			t.Fatalf("restart did not publish exact zero: %s=%v", key, states)
		}
	}
	third.shutdown(restarted)
	files, err := os.ReadDir(stateDir)
	if err != nil || len(files) != 0 {
		t.Fatalf("relay created local state files: %v, %v", files, err)
	}
}

func TestRunWireDiscoveryRejectionNeverAnnouncesOnline(t *testing.T) {
	listener, cfg := wireListener(t)
	r := startWireRelay(t, cfg)
	broker := acceptWireBroker(t, listener, cfg)
	broker.rejectCleanup = true
	for len(broker.deleted) == 0 {
		broker.read()
	}

	// Do not cancel first: the rejected PUBACK itself must stop initialization.
	for {
		if _, ok := broker.read().Content.(*packets.Disconnect); ok {
			break
		}
	}
	for _, state := range broker.availability {
		if state != "offline" {
			t.Fatalf("failed initialization announced %s", state)
		}
		if len(broker.availability) != 2 || len(broker.deleted) != 1 || len(broker.discovery) != 0 {
			t.Fatal("rejected discovery did not stop initialization before offline shutdown")
		}
	}
	select {
	case err := <-r.result:
		r.finished = true
		if err == nil || !strings.Contains(err.Error(), "MQTT initialization failed") ||
			!strings.Contains(err.Error(), "not authorized") {
			t.Fatalf("discovery rejection was not reported: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("rejected discovery did not stop the relay")
	}
}

func TestRunWireCheckpointSurvivesGracefulRestart(t *testing.T) {
	listener, cfg := wireListener(t)
	cfg.EnergyStateFile = filepath.Join(t.TempDir(), "state", "energy.json")
	r := startWireRelay(t, cfg)
	first := acceptWireBroker(t, listener, cfg)
	first.ready([8]float64{})
	first.measurement("P1", "1000", "power_l1", "1000.0000", false)
	first.measurement("P1", "1000", "power_l1", "1000.0000", false)
	first.measurement("P1", "-1000", "power_l1", "-1000.0000", false)
	first.measurement("P1", "-1000", "power_l1", "-1000.0000", false)
	want := first.energy
	if want[0] <= 0 || want[4] <= 0 {
		t.Fatal("phase did not accumulate imported and exported energy")
	}
	first.shutdown(r)
	state := readCheckpoint(t, cfg.EnergyStateFile)
	if *state.Phases[0] != want[0] || *state.ExportedPhases[0] != want[4] {
		t.Fatal("graceful shutdown did not checkpoint the latest counters")
	}
	restarted := startWireRelay(t, cfg)
	second := acceptWireBroker(t, listener, cfg)
	second.ready(want)
	second.measurement("P1", "0", "power_l1", "0.0000", false)
	if second.energy != want || len(second.states["power_total"]) != 0 {
		t.Fatal("restored counters brought back stale integration baselines")
	}
	second.shutdown(restarted)
}
