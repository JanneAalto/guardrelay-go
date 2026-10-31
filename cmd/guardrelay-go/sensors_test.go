package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

var measurementCases = []struct {
	input, payload, key, state, unit, deviceClass string
}{
	{"I4", "-0.78", "current_l4", "-0.7800", "A", "current"},
	{"I1", "-0.10", "current_l1", "-0.1000", "A", "current"},
	{"V1", "231.83", "voltage_l1", "231.8300", "V", "voltage"},
	{"P1", "78.06", "power_l1", "78.0600", "W", "power"},
	{"COS1", "0.48", "power_factor_l1", "0.4800", "", "power_factor"},
	{"F1", "49.99", "frequency_l1", "49.9900", "Hz", "frequency"},
	{"I2", "-0.04", "current_l2", "-0.0400", "A", "current"},
	{"V2", "232.46", "voltage_l2", "232.4600", "V", "voltage"},
	{"P2", "141.22", "power_l2", "141.2200", "W", "power"},
	{"COS2", "0.89", "power_factor_l2", "0.8900", "", "power_factor"},
	{"F2", "49.99", "frequency_l2", "49.9900", "Hz", "frequency"},
	{"I3", "1.37", "current_l3", "1.3700", "A", "current"},
	{"V3", "231.58", "voltage_l3", "231.5800", "V", "voltage"},
	{"P3", "290.04", "power_l3", "290.0400", "W", "power"},
	{"COS3", "0.55", "power_factor_l3", "0.5500", "", "power_factor"},
	{"F3", "49.99", "frequency_l3", "49.9900", "Hz", "frequency"},
}

type sensorPublisher struct {
	packets []*paho.Publish
	err     error
}

func (p *sensorPublisher) Publish(_ context.Context, packet *paho.Publish) (*paho.PublishResponse, error) {
	p.packets = append(p.packets, packet)
	return &paho.PublishResponse{}, p.err
}

func captureSensorLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var output bytes.Buffer
	writer := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(writer) })
	return &output
}

func TestSmartPiMeasurementMapping(t *testing.T) {
	for _, prefix := range []string{"smartpi", "meters/device/smartpi"} {
		for _, tt := range measurementCases {
			t.Run(prefix+"/"+tt.input, func(t *testing.T) {
				cfg := testConfig()
				cfg.SmartPiPrefix, cfg.MQTTBaseTopic = prefix, "custom/relay"
				relay := newSmartPiRelay(cfg)
				publisher := &sensorPublisher{}
				relay.handleSmartPi(&paho.Publish{
					Topic: prefix + "/" + tt.input, Payload: []byte(tt.payload),
				}, context.Background(), publisher)
				if len(publisher.packets) != 1 {
					t.Fatalf("expected exactly one phase output, got %d", len(publisher.packets))
				}
				packet := publisher.packets[0]
				if packet.Topic != cfg.MQTTBaseTopic+"/state/"+tt.key ||
					string(packet.Payload) != tt.state || packet.Retain || packet.QoS != 1 {
					t.Fatalf("unexpected publication: %+v", packet)
				}
				if strings.HasPrefix(tt.input, "P") {
					index := int(tt.input[1] - '1')
					if relay.phases[index].receivedAt.IsZero() {
						t.Fatal("power routing did not update its calculation phase")
					}
					for other := range relay.phases {
						if other != index && !relay.phases[other].receivedAt.IsZero() {
							t.Fatal("power routing updated the wrong phase")
						}
					}
				} else if relay.phases != [3]phaseSample{} {
					t.Fatal("non-power measurement changed power samples")
				}
			})
		}
	}
}

func TestSmartPiSuppliedSequencePreservesPhasePowerTotal(t *testing.T) {
	relay := newSmartPiRelay(testConfig())
	now := time.Unix(1000, 0)
	relay.now = func() time.Time { return now }
	publisher := &sensorPublisher{}
	for _, measurement := range measurementCases {
		relay.handleSmartPi(&paho.Publish{
			Topic: "smartpi/" + measurement.input, Payload: []byte(measurement.payload),
		}, context.Background(), publisher)
	}
	byKey := make(map[string][]string)
	for _, packet := range publisher.packets {
		key := strings.TrimPrefix(packet.Topic, "guardrelay/ampguard/state/")
		byKey[key] = append(byKey[key], string(packet.Payload))
	}
	for _, measurement := range measurementCases {
		states := byKey[measurement.key]
		if len(states) != 1 || states[0] != measurement.state {
			t.Fatalf("%s was overwritten or misrouted: %v", measurement.key, states)
		}
	}
	if states := byKey["power_total"]; len(states) != 1 || states[0] != "509.3200" {
		t.Fatalf("phase power total changed: %v", states)
	}
}

func TestSmartPiNumericPayloads(t *testing.T) {
	for _, tt := range []struct{ payload, want string }{
		{"  -12,34\n", "-12.3400"},
		{"2.3e2", "230.0000"},
		{"0", "0.0000"},
		{"-1.25", "-1.2500"},
	} {
		t.Run(tt.payload, func(t *testing.T) {
			relay := newSmartPiRelay(testConfig())
			publisher := &sensorPublisher{}
			relay.handleSmartPi(&paho.Publish{Topic: "smartpi/I1", Payload: []byte(tt.payload)},
				context.Background(), publisher)
			if len(publisher.packets) != 1 || string(publisher.packets[0].Payload) != tt.want {
				t.Fatalf("unexpected numeric output: %+v", publisher.packets)
			}
		})
	}
}

func TestSmartPiRejectsMalformedAndNonFinitePayloads(t *testing.T) {
	for _, payload := range []string{"", " \t", "bad", "--1", "1,2,3", "NaN", "+Inf", "-Inf", "Infinity", "1e309"} {
		t.Run(payload, func(t *testing.T) {
			relay := newSmartPiRelay(testConfig())
			output := captureSensorLogs(t)
			publisher := &sensorPublisher{}
			relay.handleSmartPi(&paho.Publish{Topic: "smartpi/P1", Payload: []byte(payload)},
				context.Background(), publisher)
			if len(publisher.packets) != 0 || relay.phases != [3]phaseSample{} {
				t.Fatal("invalid measurement was published or entered calculation state")
			}
			if !strings.Contains(output.String(), "not a finite number") ||
				!strings.Contains(output.String(), "smartpi/P1") {
				t.Fatalf("invalid measurement not logged: %q", output.String())
			}
		})
	}
}

func TestSmartPiIgnoresUnsupportedTopicsBeforeParsing(t *testing.T) {
	output := captureSensorLogs(t)
	cfg := testConfig()
	cfg.SmartPiPrefix = "meters/smartpi"
	relay := newSmartPiRelay(cfg)
	for _, topic := range []string{
		"smartpi/I1", "meters/smartpi", "meters/smartpi/", "meters/smartpi/I1/extra",
		"meters/smartpi//I1", "meters/smartpi/COS3/extra", "meters/smartpi/status",
		"meters/smartpi/i1", "meters/smartpi/energy_total", "meters/smartpi/I5",
		"meters/smartpi-other/I1",
	} {
		publisher := &sensorPublisher{}
		relay.handleSmartPi(&paho.Publish{Topic: topic, Payload: []byte("not a number")},
			context.Background(), publisher)
		if len(publisher.packets) != 0 {
			t.Fatalf("unsupported topic published state: %q", topic)
		}
	}
	if output.Len() != 0 || relay.phases != [3]phaseSample{} {
		t.Fatalf("unrelated topics were parsed: %q", output.String())
	}
}

func TestSmartPiPublishFailureIsLogged(t *testing.T) {
	relay := newSmartPiRelay(testConfig())
	output := captureSensorLogs(t)
	publisher := &sensorPublisher{err: errors.New("broker rejected state")}
	relay.handleSmartPi(&paho.Publish{Topic: "smartpi/I4", Payload: []byte("-0.78")},
		context.Background(), publisher)
	if !strings.Contains(output.String(), "broker rejected state") {
		t.Fatal("state publication failure was not surfaced")
	}
}

func TestDiscoveryMeasurementMetadata(t *testing.T) {
	cfg := testConfig()
	cfg.MQTTBaseTopic, cfg.HADeviceID = "custom/relay", "custom_device"
	sensors := discoverySensors(cfg)
	if len(sensors) != len(measurementCases)+1 {
		t.Fatalf("unexpected number of discovered sensors: %d", len(sensors))
	}
	byKey := make(map[string]SensorConfig)
	for _, sensor := range sensors {
		key := strings.TrimPrefix(sensor.UniqueID, cfg.HADeviceID+"_")
		if _, duplicate := byKey[key]; duplicate {
			t.Fatalf("duplicate sensor unique ID: %s", sensor.UniqueID)
		}
		byKey[key] = sensor
		if sensor.StateTopic != cfg.MQTTBaseTopic+"/state/"+key ||
			sensor.AvailabilityTopic != cfg.MQTTBaseTopic+"/status" ||
			sensor.PayloadAvailable != "online" || sensor.PayloadNotAvailable != "offline" ||
			len(sensor.Device.Identifiers) != 1 || sensor.Device.Identifiers[0] != cfg.HADeviceID ||
			sensor.Device.Name != cfg.HADeviceName || sensor.Icon == "" {
			t.Fatalf("incorrect sensor metadata: %+v", sensor)
		}
	}
	for _, tt := range measurementCases {
		sensor, ok := byKey[tt.key]
		if !ok || sensor.UnitOfMeasurement != tt.unit || sensor.DeviceClass != tt.deviceClass ||
			sensor.StateClass != "measurement" {
			t.Fatalf("%s metadata incorrect: %+v", tt.key, sensor)
		}
		if tt.deviceClass == "power_factor" {
			payload, err := json.Marshal(sensor)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(payload, &fields); err != nil {
				t.Fatal(err)
			}
			if _, exists := fields["unit_of_measurement"]; exists {
				t.Fatal("dimensionless power factor must omit unit_of_measurement")
			}
		}
	}
	for _, key := range []string{
		"current_total", "voltage_total", "phase_angle_l1", "phase_angle_l2", "phase_angle_l3",
		"energy_l1", "energy_l2", "energy_l3", "energy_total",
	} {
		if _, exists := byKey[key]; exists {
			t.Fatalf("obsolete sensor is still discovered: %s", key)
		}
	}
	if sensor := byKey["power_total"]; sensor.UnitOfMeasurement != "W" ||
		sensor.DeviceClass != "power" || sensor.StateClass != "measurement" {
		t.Fatalf("existing total power metadata changed: %+v", sensor)
	}
}

func TestDiscoveryMigration(t *testing.T) {
	cfg := testConfig()
	cfg.HADiscoveryPref, cfg.HADeviceID = "custom/discovery", "custom_device"
	publisher := &sensorPublisher{}
	if err := publishDiscovery(context.Background(), cfg, publisher); err != nil {
		t.Fatal(err)
	}
	oldKeys := []string{
		"current_total", "voltage_total", "phase_angle_l1", "phase_angle_l2", "phase_angle_l3",
		"energy_l1", "energy_l2", "energy_l3", "energy_total",
	}
	if len(publisher.packets) != len(oldKeys)+len(discoverySensors(cfg)) {
		t.Fatalf("incorrect discovery packet count: %d", len(publisher.packets))
	}
	seenTopics := make(map[string]bool)
	for i, packet := range publisher.packets {
		if !packet.Retain || packet.QoS != 1 || seenTopics[packet.Topic] {
			t.Fatalf("incorrect or duplicate discovery publication: %+v", packet)
		}
		seenTopics[packet.Topic] = true
		if i < len(oldKeys) {
			want := cfg.HADiscoveryPref + "/sensor/" + cfg.HADeviceID + "_" + oldKeys[i] + "/config"
			if packet.Topic != want || len(packet.Payload) != 0 {
				t.Fatalf("incorrect retained deletion: %+v", packet)
			}
			continue
		}
		var sensor SensorConfig
		if err := json.Unmarshal(packet.Payload, &sensor); err != nil {
			t.Fatal(err)
		}
		if packet.Topic != cfg.HADiscoveryPref+"/sensor/"+sensor.UniqueID+"/config" ||
			!strings.HasPrefix(sensor.UniqueID, cfg.HADeviceID+"_") {
			t.Fatalf("discovery escaped configured device/prefix: %+v", packet)
		}
	}
}

func TestDiscoveryMigrationFailureStopsInitialization(t *testing.T) {
	cfg, publisher := testConfig(), newFakeMQTT()
	publisher.failPrefix = cfg.HADiscoveryPref + "/sensor/" + cfg.HADeviceID + "_current_total/"
	if err := initializeMQTT(context.Background(), cfg, publisher); err == nil {
		t.Fatal("retained cleanup failure was ignored")
	}
	ops, _ := publisher.snapshot()
	for _, op := range ops {
		if strings.HasSuffix(op, "=online") || strings.Contains(op, `"unique_id"`) {
			t.Fatal("initialization continued after migration failure")
		}
	}
}
