package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

type HADevice struct {
	Identifiers  []string `json:"identifiers"`
	Manufacturer string   `json:"manufacturer,omitempty"`
	Model        string   `json:"model,omitempty"`
	Name         string   `json:"name,omitempty"`
}
type SensorConfig struct {
	Name                string   `json:"name"`
	Icon                string   `json:"icon,omitempty"`
	UniqueID            string   `json:"unique_id"`
	StateTopic          string   `json:"state_topic"`
	UnitOfMeasurement   string   `json:"unit_of_measurement,omitempty"`
	DeviceClass         string   `json:"device_class,omitempty"`
	StateClass          string   `json:"state_class,omitempty"`
	Device              HADevice `json:"device"`
	AvailabilityTopic   string   `json:"availability_topic,omitempty"`
	PayloadAvailable    string   `json:"payload_available,omitempty"`
	PayloadNotAvailable string   `json:"payload_not_available,omitempty"`
}

type sensorDefinition struct {
	input, key, icon, name, unit, stateClass, deviceClass string
	powerPhase                                            int
}

var sensorDefinitions = []sensorDefinition{
	{"I1", "current_l1", "mdi:lightning-bolt-outline", "Current L1", "A", "measurement", "current", 0},
	{"I2", "current_l2", "mdi:lightning-bolt-outline", "Current L2", "A", "measurement", "current", 0},
	{"I3", "current_l3", "mdi:lightning-bolt-outline", "Current L3", "A", "measurement", "current", 0},
	{"I4", "current_l4", "mdi:lightning-bolt-outline", "Current channel 4", "A", "measurement", "current", 0},
	{"V1", "voltage_l1", "mdi:sine-wave", "Voltage L1", "V", "measurement", "voltage", 0},
	{"V2", "voltage_l2", "mdi:sine-wave", "Voltage L2", "V", "measurement", "voltage", 0},
	{"V3", "voltage_l3", "mdi:sine-wave", "Voltage L3", "V", "measurement", "voltage", 0},
	{"P1", "power_l1", "mdi:flash-outline", "Power L1", "W", "measurement", "power", 1},
	{"P2", "power_l2", "mdi:flash-outline", "Power L2", "W", "measurement", "power", 2},
	{"P3", "power_l3", "mdi:flash-outline", "Power L3", "W", "measurement", "power", 3},
	{"", "power_total", "mdi:flash-outline", "Power Total", "W", "measurement", "power", 0},
	{"", "energy_l1", "mdi:lightning-bolt", "Energy L1", "kWh", "total_increasing", "energy", 0},
	{"", "energy_l2", "mdi:lightning-bolt", "Energy L2", "kWh", "total_increasing", "energy", 0},
	{"", "energy_l3", "mdi:lightning-bolt", "Energy L3", "kWh", "total_increasing", "energy", 0},
	{"", "energy_total", "mdi:lightning-bolt", "Energy Total", "kWh", "total_increasing", "energy", 0},
	{"COS1", "power_factor_l1", "mdi:angle-acute", "Power factor L1", "", "measurement", "power_factor", 0},
	{"COS2", "power_factor_l2", "mdi:angle-acute", "Power factor L2", "", "measurement", "power_factor", 0},
	{"COS3", "power_factor_l3", "mdi:angle-acute", "Power factor L3", "", "measurement", "power_factor", 0},
	{"F1", "frequency_l1", "mdi:sine-wave", "Frequency L1", "Hz", "measurement", "frequency", 0},
	{"F2", "frequency_l2", "mdi:sine-wave", "Frequency L2", "Hz", "measurement", "frequency", 0},
	{"F3", "frequency_l3", "mdi:sine-wave", "Frequency L3", "Hz", "measurement", "frequency", 0},
}

var obsoleteSensorKeys = []string{
	"current_total", "voltage_total", "phase_angle_l1", "phase_angle_l2", "phase_angle_l3",
}

func discoverySensors(cfg *Config) []SensorConfig {
	avail := cfg.MQTTBaseTopic + "/status"
	dev := HADevice{Identifiers: []string{cfg.HADeviceID}, Manufacturer: "Charge Amps", Model: "Amp Guard", Name: cfg.HADeviceName}
	out := make([]SensorConfig, 0, len(sensorDefinitions))
	for _, r := range sensorDefinitions {
		out = append(
			out, SensorConfig{
				Name:                r.name,
				Icon:                r.icon,
				UniqueID:            cfg.HADeviceID + "_" + r.key,
				StateTopic:          cfg.MQTTBaseTopic + "/state/" + r.key,
				UnitOfMeasurement:   r.unit,
				DeviceClass:         r.deviceClass,
				StateClass:          r.stateClass,
				Device:              dev,
				AvailabilityTopic:   avail,
				PayloadAvailable:    "online",
				PayloadNotAvailable: "offline",
			},
		)
	}
	return out
}

func publishDiscovery(ctx context.Context, cfg *Config, cm mqttPublisher) error {
	for _, key := range obsoleteSensorKeys {
		topic := fmt.Sprintf("%s/sensor/%s_%s/config", cfg.HADiscoveryPref, cfg.HADeviceID, key)
		if err := publish(ctx, cm, topic, nil, true); err != nil {
			return err
		}
	}
	for _, sc := range discoverySensors(cfg) {
		js, err := json.Marshal(sc)
		if err != nil {
			return fmt.Errorf("marshal sensor %s: %w", sc.UniqueID, err)
		}
		topic := fmt.Sprintf("%s/sensor/%s/config", cfg.HADiscoveryPref, sc.UniqueID)
		if err := publish(ctx, cm, topic, js, true); err != nil {
			return err
		}
	}
	return nil
}

func publishAvailability(ctx context.Context, cfg *Config, cm mqttPublisher, online bool) error {
	state := "offline"
	if online {
		state = "online"
	}
	return publish(ctx, cm, cfg.MQTTBaseTopic+"/status", []byte(state), true)
}

type totals struct {
	p      [3]*float64
	lastTS time.Time
}

var lastTotals totals

var tot totals

func handleSmartPi(pub *paho.Publish, cfg *Config, ctx context.Context, cm mqttPublisher) {
	topic := pub.Topic
	if !strings.HasPrefix(topic, cfg.SmartPiPrefix+"/") {
		return
	}
	key := strings.TrimPrefix(topic, cfg.SmartPiPrefix+"/")
	var sensor sensorDefinition
	found := false
	for _, definition := range sensorDefinitions {
		if definition.input != "" && definition.input == key {
			sensor, found = definition, true
			break
		}
	}
	if !found {
		return
	}

	msg := strings.TrimSpace(string(pub.Payload))
	val, err := strconv.ParseFloat(strings.ReplaceAll(msg, ",", "."), 64)
	if err != nil || math.IsNaN(val) || math.IsInf(val, 0) {
		log.Printf("SmartPi payload is not a finite number: topic=%s payload=%q", topic, msg)
		return
	}
	if sensor.powerPhase != 0 {
		tot.p[sensor.powerPhase-1] = &val
		tot.lastTS = time.Now()
	}

	stateTopic := fmt.Sprintf("%s/state/%s", cfg.MQTTBaseTopic, sensor.key)
	payload := []byte(strconv.FormatFloat(val, 'f', 4, 64))
	if err := publish(ctx, cm, stateTopic, payload, false); err != nil {
		log.Print(err)
	}

	if tot.p[0] != nil && tot.p[1] != nil && tot.p[2] != nil {
		timeSince := time.Since(tot.lastTS)
		if timeSince < 3*time.Second {
			sum := *tot.p[0] + *tot.p[1] + *tot.p[2]
			if !math.IsNaN(sum) && !math.IsInf(sum, 0) {
				lastTotals = tot
				if err := publish(
					ctx,
					cm,
					cfg.MQTTBaseTopic+"/state/power_total",
					[]byte(strconv.FormatFloat(sum, 'f', 4, 64)),
					false,
				); err != nil {
					log.Print(err)
				}
			}

			// energy calculation in kWh
			hours := timeSince.Hours()
			energy := (*lastTotals.p[0] + *lastTotals.p[1] + *lastTotals.p[2]) * hours / 1000.0
			if !math.IsNaN(energy) && !math.IsInf(energy, 0) {
				if err := publish(
					ctx,
					cm,
					cfg.MQTTBaseTopic+"/state/energy_total",
					[]byte(strconv.FormatFloat(energy, 'f', 4, 64)),
					false,
				); err != nil {
					log.Print(err)
				}
			}

			// reset totals
			tot = totals{}
		}
	}
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}
