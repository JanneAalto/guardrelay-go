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

func discoverySensors(cfg *Config) []SensorConfig {
	avail := cfg.MQTTBaseTopic + "/status"
	dev := HADevice{Identifiers: []string{cfg.HADeviceID}, Manufacturer: "Charge Amps", Model: "Amp Guard", Name: cfg.HADeviceName}
	type row struct{ key, icon, name, unit, stateClass, dclass string }
	rows := []row{
		{"current_l1", "mdi:lightning-bolt-outline", "Current L1", "A", "measurement", "current"},
		{"current_l2", "mdi:lightning-bolt-outline", "Current L2", "A", "measurement", "current"},
		{"current_l3", "mdi:lightning-bolt-outline", "Current L3", "A", "measurement", "current"},
		{"current_total", "mdi:lightning-bolt-outline", "Current Total", "A", "measurement", "current"},

		{"voltage_l1", "mdi:sine-wave", "Voltage L1", "V", "measurement", "voltage"},
		{"voltage_l2", "mdi:sine-wave", "Voltage L2", "V", "measurement", "voltage"},
		{"voltage_l3", "mdi:sine-wave", "Voltage L3", "V", "measurement", "voltage"},
		{"voltage_total", "mdi:sine-wave", "Voltage Total", "V", "measurement", "voltage"},

		{"power_l1", "mdi:flash-outline", "Power L1", "W", "measurement", "power"},
		{"power_l2", "mdi:flash-outline", "Power L2", "W", "measurement", "power"},
		{"power_l3", "mdi:flash-outline", "Power L3", "W", "measurement", "power"},
		{"power_total", "mdi:flash-outline", "Power Total", "W", "measurement", "power"},

		{"energy_l1", "mdi:lightning-bolt", "Energy L1", "kWh", "total_increasing", "energy"},
		{"energy_l2", "mdi:lightning-bolt", "Energy L2", "kWh", "total_increasing", "energy"},
		{"energy_l3", "mdi:lightning-bolt", "Energy L3", "kWh", "total_increasing", "energy"},
		{"energy_total", "mdi:lightning-bolt", "Energy Total", "kWh", "total_increasing", "energy"},

		{"phase_angle_l1", "mdi:angle-acute", "Phase angle L1", "deg", "measurement", "None"},
		{"phase_angle_l2", "mdi:angle-acute", "Phase angle L2", "deg", "measurement", "None"},
		{"phase_angle_l3", "mdi:angle-acute", "Phase angle L3", "deg", "measurement", "None"},
	}
	out := make([]SensorConfig, 0, len(rows))
	for _, r := range rows {
		out = append(
			out, SensorConfig{
				Name:                r.name,
				Icon:                r.icon,
				UniqueID:            cfg.HADeviceID + "_" + r.key,
				StateTopic:          cfg.MQTTBaseTopic + "/state/" + r.key,
				UnitOfMeasurement:   r.unit,
				DeviceClass:         r.dclass,
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
	parts := strings.Split(strings.TrimPrefix(topic, cfg.SmartPiPrefix+"/"), "/")
	if len(parts) < 1 {
		return
	}
	key := parts[0]

	msg := strings.TrimSpace(string(pub.Payload))
	val, err := strconv.ParseFloat(msg, 64)
	if err != nil {
		if v2, err2 := strconv.ParseFloat(strings.ReplaceAll(msg, ",", "."), 64); err2 == nil {
			val = v2
		} else {
			log.Printf("SmartPi payload ei numero: topic=%s payload=%q", topic, msg)
			return
		}
	}

	var outKey string
	switch key {
	case "I1":
		outKey = "current_l1"
	case "I2":
		outKey = "current_l2"
	case "I3":
		outKey = "current_l3"

	case "V1":
		outKey = "voltage_l1"
	case "V2":
		outKey = "voltage_l2"
	case "V3":
		outKey = "voltage_l3"

	case "COS1":
		outKey = "phase_angle_l1"
	case "COS2":
		outKey = "phase_angle_l2"
	case "COS3":
		outKey = "phase_angle_l2"

	case "P1":
		outKey = "power_l1"
		tot.p[0] = &val
		tot.lastTS = time.Now()
	case "P2":
		outKey = "power_l2"
		tot.p[1] = &val
		tot.lastTS = time.Now()
	case "P3":
		outKey = "power_l3"
		tot.p[2] = &val
		tot.lastTS = time.Now()

	default:
		return
	}

	stateTopic := fmt.Sprintf("%s/state/%s", cfg.MQTTBaseTopic, outKey)
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
