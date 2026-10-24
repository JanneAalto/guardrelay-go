package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

type Config struct {
	MQTTURL         string
	MQTTClientID    string
	MQTTBaseTopic   string
	HADiscoveryPref string
	HADeviceID      string
	HADeviceName    string
	LogLevel        string
	SmartPiPrefix   string
}

func mustEnv(k, def string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	return v
}

func loadConfig() (*Config, error) {
	cfg := &Config{
		MQTTURL:         mustEnv("MQTT_URL", ""),
		MQTTClientID:    mustEnv("MQTT_CLIENT_ID", "guardrelay-ampguard"),
		MQTTBaseTopic:   mustEnv("MQTT_BASE_TOPIC", "guardrelay/ampguard"),
		HADiscoveryPref: mustEnv("HA_DISCOVERY_PREFIX", "homeassistant"),
		HADeviceID:      mustEnv("HA_DEVICE_ID", "chargeamps_amp_guard"),
		HADeviceName:    mustEnv("HA_DEVICE_NAME", "Charge Amps Amp Guard"),
		LogLevel:        mustEnv("LOG_LEVEL", "info"),
		SmartPiPrefix:   mustEnv("SMARTPI_PREFIX", "smartpi"),
	}
	if cfg.MQTTURL == "" {
		return nil, fmt.Errorf("MQTT_URL is required")
	}
	return cfg, nil
}

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

func connectMQTT(ctx context.Context, cfg *Config) (*autopaho.ConnectionManager, error) {
	u, err := url.Parse(cfg.MQTTURL)
	if err != nil {
		return nil, err
	}
	var cm *autopaho.ConnectionManager
	pass, _ := u.User.Password()
	cliCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		ConnectUsername:               u.User.Username(),
		ConnectPassword:               []byte(pass),
		KeepAlive:                     30,
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         86400,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, ca *paho.Connack) {
			log.Println("MQTT connected")
			if _, subErr := cm.Subscribe(
				ctx, &paho.Subscribe{
					Subscriptions: []paho.SubscribeOptions{{Topic: cfg.SmartPiPrefix + "/#", QoS: 1}},
				},
			); subErr != nil {
				log.Printf("Failed to subscribe to topic %s: %v", cfg.SmartPiPrefix+"/#", subErr)
			}
			publishAvailability(ctx, cfg, cm, true)
			publishDiscovery(ctx, cfg, cm)
		},
		OnConnectError: func(err error) { log.Printf("MQTT connection error: %v", err) },

		WillMessage: &paho.WillMessage{
			Topic:   cfg.MQTTBaseTopic + "/status",
			Payload: []byte("offline"),
			QoS:     1,
			Retain:  true,
		},

		ClientConfig: paho.ClientConfig{
			ClientID: cfg.MQTTClientID,
			OnPublishReceived: []func(pr paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					handleSmartPi(pr.Packet, cfg, ctx, cm)
					return true, nil
				},
			},
		},
	}
	if u.Scheme == "mqtts" || u.Scheme == "ssl" || u.Scheme == "tcps" {
		cliCfg.TlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	cm, err = autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		return nil, err
	}
	if err := cm.AwaitConnection(ctx); err != nil {
		return nil, err
	}
	return cm, nil
}

func publish(ctx context.Context, cm *autopaho.ConnectionManager, topic string, payload []byte, retain bool) {
	_, err := cm.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Retain: retain, Payload: payload})
	if err != nil {
		log.Printf("Publish error %s: %v", topic, err)
	}
}

func publishDiscovery(ctx context.Context, cfg *Config, cm *autopaho.ConnectionManager) {
	for _, sc := range discoverySensors(cfg) {
		js, err := json.Marshal(sc)
		if err != nil {
			log.Printf("Failed to marshal sensor config: %v", err)
			continue
		}
		topic := fmt.Sprintf("%s/sensor/%s/config", cfg.HADiscoveryPref, sc.UniqueID)
		publish(ctx, cm, topic, js, true)
	}
}

func publishAvailability(ctx context.Context, cfg *Config, cm *autopaho.ConnectionManager, online bool) {
	state := "offline"
	if online {
		state = "online"
	}
	publish(ctx, cm, cfg.MQTTBaseTopic+"/status", []byte(state), true)
}

type totals struct {
	p      [3]*float64
	lastTS time.Time
}

var lastTotals totals

var tot totals

func handleSmartPi(pub *paho.Publish, cfg *Config, ctx context.Context, cm *autopaho.ConnectionManager) {
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
	publish(ctx, cm, stateTopic, payload, false)

	if tot.p[0] != nil && tot.p[1] != nil && tot.p[2] != nil {
		timeSince := time.Since(tot.lastTS)
		if timeSince < 3*time.Second {
			sum := *tot.p[0] + *tot.p[1] + *tot.p[2]
			if !math.IsNaN(sum) && !math.IsInf(sum, 0) {
				lastTotals = tot
				publish(ctx, cm, cfg.MQTTBaseTopic+"/state/power_total", []byte(strconv.FormatFloat(sum, 'f', 4, 64)), false)
			}

			// energy calculation in kWh
			hours := timeSince.Hours()
			energy := (*lastTotals.p[0] + *lastTotals.p[1] + *lastTotals.p[2]) * hours / 1000.0
			if !math.IsNaN(energy) && !math.IsInf(energy, 0) {
				publish(ctx, cm, cfg.MQTTBaseTopic+"/state/energy_total", []byte(strconv.FormatFloat(energy, 'f', 4, 64)), false)
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

	cm, err := connectMQTT(ctx, cfg)
	if err != nil {
		log.Fatalf("MQTT connect failed: %v", err)
	}

	defer cm.Disconnect(ctx)

	<-ctx.Done()
}
