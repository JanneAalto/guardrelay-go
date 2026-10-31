package main

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
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
	PhaseFreshness  time.Duration
	EnergyStateFile string
}

func mustEnv(k, def string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	return v
}

func loadConfig() (*Config, error) {
	freshness, err := time.ParseDuration(mustEnv("PHASE_FRESHNESS", "10s"))
	if err != nil || freshness <= 0 {
		return nil, fmt.Errorf("PHASE_FRESHNESS must be a positive Go duration (for example 3s)")
	}
	cfg := &Config{
		MQTTURL:         mustEnv("MQTT_URL", ""),
		MQTTClientID:    mustEnv("MQTT_CLIENT_ID", "guardrelay-ampguard"),
		MQTTBaseTopic:   mustEnv("MQTT_BASE_TOPIC", "guardrelay/ampguard"),
		HADiscoveryPref: mustEnv("HA_DISCOVERY_PREFIX", "homeassistant"),
		HADeviceID:      mustEnv("HA_DEVICE_ID", "chargeamps_amp_guard"),
		HADeviceName:    mustEnv("HA_DEVICE_NAME", "Charge Amps Amp Guard"),
		LogLevel:        mustEnv("LOG_LEVEL", "info"),
		SmartPiPrefix:   mustEnv("SMARTPI_PREFIX", "smartpi"),
		PhaseFreshness:  freshness,
		EnergyStateFile: mustEnv("ENERGY_STATE_FILE", ""),
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

var discoveryID = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

func (cfg *Config) validate() error {
	if _, err := brokerURL(cfg.MQTTURL); err != nil {
		return err
	}
	for _, field := range []struct{ name, value string }{
		{"MQTT_BASE_TOPIC", cfg.MQTTBaseTopic},
		{"HA_DISCOVERY_PREFIX", cfg.HADiscoveryPref},
		{"SMARTPI_PREFIX", cfg.SmartPiPrefix},
	} {
		if !validMQTTString(field.value) || strings.ContainsAny(field.value, "+#") ||
			strings.HasPrefix(field.value, "/") || strings.HasSuffix(field.value, "/") ||
			strings.Contains(field.value, "//") || strings.HasPrefix(field.value, "$") ||
			len(field.value) > 65000 {
			return fmt.Errorf("%s must be a nonempty MQTT topic prefix without wildcards, empty levels, or reserved $ prefix", field.name)
		}
	}
	for _, output := range []string{cfg.MQTTBaseTopic, cfg.HADiscoveryPref} {
		if output == cfg.SmartPiPrefix || strings.HasPrefix(output, cfg.SmartPiPrefix+"/") ||
			strings.HasPrefix(cfg.SmartPiPrefix, output+"/") {
			return fmt.Errorf("SMARTPI_PREFIX must not overlap MQTT_BASE_TOPIC or HA_DISCOVERY_PREFIX")
		}
	}
	if cfg.MQTTBaseTopic == cfg.HADiscoveryPref ||
		strings.HasPrefix(cfg.MQTTBaseTopic, cfg.HADiscoveryPref+"/") ||
		strings.HasPrefix(cfg.HADiscoveryPref, cfg.MQTTBaseTopic+"/") {
		return fmt.Errorf("MQTT_BASE_TOPIC and HA_DISCOVERY_PREFIX must not overlap")
	}
	if !discoveryID.MatchString(cfg.HADeviceID) || len(cfg.HADeviceID) > 256 {
		return fmt.Errorf("HA_DEVICE_ID must contain 1-256 letters, digits, underscores, or hyphens")
	}
	if !validMQTTString(cfg.MQTTClientID) {
		return fmt.Errorf("MQTT_CLIENT_ID must be a nonempty valid MQTT UTF-8 string")
	}
	if cfg.LogLevel != "info" && cfg.LogLevel != "debug" {
		return fmt.Errorf("LOG_LEVEL must be info or debug")
	}
	if cfg.PhaseFreshness <= 0 {
		return fmt.Errorf("PHASE_FRESHNESS must be positive")
	}
	return nil
}

func validMQTTString(s string) bool {
	if s == "" || len(s) > 65535 || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r >= 0x7f && r <= 0x9f || r >= 0xfdd0 && r <= 0xfdef ||
			r&0xffff == 0xfffe || r&0xffff == 0xffff {
			return false
		}
	}
	return true
}

func brokerURL(raw string) (*url.URL, error) {
	if raw == "" {
		return nil, fmt.Errorf("MQTT_URL is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("MQTT_URL is malformed")
	}
	switch u.Scheme {
	case "mqtt", "tcp", "ssl", "tls", "mqtts", "mqtt+ssl", "tcps", "ws", "wss":
	default:
		return nil, fmt.Errorf("MQTT_URL has an unsupported scheme; use mqtt, tcp, tls, mqtts, ssl, mqtt+ssl, tcps, ws, or wss")
	}
	if u.Hostname() == "" || strings.ContainsAny(u.Hostname(), " \t\r\n") || u.Opaque != "" || u.Fragment != "" {
		return nil, fmt.Errorf("MQTT_URL must have a broker host and no fragment")
	}
	port := u.Port()
	if port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return nil, fmt.Errorf("MQTT_URL port must be between 1 and 65535")
		}
	}
	if u.Scheme != "ws" && u.Scheme != "wss" {
		if port == "" || u.Path != "" && u.Path != "/" || u.RawQuery != "" {
			return nil, fmt.Errorf("MQTT_URL TCP/TLS transports require an explicit port and no path or query")
		}
	}
	return u, nil
}
