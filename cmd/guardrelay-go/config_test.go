package main

import (
	"context"
	"crypto/tls"
	"strings"
	"testing"
	"time"
)

func testConfig() *Config {
	return &Config{
		MQTTURL: "mqtt://localhost:1883", MQTTClientID: "guardrelay-test",
		MQTTBaseTopic: "guardrelay/ampguard", HADiscoveryPref: "homeassistant",
		HADeviceID: "chargeamps_amp_guard", HADeviceName: "Charge Amps Amp Guard",
		LogLevel: "info", SmartPiPrefix: "smartpi", PhaseFreshness: 3 * time.Second,
	}
}

func TestLoadConfig(t *testing.T) {
	for _, key := range []string{
		"MQTT_URL", "MQTT_CLIENT_ID", "MQTT_BASE_TOPIC", "HA_DISCOVERY_PREFIX",
		"HA_DEVICE_ID", "HA_DEVICE_NAME", "LOG_LEVEL", "SMARTPI_PREFIX", "PHASE_FRESHNESS",
		"ENERGY_STATE_FILE",
	} {
		t.Setenv(key, "")
	}
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "MQTT_URL") {
		t.Fatalf("missing broker: %v", err)
	}
	t.Setenv("MQTT_URL", " mqtt://localhost:1883 ")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MQTTClientID != "guardrelay-ampguard" || cfg.PhaseFreshness != 5*time.Second {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.EnergyStateFile != "" {
		t.Fatal("native checkpoint storage should be opt-in")
	}
	t.Setenv("ENERGY_STATE_FILE", "/var/lib/guardrelay/energy.json")
	cfg, err = loadConfig()
	if err != nil || cfg.EnergyStateFile != "/var/lib/guardrelay/energy.json" {
		t.Fatalf("checkpoint configuration: %+v, %v", cfg, err)
	}
	t.Setenv("PHASE_FRESHNESS", "30s")
	cfg, err = loadConfig()
	if err != nil || cfg.PhaseFreshness != 30*time.Second {
		t.Fatalf("custom freshness: %+v, %v", cfg, err)
	}
	for _, value := range []string{"0s", "-1s", "not-a-duration"} {
		t.Setenv("PHASE_FRESHNESS", value)
		if _, err := loadConfig(); err == nil {
			t.Fatalf("accepted freshness %q", value)
		}
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Config)
	}{
		{"wildcard output", func(c *Config) { c.MQTTBaseTopic = "relay/+" }},
		{"wildcard input", func(c *Config) { c.SmartPiPrefix = "smartpi/#" }},
		{"leading slash", func(c *Config) { c.SmartPiPrefix = "/smartpi" }},
		{"trailing slash", func(c *Config) { c.HADiscoveryPref = "homeassistant/" }},
		{"empty level", func(c *Config) { c.MQTTBaseTopic = "relay//device" }},
		{"reserved prefix", func(c *Config) { c.SmartPiPrefix = "$SYS" }},
		{"invalid UTF8", func(c *Config) { c.SmartPiPrefix = "\xff" }},
		{"null", func(c *Config) { c.MQTTClientID = "client\x00" }},
		{"control character", func(c *Config) { c.MQTTBaseTopic = "relay\n" }},
		{"input output overlap", func(c *Config) { c.MQTTBaseTopic = "smartpi/relay" }},
		{"output input overlap", func(c *Config) { c.SmartPiPrefix = "guardrelay/ampguard/input" }},
		{"discovery input overlap", func(c *Config) { c.HADiscoveryPref = "smartpi" }},
		{"output discovery overlap", func(c *Config) { c.MQTTBaseTopic = "homeassistant/relay" }},
		{"device slash", func(c *Config) { c.HADeviceID = "device/one" }},
		{"empty device ID", func(c *Config) { c.HADeviceID = "" }},
		{"empty client ID", func(c *Config) { c.MQTTClientID = "" }},
		{"invalid log level", func(c *Config) { c.LogLevel = "verbose" }},
		{"zero freshness", func(c *Config) { c.PhaseFreshness = 0 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			tt.change(cfg)
			if err := cfg.validate(); err == nil {
				t.Fatal("accepted invalid config")
			}
		})
	}
	cfg := testConfig()
	cfg.SmartPiPrefix = "meters/smartpi"
	cfg.MQTTBaseTopic = "meters/relay"
	if err := cfg.validate(); err != nil {
		t.Fatalf("sibling prefixes should not collide: %v", err)
	}
}

func TestBrokerURL(t *testing.T) {
	for _, raw := range []string{
		"mqtt://localhost:1883", "tcp://[::1]:1883", "tls://localhost:8883",
		"mqtts://user:password@localhost:8883", "ssl://localhost:8883",
		"mqtt+ssl://localhost:8883", "tcps://localhost:8883",
		"ws://localhost/mqtt", "wss://localhost:443/mqtt",
	} {
		if _, err := brokerURL(raw); err != nil {
			t.Errorf("rejected %q: %v", raw, err)
		}
	}
	for _, raw := range []string{
		"", "localhost:1883", "http://localhost:1883", "mqtt://:1883",
		"mqtt://localhost", "mqtt://localhost:0", "mqtt://localhost:65536",
		"mqtt://localhost:abc", "mqtt://localhost:1883/path",
		"mqtt://localhost:1883?query=value", "ws://localhost/mqtt#fragment",
		"mqtt://user:secret%zz@localhost:1883",
	} {
		if _, err := brokerURL(raw); err == nil {
			t.Errorf("accepted %q", raw)
		} else if strings.Contains(err.Error(), "secret") {
			t.Errorf("error includes credentials: %v", err)
		}
	}
}

func TestMQTTClientConfig(t *testing.T) {
	for _, scheme := range []string{"mqtt", "tcp", "tls", "mqtts", "ssl", "mqtt+ssl", "tcps", "ws", "wss"} {
		t.Run(scheme, func(t *testing.T) {
			cfg := testConfig()
			cfg.MQTTURL = scheme + "://user:secret@localhost:1883"
			ctx, cancel := context.WithCancel(context.Background())
			l := newMQTTLifecycle(ctx, cfg)
			defer func() { cancel(); <-l.done }()
			clientCfg, err := mqttClientConfig(cfg, l)
			if err != nil {
				t.Fatal(err)
			}
			if clientCfg.ConnectUsername != "user" || string(clientCfg.ConnectPassword) != "secret" {
				t.Fatal("credentials lost")
			}
			if clientCfg.ServerUrls[0].User != nil {
				t.Fatal("credentials left in transport URL")
			}
			secure := scheme != "mqtt" && scheme != "tcp" && scheme != "ws"
			if secure && (clientCfg.TlsCfg == nil || clientCfg.TlsCfg.MinVersion != tls.VersionTLS12 ||
				clientCfg.TlsCfg.InsecureSkipVerify || clientCfg.TlsCfg.ServerName != "localhost") {
				t.Fatal("TLS configuration does not enforce verified TLS 1.2+")
			}
			if !secure && clientCfg.TlsCfg != nil {
				t.Fatal("TLS configured for plaintext transport")
			}
			if clientCfg.Errors == nil || clientCfg.PahoErrors == nil ||
				clientCfg.Debug != nil || clientCfg.PahoDebug != nil {
				t.Fatal("info logging configuration is incorrect")
			}
			cfg.LogLevel = "debug"
			clientCfg, err = mqttClientConfig(cfg, l)
			if err != nil || clientCfg.Debug == nil || clientCfg.PahoDebug == nil {
				t.Fatal("debug logging not enabled")
			}
			if clientCfg.WillMessage.Topic != cfg.MQTTBaseTopic+"/status" ||
				string(clientCfg.WillMessage.Payload) != "offline" || !clientCfg.WillMessage.Retain {
				t.Fatal("incorrect offline will")
			}
			if clientCfg.DisconnectPacketBuilder().ReasonCode != 0x04 {
				t.Fatal("disconnect must request offline will publication")
			}
		})
	}
}
