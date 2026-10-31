# GuardRelay (SmartPi → MQTT → guardrelay-go → MQTT  → Home Assistant)

This is vibe coded project that uses https://github.com/AnderssonPeter/GuardRelay as a reference and modified to read from SmartPi MQTT topics. 
Peter Andersson has a newer model of AmpGuard that has websocket support. 
I have the "[grey](https://wwwchargeampscom.cdn.triggerfish.cloud/uploads/2023/11/17101246/Installationmanual_ChargeAmpsAmpGuard_en.pdf)" version.

On my amp guard there is node red installed and accessible from http://<ampguard-ip>:1880 I used `exec` to write a ssh key to `~/.ssh/authorized_keys` so I could ssh into the amp guard.
After that I created a password for the `pi` user so I could access the "smartpi" admin from http://<ampguard-ip>:1080/settings.html and on MQTT tab changed the MQTT broker to my homeassistant mosquitto broker.

This is the device I have:
 - https://wwwchargeampscom.cdn.triggerfish.cloud/uploads/2023/11/17101246/Installationmanual_ChargeAmpsAmpGuard_en.pdf
 - https://web.archive.org/web/20211024154507/https://www.enerserve.eu/en/service/smartpi/manual/api.html
 - https://shop.enerserve.eu/en/SmartPi-3.0-Bundle/100047.7
 - https://github.com/nDenerserve/SmartPi/tree/e6bd8fd2a9f78c163ac656388d44b7ab35f359bc

The rest of the project is vibe coded with gpt-5 and I basically wrote none of this..

Reads SmartPi MQTT measurements (e.g., `smartpi/I1 2.54`, `smartpi/P3 1802.23`) and republishes them in **GuardRelay format**:  
`guardrelay/ampguard/state/<key>` while creating Home Assistant entities via MQTT discovery.

---

## Features

- MQTT v5 client using [paho.golang](https://github.com/eclipse-paho/paho.golang) + `autopaho`
- Subscribes to: `smartpi/#`
- Publishes: `guardrelay/ampguard/state/{current|voltage|power}_...`
- Calculates `power_total = P1+P2+P3`
- LWT/availability: `guardrelay/ampguard/status` (`online`/`offline`)
- Home Assistant MQTT discovery (`homeassistant/sensor/<unique_id>/config`, retain)

---

## Requirements

- Go 1.25+
- MQTT broker (e.g., Mosquitto)
- SmartPi publishing topics like `smartpi/I1`, `smartpi/V1`, `smartpi/P1`
- Home Assistant with MQTT integration enabled

---

## Build Instructions

### Local build (Linux/amd64):
```bash
go mod tidy
go build -o ./dist/guardrelay ./cmd/guardrelay
```

### Cross-compile for Raspberry Pi:

```bash
# Install Taskfile first: https://taskfile.dev/

# ARMv7 (32-bit)
task build:pi-armv7

# ARM64 (aarch64)
task build:pi-arm64
```

### Deploy & Install as systemd Service
```bash
# Copy binary and service files to Raspberry Pi:
task deploy:pi-armv7 HOST=pi@raspberrypi   # For ARMv7
task deploy:pi-armv64 HOST=pi@raspberrypi   # For ARM64

# Install and enable service:
task install:pi HOST=pi@raspberrypi

# Edit environment variables on Pi:
sudo nano /etc/default/guardrelay
sudo systemctl restart guardrelay

# Enable and check service status:
sudo systemctl enable guardrelay
sudo systemctl status guardrelay
```

### Systemd Management Commands
```bash
# Start the service:
sudo systemctl start guardrelay

# Stop the service:
sudo systemctl stop guardrelay

# Restart the service:
sudo systemctl restart guardrelay

# Check logs:
sudo journalctl -u guardrelay -f
```

### Environment Variables

| Variable              | Description |
|-----------------------|-------------|
| `MQTT_URL`           | Broker URL (e.g., `mqtt://user:pass@192.168.1.10:1883`) |
| `MQTT_CLIENT_ID`     | MQTT client ID (default: `guardrelay-ampguard`) |
| `MQTT_BASE_TOPIC`    | Base topic for publishing (default: `guardrelay/ampguard`) |
| `HA_DISCOVERY_PREFIX`| Home Assistant discovery prefix (default: `homeassistant`) |
| `HA_DEVICE_ID`       | Device ID for HA (default: `chargeamps_amp_guard`) |
| `HA_DEVICE_NAME`     | Device name for HA |
| `SMARTPI_PREFIX`     | SmartPi topic prefix (default: `smartpi`) |
| `LOG_LEVEL`          | Logging level (`info`, `debug`) |
| `PHASE_FRESHNESS`    | Maximum phase sample age/energy interval, a positive Go duration (default: `3s`; calculation enforcement is added in the calculation fixes) |

`MQTT_URL` requires a supported scheme and a broker host. TCP/TLS URLs require
an explicit port, for example `mqtt://192.168.1.10:1883` or
`tls://broker.example:8883`. Supported schemes are `mqtt`, `tcp`, `tls`, `mqtts`,
`ssl`, `mqtt+ssl`, `tcps`, `ws`, and `wss`; WebSocket URLs may include a path.
Optional URL credentials are sent in MQTT CONNECT and removed from the transport
URL used in logs. TLS connections verify certificates and require TLS 1.2 or newer.

Topic prefixes must not contain MQTT wildcards or empty levels, start/end with
`/`, or use the reserved `$` prefix. Input, output, and discovery prefixes must
not overlap. `HA_DEVICE_ID` accepts letters, digits, underscores, and hyphens
(up to 256 characters). Invalid configuration fails at startup.

The relay publishes retained `offline` while initializing a connection, then
`online` only after subscription and discovery acknowledgments succeed.
Initialization failures stop the process with an error; systemd can restart it.
Reconnects repeat initialization. Graceful shutdown publishes retained `offline`
before disconnecting using fresh, bounded contexts. DISCONNECT also requests
publication of the offline will, covering cancellation during initial setup.
If a shutdown publication fails, the error is reported and disconnect is still attempted.
`info` enables normal connection/error logs; `debug` additionally enables Paho
protocol diagnostics.

## Topic Mapping

SmartPi → GuardRelay:

- `smartpi/I1` → `current_l1`
- `smartpi/I2` → `current_l2`
- `smartpi/I3` → `current_l3`
- `smartpi/I4` → `current_total`
- `smartpi/V1` → `voltage_l1`
- `smartpi/V2` → `voltage_l2`
- `smartpi/V3` → `voltage_l3`
- `smartpi/P1` → `power_l1`
- `smartpi/P2` → `power_l2`
- `smartpi/P3` → `power_l3`
- `power_total` = sum of P1 + P2 + P3


### Running Locally

```bash
export MQTT_URL="mqtt://user:pass@192.168.1.10:1883"
export MQTT_CLIENT_ID="guardrelay-ampguard"
export MQTT_BASE_TOPIC="guardrelay/ampguard"
export HA_DISCOVERY_PREFIX="homeassistant"
export HA_DEVICE_ID="chargeamps_amp_guard_ABC123"
export HA_DEVICE_NAME="Charge Amps Amp Guard"
export SMARTPI_PREFIX="smartpi"

go run ./cmd/guardrelay
```

### Logs & Status
```bash
sudo systemctl status guardrelay
sudo journalctl -u guardrelay -f
```

## License

MIT
