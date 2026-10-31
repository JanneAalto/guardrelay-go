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
- Publishes current, voltage, power, power factor, and frequency under `guardrelay/ampguard/state/<key>`
- Calculates `power_total = P1+P2+P3`
- Estimates per-phase and total imported kWh, with optional local checkpoints
- LWT/availability: `guardrelay/ampguard/status` (`online`/`offline`)
- Home Assistant MQTT discovery (`homeassistant/sensor/<unique_id>/config`, retain)

---

## Requirements

- Go 1.25+
- MQTT broker (e.g., Mosquitto)
- SmartPi publishing topics like `smartpi/I1`, `smartpi/V1`, `smartpi/P1`
- Home Assistant with MQTT integration enabled
- [Task](https://taskfile.dev/) for the build/deployment shortcuts
- For Pi deployment: SSH/SCP access and non-interactive sudo permission on a
  Debian/Raspberry Pi OS host with systemd

---

## Build Instructions

### Native build:
```bash
go build -o ./dist/guardrelay ./cmd/guardrelay-go
```

### Cross-compile for Linux/amd64:
```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./dist/linux-amd64/guardrelay ./cmd/guardrelay-go
```

### Development checks:
```bash
task test
task vet
go test -race ./...
```

The MQTT integration tests run the real Paho client/relay against a disposable
loopback protocol peer, including the supplied measurements, discovery,
reconnects, counter resets, and graceful shutdown. Deployment tests use local
command stubs and temporary directories; they require Task and bash and report
a skip if either is unavailable. Neither test suite contacts your configured
broker or Raspberry Pi.

These local checks do not establish live Home Assistant acceptance or validate
systemd hardening on a Pi; verify those on the target after deployment.

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
task deploy:pi-armv7 HOST=pi@raspberrypi MQTT_URL=mqtt://192.168.1.10:1883
# Or, for ARM64:
task deploy:pi-arm64 HOST=pi@raspberrypi MQTT_URL=mqtt://192.168.1.10:1883

# Install and enable service:
task install:pi HOST=pi@raspberrypi

# Edit environment variables on Pi:
sudo nano /etc/default/guardrelay
sudo systemctl restart guardrelay

# Enable and check service status:
sudo systemctl enable guardrelay
sudo systemctl status guardrelay
```

Deployment requires both `HOST` and `MQTT_URL` before building or copying files.
`SMARTPI_PREFIX` can optionally override `smartpi` when generating the defaults.
The generated `dist/service/guardrelay.defaults` is mode `0600`; treat it as
sensitive if the broker URL contains credentials. Avoid putting credentials in
shell history; supply `MQTT_URL` through the environment instead.

Installation uses `sudo -n`, creates the `guardrelay` system group/user if absent,
and installs the binary and hardened unit. It installs the initial
`/etc/default/guardrelay` as root-owned mode `0600` and **preserves an existing
configuration on reinstall**. The environment file is required by the service.
Edit it on the Pi when changing broker or sensor settings; rerunning deployment
does not replace those settings. Uploaded files in `/tmp` are removed after
installation. Missing files, sudo failures, and failed service activation are
reported rather than ignored.

For a binary-only update, first build the matching architecture, then:

```bash
task build:pi-arm64
task update:pi HOST=pi@raspberrypi ARCH=arm64
# For 32-bit Pi OS, use build:pi-armv7 and ARCH=armv7 instead.
```

The update requires an existing installed service and the selected local binary.
It stops the service, installs the binary, and starts the service again without
changing configuration. Both updates and reinstalls restart the process. Energy
counters survive graceful restarts when checkpoint storage is enabled.

### Installation on rpi-pihole (32-bit)

`rpi-pihole` reports `armv7l` from `uname -m` and `32` from
`getconf LONG_BIT`, so use the ARMv7 target.

Run these commands from the project directory on your development machine,
replacing `192.168.1.10` with your MQTT broker's address:

```bash
task deploy:pi-armv7 HOST=pi@rpi-pihole MQTT_URL=mqtt://192.168.1.10:1883
task install:pi HOST=pi@rpi-pihole
```

For later binary-only updates:

```bash
task build:pi-armv7
task update:pi HOST=pi@rpi-pihole ARCH=armv7
```

To enable checkpoint storage on an existing installation, rerun the deploy and
install commands above so the updated systemd unit is installed. A binary-only
update does not install the new unit. Existing broker configuration is preserved.
The first enabled run starts at zero unless a checkpoint already exists; the
upgrade cannot recover the previous memory-only totals.

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
| `MQTT_URL`           | Required broker URL (e.g., `mqtt://192.168.1.10:1883`) |
| `MQTT_CLIENT_ID`     | MQTT client ID (default: `guardrelay-ampguard`) |
| `MQTT_BASE_TOPIC`    | Base topic for publishing (default: `guardrelay/ampguard`) |
| `HA_DISCOVERY_PREFIX`| Home Assistant discovery prefix (default: `homeassistant`) |
| `HA_DEVICE_ID`       | Device ID for HA (default: `chargeamps_amp_guard`) |
| `HA_DEVICE_NAME`     | Device name for HA |
| `SMARTPI_PREFIX`     | SmartPi topic prefix (default: `smartpi`) |
| `LOG_LEVEL`          | Logging level (`info`, `debug`) |
| `PHASE_FRESHNESS`    | Maximum phase sample age/energy integration interval, a positive Go duration (default: `3s`) |
| `ENERGY_STATE_FILE`  | Optional local energy checkpoint path; unset/empty disables persistence. The Pi unit defaults to `/var/lib/guardrelay/energy.json`. |

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

SmartPi inputs map to `guardrelay/ampguard/state/<key>` by default:

| Input topic | Output key | Unit |
|-------------|------------|------|
| `smartpi/I1` | `current_l1` | A |
| `smartpi/I2` | `current_l2` | A |
| `smartpi/I3` | `current_l3` | A |
| `smartpi/I4` | `current_l4` | A |
| `smartpi/V1` | `voltage_l1` | V |
| `smartpi/V2` | `voltage_l2` | V |
| `smartpi/V3` | `voltage_l3` | V |
| `smartpi/P1` | `power_l1` | W |
| `smartpi/P2` | `power_l2` | W |
| `smartpi/P3` | `power_l3` | W |
| `smartpi/COS1` | `power_factor_l1` | Dimensionless |
| `smartpi/COS2` | `power_factor_l2` | Dimensionless |
| `smartpi/COS3` | `power_factor_l3` | Dimensionless |
| `smartpi/F1` | `frequency_l1` | Hz |
| `smartpi/F2` | `frequency_l2` | Hz |
| `smartpi/F3` | `frequency_l3` | Hz |

`I4` is the fourth measured current channel, not a calculated total or an assumed
neutral current. `COS1`-`COS3` are forwarded as power factors (for example `0.48`),
not converted to angles or percentages. Home Assistant supports the `power_factor`
device class [without a unit](https://www.home-assistant.io/integrations/sensor/#device-class).
There is no `voltage_total` sensor because summing phase voltages is not a meaningful
total for this relay.

The input prefix is configurable with `SMARTPI_PREFIX`. Only the exact input
topics above are processed; unrelated or nested topics such as `smartpi/I1/extra`
are ignored. Numeric payloads may use a decimal point or decimal comma and retain
their sign. Malformed payloads, NaN, and infinities are logged and rejected.

### Power aggregation

`power_total` is the signed sum of P1 + P2 + P3, published on live power updates
only when all three phases have been received and each is younger than
`PHASE_FRESHNESS` (default `3s`). Each phase has its own receipt timestamp.
Repeated updates reuse the other phases' latest fresh values instead of requiring
a new three-message batch. Missing/stale phases and non-finite sums suppress the
total; phase readings continue to be forwarded individually. Suppression does not
clear the last value already displayed in Home Assistant.

Disconnect/reconnect clears phase samples. Retained power messages are forwarded
as individual phase states but are excluded from aggregation because their
measurement times are unknown. Aggregation resumes after all three live phases
arrive. If SmartPi publishes less frequently than every three seconds, increase
`PHASE_FRESHNESS`, for example to `30s`.

### Imported energy

`energy_l1`-`energy_l3` accumulate estimated imported kWh for each phase.
For consecutive live power samples less than `PHASE_FRESHNESS` apart, the relay
uses the previous positive power:
`increment_kWh = max(previous_watts, 0) * elapsed_seconds / 3600000`.
The first sample establishes a baseline; zero or negative previous power adds
no imported energy. `energy_total` is the sum of the three imported counters,
not the integral of signed net power. Each phase integrates independently.

Without `ENERGY_STATE_FILE`, counters reset to zero on every process restart.
With checkpoint storage enabled, the saved per-phase counters are restored and
their sum is recalculated. Initial restored or zero states are acknowledged before
accumulation begins. Counters survive MQTT reconnects, but connection changes
clear integration baselines. Retained replay, stale intervals, non-positive time
intervals, and downtime do not add energy; the next live sample reestablishes the
baseline. This intentionally undercounts during gaps rather than inventing usage.
The estimate assumes power stays constant until the next valid sample and is
not a replacement for a billing meter.

Energy states use kWh with Home Assistant's `total_increasing` state class and
are retained in MQTT so discovery subscribers can obtain the latest counters,
including the initial restored or zero values. The relay never restores counters
from MQTT. Retained broker state is only a published snapshot, not persistence
for the relay.
Snapshots are republished on reconnect and live power updates; publication errors
are logged and initialization cannot announce online if its energy snapshot fails.

### Energy checkpoints

The Pi service enables `/var/lib/guardrelay/energy.json`. Systemd creates its
directory owned by `guardrelay` with mode `0700`; checkpoint files use mode `0600`.
Native/local runs remain memory-only unless `ENERGY_STATE_FILE` is set to a
writable path, for example `./state/energy.json`. Set `ENERGY_STATE_FILE=""` in
`/etc/default/guardrelay` to disable checkpoints for the service.

The relay saves changed counters **every 10 minutes and on graceful shutdown**.
A missing checkpoint starts at zero and is created before MQTT connects.
Unchanged counters do not trigger rewrites. Each save synchronizes a temporary
file in the same directory, atomically replaces the single JSON file, and
synchronizes the directory. The file does not grow over time; temporary files
from successful saves are not retained. An abrupt kill during a save can leave
a small temporary file, which is not used for restoration.

Only per-phase imported-energy counters and their meter identity are saved.
Phase readings and timestamps are never restored, so downtime cannot be
backfilled. Corrupt, incompatible, or differently identified checkpoints fail
startup instead of silently resetting counters. Checkpoint write failures stop
the relay with an error rather than silently operating without persistence.
Changing `HA_DEVICE_ID`, `SMARTPI_PREFIX`, or `MQTT_BASE_TOPIC` requires a separate
checkpoint path or an intentional reset: stop the service and remove the old file.

A graceful restart preserves the latest counters. Abrupt power loss can roll
them back to the last checkpoint, losing up to 10 minutes of accumulated usage.
Home Assistant may interpret that decrease as a counter reset, affecting its
statistics. Checkpoints do not account for usage while the relay is stopped.
This trades a small number of SD-card writes for limited crash-time loss; OS,
journal, and MQTT-broker persistence remain separate.

### Discovery migration

On each connection, the relay removes retained discovery configurations for
`current_total`, `voltage_total`, and `phase_angle_l1`-`phase_angle_l3`, then
publishes the corrected entities. Cleanup uses the configured `HA_DEVICE_ID` and
`HA_DISCOVERY_PREFIX`; it does not remove entities belonging to other devices.

Replace dashboard/automation references to the old current and phase-angle
entities with the new fourth-channel current and power-factor entities as
appropriate. Existing phase current, voltage, and power topics and unique IDs
are unchanged. If you change the device ID or discovery prefix, configurations
under the previous identity must be cleaned up separately.


### Running Locally

```bash
export MQTT_URL="mqtt://192.168.1.10:1883"
export MQTT_CLIENT_ID="guardrelay-ampguard"
export MQTT_BASE_TOPIC="guardrelay/ampguard"
export HA_DISCOVERY_PREFIX="homeassistant"
export HA_DEVICE_ID="chargeamps_amp_guard"
export HA_DEVICE_NAME="Charge Amps Amp Guard"
export SMARTPI_PREFIX="smartpi"

go run ./cmd/guardrelay-go
```

### Logs & Status
```bash
sudo systemctl status guardrelay
sudo journalctl -u guardrelay -f
```

## License

MIT
