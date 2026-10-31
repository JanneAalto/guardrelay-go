package deploy

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type deploymentHarness struct {
	t       *testing.T
	task    string
	project string
	root    string
	dist    string
	env     []string
}

func newDeploymentHarness(t *testing.T) *deploymentHarness {
	t.Helper()
	task, err := exec.LookPath("task")
	if err != nil {
		t.Skip("Task is required for deployment contract tests")
	}
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is required for deployment contract tests")
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	h := &deploymentHarness{
		t: t, task: task, project: filepath.Dir(cwd),
		root: t.TempDir(),
	}
	h.dist = filepath.Join(h.root, "dist with spaces")
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		switch key {
		case "PATH", "MQTT_URL", "HOST", "ARCH", "SMARTPI_PREFIX", "DIST_DIR",
			"DEPLOY_TEST_ROOT", "DEPLOY_TEST_FAIL":
			continue
		}
		h.env = append(h.env, entry)
	}
	h.env = append(h.env,
		"PATH="+filepath.Join(h.root, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"),
		"DEPLOY_TEST_ROOT="+h.root,
	)
	stubs := map[string]string{
		"ssh": `
[ "$#" -eq 2 ] && [ "$1" = test-host ] && [ "$2" = 'sudo -n bash -s' ] || exit 91
echo ssh >> "$DEPLOY_TEST_ROOT/log"
if [ "${DEPLOY_TEST_FAIL:-}" = ssh ]; then echo 'sudo: permission denied' >&2; exit 7; fi
# Redirect only this task's fixed remote paths into the disposable test root.
sed -e "s|/tmp/guardrelay|$DEPLOY_TEST_ROOT/tmp/guardrelay|g" \
    -e "s|/etc/|$DEPLOY_TEST_ROOT/etc/|g" \
    -e "s|/usr/local/bin/|$DEPLOY_TEST_ROOT/usr/local/bin/|g" | bash -s
`,
		"scp": `
[ "$#" -eq 2 ] || exit 91
case "$2" in test-host:/tmp/guardrelay*) ;; *) exit 91;; esac
echo "scp $1" >> "$DEPLOY_TEST_ROOT/log"
if [ "${DEPLOY_TEST_FAIL:-}" = scp ]; then exit 7; fi
mkdir -p "$DEPLOY_TEST_ROOT/tmp"
cp "$1" "$DEPLOY_TEST_ROOT/tmp/$(basename "$2")"
`,
		"go": `
echo "build $GOOS/$GOARCH/${GOARM:-}" >> "$DEPLOY_TEST_ROOT/log"
while [ "$#" -gt 0 ]; do
  if [ "$1" = -o ]; then
    shift
    printf 'test binary\n' > "$1"
    exit 0
  fi
  shift
done
exit 91
`,
		"file": `test -s "$1"`,
		"getent": `
[ "$2" = guardrelay ] || exit 91
test -f "$DEPLOY_TEST_ROOT/$1"
`,
		"groupadd": `
echo "groupadd $*" >> "$DEPLOY_TEST_ROOT/log"
if [ "${DEPLOY_TEST_FAIL:-}" = groupadd ]; then exit 7; fi
touch "$DEPLOY_TEST_ROOT/group"
`,
		"useradd": `
echo "useradd $*" >> "$DEPLOY_TEST_ROOT/log"
if [ "${DEPLOY_TEST_FAIL:-}" = useradd ]; then exit 7; fi
touch "$DEPLOY_TEST_ROOT/passwd"
`,
		"install": `
echo "install $*" >> "$DEPLOY_TEST_ROOT/log"
if [ "${DEPLOY_TEST_FAIL:-}" = install ]; then exit 7; fi
[ "$#" -eq 8 ] && [ "$1" = -m ] && [ "$3" = -o ] && [ "$4" = root ] \
  && [ "$5" = -g ] && [ "$6" = root ] || exit 91
mode="$2"
src="$7"
dst="$8"
case "$dst" in "$DEPLOY_TEST_ROOT"/*) ;; *) exit 91;; esac
mkdir -p "$(dirname "$dst")"
cp "$src" "$dst"
chmod "$mode" "$dst"
`,
		"systemctl": `
echo "systemctl $*" >> "$DEPLOY_TEST_ROOT/log"
if [ "$1" = "${DEPLOY_TEST_FAIL:-}" ]; then exit 7; fi
`,
	}
	for name, script := range stubs {
		h.write("bin/"+name, "#!/bin/bash\nset -euo pipefail\n"+script, 0755)
	}
	return h
}

func (h *deploymentHarness) write(path, contents string, mode os.FileMode) {
	h.t.Helper()
	path = filepath.Join(h.root, path)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		h.t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), mode); err != nil {
		h.t.Fatal(err)
	}
}

func (h *deploymentHarness) read(path string) string {
	h.t.Helper()
	data, err := os.ReadFile(filepath.Join(h.root, path))
	if err != nil {
		h.t.Fatal(err)
	}
	return string(data)
}

func (h *deploymentHarness) run(wantSuccess bool, args ...string) string {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, h.task, append([]string{
		"--dir", h.project, "--taskfile", filepath.Join(h.project, "Taskfile.yml"),
		"DIST_DIR=" + h.dist,
	}, args...)...)
	cmd.Env = h.env
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil || (err == nil) != wantSuccess {
		h.t.Fatalf("task %v: error=%v, timeout=%v\n%s", args, err, ctx.Err(), output)
	}
	return string(output)
}

func TestDeploymentRequiredVariables(t *testing.T) {
	for _, args := range [][]string{
		{"gen:service"},
		{"gen:service", "MQTT_URL="},
		{"gen:service", "MQTT_URL=   "},
		{"deploy:pi-armv7", "HOST=test-host"},
		{"deploy:pi-arm64", "HOST=test-host"},
		{"deploy:pi-arm64", "HOST=test-host", "MQTT_URL="},
		{"deploy:pi-arm64", "MQTT_URL=mqtt://broker.invalid:1883"},
		{"install:pi"},
		{"install:pi", "HOST="},
		{"update:pi", "HOST=test-host"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			h := newDeploymentHarness(t)
			output := h.run(false, args...)
			if !strings.Contains(output, "required") {
				t.Fatalf("expected missing-variable diagnostic:\n%s", output)
			}
			if _, err := os.Stat(filepath.Join(h.root, "log")); !os.IsNotExist(err) {
				t.Fatalf("required-variable failure performed deployment operations: %v", err)
			}
		})
	}
}

func TestServiceDefaults(t *testing.T) {
	h := newDeploymentHarness(t)
	url := "mqtt://broker.invalid:1883/path?literal=$value'\""
	h.env = append(h.env, "MQTT_URL="+url)
	output := h.run(true, "gen:service", "SMARTPI_PREFIX=custom/smartpi")
	path := filepath.Join(h.dist, "service/guardrelay.defaults")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{
		fmt.Sprintf("MQTT_URL=%q\n", url), `SMARTPI_PREFIX="custom/smartpi"`,
		`HA_DEVICE_ID="chargeamps_amp_guard"`, `PHASE_FRESHNESS="10s"`,
		`ENERGY_STATE_FILE="/var/lib/guardrelay/energy.json"`,
	} {
		if !strings.Contains(string(data), line) {
			t.Errorf("defaults missing %q", line)
		}
	}
	if strings.Contains(output, "$value") {
		t.Fatal("generation printed the broker URL")
	}
	unit, err := os.ReadFile(filepath.Join(h.dist, "service/guardrelay.service"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(unit), "EnvironmentFile=/etc/default/guardrelay\n") ||
		!strings.Contains(string(unit), "StateDirectory=guardrelay\n") ||
		!strings.Contains(string(unit), "StateDirectoryMode=0700\n") ||
		!strings.Contains(string(unit), "Environment=ENERGY_STATE_FILE=/var/lib/guardrelay/energy.json\n") {
		t.Fatal("unit must require configuration and provision protected checkpoint storage")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("defaults mode = %o, want 600", info.Mode().Perm())
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	h.run(true, "gen:service")
	info, err = os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("regenerated defaults permissions not restricted: %v", err)
	}
}

func TestDeploymentArchitectures(t *testing.T) {
	for _, arch := range []string{"armv7", "arm64"} {
		t.Run(arch, func(t *testing.T) {
			h := newDeploymentHarness(t)
			h.run(true, "deploy:pi-"+arch, "HOST=test-host", "MQTT_URL=mqtt://broker.invalid:1883")
			log := h.read("log")
			target := "linux/arm64/"
			if arch == "armv7" {
				target = "linux/arm/7"
			}
			if !strings.Contains(log, "build "+target) || strings.Count(log, "scp ") != 3 {
				t.Fatalf("wrong build/upload contract:\n%s", log)
			}
			h.run(true, "update:pi", "HOST=test-host", "ARCH="+arch)
			if h.read("usr/local/bin/guardrelay") != "test binary\n" {
				t.Fatal("update did not install the selected binary")
			}
			log = h.read("log")
			for _, action := range []string{"stop", "start", "is-active", "status"} {
				if !strings.Contains(log, "systemctl "+action) {
					t.Errorf("update missing systemctl %s", action)
				}
			}
		})
	}
}

func TestInstallPreservesConfiguration(t *testing.T) {
	h := newDeploymentHarness(t)
	stage := func() {
		h.write("tmp/guardrelay", "test binary\n", 0600)
		h.write("tmp/guardrelay.service", "test unit\n", 0600)
		h.write("tmp/guardrelay.defaults", "MQTT_URL=mqtt://broker.invalid:1883\n", 0600)
	}
	stage()
	h.run(true, "install:pi", "HOST=test-host")
	info, err := os.Stat(filepath.Join(h.root, "etc/default/guardrelay"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("installed defaults permissions: %v", err)
	}
	h.write("etc/default/guardrelay", "custom configuration\n", 0600)
	stage()
	h.run(true, "install:pi", "HOST=test-host")
	if h.read("etc/default/guardrelay") != "custom configuration\n" {
		t.Fatal("reinstallation replaced existing configuration")
	}
	log := h.read("log")
	if strings.Count(log, "groupadd ") != 1 || strings.Count(log, "useradd ") != 1 ||
		!strings.Contains(log, "useradd --system --gid guardrelay --no-create-home --shell /usr/sbin/nologin") {
		t.Fatalf("service account was not provisioned idempotently:\n%s", log)
	}
	for _, name := range []string{"guardrelay", "guardrelay.service", "guardrelay.defaults"} {
		if _, err := os.Stat(filepath.Join(h.root, "tmp", name)); !os.IsNotExist(err) {
			t.Errorf("uploaded %s not removed: %v", name, err)
		}
	}
}

func TestDeploymentFailures(t *testing.T) {
	t.Run("invalid architecture", func(t *testing.T) {
		h := newDeploymentHarness(t)
		output := h.run(false, "update:pi", "HOST=test-host", "ARCH=amd64")
		if !strings.Contains(output, "ARCH must be armv7 or arm64") {
			t.Fatal(output)
		}
	})
	t.Run("missing local binary", func(t *testing.T) {
		h := newDeploymentHarness(t)
		output := h.run(false, "update:pi", "HOST=test-host", "ARCH=arm64")
		if !strings.Contains(output, "Missing or empty binary") {
			t.Fatal(output)
		}
	})
	t.Run("missing remote files", func(t *testing.T) {
		h := newDeploymentHarness(t)
		output := h.run(false, "install:pi", "HOST=test-host")
		if !strings.Contains(output, "Missing or empty deployment file") ||
			strings.Contains(h.read("log"), "install ") {
			t.Fatal(output)
		}
	})
	for _, action := range []string{"scp", "ssh", "stop", "install", "start", "is-active", "status"} {
		t.Run("failed update "+action, func(t *testing.T) {
			h := newDeploymentHarness(t)
			h.write("dist with spaces/linux-arm64/guardrelay", "binary\n", 0600)
			h.env = append(h.env, "DEPLOY_TEST_FAIL="+action)
			h.run(false, "update:pi", "HOST=test-host", "ARCH=arm64")
		})
	}
	for _, action := range []string{"ssh", "groupadd", "useradd", "install", "daemon-reload", "enable", "restart", "is-active", "status"} {
		t.Run("failed install "+action, func(t *testing.T) {
			h := newDeploymentHarness(t)
			h.write("tmp/guardrelay", "binary\n", 0600)
			h.write("tmp/guardrelay.service", "unit\n", 0600)
			h.write("tmp/guardrelay.defaults", "defaults\n", 0600)
			h.env = append(h.env, "DEPLOY_TEST_FAIL="+action)
			h.run(false, "install:pi", "HOST=test-host")
		})
	}
}
