package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

type powerHarness struct {
	relay     *smartPiRelay
	publisher *sensorPublisher
	now       time.Time
}

func newPowerHarness() *powerHarness {
	h := &powerHarness{
		relay: newSmartPiRelay(testConfig()), publisher: &sensorPublisher{},
		now: time.Unix(1000, 0),
	}
	h.relay.now = func() time.Time { return h.now }
	return h
}

func (h *powerHarness) send(input, payload string, retained bool) {
	h.relay.handleSmartPi(&paho.Publish{
		Topic: h.relay.cfg.SmartPiPrefix + "/" + input, Payload: []byte(payload), Retain: retained,
	}, context.Background(), h.publisher)
}

func (h *powerHarness) totals() []string {
	var states []string
	for _, packet := range h.publisher.packets {
		if packet.Topic == h.relay.cfg.MQTTBaseTopic+"/state/power_total" {
			states = append(states, string(packet.Payload))
		}
	}
	return states
}

func TestPowerAggregationArrivalOrderAndRepeatedUpdates(t *testing.T) {
	for _, order := range [][3]int{
		{1, 2, 3}, {1, 3, 2}, {2, 1, 3}, {2, 3, 1}, {3, 1, 2}, {3, 2, 1},
	} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			h := newPowerHarness()
			for i, phase := range order {
				h.send(fmt.Sprintf("P%d", phase), fmt.Sprint(phase*100), false)
				if i < 2 && len(h.totals()) != 0 {
					t.Fatal("published total before all phases arrived")
				}
			}
			h.send("P1", "150", false)
			h.send("P1", "175", false)
			want := []string{"600.0000", "650.0000", "675.0000"}
			if got := h.totals(); fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("latest fresh samples were not reused: %v", got)
			}
		})
	}
}

func TestPowerAggregationRequiresEveryPhase(t *testing.T) {
	h := newPowerHarness()
	h.send("P1", "100", false)
	h.send("P1", "150", false)
	h.send("P2", "200", false)
	h.send("P2", "250", false)
	if len(h.totals()) != 0 || len(h.publisher.packets) != 37 {
		t.Fatal("repeated phases substituted for a missing phase")
	}
}

func TestPowerAggregationFreshnessOfEveryPhase(t *testing.T) {
	for _, tt := range []struct {
		name        string
		age, limit  time.Duration
		expectTotal bool
	}{
		{"just fresh", 3*time.Second - time.Nanosecond, 3 * time.Second, true},
		{"at boundary", 3 * time.Second, 3 * time.Second, false},
		{"stale", 3*time.Second + time.Nanosecond, 3 * time.Second, false},
		{"custom limit", 5 * time.Second, 10 * time.Second, true},
		{"future timestamp", -time.Second, 3 * time.Second, false},
	} {
		for oldest := 1; oldest <= 3; oldest++ {
			t.Run(fmt.Sprintf("%s/P%d", tt.name, oldest), func(t *testing.T) {
				h := newPowerHarness()
				h.relay.cfg.PhaseFreshness = tt.limit
				h.send(fmt.Sprintf("P%d", oldest), "100", false)
				h.now = h.now.Add(tt.age)
				for phase := 1; phase <= 3; phase++ {
					if phase != oldest {
						h.send(fmt.Sprintf("P%d", phase), "100", false)
					}
				}
				if got := h.totals(); (len(got) == 1) != tt.expectTotal {
					t.Fatalf("unexpected freshness result: %v", got)
				}
			})
		}
	}
}

func TestPowerAggregationRecoversWhenStalePhaseRefreshes(t *testing.T) {
	h := newPowerHarness()
	h.send("P1", "100", false)
	h.now = h.now.Add(2 * time.Second)
	h.send("P2", "200", false)
	h.now = h.now.Add(2 * time.Second)
	h.send("P3", "300", false)
	if len(h.totals()) != 0 {
		t.Fatal("oldest phase was accepted as fresh")
	}
	h.send("P1", "150", false)
	if got := h.totals(); len(got) != 1 || got[0] != "650.0000" {
		t.Fatalf("fresh phase update did not restore aggregate: %v", got)
	}
}

func TestPowerAggregationIgnoresUnrelatedTraffic(t *testing.T) {
	h := newPowerHarness()
	h.send("P1", "100", false)
	h.send("P2", "200", false)
	h.send("P3", "300", false)
	before := h.relay.phases
	h.now = h.now.Add(4 * time.Second)
	for _, input := range []string{"I1", "I4", "V1", "COS3", "F3", "P1/extra", "status"} {
		h.send(input, "42", false)
	}
	if h.relay.phases != before || len(h.totals()) != 1 {
		t.Fatal("unrelated measurements refreshed or republished the total")
	}
	h.send("P1", "150", false)
	if len(h.totals()) != 1 {
		t.Fatal("unrelated traffic kept stale phase samples fresh")
	}
}

func TestPowerAggregationRetainedMessagesDoNotEstablishFreshness(t *testing.T) {
	h := newPowerHarness()
	for phase := 1; phase <= 3; phase++ {
		h.send(fmt.Sprintf("P%d", phase), fmt.Sprint(phase*100), true)
	}
	if len(h.publisher.packets) != 3 || len(h.totals()) != 0 ||
		h.relay.phases != [3]phaseSample{} {
		t.Fatal("retained readings were not forwarded exclusively as phase states")
	}
	h.send("P1", "100", false)
	h.send("P2", "200", false)
	if len(h.totals()) != 0 {
		t.Fatal("retained P3 filled a missing live phase")
	}
	h.send("P3", "300", false)
	before := h.relay.phases
	h.send("P1", "999", true)
	if h.relay.phases != before || len(h.totals()) != 1 {
		t.Fatal("retained replay modified a live baseline or emitted an aggregate")
	}
	h.send("P2", "250", false)
	if got := h.totals(); len(got) != 2 || got[1] != "650.0000" {
		t.Fatalf("retained value contaminated the next aggregate: %v", got)
	}
}

func TestPowerAggregationSignedValuesAndOverflow(t *testing.T) {
	h := newPowerHarness()
	h.send("P1", "-25", false)
	h.send("P2", "0", false)
	h.send("P3", "100", false)
	if got := h.totals(); len(got) != 1 || got[0] != "75.0000" {
		t.Fatalf("total did not preserve signed power: %v", got)
	}
	output := captureSensorLogs(t)
	h = newPowerHarness()
	h.send("P1", "1e308", false)
	h.send("P2", "1e308", false)
	h.send("P3", "1e308", false)
	if len(h.totals()) != 0 || !strings.Contains(output.String(), "power total is not finite") {
		t.Fatal("non-finite sum was published or not logged")
	}
	h.send("P1", "100", false)
	h.send("P2", "200", false)
	h.send("P3", "300", false)
	got := h.totals()
	if len(got) == 0 || got[len(got)-1] != "600.0000" {
		t.Fatalf("aggregation did not recover from overflow: %v", got)
	}
}

func TestPowerAggregationStateIsOwnedByEachRelay(t *testing.T) {
	first, second := newPowerHarness(), newPowerHarness()
	first.send("P1", "100", false)
	first.send("P2", "200", false)
	second.send("P3", "300", false)
	if len(first.totals()) != 0 || len(second.totals()) != 0 {
		t.Fatal("separate relays shared phase samples")
	}
	first.send("P3", "300", false)
	if len(first.totals()) != 1 || !second.relay.phases[0].receivedAt.IsZero() {
		t.Fatal("a relay modified another relay's samples")
	}
}

func TestPowerAggregationResetsOnDisconnectAndReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l := newMQTTLifecycle(ctx, testConfig())
	defer func() { cancel(); await(t, l.done) }()
	l.connectionUp(newFakeMQTT())
	oldCtx := l.connCtx
	for phase := 0; phase < 3; phase++ {
		l.relay.recordPower(oldCtx, phase, 100, time.Now())
	}
	l.connectionDown()
	if l.relay.phases != [3]phaseSample{} || oldCtx.Err() == nil {
		t.Fatal("disconnect did not invalidate samples and in-flight context")
	}
	l.relay.recordPower(oldCtx, 0, 999, time.Now())
	if l.relay.phases != [3]phaseSample{} {
		t.Fatal("canceled in-flight message repopulated cleared samples")
	}
	// Reconnect also invalidates any samples received before connection-up.
	l.relay.recordPower(ctx, 0, 999, time.Now())
	client := newFakeMQTT()
	l.connectionUp(client)
	await(t, client.online)
	if l.relay.phases != [3]phaseSample{} {
		t.Fatal("reconnect reused previous phase samples")
	}
}

func TestPowerAggregationCanceledMessagesDoNotUpdateOrPublish(t *testing.T) {
	h := newPowerHarness()
	h.send("P1", "100", false)
	h.send("P2", "200", false)
	h.send("P3", "300", false)
	before := h.relay.phases
	count := len(h.publisher.packets)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h.relay.handleSmartPi(&paho.Publish{Topic: "smartpi/P1", Payload: []byte("999")},
		ctx, h.publisher)
	if h.relay.phases != before || len(h.publisher.packets) != count {
		t.Fatal("canceled message updated or published state")
	}
	if _, ok := h.relay.totalPower(ctx); ok {
		t.Fatal("canceled connection produced a total")
	}
}

func TestPowerAggregationResetIsSafeDuringUpdates(t *testing.T) {
	relay := newSmartPiRelay(testConfig())
	now := time.Unix(1000, 0)
	relay.now = func() time.Time { return now }
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for i := 0; i < 100; i++ {
			relay.recordPower(context.Background(), i%3, float64(i), now)
			relay.totalPower(context.Background())
		}
	}()
	go func() {
		defer workers.Done()
		for i := 0; i < 100; i++ {
			relay.resetPhases()
		}
	}()
	workers.Wait()
}

type delayedPhasePublisher struct {
	*sensorPublisher
	advance func()
}

func (p *delayedPhasePublisher) Publish(ctx context.Context, packet *paho.Publish) (*paho.PublishResponse, error) {
	if strings.HasSuffix(packet.Topic, "/state/power_l3") {
		p.advance()
	}
	return p.sensorPublisher.Publish(ctx, packet)
}

func TestPowerAggregationUsesReceiptTimeAndRechecksAfterPublish(t *testing.T) {
	h := newPowerHarness()
	h.send("P1", "100", false)
	h.send("P2", "200", false)
	receivedAt := h.now
	publisher := &delayedPhasePublisher{sensorPublisher: h.publisher, advance: func() {
		h.now = h.now.Add(3 * time.Second)
	}}
	h.relay.handleSmartPi(&paho.Publish{Topic: "smartpi/P3", Payload: []byte("300")},
		context.Background(), publisher)
	if h.relay.phases[2].receivedAt != receivedAt || len(h.totals()) != 0 {
		t.Fatal("slow phase publication disguised stale receipt timestamps")
	}
}
