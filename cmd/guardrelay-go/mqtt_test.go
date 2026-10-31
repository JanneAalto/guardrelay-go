package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
)

type fakeMQTT struct {
	mu            sync.Mutex
	operations    []string
	publishAck    *paho.PublishResponse
	publishErr    error
	failPrefix    string
	subAck        *paho.Suback
	subErr        error
	disconnectErr error
	invalidCtx    bool
	subStarted    chan struct{}
	blockSub      bool
	online        chan struct{}
	closed        chan struct{}
}

func newFakeMQTT() *fakeMQTT {
	return &fakeMQTT{
		publishAck: &paho.PublishResponse{}, subAck: &paho.Suback{Reasons: []byte{1}},
		online: make(chan struct{}, 1), closed: make(chan struct{}),
	}
}

func (f *fakeMQTT) record(ctx context.Context, operation string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	deadline, ok := ctx.Deadline()
	if ctx.Err() != nil || !ok || time.Until(deadline) > mqttOperationTimeout {
		f.invalidCtx = true
	}
	f.operations = append(f.operations, operation)
}

func (f *fakeMQTT) Publish(ctx context.Context, p *paho.Publish) (*paho.PublishResponse, error) {
	f.record(ctx, p.Topic+"="+string(p.Payload))
	if string(p.Payload) == "online" {
		select {
		case f.online <- struct{}{}:
		default:
		}
	}
	if p.QoS != 1 || strings.HasSuffix(p.Topic, "/status") && !p.Retain {
		return nil, errors.New("incorrect publish options")
	}
	if f.failPrefix != "" && strings.HasPrefix(p.Topic, f.failPrefix) {
		return &paho.PublishResponse{ReasonCode: 0x87}, nil
	}
	return f.publishAck, f.publishErr
}

func (f *fakeMQTT) Subscribe(ctx context.Context, s *paho.Subscribe) (*paho.Suback, error) {
	f.record(ctx, "subscribe "+s.Subscriptions[0].Topic)
	if f.subStarted != nil {
		close(f.subStarted)
	}
	if f.blockSub {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.subAck, f.subErr
}

func (f *fakeMQTT) Disconnect(ctx context.Context) error {
	f.record(ctx, "disconnect")
	close(f.closed)
	return f.disconnectErr
}

func (f *fakeMQTT) Done() <-chan struct{} { return f.closed }

func (f *fakeMQTT) snapshot() ([]string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.operations...), f.invalidCtx
}

func TestPublishAcknowledgments(t *testing.T) {
	for _, tt := range []struct {
		name string
		ack  *paho.PublishResponse
		err  error
		ok   bool
	}{
		{"success", &paho.PublishResponse{}, nil, true},
		{"no subscribers", &paho.PublishResponse{ReasonCode: 0x10}, nil, true},
		{"rejected", &paho.PublishResponse{ReasonCode: 0x87}, nil, false},
		{"missing", nil, nil, false},
		{"transport", nil, errors.New("connection lost"), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeMQTT()
			f.publishAck, f.publishErr = tt.ack, tt.err
			err := publish(context.Background(), f, "test/state", []byte("42"), false)
			if (err == nil) != tt.ok {
				t.Fatalf("unexpected result: %v", err)
			}

			if _, invalid := f.snapshot(); invalid {
				t.Fatal("publish was not bounded")
			}
		})
	}
}

func TestPublishCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := newFakeMQTT()
	if err := publish(ctx, f, "test/state", []byte("42"), false); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation not surfaced: %v", err)
	}
	if ops, _ := f.snapshot(); len(ops) != 0 {
		t.Fatal("attempted publication after cancellation")
	}
}

func TestInitializeMQTTOrdering(t *testing.T) {
	cfg, f := testConfig(), newFakeMQTT()
	if err := initializeMQTT(context.Background(), cfg, f); err != nil {
		t.Fatal(err)
	}
	ops, invalid := f.snapshot()
	if invalid {
		t.Fatal("unbounded initialization operation")
	}
	if ops[0] != cfg.MQTTBaseTopic+"/status=offline" || ops[1] != "subscribe smartpi/#" ||
		ops[len(ops)-1] != cfg.MQTTBaseTopic+"/status=online" ||
		len(ops) != len(discoverySensors(cfg))+len(obsoleteSensorKeys)+3 {
		t.Fatalf("unexpected initialization ordering: %v", ops)
	}
}

func TestInitializeMQTTFailures(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*fakeMQTT)
	}{
		{"offline rejected", func(f *fakeMQTT) { f.publishAck.ReasonCode = 0x87 }},
		{"discovery rejected", func(f *fakeMQTT) { f.failPrefix = "homeassistant/" }},
		{"subscribe transport", func(f *fakeMQTT) { f.subErr = errors.New("lost connection") }},
		{"subscribe rejected", func(f *fakeMQTT) { f.subAck.Reasons = []byte{0x87} }},
		{"subscribe unexpected QoS", func(f *fakeMQTT) { f.subAck.Reasons = []byte{2} }},
		{"subscribe empty", func(f *fakeMQTT) { f.subAck.Reasons = nil }},
		{"subscribe missing", func(f *fakeMQTT) { f.subAck = nil }},
		{"subscribe wrong count", func(f *fakeMQTT) { f.subAck.Reasons = []byte{1, 1} }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeMQTT()
			tt.change(f)
			if err := initializeMQTT(context.Background(), testConfig(), f); err == nil {
				t.Fatal("initialization failure was ignored")
			}
			ops, _ := f.snapshot()
			for _, op := range ops {
				if strings.HasSuffix(op, "=online") {
					t.Fatal("announced online after initialization failure")
				}
			}
		})
	}
}

func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for lifecycle event")
	}
}

func TestLifecycleReconnectCancelsObsoleteSetup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l := newMQTTLifecycle(ctx, testConfig())
	defer func() { cancel(); await(t, l.done) }()
	old := newFakeMQTT()
	old.subStarted, old.blockSub = make(chan struct{}), true
	returned := make(chan struct{})
	go func() { l.connectionUp(old); close(returned) }()
	await(t, returned)
	await(t, old.subStarted)
	l.connectionDown()
	next := newFakeMQTT()
	l.connectionUp(next)
	await(t, next.online)
	oldOps, _ := old.snapshot()
	for _, op := range oldOps {
		if strings.HasSuffix(op, "=online") {
			t.Fatal("obsolete connection announced online")
		}
	}
	select {
	case err := <-l.errs:
		t.Fatalf("canceled initialization treated as failure: %v", err)
	default:
	}
	l.connectionDown()
	third := newFakeMQTT()
	l.connectionUp(third)
	await(t, third.online)
}

func TestLifecycleReportsInitializationFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l := newMQTTLifecycle(ctx, testConfig())
	defer func() { cancel(); await(t, l.done) }()
	f := newFakeMQTT()
	f.subErr = errors.New("subscription failed")
	l.connectionUp(f)
	select {
	case err := <-l.errs:
		if !strings.Contains(err.Error(), "subscription failed") {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("initialization failure was not surfaced")
	}
}

func TestShutdownMQTT(t *testing.T) {
	for _, connected := range []bool{false, true} {
		f := newFakeMQTT()
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := shutdownMQTT(context.WithoutCancel(canceledCtx), testConfig(), f, connected); err != nil {
			t.Fatal(err)
		}
		ops, invalid := f.snapshot()
		if invalid || ops[len(ops)-1] != "disconnect" {
			t.Fatalf("invalid shutdown: %v", ops)
		}
		if connected && (len(ops) != 2 || ops[0] != "guardrelay/ampguard/status=offline") {
			t.Fatalf("offline not published before disconnect: %v", ops)
		}
		if !connected && len(ops) != 1 {
			t.Fatal("attempted offline publication before a connection existed")
		}
	}
}

func TestShutdownMQTTReportsBothErrors(t *testing.T) {
	f := newFakeMQTT()
	f.publishErr, f.disconnectErr = errors.New("offline failed"), errors.New("disconnect failed")
	err := shutdownMQTT(context.Background(), testConfig(), f, true)
	if !errors.Is(err, f.publishErr) || !errors.Is(err, f.disconnectErr) {
		t.Fatalf("shutdown errors were lost: %v", err)
	}
}

func TestRunCanceledDuringInitialConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := run(ctx, testConfig()); err != nil {
		t.Fatalf("normal cancellation failed: %v", err)
	}
}

func TestRunWithLocalMQTTBroker(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := testConfig()
	cfg.MQTTURL = "mqtt://" + listener.Addr().String()
	brokerResult := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			brokerResult <- err
			return
		}
		defer conn.Close()
		if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			brokerResult <- err
			return
		}
		brokerResult <- serveLifecycleBroker(conn, cfg, cancel)
	}()
	runResult := make(chan error, 1)
	go func() { runResult <- run(ctx, cfg) }()
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("relay did not complete local MQTT startup and shutdown")
	}
	select {
	case err := <-brokerResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("broker did not receive graceful DISCONNECT")
	}
}

func serveLifecycleBroker(conn net.Conn, cfg *Config, cancel context.CancelFunc) error {
	var online, measurement, offlineAfterOnline bool
	discoveryCount := 0
	for {
		packet, err := packets.ReadPacket(conn)
		if err != nil {
			return err
		}
		var response *packets.ControlPacket
		switch packet := packet.Content.(type) {
		case *packets.Connect:
			response = packets.NewControlPacket(packets.CONNACK)
		case *packets.Subscribe:
			response = packets.NewControlPacket(packets.SUBACK)
			ack := response.Content.(*packets.Suback)
			ack.PacketID, ack.Reasons = packet.PacketID, []byte{1}
			if _, err := response.WriteTo(conn); err != nil {
				return err
			}
			// Deliver immediately: the receive callback must not depend on a
			// connection-manager pointer assigned after startup.
			input := &packets.Publish{
				Topic: cfg.SmartPiPrefix + "/I1", Payload: []byte("12.5"),
				Properties: &packets.Properties{},
			}
			if _, err := input.WriteTo(conn); err != nil {
				return err
			}
			continue
		case *packets.Publish:
			switch packet.Topic {
			case cfg.MQTTBaseTopic + "/status":
				if !packet.Retain {
					return fmt.Errorf("availability is not retained")
				}
				if string(packet.Payload) == "online" {
					if discoveryCount != len(discoverySensors(cfg)) {
						return fmt.Errorf("online before discovery completed")
					}
					online = true
				} else if online && string(packet.Payload) == "offline" {
					offlineAfterOnline = true
				}
			case cfg.MQTTBaseTopic + "/state/current_l1":
				if string(packet.Payload) != "12.5000" {
					return fmt.Errorf("unexpected measurement: %q", packet.Payload)
				}
				measurement = true
			default:
				if strings.HasPrefix(packet.Topic, cfg.HADiscoveryPref+"/sensor/") && packet.Retain && len(packet.Payload) != 0 {
					discoveryCount++
				}
			}
			response = packets.NewControlPacket(packets.PUBACK)
			response.Content.(*packets.Puback).PacketID = packet.PacketID
		case *packets.Pingreq:
			response = packets.NewControlPacket(packets.PINGRESP)
		case *packets.Disconnect:
			if packet.ReasonCode != 0x04 {
				return fmt.Errorf("DISCONNECT does not request offline will publication")
			}
			if !online || !measurement || !offlineAfterOnline {
				return fmt.Errorf("incomplete lifecycle: online=%v measurement=%v offline=%v", online, measurement, offlineAfterOnline)
			}
			return nil
		default:
			return fmt.Errorf("unexpected packet type %T", packet)
		}
		if _, err := response.WriteTo(conn); err != nil {
			return err
		}
		if online && measurement {
			cancel()
		}
	}
}
