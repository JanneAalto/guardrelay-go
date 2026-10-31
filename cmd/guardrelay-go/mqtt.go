package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

const mqttOperationTimeout = 10 * time.Second

type mqttPublisher interface {
	Publish(context.Context, *paho.Publish) (*paho.PublishResponse, error)
}

type mqttSubscriber interface {
	mqttPublisher
	Subscribe(context.Context, *paho.Subscribe) (*paho.Suback, error)
}

type mqttSession interface {
	mqttSubscriber
	Disconnect(context.Context) error
	Done() <-chan struct{}
}

func publish(ctx context.Context, client mqttPublisher, topic string, payload []byte, retain bool) error {
	ctx, cancel := context.WithTimeout(ctx, mqttOperationTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("publish %s: %w", topic, err)
	}
	ack, err := client.Publish(ctx, &paho.Publish{Topic: topic, QoS: 1, Retain: retain, Payload: payload})
	if err != nil {
		return fmt.Errorf("publish %s: %w", topic, err)
	}
	if ack == nil {
		return fmt.Errorf("publish %s: missing acknowledgment", topic)
	}
	if ack.ReasonCode >= 0x80 {
		return fmt.Errorf("publish %s rejected: MQTT reason 0x%02x", topic, ack.ReasonCode)
	}
	return nil
}

func initializeMQTT(ctx context.Context, cfg *Config, client mqttSubscriber, relay *smartPiRelay) error {
	if err := publishAvailability(ctx, cfg, client, false); err != nil {
		return err
	}
	if err := relay.publishEnergy(ctx, client); err != nil {
		return err
	}
	subCtx, cancel := context.WithTimeout(ctx, mqttOperationTimeout)
	ack, err := client.Subscribe(subCtx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: cfg.SmartPiPrefix + "/#", QoS: 1}},
	})
	cancel()
	if err != nil {
		return fmt.Errorf("subscribe %s/#: %w", cfg.SmartPiPrefix, err)
	}
	if ack == nil || len(ack.Reasons) != 1 || ack.Reasons[0] > 1 {
		return fmt.Errorf("subscribe %s/#: missing or rejected QoS acknowledgment: %v", cfg.SmartPiPrefix, ack)
	}
	if err := publishDiscovery(ctx, cfg, client); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return publishAvailability(ctx, cfg, client, true)
}

type connectionEvent struct {
	ctx    context.Context
	client mqttSubscriber
}

type mqttLifecycle struct {
	ctx       context.Context
	cfg       *Config
	events    chan connectionEvent
	errs      chan error
	done      chan struct{}
	mu        sync.Mutex
	cancel    context.CancelFunc
	connCtx   context.Context
	connected bool
	receiveMu sync.Mutex
	relay     *smartPiRelay
}

func newMQTTLifecycle(ctx context.Context, cfg *Config) *mqttLifecycle {
	l := &mqttLifecycle{
		ctx: ctx, cfg: cfg, events: make(chan connectionEvent, 1),
		errs: make(chan error, 1), done: make(chan struct{}),
		relay: newSmartPiRelay(cfg),
	}
	go l.work()
	return l
}

func (l *mqttLifecycle) connectionUp(client mqttSubscriber) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.connected = true
	if l.ctx.Err() != nil {
		return
	}
	if l.cancel != nil {
		l.cancel()
	}
	l.relay.resetPhases()
	ctx, cancel := context.WithCancel(l.ctx)
	l.cancel = cancel
	l.connCtx = ctx
	// Only the newest connection needs initialization; one worker serializes it.
	select {
	case <-l.events:
	default:
	}
	l.events <- connectionEvent{ctx: ctx, client: client}
}

func (l *mqttLifecycle) connectionDown() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cancel != nil {
		l.cancel()
	}
	l.connected = false
	l.relay.resetPhases()
}

func (l *mqttLifecycle) work() {
	defer close(l.done)
	for {
		select {
		case <-l.ctx.Done():
			return
		case event := <-l.events:
			if event.ctx.Err() != nil {
				continue
			}
			err := initializeMQTT(event.ctx, l.cfg, event.client, l.relay)
			if event.ctx.Err() != nil {
				continue
			}
			if err != nil {
				select {
				case l.errs <- fmt.Errorf("MQTT initialization failed: %w", err):
				case <-l.ctx.Done():
				}
				return
			}
			log.Println("MQTT connected and ready")
		}
	}
}

func (l *mqttLifecycle) receive(pr paho.PublishReceived) (bool, error) {
	l.receiveMu.Lock()
	defer l.receiveMu.Unlock()
	if l.ctx.Err() != nil {
		return false, nil
	}
	if pr.Packet == nil || pr.Client == nil {
		return false, fmt.Errorf("MQTT receive callback is missing its packet or client")
	}
	l.mu.Lock()
	ctx := l.connCtx
	l.mu.Unlock()
	if ctx == nil {
		ctx = l.ctx
	}
	select {
	case <-pr.Client.Done():
		return false, nil
	default:
	}
	l.relay.handleSmartPi(pr.Packet, ctx, pr.Client)
	return true, nil
}

func mqttClientConfig(cfg *Config, l *mqttLifecycle) (autopaho.ClientConfig, error) {
	if err := cfg.validate(); err != nil {
		return autopaho.ClientConfig{}, err
	}
	u, err := brokerURL(cfg.MQTTURL)
	if err != nil {
		return autopaho.ClientConfig{}, err
	}
	username, password := "", ""
	if u.User != nil {
		username = u.User.Username()
		password, _ = u.User.Password()
	}
	// Credentials belong in CONNECT, not transport errors or debug URL logging.
	u.User = nil
	clientCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		ConnectUsername:               username,
		ConnectPassword:               []byte(password),
		KeepAlive:                     30,
		ConnectTimeout:                mqttOperationTimeout,
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         86400,
		Errors:                        log.Default(),
		PahoErrors:                    log.Default(),
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			l.connectionUp(cm)
		},
		OnConnectionDown: func() bool {
			l.connectionDown()
			log.Println("MQTT disconnected; reconnecting")
			return true
		},
		OnConnectError: func(err error) { log.Printf("MQTT connection error: %v", err) },
		// Publish the will even if shutdown races with initial connection setup.
		DisconnectPacketBuilder: func() *paho.Disconnect {
			return &paho.Disconnect{ReasonCode: 0x04}
		},
		WillMessage: &paho.WillMessage{
			Topic: cfg.MQTTBaseTopic + "/status", Payload: []byte("offline"), QoS: 1, Retain: true,
		},
		ClientConfig: paho.ClientConfig{
			ClientID: cfg.MQTTClientID, PacketTimeout: mqttOperationTimeout,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){l.receive},
		},
	}
	switch u.Scheme {
	case "ssl", "tls", "mqtts", "mqtt+ssl", "tcps", "wss":
		clientCfg.TlsCfg = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: u.Hostname()}
	}
	if cfg.LogLevel == "debug" {
		clientCfg.Debug = log.Default()
		clientCfg.PahoDebug = log.Default()
	}
	return clientCfg, nil
}

func shutdownMQTT(ctx context.Context, cfg *Config, client mqttSession, connected bool) error {
	// Reserve a separate timeout for disconnect even when offline publication fails.
	var pubErr error
	if connected {
		pubCtx, cancel := context.WithTimeout(ctx, mqttOperationTimeout)
		pubErr = publishAvailability(pubCtx, cfg, client, false)
		cancel()
	}
	disconnectCtx, cancel := context.WithTimeout(ctx, mqttOperationTimeout)
	defer cancel()
	disconnectErr := client.Disconnect(disconnectCtx)
	if disconnectErr != nil {
		disconnectErr = fmt.Errorf("MQTT disconnect: %w", disconnectErr)
	}
	return errors.Join(pubErr, disconnectErr)
}

func run(ctx context.Context, cfg *Config) error {
	lifeCtx, stopLife := context.WithCancel(ctx)
	defer stopLife()
	l := newMQTTLifecycle(lifeCtx, cfg)
	clientCfg, err := mqttClientConfig(cfg, l)
	if err != nil {
		stopLife()
		<-l.done
		return err
	}
	// Keep the transport alive after the signal so offline can be acknowledged.
	mqttCtx, stopMQTT := context.WithCancel(context.WithoutCancel(ctx))
	defer stopMQTT()
	cm, err := autopaho.NewConnection(mqttCtx, clientCfg)
	if err != nil {
		stopLife()
		<-l.done
		return err
	}
	select {
	case <-ctx.Done():
	case err = <-l.errs:
	case <-cm.Done():
		err = fmt.Errorf("MQTT connection manager stopped unexpectedly")
	}
	stopLife()
	<-l.done
	l.receiveMu.Lock()
	l.receiveMu.Unlock()
	l.mu.Lock()
	connected := l.connected
	l.mu.Unlock()
	return errors.Join(err, shutdownMQTT(context.WithoutCancel(ctx), cfg, cm, connected))
}
