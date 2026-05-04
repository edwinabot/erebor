package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"go.uber.org/zap"
)

// RawDiffEvent mirrors the Binance combined-stream wire format for a depth
// update. It is internal to the stream package and must not appear in any
// other package's public API.
type RawDiffEvent struct {
	Stream string         `json:"stream"`
	Data   RawDiffPayload `json:"data"`
}

type RawDiffPayload struct {
	EventType     string     `json:"e"`
	EventTimeMS   int64      `json:"E"`
	Symbol        string     `json:"s"`
	FirstUpdateID int64      `json:"U"`
	FinalUpdateID int64      `json:"u"`
	Bids          [][]string `json:"b"`
	Asks          [][]string `json:"a"`
}

type StreamManager interface {
	Connect(ctx context.Context) error
	Events() <-chan RawDiffEvent
	Close() error
}

type Config struct {
	BaseURL      string
	Symbols      []string
	BufferSize   int
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

type Manager struct {
	cfg    Config
	logger *zap.Logger

	events chan RawDiffEvent
	rng    *rand.Rand

	mu     sync.Mutex
	conn   *websocket.Conn
	cancel context.CancelFunc
	closed bool
	wg     sync.WaitGroup
}

func New(cfg Config, logger *zap.Logger) *Manager {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1024
	}
	if cfg.InitialDelay <= 0 {
		cfg.InitialDelay = time.Second
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 30 * time.Second
	}
	return &Manager{
		cfg:    cfg,
		logger: logger.With(zap.String("component", "stream")),
		events: make(chan RawDiffEvent, cfg.BufferSize),
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (m *Manager) Events() <-chan RawDiffEvent {
	return m.events
}

func (m *Manager) Connect(ctx context.Context) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return errors.New("stream manager closed")
	}
	runCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.mu.Unlock()

	m.wg.Add(1)
	go m.runLoop(runCtx)
	return nil
}

func (m *Manager) runLoop(ctx context.Context) {
	defer m.wg.Done()

	delay := m.cfg.InitialDelay
	for {
		if ctx.Err() != nil {
			return
		}
		err := m.runOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			m.logger.Warn("websocket session ended",
				zap.Error(err),
				zap.Duration("backoff", delay),
			)
		}
		jitter := time.Duration(m.rng.Int63n(int64(delay)))
		wait := delay + jitter/2
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		delay *= 2
		if delay > m.cfg.MaxDelay {
			delay = m.cfg.MaxDelay
		}
	}
}

func (m *Manager) runOnce(ctx context.Context) error {
	wsURL, err := buildURL(m.cfg.BaseURL, m.cfg.Symbols)
	if err != nil {
		return fmt.Errorf("build url: %w", err)
	}

	dialCtx, dialCancel := context.WithTimeout(ctx, 15*time.Second)
	defer dialCancel()

	conn, _, err := websocket.Dial(dialCtx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	conn.SetReadLimit(1 << 20)

	m.mu.Lock()
	m.conn = conn
	m.mu.Unlock()

	defer func() {
		_ = conn.Close(websocket.StatusNormalClosure, "shutdown")
		m.mu.Lock()
		m.conn = nil
		m.mu.Unlock()
	}()

	m.logger.Info("websocket connected", zap.Int("symbols", len(m.cfg.Symbols)))

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		var raw RawDiffEvent
		if err := json.Unmarshal(data, &raw); err != nil {
			// Do not log raw frame payload (security posture).
			m.logger.Warn("failed to decode frame", zap.Error(err))
			continue
		}
		if raw.Data.EventType == "" {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m.events <- raw:
		}
	}
}

func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	cancel := m.cancel
	conn := m.conn
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if conn != nil {
		_ = conn.Close(websocket.StatusNormalClosure, "close")
	}
	m.wg.Wait()
	close(m.events)
	return nil
}

func buildURL(base string, symbols []string) (string, error) {
	if len(symbols) == 0 {
		return "", errors.New("at least one symbol required")
	}
	parts := make([]string, 0, len(symbols))
	for _, s := range symbols {
		parts = append(parts, strings.ToLower(s)+"@depth")
	}
	u, err := url.Parse(strings.TrimRight(base, "/") + "/stream")
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("streams", strings.Join(parts, "/"))
	u.RawQuery = q.Encode()
	return u.String(), nil
}
