package domain

import "time"

// SessionStatus is the lifecycle state of a paper trading session.
type SessionStatus string

const (
	SessionRunning SessionStatus = "RUNNING"
	SessionStopped SessionStatus = "STOPPED"
	SessionHalted  SessionStatus = "HALTED"
)

// PaperSession tracks a continuous paper trading run.
type PaperSession struct {
	SessionID      string
	Status         SessionStatus
	Symbols        []string
	StrategyConfig string // raw JSON; same schema as backtest strategy_config
	StartedAt      time.Time
	StoppedAt      *time.Time
	Error          string
}
