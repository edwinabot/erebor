package symbol

type SymbolState int

const (
	Disconnected SymbolState = iota
	Bootstrapping
	Synced
	Resyncing
)

func (s SymbolState) String() string {
	switch s {
	case Disconnected:
		return "disconnected"
	case Bootstrapping:
		return "bootstrapping"
	case Synced:
		return "synced"
	case Resyncing:
		return "resyncing"
	default:
		return "unknown"
	}
}
