package failover

// Action describes the result of a failover evaluation.
type Action string

const (
	ActionNone                 Action = "none"
	ActionFailoverToSecondary  Action = "failover_to_secondary"
	ActionFailoverToPrimary    Action = "failover_to_primary"
)
