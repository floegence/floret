package runtime

import "github.com/floegence/floret/config"

type ReasoningLevel = config.ReasoningLevel

const (
	ReasoningLevelDefault = config.ReasoningLevelDefault
	ReasoningLevelOff     = config.ReasoningLevelOff
	ReasoningLevelMinimal = config.ReasoningLevelMinimal
	ReasoningLevelLow     = config.ReasoningLevelLow
	ReasoningLevelMedium  = config.ReasoningLevelMedium
	ReasoningLevelHigh    = config.ReasoningLevelHigh
	ReasoningLevelXHigh   = config.ReasoningLevelXHigh
	ReasoningLevelMax     = config.ReasoningLevelMax
)

type ReasoningSelection = config.ReasoningSelection
type ReasoningBudget = config.ReasoningBudget
type ReasoningCapability = config.ReasoningCapability
