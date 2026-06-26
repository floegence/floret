package engine

import (
	"fmt"
	"strings"

	"github.com/floegence/floret/internal/control"
	"github.com/floegence/floret/internal/event"
	"github.com/floegence/floret/internal/provider"
	"github.com/floegence/floret/observation"
)

type ControlDisposition string

const (
	// ControlContinue asks the engine to append OutputText as a provider-visible
	// synthetic tool result and continue the run.
	ControlContinue ControlDisposition = "continue"
	ControlWaiting  ControlDisposition = "waiting"
	ControlTerminal ControlDisposition = "terminal"
)

type ControlSignal struct {
	Disposition ControlDisposition
	Name        string
	CallID      string
	Payload     map[string]any
	Activity    *observation.ActivityPresentation
	// OutputText is the human-readable control result. For ControlContinue it is
	// provider-visible; host-only details must stay in Payload.
	OutputText string
	ArgsHash   string
	Labels     map[string]string
}

type ControlSpec struct {
	Definitions []provider.ToolDefinition
	Project     func(provider.ToolCall) (ControlSignal, bool, error)
}

type controlProjectionContext struct {
	StepText string
}

func DefaultControlSpec(policy CompletionPolicy) ControlSpec {
	return ControlSpec{
		Definitions: control.ToolDefinitions(policy == CompletionExplicitSignal),
		Project: func(call provider.ToolCall) (ControlSignal, bool, error) {
			sig, ok, err := control.Project(call)
			if err != nil || !ok {
				return ControlSignal{}, ok, err
			}
			out := ControlSignal{
				Name:       call.Name,
				CallID:     call.ID,
				ArgsHash:   providerStableHash(call.Args),
				OutputText: sig.Output,
			}
			switch sig.Kind {
			case control.SignalAskUser:
				out.Disposition = ControlWaiting
				out.OutputText = sig.Prompt
			case control.SignalTaskComplete:
				if policy != CompletionExplicitSignal {
					return ControlSignal{}, false, nil
				}
				out.Disposition = ControlTerminal
			default:
				out.Disposition = ControlContinue
			}
			return out, true, nil
		},
	}
}

func normalizeControlSpec(spec ControlSpec, policy CompletionPolicy) ControlSpec {
	if spec.Project == nil && len(spec.Definitions) == 0 {
		spec = DefaultControlSpec(policy)
	}
	defs := make([]provider.ToolDefinition, 0, len(spec.Definitions))
	for _, def := range spec.Definitions {
		def.Name = strings.TrimSpace(def.Name)
		if def.Name == "" {
			continue
		}
		defs = append(defs, def)
	}
	spec.Definitions = defs
	return spec
}

func cloneControlSpec(spec ControlSpec) ControlSpec {
	spec.Definitions = cloneProviderToolDefinitions(spec.Definitions)
	return spec
}

func (s ControlSpec) names() map[string]struct{} {
	out := make(map[string]struct{}, len(s.Definitions))
	for _, def := range s.Definitions {
		if name := strings.TrimSpace(def.Name); name != "" {
			out[name] = struct{}{}
		}
	}
	return out
}

func (s ControlSpec) isControlTool(name string) bool {
	_, ok := s.names()[strings.TrimSpace(name)]
	return ok
}

func (s ControlSpec) project(call provider.ToolCall, ctx controlProjectionContext) (ControlSignal, bool, error) {
	if !s.isControlTool(call.Name) {
		return ControlSignal{}, false, nil
	}
	if s.Project == nil {
		return ControlSignal{}, true, fmt.Errorf("control tool %q is declared without a projector", call.Name)
	}
	signal, ok, err := s.Project(call)
	if err != nil {
		return ControlSignal{}, true, err
	}
	if !ok {
		return ControlSignal{}, true, fmt.Errorf("control tool %q is declared but projector returned no signal", call.Name)
	}
	signal.Name = strings.TrimSpace(signal.Name)
	if signal.Name == "" {
		signal.Name = call.Name
	}
	if signal.CallID == "" {
		signal.CallID = call.ID
	}
	if signal.ArgsHash == "" && strings.TrimSpace(call.Args) != "" {
		signal.ArgsHash = providerStableHash(call.Args)
	}
	switch signal.Disposition {
	case ControlContinue:
		if strings.TrimSpace(signal.OutputText) == "" {
			return ControlSignal{}, true, fmt.Errorf("control signal %q continue disposition requires provider-visible output text", signal.Name)
		}
		return signal, true, nil
	case ControlWaiting:
		return signal, true, nil
	case ControlTerminal:
		if strings.TrimSpace(signal.OutputText) == "" {
			signal.OutputText = strings.TrimSpace(ctx.StepText)
		}
		if strings.TrimSpace(signal.OutputText) == "" {
			return ControlSignal{}, true, fmt.Errorf("control signal %q terminal disposition requires output text or assistant text", signal.Name)
		}
		return signal, true, nil
	default:
		return ControlSignal{}, true, fmt.Errorf("control signal %q returned invalid disposition %q", signal.Name, signal.Disposition)
	}
}

func providerStableHash(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return event.StableHash(value)
}
