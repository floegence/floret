package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/floegence/floret/v7/config"
	"github.com/floegence/floret/v7/identity"
	"github.com/floegence/floret/v7/tools"
)

// turnExecutionCapability is a provider/tool effect adapter. ThreadService is
// the sole owner of lifecycle, acceptance, cancellation, and attempt fencing.
type turnExecutionCapability struct {
	threadID identity.ThreadID
	host     *providerHost
}

func (capability *turnExecutionCapability) ExecuteAcceptedTurn(ctx context.Context, accepted acceptedTurn, req runTurnRequest) (TurnResult, error) {
	if capability == nil || capability.host == nil {
		return TurnResult{}, errors.New("turn execution capability is required")
	}
	req.ThreadID = capability.threadID
	return capability.host.ExecuteAcceptedTurn(ctx, accepted, req)
}

func (capability *turnExecutionCapability) ResumeInput(ctx context.Context, req resumeInputRequest) (TurnResult, error) {
	if capability == nil || capability.host == nil {
		return TurnResult{}, errors.New("turn execution capability is required")
	}
	return capability.host.ResumeInput(ctx, capability.threadID, req)
}

type turnExecutionOptions struct {
	config                   runtimeConfig
	modelGateway             modelGateway
	modelGatewayIdentity     modelGatewayIdentity
	modelGatewayCapabilities modelGatewayCapabilities
	tools                    *tools.Registry
	effectAuthorizationGate  EffectAuthorizationGate
	sink                     EventSink
	toolSurfaceProvider      ToolSurfaceProvider
	idGenerator              func(string) string
	loopLimits               LoopLimits
	runLabels                RunLabels
	capabilities             CapabilityOptions
	threadTitleMode          ThreadTitleMode
	initialized              bool
}

type modelGatewayCapabilities struct {
	Reasoning         *config.ReasoningCapability
	AttachmentPayload modelGatewayAttachmentPayloadMode
}

type modelGatewayAttachmentPayloadMode string

const (
	modelGatewayAttachmentPayloadDescriptors modelGatewayAttachmentPayloadMode = ""
	modelGatewayAttachmentPayloadExpanded    modelGatewayAttachmentPayloadMode = "expanded"
)

func (capabilities modelGatewayCapabilities) validate(gateway modelGateway) error {
	if gateway == nil {
		if capabilities.Reasoning != nil || capabilities.AttachmentPayload != modelGatewayAttachmentPayloadDescriptors {
			return errors.New("native provider host must not provide model gateway capabilities")
		}
		return nil
	}
	if capabilities.Reasoning == nil {
		return errors.New("model gateway reasoning capability is required")
	}
	reasoning := capabilities.Reasoning.Normalize()
	if reasoning.IsZero() {
		return errors.New("model gateway reasoning capability must be explicit; use kind none when unsupported")
	}
	if err := reasoning.Validate(); err != nil {
		return fmt.Errorf("invalid model gateway reasoning capability: %w", err)
	}
	if capabilities.AttachmentPayload != modelGatewayAttachmentPayloadDescriptors && capabilities.AttachmentPayload != modelGatewayAttachmentPayloadExpanded {
		return fmt.Errorf("unsupported model gateway attachment payload mode %q", capabilities.AttachmentPayload)
	}
	if capabilities.AttachmentPayload == modelGatewayAttachmentPayloadExpanded {
		if _, ok := gateway.(modelGatewayRequestPreparer); !ok {
			return errors.New("model gateway attachment expansion requires prepared request support")
		}
	}
	return nil
}
