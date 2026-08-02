package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/floegence/floret/v3/config"
	"github.com/floegence/floret/v3/identity"
	"github.com/floegence/floret/v3/internal/storagebridge"
	"github.com/floegence/floret/v3/provider"
	publicstorage "github.com/floegence/floret/v3/storage"
	"github.com/floegence/floret/v3/storage/spi"
	"github.com/floegence/floret/v3/tools"
)

const (
	logicalSchemaNamespace   = "floret.system"
	logicalSchemaKey         = "logical-schema"
	logicalSchemaVersion     = "3"
	logicalSchemaFingerprint = "sha256:53e8fd256bfa05b6f31f73b8230455fd28d6bb4f3be1fce7d94a9af9b5838d28"
)

var (
	// ErrMigrationRequired reports an exact legacy schema that runtime.Open
	// refuses to migrate implicitly.
	ErrMigrationRequired = errors.New("floret storage migration required")
	// ErrUnsupportedSchema reports a nonempty schema outside the v3 contract.
	ErrUnsupportedSchema = errors.New("unsupported floret logical schema")
)

// MigrationRequiredError identifies the exact legacy logical schema observed
// by runtime.Open.
type MigrationRequiredError struct {
	Version string
}

// Error describes the required explicit migration.
func (failure *MigrationRequiredError) Error() string {
	if failure == nil {
		return ErrMigrationRequired.Error()
	}
	return fmt.Sprintf("%s: observed schema %q", ErrMigrationRequired, failure.Version)
}

// Is classifies MigrationRequiredError with ErrMigrationRequired.
func (failure *MigrationRequiredError) Is(target error) bool {
	return target == ErrMigrationRequired
}

// Options configures one runtime Host.
type Options struct {
	Storage publicstorage.Source
	// IDSource exists only for v3 source compatibility.
	// Deprecated: production hosts must leave this nil; tests should use
	// florettest.NewIDSource.
	IDSource           IDSource
	SubscriptionBuffer int
}

// Host is the composition-root owner of Floret storage and narrow capability
// issuance. Application services must retain only handles issued by Host.
type Host struct {
	store              *runtimeStore
	backend            spi.Backend
	binders            hostBinders
	idSource           IDSource
	idMu               sync.Mutex
	mutationMu         sync.Mutex
	turnExecutions     keyedMutex
	subscriptionMu     sync.Mutex
	subscriptions      map[*Subscription]struct{}
	subscriptionBuffer int
	closeMu            sync.Mutex
	closing            bool
	closed             bool
	closeErr           error
	closeDone          chan struct{}
}

type keyedMutex struct {
	mu      sync.Mutex
	entries map[string]*keyedMutexEntry
}

type keyedMutexEntry struct {
	mu   sync.Mutex
	refs int
}

// runtime.Open owns one backend, so all domain and request-ledger transactions
// share this short fence instead of surfacing backend-specific write races.
type serializedBackend struct {
	mu      sync.Mutex
	backend spi.Backend
}

func (backend *serializedBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.backend.View(ctx, read)
}

func (backend *serializedBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.backend.Update(ctx, mutate)
}

func (backend *serializedBackend) Close() error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.backend.Close()
}

func (mutex *keyedMutex) lock(key string) func() {
	mutex.mu.Lock()
	if mutex.entries == nil {
		mutex.entries = make(map[string]*keyedMutexEntry)
	}
	entry := mutex.entries[key]
	if entry == nil {
		entry = &keyedMutexEntry{}
		mutex.entries[key] = entry
	}
	entry.refs++
	mutex.mu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		mutex.mu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(mutex.entries, key)
		}
		mutex.mu.Unlock()
	}
}

type hostBinders struct {
	create       *threadCreateBinder
	inventory    *threadInventoryCapability
	read         *threadReadBinder
	title        *threadTitleBinder
	fork         *threadForkBinder
	delete       *threadDeleteBinder
	turn         *turnExecutionBinder
	compact      *threadCompactionBinder
	subAgent     *subAgentBinder
	subAgentRead *subAgentReadBinder
	pending      *pendingToolRecoveryBinder
	interrupted  *interruptedTurnRecoveryBinder
}

type logicalSchemaEnvelope struct {
	Version     string `json:"version"`
	Fingerprint string `json:"fingerprint"`
}

// Open validates the exact v3 logical schema, initializes an empty Backend,
// and transfers exclusive backend lifecycle ownership to a new Host.
func Open(ctx context.Context, options Options) (*Host, error) {
	if ctx == nil {
		return nil, errors.New("runtime open context is required")
	}
	backend, err := storagebridge.Open(ctx, storagebridge.Source(options.Storage))
	if err != nil {
		if errors.Is(err, spi.ErrMigrationRequired) {
			return nil, &MigrationRequiredError{Version: "16"}
		}
		return nil, err
	}
	if backend == nil {
		return nil, errors.New("runtime storage source returned a nil backend")
	}
	if err := ensureLogicalSchema(ctx, backend); err != nil {
		_ = backend.Close()
		return nil, err
	}
	coordinatedBackend := &serializedBackend{backend: backend}
	store, err := newBackendRuntimeStore(ctx, coordinatedBackend)
	if err != nil {
		_ = coordinatedBackend.Close()
		return nil, err
	}
	idSource := options.IDSource
	if idSource == nil {
		idSource = randomIDSource{}
	}
	buffer := options.SubscriptionBuffer
	if buffer < 0 || buffer > 65_536 {
		_ = store.Close()
		_ = coordinatedBackend.Close()
		return nil, errors.New("runtime subscription buffer must be between 1 and 65536")
	}
	if buffer == 0 {
		buffer = 256
	}
	host := &Host{
		store: store, backend: coordinatedBackend, idSource: idSource, closeDone: make(chan struct{}),
		subscriptions: make(map[*Subscription]struct{}), subscriptionBuffer: buffer,
	}
	if err := configureHostCapabilities(store, func(bootstrap *hostBootstrap) error {
		constructors := []func() error{
			func() (err error) { host.binders.create, err = newThreadCreateBinder(bootstrap); return err },
			func() (err error) { host.binders.inventory, err = newThreadInventoryCapability(bootstrap); return err },
			func() (err error) { host.binders.read, err = newThreadReadBinder(bootstrap); return err },
			func() (err error) { host.binders.title, err = newThreadTitleBinder(bootstrap); return err },
			func() (err error) { host.binders.fork, err = newThreadForkBinder(bootstrap); return err },
			func() (err error) { host.binders.delete, err = newThreadDeleteBinder(bootstrap); return err },
			func() (err error) { host.binders.turn, err = newTurnExecutionBinder(bootstrap); return err },
			func() (err error) { host.binders.compact, err = newThreadCompactionBinder(bootstrap); return err },
			func() (err error) { host.binders.subAgent, err = newSubAgentBinder(bootstrap); return err },
			func() (err error) { host.binders.subAgentRead, err = newSubAgentReadBinder(bootstrap); return err },
			func() (err error) {
				host.binders.pending, err = newPendingToolRecoveryBinder(bootstrap)
				return err
			},
			func() (err error) {
				host.binders.interrupted, err = newInterruptedTurnRecoveryBinder(bootstrap)
				return err
			},
		}
		for _, construct := range constructors {
			if err := construct(); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		_ = store.Close()
		_ = backend.Close()
		return nil, err
	}
	return host, nil
}

func (host *Host) threadCreator(threadID identity.ThreadID, createIntentID createIntentID) (*threadCreatorHandle, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	inner, err := host.binders.create.Bind(threadID, createIntentID)
	if err != nil {
		return nil, err
	}
	return &threadCreatorHandle{inner: inner}, nil
}

func (host *Host) threadReader(ctx context.Context, threadID identity.ThreadID) (*threadReaderHandle, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	inner, err := host.binders.read.NewHost(ctx, threadID)
	if err != nil {
		return nil, err
	}
	return &threadReaderHandle{inner: inner, threadID: threadID}, nil
}

func (host *Host) turnRunner(ctx context.Context, threadID identity.ThreadID, agent *Agent) (*turnRunnerHandle, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	if agent == nil {
		return nil, errors.New("turn runner requires an Agent")
	}
	factory, err := host.binders.turn.Bind(threadID)
	if err != nil {
		return nil, err
	}
	inner, err := factory.NewHost(ctx, agent.turnExecutionOptions())
	if err != nil {
		return nil, err
	}
	return &turnRunnerHandle{inner: inner, threadID: threadID}, nil
}

func (host *Host) available() error {
	if host == nil {
		return errors.New("runtime Host is required")
	}
	host.closeMu.Lock()
	defer host.closeMu.Unlock()
	if host.closing || host.closed {
		return ErrHostClosed
	}
	return nil
}

// threadCreatorHandle is exact root-thread creation authority.
type threadCreatorHandle struct {
	inner *threadCreateCapability
}

// Create creates or replays the bound root thread.
func (creator *threadCreatorHandle) Create(ctx context.Context) (ThreadSummary, error) {
	if creator == nil || creator.inner == nil {
		return ThreadSummary{}, errors.New("thread creator is required")
	}
	return creator.inner.CreateThread(ctx, createThreadRequest{})
}

// threadReaderHandle is read authority for one exact root thread.
type threadReaderHandle struct {
	inner    *threadReadCapability
	threadID identity.ThreadID
}

// Read returns the current canonical thread snapshot.
func (reader *threadReaderHandle) Read(ctx context.Context) (ThreadSnapshot, error) {
	if reader == nil || reader.inner == nil {
		return ThreadSnapshot{}, errors.New("thread reader is required")
	}
	return reader.inner.ReadThread(ctx, reader.threadID)
}

// ReadTurn returns one canonical turn from the bound thread.
func (reader *threadReaderHandle) ReadTurn(ctx context.Context, turnID identity.TurnID) (ThreadTurnSnapshot, error) {
	if reader == nil || reader.inner == nil {
		return ThreadTurnSnapshot{}, errors.New("thread reader is required")
	}
	return reader.inner.ReadThreadTurn(ctx, readThreadTurnRequest{ThreadID: reader.threadID, TurnID: turnID})
}

// turnExecutionRequest describes one provider execution after ThreadID is bound by a
// turnRunnerHandle.
type turnExecutionRequest struct {
	RunID               identity.RunID
	TurnID              identity.TurnID
	Input               TurnInput
	SupplementalContext []TurnSupplementalContextItem
	Labels              RunLabels
	Completion          TurnCompletionPolicy
	Signals             TurnSignalSpec
	Limits              TurnLimits
	Reasoning           config.ReasoningSelection
	ManualCompactions   ManualCompactionSource
	ToolSurfaceProvider ToolSurfaceProvider
}

type admittedTurnExecutionRequest struct {
	Admission           turnAdmissionResult
	RunID               identity.RunID
	TurnID              identity.TurnID
	Input               TurnInput
	SupplementalContext []TurnSupplementalContextItem
	Labels              RunLabels
	Completion          TurnCompletionPolicy
	Signals             TurnSignalSpec
	Limits              TurnLimits
	Reasoning           config.ReasoningSelection
	ManualCompactions   ManualCompactionSource
	ToolSurfaceProvider ToolSurfaceProvider
}

// turnRunnerHandle owns provider-backed execution for one exact root thread.
type turnRunnerHandle struct {
	inner    *turnExecutionCapability
	threadID identity.ThreadID
}

// Admit records one canonical user message on the bound thread before provider
// execution starts.
func (runner *turnRunnerHandle) Admit(ctx context.Context, request turnExecutionRequest) (turnAdmissionResult, error) {
	if runner == nil || runner.inner == nil {
		return turnAdmissionResult{}, errors.New("turn runner is required")
	}
	return runner.inner.AdmitTurn(ctx, runTurnRequest{
		RunID: request.RunID, ThreadID: runner.threadID, TurnID: request.TurnID,
		Input: request.Input, SupplementalContext: request.SupplementalContext,
		Labels: request.Labels, Completion: request.Completion, Signals: request.Signals,
		Limits: request.Limits, Reasoning: request.Reasoning,
		ManualCompactions: request.ManualCompactions, ToolSurfaceProvider: request.ToolSurfaceProvider,
	})
}

// ExecuteAdmitted runs provider execution for an already admitted turn.
func (runner *turnRunnerHandle) ExecuteAdmitted(ctx context.Context, request admittedTurnExecutionRequest) (TurnResult, error) {
	if runner == nil || runner.inner == nil {
		return TurnResult{}, errors.New("turn runner is required")
	}
	return runner.inner.ExecuteAdmittedTurn(ctx, request.Admission, runTurnRequest{
		RunID: request.RunID, ThreadID: runner.threadID, TurnID: request.TurnID,
		Input: request.Input, SupplementalContext: request.SupplementalContext,
		Labels: request.Labels, Completion: request.Completion, Signals: request.Signals,
		Limits: request.Limits, Reasoning: request.Reasoning,
		ManualCompactions: request.ManualCompactions, ToolSurfaceProvider: request.ToolSurfaceProvider,
	})
}

// Run admits and executes one turn on the bound thread.
func (runner *turnRunnerHandle) Run(ctx context.Context, request turnExecutionRequest) (TurnResult, error) {
	if runner == nil || runner.inner == nil {
		return TurnResult{}, errors.New("turn runner is required")
	}
	return runner.inner.RunTurn(ctx, runTurnRequest{
		RunID: request.RunID, ThreadID: runner.threadID, TurnID: request.TurnID,
		Input: request.Input, SupplementalContext: request.SupplementalContext,
		Labels: request.Labels, Completion: request.Completion, Signals: request.Signals,
		Limits: request.Limits, Reasoning: request.Reasoning,
		ManualCompactions: request.ManualCompactions, ToolSurfaceProvider: request.ToolSurfaceProvider,
	})
}

func ensureLogicalSchema(ctx context.Context, backend spi.Backend) error {
	return backend.Update(ctx, func(tx spi.WriteTx) error {
		encoded, err := tx.Get(logicalSchemaNamespace, []byte(logicalSchemaKey))
		if errors.Is(err, spi.ErrNotFound) {
			envelope, marshalErr := json.Marshal(logicalSchemaEnvelope{Version: logicalSchemaVersion, Fingerprint: logicalSchemaFingerprint})
			if marshalErr != nil {
				return marshalErr
			}
			return tx.Put(logicalSchemaNamespace, []byte(logicalSchemaKey), envelope)
		}
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(strings.NewReader(string(encoded)))
		decoder.DisallowUnknownFields()
		var envelope logicalSchemaEnvelope
		if err := decoder.Decode(&envelope); err != nil {
			return fmt.Errorf("%w: invalid schema envelope: %v", ErrUnsupportedSchema, err)
		}
		if decoder.More() {
			return fmt.Errorf("%w: trailing schema data", ErrUnsupportedSchema)
		}
		if envelope.Version == "16" {
			return &MigrationRequiredError{Version: envelope.Version}
		}
		if envelope.Version != logicalSchemaVersion || envelope.Fingerprint != logicalSchemaFingerprint {
			return fmt.Errorf("%w: version %q fingerprint %q", ErrUnsupportedSchema, envelope.Version, envelope.Fingerprint)
		}
		return nil
	})
}

func (agent *Agent) turnExecutionOptions() turnExecutionOptions {
	identity := agent.gateway.Identity()
	capabilities := agent.gateway.Capabilities()
	reasoning := capabilities.ReasoningCapability
	if capabilities.Reasoning == provider.ReasoningUnsupported {
		reasoning = config.ReasoningCapability{Kind: config.ReasoningKindNone}
	}
	attachmentMode := modelGatewayAttachmentPayloadDescriptors
	if capabilities.AttachmentPayload == provider.AttachmentExpanded {
		attachmentMode = modelGatewayAttachmentPayloadExpanded
	}
	return turnExecutionOptions{
		config: runtimeConfig{
			SystemPrompt:  agent.configuration.SystemPrompt,
			ContextPolicy: agent.configuration.Context, Reasoning: agent.configuration.Reasoning,
		},
		modelGateway:             agentGatewayAdapter{gateway: agent.gateway},
		modelGatewayIdentity:     modelGatewayIdentity(identity),
		modelGatewayCapabilities: modelGatewayCapabilities{Reasoning: &reasoning, AttachmentPayload: attachmentMode},
		tools:                    agent.tools, effectAuthorizationGate: agent.effectAuthorization,
		sink: agent.eventSink, toolSurfaceProvider: agent.toolSurface, idGenerator: agent.idGenerator,
		loopLimits: agent.loopLimits, capabilities: agent.capabilities,
		threadTitleMode: agent.threadTitleMode, initialized: true,
	}
}

type agentGatewayAdapter struct {
	gateway provider.Gateway
}

func (adapter agentGatewayAdapter) StreamModel(ctx context.Context, request modelRequest) (<-chan modelEvent, error) {
	stream, err := adapter.gateway.Stream(ctx, providerRequest(request))
	if err != nil {
		return nil, err
	}
	return modelEventStream(ctx, stream), nil
}

func (adapter agentGatewayAdapter) PrepareModelRequest(ctx context.Context, request modelRequest) (preparedModelRequest, error) {
	preparer, ok := adapter.gateway.(provider.RequestPreparer)
	if !ok {
		return nil, errors.New("provider gateway does not prepare requests")
	}
	prepared, err := preparer.Prepare(ctx, providerRequest(request))
	if err != nil {
		return nil, err
	}
	if prepared == nil {
		return nil, errors.New("provider gateway returned a nil prepared request")
	}
	return agentPreparedRequest{prepared: prepared}, nil
}

type agentPreparedRequest struct {
	prepared provider.PreparedRequest
}

func (request agentPreparedRequest) StreamModel(ctx context.Context) (<-chan modelEvent, error) {
	stream, err := request.prepared.Stream(ctx)
	if err != nil {
		return nil, err
	}
	return modelEventStream(ctx, stream), nil
}

func (request agentPreparedRequest) TokenEstimate() modelRequestTokenEstimate {
	estimate := request.prepared.TokenEstimate()
	return modelRequestTokenEstimate{
		PrefixTokens: estimate.PrefixTokens, MessageTokens: estimate.MessageTokens,
		ToolDefinitionTokens: estimate.ToolDefinitionTokens, EstimatedInputTokens: estimate.EstimatedInputTokens,
		Source: estimate.Source, Method: estimate.Method, Confidence: estimate.Confidence,
		Coverage: modelRequestTokenEstimateCoverage(estimate.Coverage),
	}
}

func (request agentPreparedRequest) RenderedPayloadFingerprint() string {
	return request.prepared.RenderedPayloadFingerprint()
}

func (request agentPreparedRequest) Close() error { return request.prepared.Close() }

func providerRequest(request modelRequest) provider.Request {
	messages := make([]provider.Message, len(request.Messages))
	for index, message := range request.Messages {
		messages[index] = providerMessage(message)
	}
	definitions := make([]tools.ToolDefinition, len(request.Tools))
	for index, definition := range request.Tools {
		definitions[index] = tools.ToolDefinition{
			Name: definition.Name, Title: definition.Title, Description: definition.Description,
			InputSchema: definition.InputSchema, OutputSchema: definition.OutputSchema,
			Strict: definition.Strict, Annotations: definition.Annotations,
		}
	}
	hosted := append([]provider.HostedToolDefinition(nil), request.HostedTools...)
	return provider.Request{
		RunID: identity.RunID(request.RunID), ThreadID: identity.ThreadID(request.ThreadID), TurnID: identity.TurnID(request.TurnID),
		TraceID: identity.TraceID(request.TraceID), PromptScopeID: identity.PromptScopeID(request.PromptScopeID), Step: request.Step,
		Messages: messages, Tools: definitions, HostedTools: hosted, MaxOutputTokens: request.MaxOutputTokens,
		Reasoning: request.Reasoning, PreviousState: gatewayProviderState(request.PreviousState),
		Labels: provider.Labels{Correlation: request.Labels.Correlation, Host: request.Labels.Host},
	}
}

func providerMessage(message modelMessage) provider.Message {
	attachments := make([]provider.Attachment, len(message.Attachments))
	for index, attachment := range message.Attachments {
		attachments[index] = provider.Attachment{
			ResourceRef: attachment.ResourceRef, Name: attachment.Name, MIMEType: attachment.MIMEType, SizeBytes: attachment.SizeBytes,
		}
		if attachment.TextStats != nil {
			attachments[index].TextStats = &provider.AttachmentTextStats{
				UnicodeCodePointCount: attachment.TextStats.UnicodeCodePointCount,
				LogicalLineCount:      attachment.TextStats.LogicalLineCount,
			}
		}
	}
	calls := make([]provider.ToolCall, len(message.ToolCalls))
	for index, call := range message.ToolCalls {
		calls[index] = provider.ToolCall{ID: call.ID, Name: call.Name, Args: call.Args}
	}
	var result *provider.ToolResult
	if message.ToolResult != nil {
		result = &provider.ToolResult{CallID: message.ToolResult.CallID, ToolName: message.ToolResult.ToolName, Text: message.ToolResult.Text}
	}
	return provider.Message{
		Role: provider.MessageRole(message.Role), Text: message.Text, Attachments: attachments,
		Reasoning: message.Reasoning, ToolCalls: calls, ToolResult: result,
	}
}

func gatewayProviderState(state *modelStateEnvelope) *provider.State {
	if state == nil {
		return nil
	}
	attributes := make(map[string]string, len(state.Attributes))
	for key, value := range state.Attributes {
		attributes[key] = value
	}
	return &provider.State{Kind: state.Kind, ID: state.ID, Attributes: attributes}
}

func modelEventStream(ctx context.Context, source <-chan provider.Event) <-chan modelEvent {
	output := make(chan modelEvent)
	go func() {
		defer close(output)
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-source:
				if !ok {
					return
				}
				select {
				case output <- projectModelEvent(event):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output
}

func projectModelEvent(event provider.Event) modelEvent {
	calls := make([]tools.ToolCall, len(event.ToolCalls))
	for index, call := range event.ToolCalls {
		calls[index] = tools.ToolCall{ID: call.ID, Name: call.Name, Args: call.Args}
	}
	sources := append([]provider.Source(nil), event.Sources...)
	var stream *ToolCallStream
	if event.ToolCallStream != nil {
		stream = &ToolCallStream{ID: event.ToolCallStream.ID, Name: event.ToolCallStream.Name}
	}
	var state *modelStateEnvelope
	if event.ResponseState != nil {
		attributes := make(map[string]string, len(event.ResponseState.Attributes))
		for key, value := range event.ResponseState.Attributes {
			attributes[key] = value
		}
		state = &modelStateEnvelope{Kind: event.ResponseState.Kind, ID: event.ResponseState.ID, Attributes: attributes}
	}
	var hostedCall *provider.ToolCall
	if event.HostedToolCall != nil {
		call := *event.HostedToolCall
		hostedCall = &call
	}
	var hostedResult *provider.HostedToolResult
	if event.HostedResult != nil {
		result := *event.HostedResult
		result.Results = append([]provider.HostedToolResultItem(nil), event.HostedResult.Results...)
		if event.HostedResult.Error != nil {
			resultError := *event.HostedResult.Error
			result.Error = &resultError
		}
		result.Metadata = cloneAnyMap(event.HostedResult.Metadata)
		for index := range result.Results {
			result.Results[index].Metadata = cloneAnyMap(result.Results[index].Metadata)
		}
		hostedResult = &result
	}
	return modelEvent{
		Type: modelEventType(event.Type), Text: event.Text, ToolCallStream: stream, ToolCalls: calls,
		HostedToolCall: hostedCall, HostedResult: hostedResult,
		Sources: sources, Reason: event.Reason,
		Usage:      event.Usage,
		ResponseID: event.ResponseID, ResponseState: state, Err: event.Err,
	}
}
