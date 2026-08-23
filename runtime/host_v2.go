package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/floegence/floret/v5/config"
	"github.com/floegence/floret/v5/identity"
	"github.com/floegence/floret/v5/internal/agentharness"
	"github.com/floegence/floret/v5/internal/sessiontree"
	internalstorage "github.com/floegence/floret/v5/internal/storage"
	"github.com/floegence/floret/v5/internal/storagebridge"
	"github.com/floegence/floret/v5/provider"
	publicstorage "github.com/floegence/floret/v5/storage"
	"github.com/floegence/floret/v5/storage/spi"
	"github.com/floegence/floret/v5/tools"
)

const (
	logicalSchemaNamespace           = "floret.system"
	logicalSchemaKey                 = "logical-schema"
	logicalSchemaVersion             = "5"
	logicalSchemaFingerprint         = "sha256:55e73dedc2642ccb7f97d285c8720484b885f2050985092f62a8dd15e279385e"
	previousLogicalSchemaVersion     = "3"
	previousLogicalSchemaFingerprint = "sha256:53e8fd256bfa05b6f31f73b8230455fd28d6bb4f3be1fce7d94a9af9b5838d28"
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
	IDSource IDSource
}

// Host is the composition-root owner of Floret storage and narrow capability
// issuance. Application services must retain only handles issued by Host.
type Host struct {
	store    *runtimeStore
	backend  spi.Backend
	idSource IDSource
	idMu     sync.Mutex
	// mutationMu protects inventory-wide and cross-thread mutations only.
	mutationMu      sync.Mutex
	threadRuntimeMu sync.Mutex
	threadRuntime   *threadRuntimeService
	closeMu         sync.Mutex
	closing         bool
	closed          bool
	closeErr        error
	closeDone       chan struct{}
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
	gateOnce sync.Once
	gate     chan struct{}
	backend  spi.Backend
}

func (backend *serializedBackend) View(ctx context.Context, read func(spi.ReadTx) error) error {
	if err := backend.acquire(ctx); err != nil {
		return err
	}
	defer backend.release()
	return backend.backend.View(ctx, read)
}

func (backend *serializedBackend) Update(ctx context.Context, mutate func(spi.WriteTx) error) error {
	if err := backend.acquire(ctx); err != nil {
		return err
	}
	defer backend.release()
	return backend.backend.Update(ctx, mutate)
}

func (backend *serializedBackend) Close() error {
	if err := backend.acquire(context.Background()); err != nil {
		return err
	}
	defer backend.release()
	return backend.backend.Close()
}

func (backend *serializedBackend) acquire(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: backend operation context is required", spi.ErrInvalidArgument)
	}
	backend.gateOnce.Do(func() {
		backend.gate = make(chan struct{}, 1)
		backend.gate <- struct{}{}
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-backend.gate:
		if err := ctx.Err(); err != nil {
			backend.release()
			return err
		}
		return nil
	}
}

func (backend *serializedBackend) release() {
	backend.gate <- struct{}{}
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
	coordinatedBackend := &serializedBackend{backend: backend}
	var kernel *internalstorage.BackendKernel
	err = coordinatedBackend.Update(ctx, func(tx spi.WriteTx) error {
		logicalState, inspectErr := inspectLogicalSchemaTransaction(tx)
		if inspectErr != nil {
			return inspectErr
		}
		kernel, inspectErr = internalstorage.NewBackendKernelInTransaction(ctx, coordinatedBackend, tx, time.Now)
		if inspectErr != nil {
			return inspectErr
		}
		if inspectErr = commitLogicalSchemaTransaction(tx, logicalState); inspectErr != nil {
			return inspectErr
		}
		return kernel.VerifyCurrentStateInTransaction(ctx, tx)
	})
	if err != nil {
		_ = coordinatedBackend.Close()
		return nil, err
	}
	store, err := newBackendRuntimeStoreWithKernel(coordinatedBackend, kernel)
	if err != nil {
		_ = coordinatedBackend.Close()
		return nil, err
	}
	idSource := options.IDSource
	if idSource == nil {
		idSource = randomIDSource{}
	}
	host := &Host{
		store: store, backend: coordinatedBackend, idSource: idSource, closeDone: make(chan struct{}),
	}
	return host, nil
}

func (host *Host) turnRunner(ctx context.Context, threadID identity.ThreadID, agent *Agent) (*turnRunnerHandle, error) {
	if err := host.available(); err != nil {
		return nil, err
	}
	if agent == nil {
		return nil, errors.New("turn runner requires an Agent")
	}
	if _, err := host.store.repo.Thread(ctx, threadID.String()); err != nil {
		return nil, runtimeHostError(err)
	}
	opts := agent.turnExecutionOptions()
	if err := opts.validate(); err != nil {
		return nil, err
	}
	provider, err := newProviderHost(providerHostOptions{
		Config: opts.config, modelGateway: opts.modelGateway, modelGatewayIdentity: opts.modelGatewayIdentity,
		modelGatewayCapabilities: opts.modelGatewayCapabilities, store: host.store, Tools: opts.tools,
		EffectAuthorizationGate: opts.effectAuthorizationGate, Sink: opts.sink,
		ToolSurfaceProvider: opts.toolSurfaceProvider, IDGenerator: opts.idGenerator,
		LoopLimits: opts.loopLimits, Capabilities: opts.capabilities, ThreadTitleMode: opts.threadTitleMode,
	})
	if err != nil {
		return nil, err
	}
	return &turnRunnerHandle{inner: &turnExecutionCapability{threadID: threadID, host: provider}, threadID: threadID}, nil
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

// Shutdown stops in-memory executions and closes the owned canonical store.
func (host *Host) Shutdown(ctx context.Context) error {
	if host == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("runtime shutdown context is required")
	}
	host.closeMu.Lock()
	if host.closed {
		err := host.closeErr
		host.closeMu.Unlock()
		return err
	}
	if host.closing {
		done := host.closeDone
		host.closeMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
		}
		host.closeMu.Lock()
		err := host.closeErr
		host.closeMu.Unlock()
		return err
	}
	host.closing = true
	done := host.closeDone
	host.closeMu.Unlock()
	go host.finishShutdown()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}
	host.closeMu.Lock()
	err := host.closeErr
	host.closeMu.Unlock()
	return err
}

func (host *Host) finishShutdown() {
	host.mutationMu.Lock()
	defer host.mutationMu.Unlock()
	host.threadRuntimeMu.Lock()
	service := host.threadRuntime
	host.threadRuntimeMu.Unlock()
	if service != nil {
		service.close()
	}
	err := host.store.Close()
	host.closeMu.Lock()
	host.closeErr, host.closed, host.closing = err, true, false
	close(host.closeDone)
	host.closeMu.Unlock()
}

func (host *Host) applyThreadMutation(ctx context.Context, threadID identity.ThreadID, mutate func() error) error {
	if host == nil {
		return errors.New("runtime Host is required")
	}
	if _, err := identity.ParseThreadID(threadID.String()); err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("thread mutation context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	host.mutationMu.Lock()
	defer host.mutationMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return mutate()
}

// turnExecutionRequest describes one provider execution after ThreadID is bound by a
// turnRunnerHandle.
type turnExecutionRequest struct {
	LogicalRequestID            identity.LogicalRequestID
	RunID                       identity.RunID
	TurnID                      identity.TurnID
	Input                       TurnInput
	SupplementalContext         []TurnSupplementalContextItem
	Labels                      RunLabels
	Completion                  TurnCompletionPolicy
	Signals                     TurnSignalSpec
	Limits                      TurnLimits
	Reasoning                   config.ReasoningSelection
	ManualCompactions           ManualCompactionSource
	ToolSurfaceProvider         ToolSurfaceProvider
	PromotedQueueID             string
	PromotionRequestKey         string
	PromotionRequestFingerprint string
	InputFingerprint            string
	RetrySourceTurnID           identity.TurnID
	RetrySourceEntryID          string
}

// ResumeInputRequest continues an existing waiting turn without admitting a
// second canonical user message.
type resumeInputRequest struct {
	TurnID       identity.TurnID
	RunID        identity.RunID
	WaitingRunID identity.RunID
	Answer       string
	Options      turnExecutionRequest
}

type acceptedTurnExecutionRequest struct {
	Accepted                    acceptedTurn
	LogicalRequestID            identity.LogicalRequestID
	RunID                       identity.RunID
	TurnID                      identity.TurnID
	Input                       TurnInput
	SupplementalContext         []TurnSupplementalContextItem
	Labels                      RunLabels
	Completion                  TurnCompletionPolicy
	Signals                     TurnSignalSpec
	Limits                      TurnLimits
	Reasoning                   config.ReasoningSelection
	ManualCompactions           ManualCompactionSource
	ToolSurfaceProvider         ToolSurfaceProvider
	PromotedQueueID             string
	PromotionRequestKey         string
	PromotionRequestFingerprint string
	InputFingerprint            string
}

// turnRunnerHandle is the engine effect adapter bound to one thread runtime.
type turnRunnerHandle struct {
	inner    *turnExecutionCapability
	threadID identity.ThreadID
}

// ExecuteAccepted runs provider execution after canonical acceptance.
func (runner *turnRunnerHandle) ExecuteAccepted(ctx context.Context, request acceptedTurnExecutionRequest) (TurnResult, error) {
	if runner == nil || runner.inner == nil {
		return TurnResult{}, errors.New("turn runner is required")
	}
	return runner.inner.ExecuteAcceptedTurn(ctx, request.Accepted, runTurnRequest{
		LogicalRequestID: request.LogicalRequestID, RunID: request.RunID, ThreadID: runner.threadID, TurnID: request.TurnID,
		Input: request.Input, SupplementalContext: request.SupplementalContext,
		Labels: request.Labels, Completion: request.Completion, Signals: request.Signals,
		Limits: request.Limits, Reasoning: request.Reasoning,
		ManualCompactions: request.ManualCompactions, ToolSurfaceProvider: request.ToolSurfaceProvider,
		PromotedQueueID:             request.PromotedQueueID,
		PromotionRequestKey:         request.PromotionRequestKey,
		PromotionRequestFingerprint: request.PromotionRequestFingerprint,
		InputFingerprint:            request.InputFingerprint,
	})
}

func (runner *turnRunnerHandle) ResumeInput(ctx context.Context, request resumeInputRequest) (TurnResult, error) {
	if runner == nil || runner.inner == nil {
		return TurnResult{}, errors.New("turn runner is required")
	}
	return runner.inner.ResumeInput(ctx, request)
}

func (runner *turnRunnerHandle) RetryUnknownEffect(ctx context.Context, sourceAttemptID, requestKey string) (sessiontree.Entry, error) {
	if runner == nil || runner.inner == nil || runner.inner.host == nil || runner.inner.host.harness == nil {
		return sessiontree.Entry{}, errors.New("turn runner is required")
	}
	thread, err := runner.inner.host.harness.ResumeThread(ctx, runner.threadID.String(), agentharness.ResumeOptions{})
	if err != nil {
		return sessiontree.Entry{}, err
	}
	return thread.RetryUnknownEffect(ctx, sourceAttemptID, requestKey)
}

type logicalSchemaState string

const (
	logicalSchemaMissing logicalSchemaState = "missing"
	logicalSchemaCurrent logicalSchemaState = "current"
	logicalSchemaV4      logicalSchemaState = "v4"
)

func inspectLogicalSchema(ctx context.Context, backend spi.Backend) (logicalSchemaState, error) {
	var state logicalSchemaState
	err := backend.View(ctx, func(tx spi.ReadTx) error {
		var err error
		state, err = inspectLogicalSchemaTransaction(tx)
		return err
	})
	return state, err
}

func inspectLogicalSchemaTransaction(tx spi.ReadTx) (logicalSchemaState, error) {
	encoded, err := tx.Get(logicalSchemaNamespace, []byte(logicalSchemaKey))
	if errors.Is(err, spi.ErrNotFound) {
		return logicalSchemaMissing, nil
	}
	if err != nil {
		return "", err
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	var envelope logicalSchemaEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return "", fmt.Errorf("%w: invalid schema envelope: %v", ErrUnsupportedSchema, err)
	}
	if decoder.More() {
		return "", fmt.Errorf("%w: trailing schema data", ErrUnsupportedSchema)
	}
	if envelope.Version == "16" {
		return "", &MigrationRequiredError{Version: envelope.Version}
	}
	if envelope.Version == previousLogicalSchemaVersion {
		if envelope.Fingerprint != previousLogicalSchemaFingerprint {
			return "", fmt.Errorf("%w: version %q fingerprint %q", ErrUnsupportedSchema, envelope.Version, envelope.Fingerprint)
		}
		return logicalSchemaV4, nil
	}
	if envelope.Version != logicalSchemaVersion || envelope.Fingerprint != logicalSchemaFingerprint {
		return "", fmt.Errorf("%w: version %q fingerprint %q", ErrUnsupportedSchema, envelope.Version, envelope.Fingerprint)
	}
	return logicalSchemaCurrent, nil
}

func commitLogicalSchema(ctx context.Context, backend spi.Backend, observed logicalSchemaState) error {
	if observed == logicalSchemaCurrent {
		return nil
	}
	return backend.Update(ctx, func(tx spi.WriteTx) error {
		return commitLogicalSchemaTransaction(tx, observed)
	})
}

func commitLogicalSchemaTransaction(tx spi.WriteTx, observed logicalSchemaState) error {
	if observed == logicalSchemaCurrent {
		return nil
	}
	envelope, err := json.Marshal(logicalSchemaEnvelope{Version: logicalSchemaVersion, Fingerprint: logicalSchemaFingerprint})
	if err != nil {
		return err
	}
	return tx.Put(logicalSchemaNamespace, []byte(logicalSchemaKey), envelope)
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
		TraceID: identity.TraceID(request.TraceID), PromptScopeID: identity.PromptScopeID(request.PromptScopeID), LogicalRequestID: identity.LogicalRequestID(request.LogicalRequestID), AttemptID: request.AttemptID, AttemptEpoch: request.AttemptEpoch, Step: request.Step,
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
