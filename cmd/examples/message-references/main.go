package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/floegence/floret/config"
	floretruntime "github.com/floegence/floret/runtime"
)

const supplementalSecret = "ephemeral-secret-current-turn-only"

type inspectingGateway struct {
	sawSupplemental bool
}

func (g *inspectingGateway) StreamModel(_ context.Context, req floretruntime.ModelRequest) (<-chan floretruntime.ModelEvent, error) {
	encoded, err := json.Marshal(req.Messages)
	if err != nil {
		return nil, err
	}
	g.sawSupplemental = strings.Contains(string(encoded), supplementalSecret)
	events := make(chan floretruntime.ModelEvent, 2)
	events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDelta, Text: "References inspected."}
	events <- floretruntime.ModelEvent{Type: floretruntime.ModelEventDone, Reason: "stop"}
	close(events)
	return events, nil
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	store := floretruntime.NewMemoryStore()
	defer store.Close()

	var createBinder *floretruntime.ThreadCreateHostBinder
	var turnBinder *floretruntime.TurnExecutionHostBinder
	var readBinder *floretruntime.ThreadReadHostBinder
	if err := floretruntime.ConfigureHostCapabilities(store, func(bootstrap *floretruntime.HostBootstrap) error {
		var configureErr error
		createBinder, configureErr = floretruntime.NewThreadCreateHostBinder(bootstrap)
		if configureErr != nil {
			return configureErr
		}
		turnBinder, configureErr = floretruntime.NewTurnExecutionHostBinder(bootstrap)
		if configureErr != nil {
			return configureErr
		}
		readBinder, configureErr = floretruntime.NewThreadReadHostBinder(bootstrap)
		return configureErr
	}); err != nil {
		return err
	}

	const threadID = floretruntime.ThreadID("reference-thread")
	const createIntentID = floretruntime.CreateIntentID("create-reference-thread")
	createHost, err := createBinder.Bind(threadID, createIntentID)
	if err != nil {
		return err
	}
	if _, err := createHost.CreateThread(ctx, floretruntime.CreateThreadRequest{ThreadID: threadID, CreateIntentID: createIntentID}); err != nil {
		return err
	}

	gateway := &inspectingGateway{}
	reasoning := config.ReasoningCapability{Kind: config.ReasoningKindNone}
	turnFactory, err := turnBinder.Bind(threadID)
	if err != nil {
		return err
	}
	turnHost, err := turnFactory.NewHost(ctx, floretruntime.TurnExecutionHostOptions{
		Config: config.Config{
			SystemPrompt:  "Inspect the supplied context.",
			ContextPolicy: config.ContextPolicy{ContextWindowTokens: config.DefaultContextWindowTokens},
		},
		ModelGateway: gateway,
		ModelGatewayIdentity: floretruntime.ModelGatewayIdentity{
			Provider: "reference-example", Model: "local-scripted-model", StateCompatibilityKey: "reference-example:v1",
		},
		ModelGatewayCapabilities: floretruntime.ModelGatewayCapabilities{Reasoning: &reasoning},
	})
	if err != nil {
		return err
	}

	references := []floretruntime.MessageReference{
		{ReferenceID: "ref-text", Kind: floretruntime.MessageReferenceText, Label: "Selected text", Text: "service ready"},
		{ReferenceID: "ref-file", Kind: floretruntime.MessageReferenceFile, Label: "config.yaml", ResourceRef: "host-resource:file:config"},
		{ReferenceID: "ref-directory", Kind: floretruntime.MessageReferenceDirectory, Label: "workspace", ResourceRef: "host-resource:directory:workspace"},
		{ReferenceID: "ref-terminal", Kind: floretruntime.MessageReferenceTerminal, Label: "Terminal output", Text: "listening on :8080"},
		{ReferenceID: "ref-process", Kind: floretruntime.MessageReferenceProcess, Label: "Process", Text: "server pid 42 running"},
	}
	if _, err := turnHost.RunTurn(ctx, floretruntime.RunTurnRequest{
		ThreadID: threadID, TurnID: "reference-turn", RunID: "reference-run",
		Input: floretruntime.TurnInput{Text: "Inspect these references.", References: references},
		SupplementalContext: []floretruntime.TurnSupplementalContextItem{{
			Kind: "host_resolution", Title: "Current authorization", Text: supplementalSecret, Sensitive: true,
		}},
	}); err != nil {
		return err
	}
	if !gateway.sawSupplemental {
		return fmt.Errorf("gateway did not receive current-turn supplemental context")
	}

	readHost, err := readBinder.NewHost(ctx, threadID)
	if err != nil {
		return err
	}
	page, err := readHost.ListThreadTurns(ctx, floretruntime.ListThreadTurnsRequest{ThreadID: threadID, Tail: 1})
	if err != nil {
		return err
	}
	if len(page.Turns) != 1 || !reflect.DeepEqual(page.Turns[0].UserReferences, references) {
		return fmt.Errorf("canonical references differ from admitted references")
	}
	detail, err := readHost.ListThreadDetailEvents(ctx, floretruntime.ListThreadDetailEventsRequest{
		ThreadID: threadID, IncludeRaw: true,
	})
	if err != nil {
		return err
	}
	encodedDurableState, err := json.Marshal(struct {
		Page   floretruntime.ThreadTurnsPage
		Detail floretruntime.ThreadDetailEvents
	}{Page: page, Detail: detail})
	if err != nil {
		return err
	}
	if strings.Contains(string(encodedDurableState), supplementalSecret) {
		return fmt.Errorf("supplemental context leaked into durable thread state")
	}
	fmt.Printf("durable_references=%d supplemental_visible_to_gateway=%t supplemental_persisted=false\n",
		len(page.Turns[0].UserReferences), gateway.sawSupplemental)
	return nil
}
