package provider

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/floegence/floret/v7/identity"
)

func TestRequestReusePolicyRoundTripAndValidation(t *testing.T) {
	for _, policy := range []*RequestReusePolicy{nil, {}, {PromptCacheEnabled: true, PromptCacheNamespace: "scope", ProviderStateAllowed: true, PreferContinuation: true}} {
		request := Request{RunID: identity.RunID("run"), PromptScopeID: identity.PromptScopeID("scope"), Messages: []Message{{Role: RoleUser, Text: "hello"}}, ReusePolicy: policy}
		if err := request.Validate(); err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		var restored Request
		if err = json.Unmarshal(raw, &restored); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(policy, restored.ReusePolicy) {
			t.Fatalf("policy lost: %s", raw)
		}
	}
	for _, policy := range []RequestReusePolicy{{PromptCacheEnabled: true}, {PreferContinuation: true}} {
		if policy.Validate() == nil {
			t.Fatalf("accepted invalid policy: %+v", policy)
		}
	}
	request := Request{ReusePolicy: &RequestReusePolicy{}, PreviousState: &State{ID: "response"}}
	if request.Validate() == nil {
		t.Fatal("accepted forbidden previous state")
	}
}

func TestOfficialRequestPreservesExplicitCachePolicy(t *testing.T) {
	request := Request{ReusePolicy: &RequestReusePolicy{PromptCacheEnabled: true, PromptCacheNamespace: "namespace", ProviderStateAllowed: true, PreferContinuation: true}}
	converted := internalRequest(request, Identity{Provider: "openai", Model: "test"})
	if !converted.Cache.Enabled || converted.Cache.Namespace != "namespace" || !converted.Cache.PreferContinuation {
		t.Fatalf("policy lost: %+v", converted.Cache)
	}
}
