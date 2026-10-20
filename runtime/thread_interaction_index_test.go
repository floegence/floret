package runtime

import (
	"fmt"
	"reflect"
	"testing"
)

func TestInteractionIndexPreservesAttentionAcrossReplacementAndResolution(t *testing.T) {
	actor := &threadRuntimeState{state: threadRuntimeData{view: ThreadView{Activity: ThreadActivityActive}}}
	check := func(want AttentionSummary) {
		t.Helper()
		actor.refreshAttention()
		actor.normalizeRunProgress()
		if actor.state.view.Attention != want || (actor.state.view.RunProgress == nil) != (want != (AttentionSummary{})) {
			t.Fatalf("attention=%+v progress=%+v want=%+v", actor.state.view.Attention, actor.state.view.RunProgress, want)
		}
		for index, interaction := range actor.state.view.Interactions {
			if actor.interactionIndex[interaction.ID] != index {
				t.Fatalf("wrong index for %s", interaction.ID)
			}
		}
	}
	approval := ThreadInteraction{ID: "approval", Kind: ThreadInteractionApproval}
	question := ThreadInteraction{ID: "input", Kind: ThreadInteractionInput}
	actor.installInteraction(approval, 0)
	actor.installInteraction(question, 0)
	check(AttentionSummary{ApprovalCount: 1, InputCount: 1})
	actor.state.view.Items = []ThreadItem{{ID: "tool", Interaction: &approval}, {ID: "question", Interaction: &question}}
	actor.resolveInteraction(approval.ID, InteractionResolution{Accepted: true})
	check(AttentionSummary{InputCount: 1})
	if !actor.state.view.Items[0].Interaction.Resolved || actor.state.view.Items[1].Interaction.Resolved {
		t.Fatal("resolution did not update the exact item")
	}
	actor.installInteraction(approval, 1)
	check(AttentionSummary{InputCount: 1})
	actor.resolveInteraction(approval.ID, InteractionResolution{Accepted: true})
	check(AttentionSummary{InputCount: 1})
	actor.replaceInteractions([]ThreadInteraction{question})
	check(AttentionSummary{InputCount: 1})
	actor.resolveInteraction(question.ID, InteractionResolution{Accepted: true})
	check(AttentionSummary{})
	actor.replaceInteractions(nil)
	check(AttentionSummary{})
	before := cloneThreadRuntimeView(actor.state.view)
	actor.resolveInteraction("missing", InteractionResolution{})
	if !reflect.DeepEqual(before, cloneThreadRuntimeView(actor.state.view)) {
		t.Fatal("unknown resolution changed accepted state")
	}
}

func BenchmarkInteractionFixedHistoryChange(b *testing.B) {
	for _, count := range []int{1096, 10960, 109600} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			actor := &threadRuntimeState{state: threadRuntimeData{view: ThreadView{Activity: ThreadActivityActive}}}
			for i := range count {
				actor.installInteraction(ThreadInteraction{ID: fmt.Sprint(i), Kind: ThreadInteractionApproval, Resolved: true}, i)
			}
			actor.ensureInteractionIndex()
			actor.ensureItemIndex()
			ids := make([]string, b.N)
			for i := range ids {
				ids[i] = fmt.Sprintf("current-%d", i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				actor.mutations = threadMutations{}
				actor.installInteraction(ThreadInteraction{ID: ids[i], Kind: ThreadInteractionApproval}, count+i)
				actor.resolveInteraction(ids[i], InteractionResolution{Accepted: true})
				actor.normalizeRunProgress()
				actor.refreshAttention()
			}
		})
	}
}
