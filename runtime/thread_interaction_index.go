package runtime

// Interaction positions and attention index the accepted actor view. Ordinary
// changes touch one control; explicit full replacement rebuilds the index once.
func (actor *threadRuntimeState) ensureInteractionIndex() {
	if actor.interactionIndex != nil && actor.indexedInteractions == len(actor.state.view.Interactions) {
		return
	}
	actor.interactionIndex = make(map[string]int, len(actor.state.view.Interactions))
	actor.interactionAttention = AttentionSummary{}
	for index, interaction := range actor.state.view.Interactions {
		actor.interactionIndex[interaction.ID] = index
		actor.countInteraction(interaction, 1)
	}
	actor.indexedInteractions = len(actor.state.view.Interactions)
}

func (actor *threadRuntimeState) countInteraction(interaction ThreadInteraction, delta int) {
	if interaction.Resolved {
		return
	}
	switch interaction.Kind {
	case ThreadInteractionApproval:
		actor.interactionAttention.ApprovalCount += delta
	case ThreadInteractionInput:
		actor.interactionAttention.InputCount += delta
	}
}

func (actor *threadRuntimeState) replaceInteractions(interactions []ThreadInteraction) {
	actor.state.view.Interactions = interactions
	actor.interactionIndex = nil
	actor.indexedInteractions = 0
	actor.markComponents(threadInteractionsChanged)
}

func (actor *threadRuntimeState) installInteraction(interaction ThreadInteraction, position int) {
	actor.ensureInteractionIndex()
	if index, exists := actor.interactionIndex[interaction.ID]; exists {
		previous := actor.state.view.Interactions[index]
		if previous.Resolved && !interaction.Resolved {
			return
		}
		actor.countInteraction(previous, -1)
		actor.state.view.Interactions[index] = cloneThreadInteraction(interaction)
	} else {
		actor.state.view.Interactions = append(actor.state.view.Interactions, ThreadInteraction{})
		copy(actor.state.view.Interactions[position+1:], actor.state.view.Interactions[position:])
		actor.state.view.Interactions[position] = cloneThreadInteraction(interaction)
		for index := position; index < len(actor.state.view.Interactions); index++ {
			actor.interactionIndex[actor.state.view.Interactions[index].ID] = index
		}
		actor.indexedInteractions = len(actor.state.view.Interactions)
	}
	actor.countInteraction(interaction, 1)
	actor.markComponents(threadInteractionsChanged)
}

func (actor *threadRuntimeState) normalizeRunProgress() {
	if actor.state.view.Activity != ThreadActivityActive {
		return
	}
	actor.ensureInteractionIndex()
	actor.state.view.Attention = actor.interactionAttention
	if actor.interactionAttention.ApprovalCount != 0 || actor.interactionAttention.InputCount != 0 {
		actor.state.view.RunProgress = nil
	} else if actor.state.view.RunProgress == nil {
		actor.state.view.RunProgress = &ThreadRunProgress{Phase: ThreadRunPhasePreparing}
	}
}
