package runtime

func (service *threadRuntimeService) publish(current ThreadView) {
	actor := service.runtime(current.ThreadID)
	actor.mu.Lock()
	defer actor.mu.Unlock()
	service.flushUpdateLocked(actor)
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.subscriptionsClosed || actor.state.contextErr != nil || current.ViewVersion == 0 || current.ViewVersion <= service.published[current.ThreadID.String()] {
		return
	}
	service.published[current.ThreadID.String()] = current.ViewVersion
	for subscriber := range service.subscribers {
		subscriber.mu.Lock()
		if !subscriber.closed {
			if subscriber.wakeOnly {
				select {
				case subscriber.changed <- struct{}{}:
				default:
				}
			} else {
				view := cloneThreadRuntimeView(current)
				select {
				case subscriber.views <- view:
				default:
					select {
					case <-subscriber.views:
					default:
					}
					select {
					case subscriber.views <- view:
					default:
					}
				}
			}
		}
		subscriber.mu.Unlock()
	}
}
