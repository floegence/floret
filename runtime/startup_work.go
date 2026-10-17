package runtime

// StartupWork reports real completed work in the current phase. Total is zero
// when the owner does not know the total. Counts are thread projections.
type StartupWork struct {
	Phase     StartupPhase
	Completed int64
	Total     int64
}

// StartupWorkProgress is an optional synchronous workload observer. Callbacks
// must return promptly; cancellation belongs to Open's context.
type StartupWorkProgress interface{ OnStartupWork(StartupWork) }

// StartupWorkProgressFunc adapts a function to StartupWorkProgress.
type StartupWorkProgressFunc func(StartupWork)

func (progress StartupWorkProgressFunc) OnStartupWork(work StartupWork) {
	if progress != nil {
		progress(work)
	}
}
