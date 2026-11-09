// Command writer qualifies only public Runtime behavior in a blank consumer.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/floegence/floret/v7/runtime"
	"github.com/floegence/floret/v7/storage"
)

func main() {
	if len(os.Args) != 2 {
		panic("database path required")
	}
	ctx := context.Background()
	var previous string
	for round := range 2 {
		host, err := runtime.Open(ctx, runtime.Options{Storage: storage.SQLite(os.Args[1]), DeferExecution: true})
		if err != nil {
			panic(err)
		}
		service, err := host.ThreadService(runtime.AgentFactoryFunc(func(context.Context, runtime.AgentRequest) (*runtime.Agent, error) {
			panic("deferred qualification dispatched")
		}))
		if err != nil {
			panic(err)
		}
		view, err := service.Create(ctx, runtime.CreateThreadInput{RequestKey: "logical-fence-retained-thread"})
		if err != nil {
			panic(err)
		}
		if round == 0 {
			previous = view.ThreadID.String()
		} else if view.ThreadID.String() != previous {
			panic("thread identity changed after restart")
		}
		if err := host.PrepareThread(ctx, view.ThreadID); err != nil {
			panic(err)
		}
		result, err := service.ImportPendingInputs(ctx, runtime.ImportPendingInputsInput{ThreadID: view.ThreadID, Items: []runtime.ImportedPendingInput{{RequestKey: "retained-input", Input: runtime.UserInput{Text: "logical fence preserves user input"}}}})
		if err != nil {
			panic(err)
		}
		if len(result.View.Queue) != 1 || result.View.Queue[0].RequestKey != "retained-input" || result.View.Queue[0].Input.Text != "logical fence preserves user input" {
			panic("pending input lost or duplicated")
		}
		if err := host.Shutdown(ctx); err != nil {
			panic(err)
		}
	}
	fmt.Println("current writer: deferred thread and pending input survived restart")
}
