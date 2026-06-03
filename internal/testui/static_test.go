package testui

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStaticConsoleDocumentsWebFetchAndWebSearchSeparately(t *testing.T) {
	toolMatrix := readStaticTestFile(t, "components", "toolMatrix.js")
	settings := readStaticTestFile(t, "views", "settings.js")
	inspector := readStaticTestFile(t, "views", "inspector.js")

	if !strings.Contains(toolMatrix, "web_fetch fetches a known URL") || !strings.Contains(toolMatrix, "web_search searches by query") {
		t.Fatalf("tool matrix does not describe web_fetch and web_search separately")
	}
	if !strings.Contains(settings, "Client web_search uses Brave Search") || !strings.Contains(settings, "Hosted provider tools remain separate") {
		t.Fatalf("settings view does not explain client and hosted search split")
	}
	if !strings.Contains(inspector, "Local client tools") || !strings.Contains(inspector, "Provider-hosted tools") {
		t.Fatalf("inspector does not split local and hosted tool capabilities")
	}
}

func TestStaticConsoleToolSelectionSemanticsStayAuditable(t *testing.T) {
	stateJS := readStaticTestFile(t, "state.js")
	appJS := readStaticTestFile(t, "app.js")
	newSession := readStaticTestFile(t, "views", "newSession.js")

	if !strings.Contains(stateJS, `case "all":`) || !strings.Contains(stateJS, "(tools || []).map((tool) => tool.name)") {
		t.Fatalf("All preset should derive from the server tool catalog")
	}
	if !strings.Contains(newSession, "selected_tools: readSelectedTools") {
		t.Fatalf("new session form must send selected_tools")
	}
	if !strings.Contains(appJS, "api.appendTurn(state.activeSession.id, { message })") {
		t.Fatalf("append turn must not smuggle selected_tools")
	}
}

func TestStaticConsoleSettingsSavesSearchProviderContract(t *testing.T) {
	settings := readStaticTestFile(t, "views", "settings.js")
	for _, want := range []string{"search_provider", "search_api_key", "search_endpoint", `provider: "brave"`} {
		if !strings.Contains(settings, want) {
			t.Fatalf("settings view missing %q", want)
		}
	}
}

func readStaticTestFile(t *testing.T, parts ...string) string {
	t.Helper()
	path := filepath.Join(append([]string{"static"}, parts...)...)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
