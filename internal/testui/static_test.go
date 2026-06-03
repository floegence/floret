package testui

import (
	"os"
	"os/exec"
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
	if !strings.Contains(appJS, "api.appendTurn(sessionID, { message })") {
		t.Fatalf("append turn must not smuggle selected_tools")
	}
	appendStart := strings.Index(appJS, "async function appendTurn")
	appendEnd := strings.Index(appJS[appendStart:], "async function updateSessionTools")
	if appendStart < 0 || appendEnd < 0 {
		t.Fatalf("append turn function not found")
	}
	if strings.Contains(appJS[appendStart:appendStart+appendEnd], "selected_tools") {
		t.Fatalf("append turn must not mention selected_tools")
	}
}

func TestStaticConsoleActionLifecycleAndToastFeedback(t *testing.T) {
	stateJS := readStaticTestFile(t, "state.js")
	appJS := readStaticTestFile(t, "app.js")
	html := readStaticTestFile(t, "index.html")
	newSession := readStaticTestFile(t, "views", "newSession.js")
	workspace := readStaticTestFile(t, "views", "sessionWorkspace.js")
	inspector := readStaticTestFile(t, "views", "inspector.js")
	settings := readStaticTestFile(t, "views", "settings.js")

	for _, want := range []string{"action: \"\"", "actionTarget: \"\"", "actionToken: 0", "mutationToken: 0", "refreshToken: 0", "toasts: []", "composerDrafts: {}", "settingsDraft: null", "toolEditDrafts: {}"} {
		if !strings.Contains(stateJS, want) {
			t.Fatalf("state missing action/toast lifecycle field %q", want)
		}
	}
	for _, want := range []string{"toastRegion", "renderToasts", "addToast", "dismissToast", "runWithStatus({ status:", "successMessage", "role=\"${toast.kind === \"error\" ? \"alert\" : \"status\"}", "state.actionToken", "state.mutationToken", "state.refreshToken", "result !== false && successMessage"} {
		if !strings.Contains(appJS, want) {
			t.Fatalf("app missing action/toast lifecycle %q", want)
		}
	}
	if strings.Contains(appJS, "window.alert") {
		t.Fatalf("test UI should route errors through toast instead of window.alert")
	}
	if !strings.Contains(html, `id="toastRegion"`) {
		t.Fatalf("toast region missing from index.html")
	}
	if !strings.Contains(html, `aria-label="Notifications"`) {
		t.Fatalf("toast region should be named for assistive technology")
	}
	for _, pair := range []struct {
		file string
		want string
	}{
		{newSession, "Creating..."},
		{newSession, "Validating..."},
		{workspace, "Refreshing..."},
		{workspace, "Sending..."},
		{inspector, "Updating..."},
		{settings, "Saving..."},
		{settings, "Running..."},
	} {
		if !strings.Contains(pair.file, pair.want) {
			t.Fatalf("pending label %q missing", pair.want)
		}
	}
}

func TestStaticConsolePreservesDraftsAndSeparatesRefreshFailures(t *testing.T) {
	appJS := readStaticTestFile(t, "app.js")
	newSession := readStaticTestFile(t, "views", "newSession.js")
	workspace := readStaticTestFile(t, "views", "sessionWorkspace.js")
	inspector := readStaticTestFile(t, "views", "inspector.js")
	settings := readStaticTestFile(t, "views", "settings.js")
	css := readStaticTestFile(t, "styles.css")

	for _, want := range []string{"captureActiveDrafts", "readNewSessionDraft", "readSettingsDraft", "readToolEditDraft", "refreshSessionsNonBlocking", "Action completed, but the session list could not refresh"} {
		if !strings.Contains(appJS, want) {
			t.Fatalf("app missing draft/refresh hardening %q", want)
		}
	}
	if !strings.Contains(newSession, `form?.addEventListener("input", persistDraft)`) || !strings.Contains(newSession, "bindToolPresets(toolArea, state.config?.tools || [], \"new-tools\", persistDraft)") {
		t.Fatalf("new session form does not persist ordinary edits")
	}
	if !strings.Contains(newSession, `hasOwnProperty.call(draft, "message")`) || !strings.Contains(newSession, `hasOwnProperty.call(draft, "system_prompt")`) {
		t.Fatalf("new session empty draft fields should not be replaced by defaults")
	}
	if !strings.Contains(workspace, "state.composerDrafts[session.id]") || !strings.Contains(workspace, "onComposerDraft") {
		t.Fatalf("composer draft is not preserved across errors")
	}
	if !strings.Contains(inspector, "state.toolEditDrafts[session.id]") || !strings.Contains(inspector, "onToolEditDraft") {
		t.Fatalf("tool edit draft is not preserved across errors")
	}
	if !strings.Contains(settings, "state.settingsDraft") || !strings.Contains(settings, "data-current-profile-id") {
		t.Fatalf("settings draft is not preserved across profile/provider changes")
	}
	if !strings.Contains(settings, "export function readSettingsDraft") || !strings.Contains(appJS, "import { bindSettings, readSettingsDraft, renderSettings }") {
		t.Fatalf("settings draft reader should have one shared implementation")
	}
	if !strings.Contains(settings, "profileKeyDraft") || !strings.Contains(settings, "searchKeyDraft") {
		t.Fatalf("settings API key drafts should survive re-render before save")
	}
	if !strings.Contains(css, ".sessions-layout.show-sessions .session-rail") || !strings.Contains(css, "display: block") {
		t.Fatalf("mobile session and inspector panels should expand without overlaying the topbar")
	}
}

func TestStaticConsoleUsesExplicitLocalTimeFormatting(t *testing.T) {
	stateJS := readStaticTestFile(t, "state.js")
	workspace := readStaticTestFile(t, "views", "sessionWorkspace.js")
	inspector := readStaticTestFile(t, "views", "inspector.js")

	if !strings.Contains(stateJS, "formatLocalTime") || !strings.Contains(stateJS, "offset_label") || !strings.Contains(stateJS, "offset_minutes") {
		t.Fatalf("state missing explicit local time formatter")
	}
	if strings.Contains(stateJS, "toLocaleString") {
		t.Fatalf("state should not use ambiguous toLocaleString time formatting")
	}
	if !strings.Contains(workspace, "relativeTime") || !strings.Contains(workspace, "formatLocalTime") {
		t.Fatalf("workspace should show relative and exact local session times")
	}
	if !strings.Contains(inspector, "formatLocalTime") {
		t.Fatalf("inspector audit times should use explicit local time")
	}
}

func TestStaticConsoleCreateSessionActivatesNewWorkspace(t *testing.T) {
	appJS := readStaticTestFile(t, "app.js")

	for _, want := range []string{"async function activateSession", "state.inspectorTab = \"requests\"", "replaceRoute({ name: \"sessions\", id: result.session_id })", "Session created and opened"} {
		if !strings.Contains(appJS, want) {
			t.Fatalf("create session activation flow missing %q", want)
		}
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

func TestStaticConsoleFormatLocalTimeBehavior(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node is not installed; skipping optional static JS behavior test")
	}
	cmd := exec.Command("node", "--input-type=module", "-e", `
import { state, formatLocalTime } from './static/state.js';
state.config = { local_time: { offset_minutes: 480, offset_label: 'UTC+08:00' } };
if (formatLocalTime('2026-06-03T18:30:05Z') !== '2026-06-04 02:30:05 UTC+08:00') {
  throw new Error('UTC+08 format failed: ' + formatLocalTime('2026-06-03T18:30:05Z'));
}
state.config = { local_time: { offset_minutes: -420, offset_label: 'UTC-07:00' } };
if (formatLocalTime('2026-06-03T02:30:05Z') !== '2026-06-02 19:30:05 UTC-07:00') {
  throw new Error('UTC-07 format failed: ' + formatLocalTime('2026-06-03T02:30:05Z'));
}
if (formatLocalTime('not a date') !== '-') {
  throw new Error('invalid date should be dash');
}
`)
	cmd.Dir = filepath.Join("static", "..")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("formatLocalTime behavior failed: %v\n%s", err, output)
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
