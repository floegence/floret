package testui

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestStaticConsoleDocumentsWebSearchAndExternalFetchBoundary(t *testing.T) {
	toolMatrix := readStaticTestFile(t, "components", "toolMatrix.js")
	settings := readStaticTestFile(t, "views", "settings.js")
	inspector := readStaticTestFile(t, "views", "inspector.js")

	if strings.Contains(toolMatrix, "web_fetch") || !strings.Contains(toolMatrix, "web_search searches by query through either provider-hosted search or the configured client search provider") || !strings.Contains(toolMatrix, "Opening URLs or calling HTTP APIs belongs to shell, MCP, extensions, or user tools") {
		t.Fatalf("tool matrix does not describe the web_search/external-fetch boundary")
	}
	if !strings.Contains(settings, "Provider-hosted web search") || !strings.Contains(settings, "Client search via Brave") || !strings.Contains(settings, "readWebSearchCapability") {
		t.Fatalf("settings view does not expose provider-hosted/client/disabled search configuration")
	}
	if strings.Contains(settings, "web_fetch") || !strings.Contains(settings, "web_search + shell curl") {
		t.Fatalf("settings view should not describe web_fetch as an available scenario capability")
	}
	if !strings.Contains(inspector, "Local client tools") || !strings.Contains(inspector, "Provider-hosted tools") || !strings.Contains(inspector, "Unavailable") {
		t.Fatalf("inspector does not split local and hosted tool capabilities")
	}
}

func TestStaticConsoleToolSelectionSemanticsStayAuditable(t *testing.T) {
	stateJS := readStaticTestFile(t, "state.js")
	appJS := readStaticTestFile(t, "app.js")
	newSession := readStaticTestFile(t, "views", "newSession.js")

	if !strings.Contains(stateJS, `case "all":`) || !strings.Contains(stateJS, "availableTools.map((tool) => tool.name)") || !strings.Contains(stateJS, "tool.available !== false") {
		t.Fatalf("All preset should derive from available server tool catalog entries")
	}
	if !strings.Contains(readStaticTestFile(t, "components", "toolMatrix.js"), "tool.available !== false") || !strings.Contains(readStaticTestFile(t, "components", "toolMatrix.js"), "source-badge") || !strings.Contains(readStaticTestFile(t, "components", "toolMatrix.js"), "Unavailable:") {
		t.Fatalf("tool matrix should disable unavailable tools and expose source/unavailable state")
	}
	for _, want := range []string{"Provider-hosted capabilities", "Local client tools", "Disabled / unavailable capabilities"} {
		if !strings.Contains(stateJS, want) {
			t.Fatalf("tool grouping should expose %q", want)
		}
	}
	if !strings.Contains(newSession, "selected_tools: readSelectedTools") {
		t.Fatalf("new session form must send selected_tools")
	}
	if !strings.Contains(appJS, "api.streamTurn(sessionID, { message: trimmed }") {
		t.Fatalf("append turn should use the streaming endpoint")
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

func TestStaticConsoleCreatesSessionBeforeRunningInitialTurn(t *testing.T) {
	appJS := readStaticTestFile(t, "app.js")
	apiJS := readStaticTestFile(t, "api.js")

	for _, want := range []string{"api.createSession(payload)", "activateSessionSnapshot(session)", "void queueInitialTurn(session.id, payload.message, token)", "async function queueInitialTurn", "api.streamTurn(sessionID, { message: trimmed }"} {
		if !strings.Contains(appJS, want) {
			t.Fatalf("app missing create-before-run flow %q", want)
		}
	}
	if !strings.Contains(apiJS, `requestJSON("/api/agent/sessions"`) || !strings.Contains(apiJS, `requestJSON("/api/agent/sessions/run"`) {
		t.Fatalf("api should expose create-only and compatibility create-and-run paths")
	}
	createStart := strings.Index(appJS, "async function createSession")
	createEnd := strings.Index(appJS[createStart:], "async function queueInitialTurn")
	if createStart < 0 || createEnd < 0 {
		t.Fatalf("create/initial turn functions not found")
	}
	createBody := appJS[createStart : createStart+createEnd]
	if strings.Contains(createBody, "await api.appendTurn") {
		t.Fatalf("create session should not await the initial agent turn")
	}
	if strings.Contains(createBody, "api.createAndRunSession") {
		t.Fatalf("frontend should not use compatibility create-and-run path")
	}
}

func TestStaticConsoleNewSessionFieldsAreExplicitlyLabelled(t *testing.T) {
	newSession := readStaticTestFile(t, "views", "newSession.js")

	for _, want := range []string{
		`for="new-profile-id"`,
		`id="new-profile-id" name="profile_id" aria-label="Profile"`,
		`for="new-initial-task"`,
		`id="new-initial-task" name="message" aria-label="Initial task"`,
		`for="new-system-prompt"`,
		`id="new-system-prompt" name="system_prompt" aria-label="System prompt"`,
		`for="new-context-window"`,
		`id="new-context-window" name="context_window_tokens" aria-label="Context window"`,
		`for="new-max-output"`,
		`id="new-max-output" name="max_output_tokens" aria-label="Max output"`,
		`for="new-recent-tail"`,
		`id="new-recent-tail" name="recent_tail_tokens" aria-label="Recent tail"`,
	} {
		if !strings.Contains(newSession, want) {
			t.Fatalf("new session form is missing explicit label/control binding %q", want)
		}
	}
}

func TestStaticConsoleStreamsTurnsIncrementally(t *testing.T) {
	apiJS := readStaticTestFile(t, "api.js")
	appJS := readStaticTestFile(t, "app.js")
	workspace := readStaticTestFile(t, "views", "sessionWorkspace.js")
	css := readStaticTestFile(t, "styles.css")

	for _, want := range []string{"streamTurn(id, payload, onEvent)", "/turns/stream", "readSSE", "consumeSSEFrame"} {
		if !strings.Contains(apiJS, want) {
			t.Fatalf("api missing streaming turn support %q", want)
		}
	}
	for _, want := range []string{"createLiveTurn", "applyStreamEvent", "state.liveTurn", "assistant_delta", "provider_requests", "session_snapshot", "delete state.composerDrafts[sessionID]"} {
		if !strings.Contains(appJS, want) {
			t.Fatalf("app missing streaming reducer behavior %q", want)
		}
	}
	if strings.Contains(appJS, "await api.appendTurn(sessionID, { message") {
		t.Fatalf("frontend should not wait for the synchronous append JSON path")
	}
	for _, want := range []string{"renderLiveTurn", "streaming", "pending", "stream-caret", "Live turn"} {
		if !strings.Contains(workspace+css, want) {
			t.Fatalf("workspace missing live turn rendering %q", want)
		}
	}
}

func TestStaticConsoleShowsInterruptedLifecycle(t *testing.T) {
	workspace := readStaticTestFile(t, "views", "sessionWorkspace.js")
	css := readStaticTestFile(t, "styles.css")

	for _, want := range []string{"session?.status === \"interrupted\"", "interrupted turn", "Inspect or recover"} {
		if !strings.Contains(workspace, want) {
			t.Fatalf("workspace missing interrupted lifecycle copy %q", want)
		}
	}
	for _, want := range []string{".status-pill.interrupted", ".global-status.interrupted"} {
		if !strings.Contains(css, want) {
			t.Fatalf("styles missing interrupted lifecycle treatment %q", want)
		}
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
	if !strings.Contains(newSession, `form?.addEventListener("input"`) || !strings.Contains(newSession, "if (event.isComposing) return") || !strings.Contains(newSession, "persistDraft();") || !strings.Contains(newSession, "bindToolPresets(toolArea, state.config?.tools || [], \"new-tools\", persistDraft)") {
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

	for _, want := range []string{"async function activateSession", "function activateSessionSnapshot", "state.inspectorTab = \"requests\"", "replaceRoute({ name: \"sessions\", id: session.id })", "Session created and opened"} {
		if !strings.Contains(appJS, want) {
			t.Fatalf("create session activation flow missing %q", want)
		}
	}
}

func TestStaticConsoleSessionOperationsAndPolling(t *testing.T) {
	apiJS := readStaticTestFile(t, "api.js")
	appJS := readStaticTestFile(t, "app.js")
	workspace := readStaticTestFile(t, "views", "sessionWorkspace.js")

	for _, want := range []string{"deleteSession(id)", `method: "DELETE"`, "api.deleteSession(sessionID)", "async function deleteSession", "Delete session ${sessionID}?", "Session deleted"} {
		if !strings.Contains(apiJS+appJS, want) {
			t.Fatalf("session delete flow missing %q", want)
		}
	}
	for _, want := range []string{"data-delete-session", "Copy ID", "data-copy-key", "copyPayloads", "copyButton", "navigator.clipboard.writeText"} {
		if !strings.Contains(workspace+appJS, want) {
			t.Fatalf("session/message copy controls missing %q", want)
		}
	}
	if strings.Contains(workspace, "data-copy-text") {
		t.Fatalf("copy controls should not duplicate large message bodies into data-copy-text attributes")
	}
	for _, want := range []string{"let autoRefreshTimer = 0", "visibilitychange", "scheduleAutoRefresh", "refreshActiveSessionSnapshot", "document.hidden", "state.route.name !== \"sessions\"", "api.session(sessionID)", "1000", "2000"} {
		if !strings.Contains(appJS, want) {
			t.Fatalf("active session polling missing %q", want)
		}
	}
	for _, want := range []string{"captureActiveDrafts();", "render({ preserveFocus: true })", "captureFocusState", "restoreFocusState", "focusSelectorFor", "selectionStart", "selectionEnd", "preventScroll", "data-append-form", "data-tool-edit-form"} {
		if !strings.Contains(appJS, want) {
			t.Fatalf("active session polling must preserve input focus: missing %q", want)
		}
	}
}

func TestStaticConsoleProtectsIMECompositionFromRefreshRenders(t *testing.T) {
	stateJS := readStaticTestFile(t, "state.js")
	appJS := readStaticTestFile(t, "app.js")
	workspace := readStaticTestFile(t, "views", "sessionWorkspace.js")
	newSession := readStaticTestFile(t, "views", "newSession.js")
	settings := readStaticTestFile(t, "views", "settings.js")
	inspector := readStaticTestFile(t, "views", "inspector.js")

	for _, want := range []string{"imeComposition", "active: false", "pendingRender: false", "pendingRefresh: false"} {
		if !strings.Contains(stateJS, want) {
			t.Fatalf("state missing IME composition guard field %q", want)
		}
	}
	for _, want := range []string{
		`document.addEventListener("compositionstart"`,
		`document.addEventListener("compositionend"`,
		"IMPORTANT: IME composition owns the editable DOM node",
		"persistEditableDraft(target)",
		"state.imeComposition.active && !options.force",
		"state.imeComposition.pendingRender = true",
		"state.imeComposition.pendingRefresh = true",
		"if (state.imeComposition.active)",
		"render({ preserveFocus: true, scheduleRefresh: true })",
	} {
		if !strings.Contains(appJS, want) {
			t.Fatalf("app missing IME-safe render/refresh guard %q", want)
		}
	}
	for _, file := range []struct {
		name string
		body string
	}{
		{"session workspace", workspace},
		{"new session", newSession},
		{"settings", settings},
		{"inspector", inspector},
	} {
		if !strings.Contains(file.body, "event.isComposing") {
			t.Fatalf("%s input handler must avoid persisting draft during IME composition", file.name)
		}
	}
}

func TestStaticConsoleStreamingTurnKeepsFinalSessionSnapshot(t *testing.T) {
	appJS := readStaticTestFile(t, "app.js")

	for _, want := range []string{"let finalSession = null", "finalSession = await api.session(sessionID)", "setActiveSessionSnapshot(finalSession)", "if (finalSession) result.session = finalSession"} {
		if !strings.Contains(appJS, want) {
			t.Fatalf("streaming turn final snapshot guard missing %q", want)
		}
	}
	if strings.Contains(appJS, "state.activeSession = result.session") {
		t.Fatalf("streaming turn must not let an older live result session overwrite the final refreshed session")
	}
	for _, want := range []string{"setActiveSessionSnapshot", "shouldAcceptSessionSnapshot", "nextTime < currentTime", "current.can_append_message && current.status !== \"running\" && next.status === \"running\""} {
		if !strings.Contains(appJS, want) {
			t.Fatalf("active session snapshot regression guard missing %q", want)
		}
	}
}

func TestStaticConsoleTimelineLongMessagesCollapseAndCopy(t *testing.T) {
	workspace := readStaticTestFile(t, "views", "sessionWorkspace.js")
	css := readStaticTestFile(t, "styles.css")

	for _, want := range []string{"renderMessageBody", "message-fold", "text.length > 1200", "lineCount > 12", "<details", "<summary>", "Copy", "structuredEntryCopy"} {
		if !strings.Contains(workspace, want) {
			t.Fatalf("timeline long message/copy rendering missing %q", want)
		}
	}
	for _, want := range []string{".message-fold", ".message-fold summary", ".copy-inline", ".row-actions", ".session-select"} {
		if !strings.Contains(css, want) {
			t.Fatalf("timeline/session operation styles missing %q", want)
		}
	}
}

func TestStaticConsoleSettingsSavesSearchProviderContract(t *testing.T) {
	settings := readStaticTestFile(t, "views", "settings.js")
	for _, want := range []string{"search_provider", "search_api_key", "search_endpoint", `provider: "brave"`, "web_search: readWebSearchCapability", "search_mode", "search_wire_shape"} {
		if !strings.Contains(settings, want) {
			t.Fatalf("settings view missing %q", want)
		}
	}
}

func TestStaticConsoleExposesSavedToolScenarioChecks(t *testing.T) {
	appJS := readStaticTestFile(t, "app.js")
	apiJS := readStaticTestFile(t, "api.js")
	settings := readStaticTestFile(t, "views", "settings.js")

	for _, want := range []string{"Tool Scenario Checks", "tool-scenarios", "live-tool-scenarios", "multi-tool, multi-turn", "saved active provider profile"} {
		if !strings.Contains(settings, want) {
			t.Fatalf("settings view missing tool scenario check copy %q", want)
		}
	}
	for _, want := range []string{"api.runCheck(target, payload)", `target === "live-tool-scenarios"`, "profile_id: state.config?.active_profile_id"} {
		if !strings.Contains(appJS, want) {
			t.Fatalf("app missing live tool scenario run payload %q", want)
		}
	}
	if !strings.Contains(apiJS, "runCheck(target, payload = {})") || !strings.Contains(apiJS, "JSON.stringify({ target, ...payload })") {
		t.Fatalf("api.runCheck should preserve target and optional profile payload")
	}
}

func TestStaticConsoleDisablesStaticAssetCaching(t *testing.T) {
	index := readStaticTestFile(t, "index.html")
	server := readStaticTestFile(t, "..", "server.go")

	for _, want := range []string{"/styles.css?v=testui", "/app.js?v=testui"} {
		if !strings.Contains(index, want) {
			t.Fatalf("index should version static asset %q", want)
		}
	}
	if !strings.Contains(server, `w.Header().Set("Cache-Control", "no-store")`) {
		t.Fatalf("static handler should disable browser caching for test UI assets")
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
