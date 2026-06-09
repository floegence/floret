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

	if strings.Contains(toolMatrix, "web_fetch") || !strings.Contains(toolMatrix, "web_search searches by query through the single selected source for the ${escapeHTML(profileScope)}") || !strings.Contains(toolMatrix, "Opening URLs or calling HTTP APIs belongs to shell, MCP, extensions, or user tools") {
		t.Fatalf("tool matrix does not describe the web_search/external-fetch boundary")
	}
	if !strings.Contains(readStaticTestFile(t, "views", "newSession.js"), `profileScope: "selected profile"`) || !strings.Contains(readStaticTestFile(t, "views", "inspector.js"), `profileScope: "session profile"`) {
		t.Fatalf("tool matrix callers should describe whether the source comes from the selected or session profile")
	}
	if !strings.Contains(settings, "Provider-hosted web search") || !strings.Contains(settings, "External: Brave") || !strings.Contains(settings, "readWebSearchCapability") || !strings.Contains(settings, `source: "external_brave"`) {
		t.Fatalf("settings view does not expose provider-hosted/external Brave/disabled search configuration")
	}
	if strings.Contains(settings, "web_fetch") || !strings.Contains(settings, "web_search + shell curl") {
		t.Fatalf("settings view should not describe web_fetch as an available scenario capability")
	}
	if !strings.Contains(inspector, "Selected tools/capabilities") || !strings.Contains(inspector, "Provider-hosted tools") || !strings.Contains(inspector, "Unavailable") {
		t.Fatalf("inspector does not split selected, hosted, and unavailable capabilities")
	}
}

func TestStaticConsoleToolSelectionSemanticsStayAuditable(t *testing.T) {
	stateJS := readStaticTestFile(t, "state.js")
	appJS := readStaticTestFile(t, "app.js")
	newSession := readStaticTestFile(t, "views", "newSession.js")
	toolSelection := readRepoTestFile(t, "internal", "testui", "tool_selection.go")
	shellTool := readRepoTestFile(t, "tools", "builtin", "shell.go")

	if !strings.Contains(stateJS, `case "all":`) || !strings.Contains(stateJS, "availableTools.map((tool) => tool.name)") || !strings.Contains(stateJS, "tool.available !== false") {
		t.Fatalf("All preset should derive from available server tool catalog entries")
	}
	if !strings.Contains(readStaticTestFile(t, "components", "toolMatrix.js"), "tool.available !== false") || !strings.Contains(readStaticTestFile(t, "components", "toolMatrix.js"), "source-badge") || !strings.Contains(readStaticTestFile(t, "components", "toolMatrix.js"), "Unavailable:") {
		t.Fatalf("tool matrix should disable unavailable tools and expose source/unavailable state")
	}
	for _, want := range []string{"Provider-hosted capabilities", "External search capabilities", "Disabled / unavailable capabilities"} {
		if !strings.Contains(stateJS, want) {
			t.Fatalf("tool grouping should expose %q", want)
		}
	}
	if !strings.Contains(newSession, "selected_tools: readSelectedTools") {
		t.Fatalf("new session form must send selected_tools")
	}
	if !strings.Contains(appJS, "api.streamTurn(sessionID, withDebugRaw({ message: trimmed })") {
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
	for _, stale := range []string{"ToolEdit", `Name: "edit"`, "Replace exact text in a file.", "read/grep/list/edit/write/apply_patch"} {
		if strings.Contains(toolSelection+shellTool, stale) {
			t.Fatalf("old edit tool surface should not remain: %q", stale)
		}
	}
	for _, want := range []string{`ToolApplyPatch, Title: "Apply patch"`, `Risk: "writes files", Permission: "ask"`, `ToolWrite, Title: "Write"`, `Risk: "overwrites files", Permission: "ask"`} {
		if !strings.Contains(toolSelection, want) {
			t.Fatalf("local write tool catalog should expose explicit ask permission: missing %q", want)
		}
	}
	if !strings.Contains(readStaticTestFile(t, "components", "toolMatrix.js"), "tool.permission_mode || tool.annotations?.permission_mode") {
		t.Fatalf("tool matrix should prefer explicit catalog permission_mode before fallback")
	}
}

func TestStaticConsoleWebSearchToolMatrixBehavior(t *testing.T) {
	script := `
import assert from "node:assert/strict";
import { groupTools, toolNamesForPreset } from "./static/state.js";

const unavailable = [
  { name: "read", group: "workspace_read", available: true },
  { name: "web_search", group: "network", available: false, source: "external_brave", unavailable: "Brave Search API key is not configured" },
];
assert.deepEqual(toolNamesForPreset("all", unavailable), ["read"]);
assert.equal(groupTools(unavailable).filter((group) => group.tools.some((tool) => tool.name === "web_search")).length, 1);
assert.equal(groupTools(unavailable).find((group) => group.tools.some((tool) => tool.name === "web_search")).title, "Disabled / unavailable capabilities");

const hosted = [{ name: "web_search", available: true, source: "provider_hosted", wire_shape: "anthropic_server_web_search" }];
assert.deepEqual(toolNamesForPreset("all", hosted), ["web_search"]);
assert.equal(groupTools(hosted)[0].title, "Provider-hosted capabilities");

const brave = [{ name: "web_search", available: true, source: "external_brave" }];
assert.deepEqual(toolNamesForPreset("all", brave), ["web_search"]);
assert.equal(groupTools(brave)[0].title, "External search capabilities");
`
	runNodeStaticScript(t, script)
}

func TestStaticConsoleWebSearchToolsFollowSelectedProfile(t *testing.T) {
	script := `
import assert from "node:assert/strict";
import { defaultWebSearchForProvider, resolveWebSearchForProfile, state, toolsForProfile, toolNamesForPreset } from "./static/state.js";

state.config = {
  search_provider: { api_key_set: false },
  catalog: [
    { id: "openai", name: "OpenAI", web_search: {} },
    { id: "anthropic", name: "Anthropic", web_search: { default_source: "provider_hosted", hosted_wire_shape: "anthropic_server_web_search", hosted_wire_shapes: ["anthropic_server_web_search"] } },
    { id: "fake", name: "Fake", web_search: {} },
    { id: "deepseek", name: "DeepSeek", web_search: {} },
  ],
  tools: [
    { name: "read", group: "workspace_read", available: true },
    { name: "web_search", group: "network", available: false, source: "disabled", unavailable: "web search disabled" },
  ],
};

assert.deepEqual(defaultWebSearchForProvider("openai"), { source: "disabled" });
assert.deepEqual(defaultWebSearchForProvider("anthropic"), { source: "provider_hosted", hosted: { wire_shape: "anthropic_server_web_search" } });
assert.deepEqual(defaultWebSearchForProvider("fake"), { source: "disabled" });

let tools = toolsForProfile({ provider: "anthropic", web_search: { source: "provider_hosted", hosted: { wire_shape: "anthropic_server_web_search" } } });
let search = tools.find((tool) => tool.name === "web_search");
assert.equal(search.available, true);
assert.equal(search.source, "provider_hosted");
assert.equal(search.wire_shape, "anthropic_server_web_search");
assert.deepEqual(toolNamesForPreset("all", tools), ["read", "web_search"]);

tools = toolsForProfile({ provider: "openai", web_search: { source: "provider_hosted", hosted: { wire_shape: "anthropic_server_web_search" } } });
search = tools.find((tool) => tool.name === "web_search");
assert.equal(search.available, false);
assert.match(search.unavailable, /provider-hosted web_search is not supported by this profile/);
assert.deepEqual(toolNamesForPreset("all", tools), ["read"]);

tools = toolsForProfile({ provider: "deepseek", web_search: { source: "provider_hosted", hosted: { wire_shape: "anthropic_server_web_search" } } });
search = tools.find((tool) => tool.name === "web_search");
assert.equal(search.available, false);
assert.match(search.unavailable, /provider-hosted web_search is not supported by this profile/);
assert.deepEqual(toolNamesForPreset("all", tools), ["read"]);

tools = toolsForProfile({ provider: "fake", web_search: { source: "external_brave", brave: { provider: "brave" } } });
search = tools.find((tool) => tool.name === "web_search");
assert.equal(search.available, false);
assert.equal(search.source, "external_brave");
assert.match(search.unavailable, /API key/);

state.config.search_provider.api_key_set = true;
assert.equal(resolveWebSearchForProfile({ provider: "fake", web_search: { source: "external_brave" } }).available, true);
`
	runNodeStaticScript(t, script)
}

func TestStaticConsoleCreatesSessionBeforeRunningInitialTurn(t *testing.T) {
	appJS := readStaticTestFile(t, "app.js")
	apiJS := readStaticTestFile(t, "api.js")

	for _, want := range []string{"api.createSession(withDebugRaw(payload))", "activateSessionSnapshot(session)", "void queueInitialTurn(session.id, payload.message, token)", "async function queueInitialTurn", "api.streamTurn(sessionID, withDebugRaw({ message: trimmed })"} {
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
	for _, want := range []string{
		`Recent tail controls the verbatim assistant, tool, and nearby message tail kept after the checkpoint.`,
		`Recent user inputs outside the tail are protected inside the checkpoint up to 15k tokens, and the latest user message is always represented.`,
	} {
		if !strings.Contains(newSession, want) {
			t.Fatalf("new session form is missing recent tail help text %q", want)
		}
	}
}

func TestStaticConsoleNewSessionContextPolicyIsAdvancedAndStepSafe(t *testing.T) {
	newSession := readStaticTestFile(t, "views", "newSession.js")

	for _, want := range []string{
		`<details class="advanced-options" data-context-policy-options>`,
		`<summary>Advanced options</summary>`,
		`id="new-context-window" name="context_window_tokens" aria-label="Context window" type="number" min="1024" step="1"`,
		`id="new-max-output" name="max_output_tokens" aria-label="Max output" type="number" min="0" step="1"`,
		`id="new-recent-tail" name="recent_tail_tokens" aria-label="Recent tail" aria-description="Controls the verbatim assistant, tool, and nearby message tail kept after the checkpoint. Recent user inputs outside the tail are protected inside the checkpoint up to 15k tokens, and the latest user message is always represented." type="number" min="256" step="1"`,
	} {
		if !strings.Contains(newSession, want) {
			t.Fatalf("new session context policy controls missing advanced/step-safe markup %q", want)
		}
	}
	if strings.Contains(newSession, `data-context-policy-options open`) || strings.Contains(newSession, `open data-context-policy-options`) {
		t.Fatalf("context policy advanced options should be collapsed by default")
	}
	for _, stale := range []string{`step="1024"`, `step="256"`} {
		if strings.Contains(newSession, stale) {
			t.Fatalf("context policy inputs should not use catalog-breaking token steps %q", stale)
		}
	}
}

func TestStaticConsoleNewSessionDefaultsFollowBackendAndProviderCatalog(t *testing.T) {
	stateJS := readStaticTestFile(t, "state.js")
	newSession := readStaticTestFile(t, "views", "newSession.js")
	appJS := readStaticTestFile(t, "app.js")

	for _, want := range []string{"contextPolicyForProfile", "defaultContextPolicy()", "providerModel", "context_policy_defaults", "model?.context_window", "model?.max_tokens"} {
		if !strings.Contains(stateJS, want) {
			t.Fatalf("state.js missing provider/model context default logic %q", want)
		}
	}
	if !strings.Contains(newSession, "contextPolicyForProfile(profile)") || !strings.Contains(appJS, "switchNewSessionProfile") {
		t.Fatalf("new session flow should derive context policy from selected profile")
	}
	for _, want := range []string{"toolsForProfile(profile)", "bindToolPresets(toolArea, toolsForProfile(profile)", "toolsForProfile(state.activeSession?.profile)", "defaultWebSearchForProvider(provider)"} {
		if !strings.Contains(stateJS+newSession+appJS, want) {
			t.Fatalf("profile-specific tool catalog flow missing %q", want)
		}
	}
	if strings.Contains(newSession, "defaultContextPolicy") || strings.Contains(stateJS, "recent_tail_tokens: 4096") {
		t.Fatalf("new session defaults should not use stale hard-coded context policy values")
	}
	for _, want := range []string{"MIN_SUPPORTED_CONTEXT_WINDOW_TOKENS = 256000", "max_output_tokens: 0", "reserved_output_tokens: 64000", "reserved_summary_tokens: 20000", "recent_user_tokens: 15000", "model?.max_tokens ?? baseDefaults.max_output_tokens", "Number(defaults.reserved_output_tokens ?? 64000)", "modelRiskMessages"} {
		if !strings.Contains(stateJS, want) {
			t.Fatalf("state.js missing no-cap context policy default logic %q", want)
		}
	}
	if !strings.Contains(stateJS, "const baseDefaults = baseContextPolicyDefaults()") {
		t.Fatalf("state.js should use base context defaults for selected-profile max output fallback")
	}
	if strings.Contains(stateJS, "Math.min(maxOutput") {
		t.Fatalf("reserved output budget should not be derived from ordinary max output cap")
	}
}

func TestStaticConsoleInspectorShowsContextStatusAndDebugBreakdown(t *testing.T) {
	inspector := readStaticTestFile(t, "views", "inspector.js")
	for _, want := range []string{"renderContextStatusMetrics", "Context ", "Output room", "Compaction ", "Request Debug", "pressure_signal", "pressure_source", "confidence", "threshold_tokens", "request_safe_limit_tokens", "output_headroom_tokens", "tool_definition_tokens", "estimate_source"} {
		if !strings.Contains(inspector, want) {
			t.Fatalf("inspector should expose context status/debug field %q", want)
		}
	}
	for _, forbidden := range []string{"est tokens", "estimator_source", "estimator_confidence", "estimator "} {
		if strings.Contains(inspector, forbidden) {
			t.Fatalf("inspector should not expose old provider request wording %q", forbidden)
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
	for _, want := range []string{"createLiveTurn", "applyStreamEvent", "state.liveTurn", "assistant_delta", "provider_requests", "session_snapshot", "delete state.composerDrafts[sessionID]", "withDebugRaw({ message: trimmed })"} {
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
	for _, want := range []string{"mergeTimelineEntries", "timelineEntryKey", "live.entries", "tool_call", "tool_result", "renderToolActivity", "tool-activity", "tool-summary-preview"} {
		if !strings.Contains(workspace+css, want) {
			t.Fatalf("workspace missing live tool timeline rendering %q", want)
		}
	}
}

func TestStaticConsoleSkillsInstallPageAndDemoFlow(t *testing.T) {
	apiJS := readStaticTestFile(t, "api.js")
	stateJS := readStaticTestFile(t, "state.js")
	appJS := readStaticTestFile(t, "app.js")
	html := readStaticTestFile(t, "index.html")
	skills := readStaticTestFile(t, "views", "skills.js")
	css := readStaticTestFile(t, "styles.css")

	for _, want := range []string{
		`href="/skills" data-link data-route="skills">Skills</a>`,
		`if (pathname === "/skills") return { name: "skills", id: "" }`,
		`if (route.name === "skills") return "/skills"`,
		`case "skills":`,
		"renderSkills",
		"bindSkills",
	} {
		if !strings.Contains(html+stateJS+appJS, want) {
			t.Fatalf("skills route/nav missing %q", want)
		}
	}
	for _, want := range []string{
		"previewSkill(payload)",
		`requestJSON("/api/skills/preview"`,
		"installSkill(payload)",
		`requestJSON("/api/skills/install"`,
		"Skill preview ready",
		"Skill installed",
		"Skill replaced",
		"state.config.capabilities = response.capabilities",
		"Preview the current skill URL before installing it",
	} {
		if !strings.Contains(apiJS+appJS, want) {
			t.Fatalf("skills API flow missing %q", want)
		}
	}
	for _, want := range []string{
		"Install Agent Skill",
		"Installed Skills",
		"GitHub skill URL",
		"data-skill-install-form",
		"data-preview-skill",
		`querySelectorAll("[data-use-landing-demo]")`,
		"Replace the installed copy",
		`name="replace"`,
		"skill_sources",
		"Diagnostics",
		"No skills detected",
	} {
		if !strings.Contains(skills, want) {
			t.Fatalf("skills page missing %q", want)
		}
	}
	for _, want := range []string{
		"First call the read-only skill tool",
		`{"name":"frontend-design"}`,
		".floret-test-ui/artifacts/frontend-design-landing/index.html",
		"/artifacts/frontend-design-landing/index.html",
		"LANDING_DEMO_TOOLS = [\"read\", \"list\", \"glob\", \"grep\", \"apply_patch\", \"write\"]",
		"selected_tools: LANDING_DEMO_TOOLS.filter",
		"Prefer apply_patch",
		"Do not use shell.",
		"Use in Landing Demo",
		"state.newSessionDraft = landingDemoDraft",
		`navigate({ name: "new", id: "" })`,
	} {
		if !strings.Contains(skills+appJS, want) {
			t.Fatalf("landing demo flow missing %q", want)
		}
	}
	if strings.Contains(skills, `LANDING_DEMO_TOOLS = ["read", "list", "glob", "grep", "write", "shell"]`) {
		t.Fatalf("landing demo must not enable shell by default")
	}
	if strings.Contains(skills, `LANDING_DEMO_TOOLS = ["read", "list", "glob", "grep", "write"]`) {
		t.Fatalf("landing demo should include apply_patch alongside write")
	}
	for _, want := range []string{
		"skillsInstallDraft",
		"skillsPreview",
		"readSkillInstallDraft",
		`[data-skill-install-form] [name="${cssEscape(name)}"]`,
		"state.skillsInstallDraft = readSkillInstallDraft",
	} {
		if !strings.Contains(stateJS+appJS+skills, want) {
			t.Fatalf("skills draft/focus preservation missing %q", want)
		}
	}
	for _, want := range []string{".skills-layout", ".skill-install-panel", ".skill-preview", ".replace-confirm", ".skill-row", ".skill-facts"} {
		if !strings.Contains(css, want) {
			t.Fatalf("skills page CSS missing %q", want)
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
	if !strings.Contains(newSession, `form?.addEventListener("input"`) || !strings.Contains(newSession, "if (event.isComposing) return") || !strings.Contains(newSession, "persistDraft();") || !strings.Contains(newSession, "bindToolPresets(toolArea, toolsForProfile(profile), \"new-tools\", persistDraft)") {
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
	if !strings.Contains(css, ".sessions-layout.show-sessions .session-rail") || !strings.Contains(css, "display: flex") {
		t.Fatalf("mobile session and inspector panels should expand without overlaying the topbar")
	}
}

func TestStaticConsoleSessionsUseIndependentScrollRegions(t *testing.T) {
	css := readStaticTestFile(t, "styles.css")

	for _, want := range []string{
		".sessions-layout",
		"height: calc(100vh - var(--topbar-height))",
		"overflow: hidden",
		".session-rail,\n.inspector",
		"flex-direction: column",
		".workspace",
		"grid-template-rows: auto minmax(0, 1fr) auto",
		".conversation",
		"overflow: auto",
		".session-list",
		".inspector-body",
	} {
		if !strings.Contains(css, want) {
			t.Fatalf("sessions scroll isolation CSS missing %q", want)
		}
	}
	if strings.Contains(css, "max-height: calc(100vh - 196px)") || strings.Contains(css, "max-height: calc(100vh - 158px)") {
		t.Fatalf("session list and inspector should not use viewport max-height calculations")
	}
}

func TestStaticConsoleStreamingCleanupClearsLiveTurnAfterSnapshotFetchOnErrors(t *testing.T) {
	appJS := readStaticTestFile(t, "app.js")

	for _, want := range []string{"let streamError = null", "streamError = error", "finalSession = await fetchSessionSnapshot(sessionID)", "setActiveSessionSnapshot(finalSession)", "state.liveTurn = null", "if (streamError) throw streamError"} {
		if !strings.Contains(appJS, want) {
			t.Fatalf("streaming failure cleanup missing %q", want)
		}
	}
	if !strings.Contains(appJS, "await refreshSessionsNonBlocking(token);\n    if (streamError) throw streamError") {
		t.Fatalf("streaming cleanup should refresh after clearing liveTurn before surfacing stream errors")
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
	for _, want := range []string{"let autoRefreshTimer = 0", "visibilitychange", "scheduleAutoRefresh", "refreshActiveSessionSnapshot", "document.hidden", "state.route.name !== \"sessions\"", "fetchSessionSnapshot(sessionID)", "1000"} {
		if !strings.Contains(appJS, want) {
			t.Fatalf("active session polling missing %q", want)
		}
	}
	if !strings.Contains(appJS, `state.activeSession.status !== "running" && state.activeSession.phase !== "turn"`) || strings.Contains(appJS, " ? 1000 : 2000") {
		t.Fatalf("completed sessions should not keep polling and replacing selected text")
	}
	for _, want := range []string{"captureActiveDrafts();", "render({ preserveFocus: true })", "captureFocusState", "restoreFocusState", "focusSelectorFor", "selectionStart", "selectionEnd", "preventScroll", "data-append-form", "data-tool-edit-form"} {
		if !strings.Contains(appJS, want) {
			t.Fatalf("active session polling must preserve input focus: missing %q", want)
		}
	}
	for _, want := range []string{"selectionchange", "hasActiveAppTextSelection", "window.getSelection", "range.intersectsNode", "deferForTextSelection", "state.deferredRender"} {
		if !strings.Contains(appJS, want) {
			t.Fatalf("active session polling must defer renders while text is selected: missing %q", want)
		}
	}
	for _, want := range []string{"captureSessionViewportState", "restoreSessionViewportState", "captureConversationScroll", "restoreConversationScroll", "bottomPinned", "conversation.scrollTop", "captureTimelineExpanded", "renderedSessionID"} {
		if !strings.Contains(appJS, want) {
			t.Fatalf("active session polling must preserve conversation viewport: missing %q", want)
		}
	}
	if !strings.Contains(appJS, "const viewportState = captureSessionViewportState();") || !strings.Contains(appJS, "restoreSessionViewportState(viewportState);") {
		t.Fatalf("render should capture and restore session viewport around DOM replacement")
	}
	for _, want := range []string{"[data-session-select-id]", "dataset.sessionSelectId", "data-session-select-id"} {
		if !strings.Contains(workspace, want) {
			t.Fatalf("session row selection should use a dedicated click target: missing %q", want)
		}
	}
	if strings.Contains(workspace, `querySelectorAll("[data-session-id]")`) {
		t.Fatalf("conversation data-session-id is scroll state metadata and must not be bound as a session select click target")
	}
	if !strings.Contains(appJS, "state.activeSession?.id === id") {
		t.Fatalf("selecting the already active session should be a no-op to avoid replacing selected message text")
	}
}

func TestStaticConsoleSessionDebugRawAndToolRuns(t *testing.T) {
	apiJS := readStaticTestFile(t, "api.js")
	appJS := readStaticTestFile(t, "app.js")
	stateJS := readStaticTestFile(t, "state.js")
	workspace := readStaticTestFile(t, "views", "sessionWorkspace.js")
	css := readStaticTestFile(t, "styles.css")

	for _, want := range []string{"debug_raw_enabled", "deferredRender"} {
		if !strings.Contains(stateJS+appJS, want) {
			t.Fatalf("session UI missing debug/selection state %q", want)
		}
	}
	for _, want := range []string{`options.debugRaw ? "?debug_raw=1" : ""`, "debugRawEnabled()", "withDebugRaw(payload)", "withDebugRaw({ message: trimmed })"} {
		if !strings.Contains(apiJS+appJS, want) {
			t.Fatalf("session UI should request debug raw data when enabled: missing %q", want)
		}
	}
	for _, want := range []string{"timelineItems", "renderTimelineItem", "renderToolRun", "toolRunKey", "tool run", "Arguments", "Result", "Raw arguments/results are redacted", "tool-detail-section"} {
		if !strings.Contains(workspace+css, want) {
			t.Fatalf("workspace should merge and expose tool run details: missing %q", want)
		}
	}
}

func TestStaticConsolePreservesTimelineExpansionState(t *testing.T) {
	stateJS := readStaticTestFile(t, "state.js")
	appJS := readStaticTestFile(t, "app.js")
	workspace := readStaticTestFile(t, "views", "sessionWorkspace.js")

	for _, want := range []string{"conversationScroll: {}", "timelineExpanded: {}"} {
		if !strings.Contains(stateJS, want) {
			t.Fatalf("state missing timeline preservation field %q", want)
		}
	}
	for _, want := range []string{"details[data-expand-key]", "addEventListener(\"toggle\"", "state.timelineExpanded[details.dataset.expandKey || \"\"] = details.open"} {
		if !strings.Contains(workspace, want) {
			t.Fatalf("workspace must persist details expansion state: missing %q", want)
		}
	}
	for _, want := range []string{"data-expand-key", "detailStateAttributes", "entryExpandKey(entry", "liveExpandKey(session", "timelineEntryKey(entry)"} {
		if !strings.Contains(workspace, want) {
			t.Fatalf("workspace must render stable expandable keys: missing %q", want)
		}
	}
	if !strings.Contains(workspace, `class="conversation" data-session-id`) {
		t.Fatalf("conversation scroll state should be keyed by the rendered session id")
	}
	if !strings.Contains(appJS, "clearSessionViewportState(sessionID)") || !strings.Contains(appJS, "delete state.conversationScroll[sessionID]") || !strings.Contains(appJS, "key.startsWith(prefix)") {
		t.Fatalf("session deletion should clear saved viewport state")
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

	for _, want := range []string{"let finalSession = null", "finalSession = await fetchSessionSnapshot(sessionID)", "setActiveSessionSnapshot(finalSession)", "if (finalSession) result.session = finalSession"} {
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

	for _, want := range []string{"renderExpandableBody", "message-fold", "text.length > 1200", "lines > 12", "<details", "<summary>", "Copy", "structuredEntryCopy"} {
		if !strings.Contains(workspace, want) {
			t.Fatalf("timeline long message/copy rendering missing %q", want)
		}
	}
	for _, want := range []string{".message-fold", ".message-fold summary", ".copy-inline", ".row-actions", ".session-select"} {
		if !strings.Contains(css, want) {
			t.Fatalf("timeline/session operation styles missing %q", want)
		}
	}
	for _, want := range []string{"renderToolActivity", "toolActivityStatus", "toolPreview", "tool_call", "tool_result", "tool-activity", "tool-summary-main", "tool-summary-preview", "tool-activity-body"} {
		if !strings.Contains(workspace+css, want) {
			t.Fatalf("timeline tool call/result collapse rendering missing %q", want)
		}
	}
	if strings.Contains(workspace, "data-copy-text") {
		t.Fatalf("tool copy controls should keep large payloads out of data attributes")
	}
}

func TestStaticConsoleInspectorEventsAreExpandable(t *testing.T) {
	inspector := readStaticTestFile(t, "views", "inspector.js")
	css := readStaticTestFile(t, "styles.css")

	for _, want := range []string{"renderEventRow", "event-row", "<details", "<summary>", "event-summary-main", "event-summary-preview", "event-row-body", "renderEventFacts", "JSON.stringify(event, null, 2)"} {
		if !strings.Contains(inspector+css, want) {
			t.Fatalf("inspector events should be expandable and auditable: missing %q", want)
		}
	}
}

func TestStaticConsolePrioritizesAssistantFinalOverReasoning(t *testing.T) {
	workspace := readStaticTestFile(t, "views", "sessionWorkspace.js")
	css := readStaticTestFile(t, "styles.css")

	for _, want := range []string{"renderAssistantMessage", "assistant final", `renderExpandableBody(body, { label: "assistant final", mode: "final-answer", expandKey: entryExpandKey(entry, "answer") })`, "renderReasoningBlock(msg.reasoning", "Copy reasoning"} {
		if !strings.Contains(workspace, want) {
			t.Fatalf("assistant final/reasoning rendering missing %q", want)
		}
	}
	if strings.Contains(workspace, "msg.reasoning ? `<pre class=\"code-block\"") {
		t.Fatalf("assistant reasoning should not be directly expanded before the final answer")
	}
	for _, want := range []string{".message-text.final-answer", ".reasoning-fold", ".answer-expand", "Show full answer"} {
		if !strings.Contains(workspace+css, want) {
			t.Fatalf("assistant final answer treatment missing %q", want)
		}
	}
}

func TestStaticConsoleTimelineToolActivityIsCompactAndExpandable(t *testing.T) {
	workspace := readStaticTestFile(t, "views", "sessionWorkspace.js")
	css := readStaticTestFile(t, "styles.css")

	for _, want := range []string{"renderToolActivity", "toolActivityStatus", "toolPreview", "tool_call", "tool_result", "tool-activity", "tool-summary-main", "tool-summary-preview", "tool-activity-body"} {
		if !strings.Contains(workspace+css, want) {
			t.Fatalf("timeline tool call/result collapse rendering missing %q", want)
		}
	}
	if strings.Contains(workspace, "data-copy-text") {
		t.Fatalf("tool copy controls should keep large payloads out of data attributes")
	}
}

func TestStaticConsoleSettingsSavesSearchProviderContract(t *testing.T) {
	settings := readStaticTestFile(t, "views", "settings.js")
	appJS := readStaticTestFile(t, "app.js")
	css := readStaticTestFile(t, "styles.css")
	for _, want := range []string{"search_provider", "search_api_key", "search_endpoint", `provider: "brave"`, "web_search: readWebSearchCapability", "search_mode", "search_wire_shape", `source: "provider_hosted"`, `source: "external_brave"`, `source: "disabled"`, "renderSettingsError", "Settings were not saved", "resolveWebSearchForProfile"} {
		if !strings.Contains(settings, want) {
			t.Fatalf("settings view missing %q", want)
		}
	}
	for _, want := range []string{"state.settingsError", "onError:", "defaultWebSearchForProvider(provider)", ".settings-error"} {
		if !strings.Contains(appJS+css, want) {
			t.Fatalf("settings persistent error/provider switch flow missing %q", want)
		}
	}
	for _, stale := range []string{`client: { enabled`, `provider_hosted: {`, `"Client search via Brave"`, "preferred when enabled"} {
		if strings.Contains(settings, stale) {
			t.Fatalf("settings view should not submit or describe legacy web search shape %q", stale)
		}
	}
}

func TestStaticConsoleSettingsDoesNotRenderLegacyUnsupportedHostedWireShape(t *testing.T) {
	script := `
import assert from "node:assert/strict";
import { renderSettings } from "./static/views/settings.js";
import { state } from "./static/state.js";

const legacyShape = "openai_chat_" + "web_search_" + "options";
state.config = {
  active_profile_id: "legacy-openai",
  catalog: [
    { id: "openai", name: "OpenAI", default_model: "gpt-5.4", web_search: {} },
    { id: "anthropic", name: "Anthropic", default_model: "claude", web_search: { default_source: "provider_hosted", hosted_wire_shape: "anthropic_server_web_search", hosted_wire_shapes: ["anthropic_server_web_search"] } },
  ],
  profiles: [{
    id: "legacy-openai",
    name: "Legacy OpenAI",
    provider: "openai",
    model: "gpt-5.4",
    web_search: { source: "provider_hosted", hosted: { wire_shape: legacyShape } },
  }],
  search_provider: {},
};
state.settingsDraft = null;
state.settingsError = null;
const html = renderSettings();
assert.ok(html.includes("Provider-hosted web search (unsupported)"));
assert.ok(html.includes("No provider-hosted search wire shape"));
assert.ok(!html.includes(legacyShape));
`
	runNodeStaticScript(t, script)
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

func TestStaticConsoleTimelineExpansionBehavior(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node is not installed; skipping optional static JS behavior test")
	}
	cmd := exec.Command("node", "--input-type=module", "-e", `
import { state } from './static/state.js';
import { renderSessionWorkspace } from './static/views/sessionWorkspace.js';

const longBody = Array.from({ length: 90 }, (_, i) => 'line ' + i).join('\n');
const session = {
  id: 'session-1',
  status: 'completed',
  can_append_message: true,
  turns: [{ id: 'turn-1' }],
  selected_tools: [],
  profile: { name: 'Fake', model: 'fake-model' },
  aggregate_metrics: { usage: {} },
  path_entries: [{
    id: 'entry-1',
    thread_id: 'session-1',
    turn_id: 'turn-1',
    type: 'assistant_message',
    message: { role: 'assistant', content: longBody, reasoning: 'hidden reasoning' },
  }],
};
state.activeSession = session;
let html = renderSessionWorkspace({ sessions: [session], activeSession: session, result: null, tools: [], inspectorTab: 'tools' });
const key = 'session:session-1:id:entry-1:answer';
if (!html.includes('data-expand-key="' + key + '"')) {
  throw new Error('missing stable answer expand key');
}
if (html.includes('data-expand-key="' + key + '" open')) {
  throw new Error('answer should be closed before user state is saved');
}
state.timelineExpanded[key] = true;
html = renderSessionWorkspace({ sessions: [session], activeSession: session, result: null, tools: [], inspectorTab: 'tools' });
if (!html.includes('data-expand-key="' + key + '" open')) {
  throw new Error('saved open state was not rendered');
}
state.timelineExpanded[key] = false;
html = renderSessionWorkspace({ sessions: [session], activeSession: session, result: null, tools: [], inspectorTab: 'tools' });
if (html.includes('data-expand-key="' + key + '" open')) {
  throw new Error('saved closed state was not rendered');
}
`)
	cmd.Dir = filepath.Join("static", "..")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("timeline expansion behavior failed: %v\n%s", err, output)
	}
}

func TestStaticConsoleRendersToolProjectionAndArtifactLink(t *testing.T) {
	script := `
import assert from "node:assert/strict";
import { state } from "./static/state.js";
import { renderSessionWorkspace } from "./static/views/sessionWorkspace.js";

const session = {
  id: "session-1",
  status: "completed",
  can_append_message: true,
  turns: [{ id: "turn-1" }],
  selected_tools: ["shell"],
  profile: { name: "Fake", model: "fake-model" },
  aggregate_metrics: { usage: {} },
  path_entries: [
    {
      id: "call-entry",
      thread_id: "session-1",
      turn_id: "turn-1",
      type: "tool_call",
      message: {
        role: "assistant",
        content: "tool_call",
        tool_name: "shell",
        tool_call_id: "shell-1",
        tool_args: "{\"command\":\"printf test\"}",
      },
    },
    {
      id: "result-entry",
      thread_id: "session-1",
      turn_id: "turn-1",
      type: "tool_result",
      message: {
        role: "tool",
        content: "89abcdef",
        tool_name: "shell",
        tool_call_id: "shell-1",
        tool_result: {
          truncated: true,
          original_bytes: 16,
          visible_bytes: 8,
          original_lines: 1,
          visible_lines: 1,
          strategy: "tail",
          content_sha256: "abcdef1234567890abcdef1234567890",
          full_output: {
            id: "artifact-1",
            safe_label: "shell-output-000001.log",
            url: "/artifacts/session-1/shell-output-000001.log",
            kind: "tool_output",
            mime: "text/plain; charset=utf-8",
            size_bytes: 16,
            sha256: "abcdef1234567890abcdef1234567890",
          },
        },
      },
      metadata: { truncated: true, original_bytes: 16, visible_bytes: 8 },
    },
  ],
};
state.activeSession = session;
const html = renderSessionWorkspace({ sessions: [session], activeSession: session, result: null, tools: [], inspectorTab: "tools" });
assert.ok(html.includes("Model-visible output"));
assert.ok(html.includes("Projection"));
assert.ok(html.includes("truncated"));
assert.ok(html.includes("Strategy"));
assert.ok(html.includes("tail"));
assert.ok(html.includes("Visible"));
assert.ok(html.includes("8 B"));
assert.ok(html.includes("Original"));
assert.ok(html.includes("16 B"));
assert.ok(html.includes("shell-output-000001.log"));
assert.ok(html.includes('href="/artifacts/session-1/shell-output-000001.log"'));
assert.ok(html.includes("Open artifact"));
`
	runNodeStaticScript(t, script)
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

func runNodeStaticScript(t *testing.T, script string) {
	t.Helper()
	cmd := exec.Command("node", "--input-type=module", "-e", script)
	cmd.Dir = "."
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("node static behavior test failed: %v\n%s", err, output)
	}
}

func readRepoTestFile(t *testing.T, parts ...string) string {
	t.Helper()
	path := filepath.Join(append([]string{"..", ".."}, parts...)...)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
