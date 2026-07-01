import { contextPolicyForProfile, currentProfile, escapeHTML, modelRiskMessages, profileLabel, state, toolsForProfile } from "../state.js";
import { bindToolPresets, readSelectedTools, renderToolMatrix } from "../components/toolMatrix.js";

export function renderNewSession() {
  const draft = state.newSessionDraft || {};
  const profiles = state.config?.profiles || [];
  const profile = profiles.find((item) => item.id === draft.profile_id) || currentProfile();
  const defaultPolicy = contextPolicyForProfile(profile);
  const policy = { ...defaultPolicy, ...(draft.context_policy || {}) };
  const isCreating = state.action === "create-session";
  const isProbing = state.action === "run-probe";
  const selectedTools = draft.selected_tools || [];
  const tools = toolsForProfile(profile);
  const riskMessages = modelRiskMessages(profile, policy);
  const message = Object.prototype.hasOwnProperty.call(draft, "message") ? draft.message : "Say hello from Floret and complete the task.";
  const agentProfile = draft.agent_profile || state.config?.agent_profile || defaultAgentProfile();
  const promptIdentity = state.config?.prompt_identity || {};
  const hasCustomPromptState = Object.prototype.hasOwnProperty.call(draft, "custom_prompt");
  const customPromptEnabled = Boolean(draft.custom_prompt) || (!hasCustomPromptState && Object.prototype.hasOwnProperty.call(draft, "system_prompt") && String(draft.system_prompt || "").trim() !== "");
  const systemPrompt = customPromptEnabled && Object.prototype.hasOwnProperty.call(draft, "system_prompt") ? draft.system_prompt : "";
  return `
    <section class="new-page">
      <header class="new-head">
        <div>
          <h1>New Session</h1>
          <p class="muted">Create a clean agent session with an explicit model, agent profile, context policy, and toolset.</p>
        </div>
        <a class="button ghost" href="/sessions" data-link>Cancel</a>
      </header>
      <form class="form-grid" data-new-session-form>
        <label class="field" for="new-profile-id">
          <span>Provider profile</span>
          <select id="new-profile-id" name="profile_id" aria-label="Profile">
            ${(state.config?.profiles || []).map((item) => `<option value="${escapeHTML(item.id)}" ${item.id === (draft.profile_id || profile.id) ? "selected" : ""}>${escapeHTML(profileLabel(item))}</option>`).join("")}
          </select>
        </label>
        <label class="field" for="new-initial-task">
          <span>Initial task</span>
          <textarea id="new-initial-task" name="message" aria-label="Initial task" required>${escapeHTML(message)}</textarea>
        </label>
        <section class="profile-card agent-profile-card" data-agent-profile>
          <div class="agent-profile-head">
            <div>
              <h2>Agent profile</h2>
              <p class="muted">${escapeHTML(agentProfile.description || "Default interactive agent.")}</p>
            </div>
            <span class="tiny-pill">${escapeHTML(promptIdentity.source || "default_agent")}</span>
          </div>
          <div class="agent-profile-grid">
            <div>
              <strong>ID</strong>
              <span>${escapeHTML(agentProfile.id || "floret")}</span>
            </div>
            <div>
              <strong>Name</strong>
              <span>${escapeHTML(agentProfile.name || "Default assistant")}</span>
            </div>
            <div>
              <strong>Prompt hash</strong>
              <span>${escapeHTML((promptIdentity.system_prompt_hash || "").slice(0, 12) || "pending")}</span>
            </div>
          </div>
          <label class="checkbox-field" for="new-custom-prompt">
            <input id="new-custom-prompt" name="custom_prompt" type="checkbox" ${customPromptEnabled ? "checked" : ""} />
            <span>Custom system prompt</span>
          </label>
          <div class="custom-prompt-panel" data-custom-prompt-panel ${customPromptEnabled ? "" : "hidden"}>
            <label class="field" for="new-system-prompt">
              <span>System prompt</span>
              <textarea id="new-system-prompt" name="system_prompt" aria-label="System prompt" ${customPromptEnabled ? "required" : "disabled"}>${escapeHTML(systemPrompt)}</textarea>
            </label>
          </div>
        </section>
        <details class="advanced-options" data-context-policy-options>
          <summary>Advanced options</summary>
          <div class="field-row">
            <label class="field" for="new-context-window">
              <span>Context window</span>
              <input id="new-context-window" name="context_window_tokens" aria-label="Context window" type="number" min="1024" step="1" value="${policy.context_window_tokens}" />
            </label>
            <label class="field" for="new-max-output">
              <span>Max output</span>
              <input id="new-max-output" name="max_output_tokens" aria-label="Max output" type="number" min="0" step="1" value="${policy.max_output_tokens}" />
            </label>
            <label class="field" for="new-recent-tail">
              <span title="Recent tail controls the verbatim assistant, tool, and nearby message tail kept after the checkpoint. Recent user inputs outside the tail are protected inside the checkpoint up to 15k tokens, and the latest user message is always represented.">Recent tail</span>
              <input id="new-recent-tail" name="recent_tail_tokens" aria-label="Recent tail" aria-description="Controls the verbatim assistant, tool, and nearby message tail kept after the checkpoint. Recent user inputs outside the tail are protected inside the checkpoint up to 15k tokens, and the latest user message is always represented." type="number" min="256" step="1" value="${policy.recent_tail_tokens}" />
            </label>
          </div>
          ${renderModelRiskNotice(riskMessages)}
        </details>
        <section class="profile-card" data-new-tools>
          <div>
            <h2>Tools</h2>
            <p class="muted">Choose the tools and capabilities available to this session. You can update this toolset later from the session Inspector.</p>
          </div>
          ${renderCapabilitySummary(state.config?.capabilities)}
          ${renderToolMatrix({ tools, selected: selectedTools, editable: true, name: "new-tools", profileScope: "selected profile" })}
        </section>
        <section class="profile-card">
          <div>
            <h2>Validate Tool Contract</h2>
            <p class="muted">Runs an isolated definition and low-risk handoff probe. It does not execute every selected tool.</p>
          </div>
          <div class="form-actions">
            <span id="probeResult" class="muted">${escapeHTML(state.probeResult || "No probe has run.")}</span>
            <button type="button" class="${isProbing ? "is-pending" : ""}" data-run-probe ${isCreating || isProbing ? "disabled" : ""}>${isProbing ? "Validating..." : "Validate Tool Contract"}</button>
          </div>
        </section>
        <div class="form-actions">
          <span class="muted">The session will start immediately with the initial task.</span>
          <button class="primary ${isCreating ? "is-pending" : ""}" type="submit" ${isCreating || isProbing ? "disabled" : ""}>${isCreating ? "Creating..." : "Create Session & Send"}</button>
        </div>
      </form>
    </section>
  `;
}

function renderModelRiskNotice(messages) {
  if (!messages.length) return "";
  return `<div class="notice" role="status">${messages.map((message) => `<span>${escapeHTML(message)}</span>`).join("")}</div>`;
}

function renderCapabilitySummary(capabilities) {
  const skills = capabilities?.skills || [];
  const mcpServers = capabilities?.mcp_servers || [];
  const diagnostics = capabilities?.diagnostics || [];
  return `
    <section class="section">
      <h3>Capabilities</h3>
      ${renderCapabilityRows("MCP Servers", mcpServers, (item) => [
        item.name || "server",
        item.status || "unknown",
        item.transport || "transport n/a",
        `${item.tool_count || 0} tools`,
        item.permission_mode || "ask",
        item.next_action || "",
      ])}
      ${renderCapabilityRows("Agent Skills", skills, (item) => [
        item.name || "skill",
        item.status || "unknown",
        item.source_label || item.source_kind || "source n/a",
        item.relative_path || "",
        item.description || "",
      ])}
      ${renderCapabilityRows("Diagnostics", diagnostics, (item) => [
        item.kind || "diagnostic",
        item.capability || "",
        item.message || "",
        item.next_action || "",
      ])}
    </section>
  `;
}

function defaultAgentProfile() {
  return {
    id: "default",
    name: "Default assistant",
    description: "Default interactive agent.",
    system_prompt: "",
  };
}

function renderCapabilityRows(title, rows, valuesFor) {
  if (!rows.length) return `<div class="event-item"><strong>${escapeHTML(title)}</strong><span class="muted">none</span></div>`;
  return `
    <div class="event-list">
      <strong>${escapeHTML(title)}</strong>
      ${rows.map((row) => `
        <div class="event-item">
          ${valuesFor(row).filter(Boolean).map((value, index) => index === 0 ? `<strong>${escapeHTML(value)}</strong>` : `<span class="muted">${escapeHTML(value)}</span>`).join("")}
        </div>
      `).join("")}
    </div>
  `;
}

export function bindNewSession(root, handlers) {
  const form = root.querySelector("[data-new-session-form]");
  const toolArea = root.querySelector("[data-new-tools]");
  const persistDraft = () => {
    state.newSessionDraft = readDraft(form, toolArea);
  };
  form?.addEventListener("input", (event) => {
    if (event.isComposing) return;
    persistDraft();
  });
  form?.addEventListener("change", (event) => {
    persistDraft();
    if (event.target === form.elements.custom_prompt) {
      syncCustomPromptPanel(form);
    }
    if (event.target === form.elements.profile_id) {
      handlers.onSwitchProfile?.(form.elements.profile_id.value);
    }
  });
  const profiles = state.config?.profiles || [];
  const profile = profiles.find((item) => item.id === form?.elements.profile_id?.value) || currentProfile();
  bindToolPresets(toolArea, toolsForProfile(profile), "new-tools", persistDraft);
  syncCustomPromptPanel(form);
  root.querySelector("[data-run-probe]")?.addEventListener("click", () => {
    persistDraft();
    handlers.onProbe(readSelectedTools(toolArea, "new-tools"));
  });
  form?.addEventListener("submit", (event) => {
    event.preventDefault();
    handlers.onCreate(readDraft(form, toolArea));
  });
}

function readDraft(form, toolArea) {
  const data = new FormData(form);
  const customPrompt = data.get("custom_prompt") === "on";
  return {
    profile_id: String(data.get("profile_id") || ""),
    message: String(data.get("message") || ""),
    agent_profile: state.config?.agent_profile || defaultAgentProfile(),
    prompt_identity: state.config?.prompt_identity || {},
    custom_prompt: customPrompt,
    system_prompt: customPrompt ? String(data.get("system_prompt") || "") : "",
    selected_tools: readSelectedTools(toolArea, "new-tools"),
    context_policy: {
      context_window_tokens: Number(data.get("context_window_tokens") || 0),
      max_output_tokens: Number(data.get("max_output_tokens") || 0),
      recent_tail_tokens: Number(data.get("recent_tail_tokens") || 0),
    },
  };
}

function syncCustomPromptPanel(form) {
  if (!form) return;
  const enabled = Boolean(form.elements.custom_prompt?.checked);
  const panel = form.querySelector("[data-custom-prompt-panel]");
  const textarea = form.elements.system_prompt;
  if (panel) panel.hidden = !enabled;
  if (textarea) textarea.disabled = !enabled;
  if (textarea) textarea.required = enabled;
}
