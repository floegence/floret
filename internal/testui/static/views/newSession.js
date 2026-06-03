import { currentProfile, defaultContextPolicy, escapeHTML, profileLabel, state } from "../state.js";
import { bindToolPresets, readSelectedTools, renderToolMatrix } from "../components/toolMatrix.js";

export function renderNewSession() {
  const profile = currentProfile();
  const policy = defaultContextPolicy();
  return `
    <section class="new-page">
      <header class="new-head">
        <div>
          <h1>New Session</h1>
          <p class="muted">Create a clean agent session with an explicit model, prompt, context policy, and toolset.</p>
        </div>
        <a class="button ghost" href="/sessions" data-link>Cancel</a>
      </header>
      <form class="form-grid" data-new-session-form>
        <label class="field">
          <span>Profile</span>
          <select name="profile_id">
            ${(state.config?.profiles || []).map((item) => `<option value="${escapeHTML(item.id)}" ${item.id === profile.id ? "selected" : ""}>${escapeHTML(profileLabel(item))}</option>`).join("")}
          </select>
        </label>
        <label class="field">
          <span>Initial task</span>
          <textarea name="message" required>Say hello from Floret and complete the task.</textarea>
        </label>
        <label class="field">
          <span>System prompt</span>
          <textarea name="system_prompt" required>You are Floret. Answer naturally when the user's request is complete, or call ask_user if you need missing information.</textarea>
        </label>
        <div class="field-row">
          <label class="field">
            <span>Context window</span>
            <input name="context_window_tokens" type="number" min="1024" step="1024" value="${policy.context_window_tokens}" />
          </label>
          <label class="field">
            <span>Max output</span>
            <input name="max_output_tokens" type="number" min="256" step="256" value="${policy.max_output_tokens}" />
          </label>
          <label class="field">
            <span>Recent tail</span>
            <input name="recent_tail_tokens" type="number" min="256" step="256" value="${policy.recent_tail_tokens}" />
          </label>
        </div>
        <section class="profile-card" data-new-tools>
          <div>
            <h2>Tools</h2>
            <p class="muted">Choose the local tools available to this session. You can edit them later from the session Inspector.</p>
          </div>
          ${renderToolMatrix({ tools: state.config?.tools || [], selected: [], editable: true, name: "new-tools" })}
        </section>
        <section class="profile-card">
          <div>
            <h2>Validate Tool Contract</h2>
            <p class="muted">Runs an isolated definition and low-risk handoff probe. It does not execute every selected tool.</p>
          </div>
          <div class="form-actions">
            <span id="probeResult" class="muted">${escapeHTML(state.probeResult || "No probe has run.")}</span>
            <button type="button" data-run-probe>Validate Tool Contract</button>
          </div>
        </section>
        <div class="form-actions">
          <span class="muted">The session will start immediately with the initial task.</span>
          <button class="primary" type="submit">Create Session & Send</button>
        </div>
      </form>
    </section>
  `;
}

export function bindNewSession(root, handlers) {
  const form = root.querySelector("[data-new-session-form]");
  const toolArea = root.querySelector("[data-new-tools]");
  bindToolPresets(toolArea, state.config?.tools || [], "new-tools");
  root.querySelector("[data-run-probe]")?.addEventListener("click", () => {
    handlers.onProbe(readSelectedTools(toolArea, "new-tools"));
  });
  form?.addEventListener("submit", (event) => {
    event.preventDefault();
    const data = new FormData(form);
    handlers.onCreate({
      profile_id: String(data.get("profile_id") || ""),
      message: String(data.get("message") || ""),
      system_prompt: String(data.get("system_prompt") || ""),
      selected_tools: readSelectedTools(toolArea, "new-tools"),
      context_policy: {
        context_window_tokens: Number(data.get("context_window_tokens") || 0),
        max_output_tokens: Number(data.get("max_output_tokens") || 0),
        recent_tail_tokens: Number(data.get("recent_tail_tokens") || 0),
      },
    });
  });
}
