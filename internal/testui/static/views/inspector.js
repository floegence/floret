import { escapeHTML, formatDuration, formatLocalTime, state, toolLabelList, totalTokens } from "../state.js";
import { bindToolPresets, readSelectedTools, renderToolMatrix } from "../components/toolMatrix.js";

export function renderInspector({ session, result, tools, tab }) {
  if (!session) {
    return `
      <aside class="inspector">
        <div class="inspector-head"><h2>Inspector</h2></div>
        <div class="inspector-body muted">Select or create a session to inspect tools, requests, events, and context.</div>
      </aside>
    `;
  }
  const activeTab = tab || "tools";
  const observation = result?.session_id === session.id ? result.observation || {} : session.observation || {};
  return `
    <aside class="inspector">
      <div class="inspector-head">
        <h2>Inspector</h2>
        <span class="tiny-pill">${escapeHTML(toolLabelList(session.selected_tools || []))}</span>
      </div>
      <div class="inspector-tabs" role="tablist" aria-label="Inspector">
        ${["tools", "requests", "events", "context", "raw"].map((item) => `<button type="button" data-inspector-tab="${item}" class="${activeTab === item ? "active" : ""}">${label(item)}</button>`).join("")}
      </div>
      <div class="inspector-body">
        ${renderTab(activeTab, session, observation, result, tools)}
      </div>
    </aside>
  `;
}

export function bindInspector(root, { tools, onEditTools, onToolEditDraft, onTab }) {
  root.querySelectorAll("[data-inspector-tab]").forEach((button) => {
    button.addEventListener("click", () => onTab(button.dataset.inspectorTab || "tools"));
  });
  const editForm = root.querySelector("[data-tool-edit-form]");
  if (editForm) {
    const persistDraft = () => onToolEditDraft(editForm.dataset.sessionId || "", readDraft(editForm));
    bindToolPresets(editForm, tools, "session-tools", persistDraft);
    editForm.addEventListener("input", persistDraft);
    editForm.addEventListener("change", persistDraft);
    editForm.addEventListener("submit", (event) => {
      event.preventDefault();
      onEditTools(readSelectedTools(editForm, "session-tools"), editForm.elements.reason?.value || "");
    });
  }
}

function renderTab(tab, session, observation, result, tools) {
  switch (tab) {
    case "requests":
      return renderRequests(observation.provider_requests || []);
    case "events":
      return renderEvents([...(result?.events || []), ...(result?.harness_events || [])], observation.provider_events || []);
    case "context":
      return renderContext(session);
    case "raw":
      return `<pre class="json-block">${escapeHTML(JSON.stringify({ session, result }, null, 2))}</pre>`;
    case "tools":
    default:
      return renderTools(session, tools, observation);
  }
}

function renderTools(session, tools, observation) {
  const audit = (session.path_entries || []).filter((entry) => entry.type === "active_tools_change").slice().reverse();
  const request = latestProviderRequest(observation.provider_requests || []);
  const isUpdating = state.action === "update-tools";
  const draft = state.toolEditDrafts[session.id] || {};
  const selected = draft.selected_tools || session.selected_tools || [];
  return `
    <form class="profile-card" data-tool-edit-form data-session-id="${escapeHTML(session.id)}">
      <div>
        <h3>Session Tools</h3>
        <p class="muted">These tools are bound to this session. Changes here affect future turns only.</p>
      </div>
      <div class="tool-boundary-grid">
        <div>
          <strong>Local client tools</strong>
          <span>${escapeHTML(toolLabelList(session.selected_tools || []))}</span>
        </div>
        <div>
          <strong>Always available</strong>
          <span>ask_user</span>
        </div>
        <div>
          <strong>Provider-hosted tools</strong>
          <span>${escapeHTML(hostedToolLabel(session.hosted_tools || request?.hosted_tools || []))}</span>
        </div>
        <div>
          <strong>Unavailable</strong>
          <span>${escapeHTML((session.unavailable_capabilities || request?.unavailable_capabilities || []).join("; ") || "none")}</span>
        </div>
      </div>
      ${renderToolMatrix({ tools, selected, editable: true, name: "session-tools" })}
      <label class="field">
        <span>Audit note</span>
        <input name="reason" value="${escapeHTML(draft.reason || "")}" placeholder="why this toolset changed" />
      </label>
      <div class="form-actions">
        <span class="muted">Control capability: ask_user is always available.</span>
        <button class="primary ${isUpdating ? "is-pending" : ""}" type="submit" ${isUpdating ? "disabled" : ""}>${isUpdating ? "Updating..." : "Update Session Tools"}</button>
      </div>
    </form>
    <section class="section">
      <h3>Tool Change Audit</h3>
      ${audit.length ? audit.map(renderToolAudit).join("") : `<p class="muted">No tool changes after session creation.</p>`}
    </section>
  `;
}

function readDraft(form) {
  return {
    selected_tools: readSelectedTools(form, "session-tools"),
    reason: form.elements.reason?.value || "",
  };
}

function renderToolAudit(entry) {
  const meta = entry.metadata || {};
  return `
    <div class="event-item">
      <strong>${escapeHTML(meta.previous_tools || "none")} -> ${escapeHTML(meta.selected_tools || "none")}</strong>
      <span class="muted">${escapeHTML(formatLocalTime(entry.created_at))}${meta.reason ? ` · ${escapeHTML(meta.reason)}` : ""}</span>
    </div>
  `;
}

function renderRequests(requests) {
  if (!requests.length) return `<p class="muted">No provider request captured for the selected session view.</p>`;
  return `
    <div class="request-list">
      ${requests.map((request, index) => `
        <article class="request-item">
          <strong>Step ${escapeHTML(request.step || index + 1)} · ${escapeHTML(request.provider || "")} / ${escapeHTML(request.model || "")}</strong>
          <div class="metric-strip">
            <span class="metric">${(request.messages || []).length} messages</span>
            <span class="metric">${(request.tools || []).length} tools</span>
            <span class="metric">${escapeHTML(request.cache_summary?.toolset_id || "toolset n/a")}</span>
            <span class="metric">${totalTokens(request.context_usage)} est tokens</span>
          </div>
          <div class="key-value"><span>Tools</span><span>${escapeHTML((request.tools || []).map((tool) => tool.name).join(", ") || "none")}</span></div>
          <div class="key-value"><span>Hosted</span><span>${escapeHTML(hostedToolLabel(request.hosted_tools || []))}</span></div>
          <div class="key-value"><span>Unavailable</span><span>${escapeHTML((request.unavailable_capabilities || []).join("; ") || "none")}</span></div>
          <details>
            <summary>Messages</summary>
            <pre class="json-block">${escapeHTML(JSON.stringify(request.messages || [], null, 2))}</pre>
          </details>
        </article>
      `).join("")}
    </div>
  `;
}

function latestProviderRequest(requests) {
  return requests[requests.length - 1] || null;
}

function hostedToolLabel(tools) {
  if (!tools || !tools.length) return "none";
  return tools.map((tool) => {
    const shape = tool.options?.wire_shape ? ` · ${tool.options.wire_shape}` : "";
    return `${tool.name || tool.type} (${tool.type || "hosted"}${shape})`;
  }).join(", ");
}

function renderEvents(events, providerEvents) {
  const all = [
    ...events.map((event) => ({ source: "event", ...event })),
    ...providerEvents.map((event) => ({ source: "provider", ...event })),
  ];
  if (!all.length) return `<p class="muted">No events captured yet.</p>`;
  return `
    <div class="event-list">
      ${all.map((event) => `
        <article class="event-item">
          <strong>${escapeHTML(event.source)} · ${escapeHTML(event.type || "")}</strong>
          <span class="muted">${escapeHTML(event.reason || event.message || event.text || event.reasoning || "")}</span>
          ${event.duration_ms ? `<span class="tiny-pill">${escapeHTML(formatDuration(event.duration_ms))}</span>` : ""}
        </article>
      `).join("")}
    </div>
  `;
}

function renderContext(session) {
  const path = session.path_entries || [];
  if (!path.length) return `<p class="muted">No durable entries yet.</p>`;
  return `
    <div class="event-list">
      ${path.map((entry) => `
        <article class="context-item">
          <strong>${escapeHTML(entry.type)}${entry.turn_status ? ` · ${escapeHTML(entry.turn_status)}` : ""}</strong>
          <span class="muted">${escapeHTML(entry.id || "")}</span>
          ${entry.message?.content ? `<pre class="code-block">${escapeHTML(entry.message.content)}</pre>` : ""}
          ${entry.error ? `<pre class="code-block">${escapeHTML(entry.error)}</pre>` : ""}
          ${entry.metadata ? `<pre class="json-block">${escapeHTML(JSON.stringify(entry.metadata, null, 2))}</pre>` : ""}
        </article>
      `).join("")}
    </div>
  `;
}

function label(tab) {
  switch (tab) {
    case "tools":
      return "Tools";
    case "requests":
      return "Requests";
    case "events":
      return "Events";
    case "context":
      return "Context";
    case "raw":
      return "Raw";
    default:
      return tab;
  }
}
