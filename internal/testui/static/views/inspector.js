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
    editForm.addEventListener("input", (event) => {
      if (event.isComposing) return;
      persistDraft();
    });
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
      ${renderCapabilitySummary(session, observation)}
      <div class="tool-boundary-grid">
        <div>
          <strong>Selected tools/capabilities</strong>
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
      ${renderToolMatrix({ tools, selected, editable: true, name: "session-tools", profileScope: "session profile" })}
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

function renderCapabilitySummary(session, observation) {
  const caps = session.capabilities || {};
  const request = latestProviderRequest(observation.provider_requests || []);
  const requestTools = request?.tools || [];
  const mcpTools = requestTools.filter((tool) => tool.annotations?.source === "mcp");
  const skillTools = requestTools.filter((tool) => tool.annotations?.source === "skill");
  const mcpServers = caps.mcp_servers || [];
  const skills = caps.skills || [];
  const diagnostics = caps.diagnostics || [];
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
      ${renderCapabilityRows("MCP Tools", mcpTools, (tool) => [
        tool.name || "tool",
        `remote: ${tool.annotations?.mcp_tool || "unknown"}`,
        `server: ${tool.annotations?.mcp_server || "unknown"}`,
        `permission: ${tool.annotations?.permission_mode || "ask"}`,
      ])}
      ${renderCapabilityRows("Agent Skills", skills, (item) => [
        item.name || "skill",
        item.status || "unknown",
        item.source_label || item.source_kind || "source n/a",
        item.relative_path || "",
        item.description || "",
      ])}
      ${renderCapabilityRows("Skill Tool", skillTools, (tool) => [
        tool.name || "skill",
        `permission: ${tool.annotations?.permission_mode || "allow"}`,
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
            ${renderContextBudgetMetrics(request.context_usage)}
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

function renderContextBudgetMetrics(usage) {
  if (!usage) return "";
  const threshold = usage.threshold_tokens ?? usage.ThresholdTokens;
  const ratioLimit = usage.ratio_limit_tokens ?? usage.RatioLimitTokens;
  const requestSafe = usage.request_safe_limit_tokens ?? usage.RequestSafeLimit;
  const headroom = usage.output_headroom_tokens ?? usage.OutputHeadroom;
  const maxOutput = usage.max_output_tokens ?? usage.MaxOutputTokens;
  const ratio = usage.auto_compact_ratio_pct ?? usage.AutoCompactRatio;
  return [
    threshold || threshold === 0 ? `<span class="metric">threshold ${escapeHTML(threshold)}</span>` : "",
    requestSafe || requestSafe === 0 ? `<span class="metric">request safe ${escapeHTML(requestSafe)}</span>` : "",
    ratioLimit || ratioLimit === 0 ? `<span class="metric">ratio limit ${escapeHTML(ratioLimit)}</span>` : "",
    headroom || headroom === 0 ? `<span class="metric">output headroom ${escapeHTML(headroom)}</span>` : "",
    maxOutput || maxOutput === 0 ? `<span class="metric">max output ${escapeHTML(maxOutput)}</span>` : "",
    ratio || ratio === 0 ? `<span class="metric">auto compact ${escapeHTML(ratio)}%</span>` : "",
  ].join("");
}

function latestProviderRequest(requests) {
  return requests[requests.length - 1] || null;
}

function hostedToolLabel(tools) {
  if (!tools || !tools.length) return "none";
  return tools.map((tool) => {
    const shape = tool.options?.wire_shape || tool.wire_shape || hostedWireShapeFromType(tool.type);
    const shapeLabel = shape ? ` · ${shape}` : "";
    return `${tool.name || tool.type} (${tool.type || "hosted"}${shapeLabel})`;
  }).join(", ");
}

function hostedWireShapeFromType(type) {
  if (type === "web_search_20250305") return "anthropic_server_web_search";
  return "";
}

function renderEvents(events, providerEvents) {
  const all = [
    ...events.map((event) => ({ source: "event", ...event })),
    ...providerEvents.map((event) => ({ source: "provider", ...event })),
  ];
  if (!all.length) return `<p class="muted">No events captured yet.</p>`;
  return `
    <div class="event-list">
      ${all.map(renderEventRow).join("")}
    </div>
  `;
}

function renderEventRow(event) {
  const error = event.err || event.error || "";
  const summary = eventSummary(event);
  return `
    <details class="event-row ${error ? "event-error" : ""}">
      <summary>
        <span class="event-summary-main">
          <strong>${escapeHTML(event.source)} · ${escapeHTML(event.type || "")}</strong>
          ${event.step ? `<span class="tiny-pill">step ${escapeHTML(event.step)}</span>` : ""}
          ${event.run_id || event.runID ? `<span class="tiny-pill">${escapeHTML(event.run_id || event.runID)}</span>` : ""}
          ${event.tool_name || event.ToolName ? `<span class="tiny-pill">${escapeHTML(event.tool_name || event.ToolName)}</span>` : ""}
          ${event.duration_ms ? `<span class="tiny-pill">${escapeHTML(formatDuration(event.duration_ms))}</span>` : ""}
        </span>
        <span class="event-summary-preview">${escapeHTML(summary)}</span>
      </summary>
      <div class="event-row-body">
        ${renderEventFacts(event)}
        <pre class="json-block">${escapeHTML(JSON.stringify(event, null, 2))}</pre>
      </div>
    </details>
  `;
}

function renderEventFacts(event) {
  const facts = [
    ["Source", event.source],
    ["Type", event.type],
    ["Run", event.run_id || event.runID],
    ["Session", event.session_id || event.sessionID],
    ["Step", event.step],
    ["Tool", event.tool_name || event.toolName],
    ["Tool ID", event.tool_id || event.toolID],
    ["Finish", event.finish_reason || event.finishReason || event.raw_finish_reason],
    ["Observed", event.observed_at || event.timestamp],
    ["Message", event.message || event.reason || event.text || event.reasoning || event.err || event.error],
  ].filter(([, value]) => value || value === 0);
  if (!facts.length) return "";
  return `
    <div class="event-facts">
      ${facts.map(([key, value]) => `<div class="key-value"><span>${escapeHTML(key)}</span><span>${escapeHTML(value)}</span></div>`).join("")}
    </div>
  `;
}

function eventSummary(event) {
  const toolCalls = event.tool_calls || event.toolCalls || [];
  const candidates = [
    event.err,
    event.error,
    event.message,
    event.reason,
    event.text,
    event.reasoning,
    event.result,
    toolCalls.length ? `${toolCalls.length} tool call${toolCalls.length === 1 ? "" : "s"}` : "",
  ];
  return truncateOneLine(candidates.find(Boolean) || "Open for details.", 140);
}

function truncateOneLine(value, limit) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (text.length <= limit) return text;
  return `${text.slice(0, Math.max(0, limit - 3))}...`;
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
