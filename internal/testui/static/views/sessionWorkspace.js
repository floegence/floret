import { escapeHTML, formatTime, profileLabel, shortID, state, toolLabelList, totalTokens } from "../state.js";
import { bindInspector, renderInspector } from "./inspector.js";

export function renderSessionWorkspace({ sessions, activeSession, result, tools, inspectorTab }) {
  return `
    <div class="sessions-layout ${state.mobilePanel === "sessions" ? "show-sessions" : ""} ${state.mobilePanel === "inspector" ? "show-inspector" : ""}">
      ${renderSessionRail(sessions, activeSession)}
      ${renderWorkspace(activeSession, result)}
      ${renderInspector({ session: activeSession, result, tools, tab: inspectorTab })}
    </div>
  `;
}

export function bindSessionWorkspace(root, handlers) {
  root.querySelector("[data-session-filter]")?.addEventListener("input", (event) => handlers.onFilter(event.target.value));
  root.querySelector("[data-refresh-sessions]")?.addEventListener("click", handlers.onRefresh);
  root.querySelectorAll("[data-mobile-panel]").forEach((button) => {
    button.addEventListener("click", () => handlers.onMobilePanel(button.dataset.mobilePanel || ""));
  });
  root.querySelectorAll("[data-session-id]").forEach((button) => {
    button.addEventListener("click", () => handlers.onSelect(button.dataset.sessionId || ""));
  });
  root.querySelector("[data-append-form]")?.addEventListener("submit", (event) => {
    event.preventDefault();
    const message = event.currentTarget.elements.message.value.trim();
    if (message) handlers.onAppend(message);
  });
  bindInspector(root, {
    tools: handlers.tools,
    onEditTools: handlers.onEditTools,
    onTab: handlers.onInspectorTab,
  });
}

function renderSessionRail(sessions, activeSession) {
  const filter = state.sessionFilter.trim().toLowerCase();
  const filtered = (sessions || []).filter((session) => {
    if (!filter) return true;
    return [session.id, session.status, session.profile?.model, (session.selected_tools || []).join(" ")].join(" ").toLowerCase().includes(filter);
  });
  return `
    <aside class="session-rail">
      <div class="rail-head">
        <h2>Sessions</h2>
        <a class="button primary small" href="/sessions/new" data-link>New Session</a>
      </div>
      <div class="rail-tools">
        <input class="search-input" data-session-filter placeholder="Filter sessions, model, tools" value="${escapeHTML(state.sessionFilter)}" />
        <button class="small" type="button" data-refresh-sessions>Refresh</button>
      </div>
      <div class="session-list">
        ${filtered.length ? filtered.map((session) => renderSessionRow(session, activeSession?.id)).join("") : `<div class="section muted">No sessions match the filter.</div>`}
      </div>
    </aside>
  `;
}

function renderSessionRow(session, activeID) {
  const turns = session.turns?.length || 0;
  return `
    <button class="session-row ${session.id === activeID ? "active" : ""}" type="button" data-session-id="${escapeHTML(session.id)}">
      <strong>${escapeHTML(shortID(session.id))}</strong>
      <span class="row-meta">${escapeHTML(session.status || "idle")} · ${turns} turn${turns === 1 ? "" : "s"} · ${escapeHTML(session.profile?.model || "model")}</span>
      <span class="row-pills">
        <span class="tiny-pill">${escapeHTML((session.selected_tools || []).length)} tools</span>
        <span class="tiny-pill">${escapeHTML(formatTime(session.updated_at))}</span>
      </span>
    </button>
  `;
}

function renderWorkspace(session, result) {
  if (!session) {
    return `
      <section class="workspace">
        <div class="empty-state">
          <h1>No active session</h1>
          <p>Create a session to bind a model, system prompt, context policy, and toolset. Existing sessions appear in the left rail.</p>
          <a class="button primary" href="/sessions/new" data-link>New Session</a>
        </div>
      </section>
    `;
  }
  const turns = session.turns || [];
  const canAppend = session.can_append_message && !state.running;
  return `
    <section class="workspace">
      <header class="workspace-head">
        <div class="workspace-title">
          <h1>${escapeHTML(shortID(session.id))}</h1>
          <p>${escapeHTML(profileLabel(session.profile))} · ${escapeHTML(toolLabelList(session.selected_tools || []))}</p>
        </div>
        <div class="header-meta">
          <span class="status-pill ${escapeHTML(session.status || "idle")}">${escapeHTML(session.status || "idle")}</span>
          <span class="metric">${turns.length} turns</span>
          <span class="metric">${totalTokens(session.aggregate_metrics?.usage)} tokens</span>
        </div>
        <div class="mobile-workspace-actions">
          <button type="button" class="small" data-mobile-panel="sessions">Sessions</button>
          <button type="button" class="small" data-mobile-panel="inspector">Inspector</button>
        </div>
      </header>
      <div class="conversation">
        ${renderTimeline(session, result)}
      </div>
      <form class="composer-bar" data-append-form>
        <textarea name="message" placeholder="${canAppend ? "Send the next message to this session" : appendDisabledReason(session)}" ${canAppend ? "" : "disabled"}></textarea>
        <div class="composer-actions">
          <span class="muted">${escapeHTML(appendDisabledReason(session))}</span>
          <button class="primary" type="submit" ${canAppend ? "" : "disabled"}>Send</button>
        </div>
      </form>
    </section>
  `;
}

function renderTimeline(session, result) {
  const entries = session.path_entries || [];
  if (!entries.length) return `<div class="message entry"><div class="message-text muted">No durable messages yet.</div></div>`;
  const messages = entries.filter((entry) => {
    return ["user_message", "assistant_message", "tool_call", "tool_result", "active_tools_change", "run_failure", "turn_marker"].includes(entry.type);
  });
  return messages.map(renderEntry).join("");
}

function renderEntry(entry) {
  if (entry.type === "turn_marker") {
    return `
      <article class="message entry">
        <div class="message-head"><span>turn ${escapeHTML(entry.turn_status || "")}</span><span>${escapeHTML(entry.turn_id || "")}</span></div>
      </article>
    `;
  }
  if (entry.type === "active_tools_change") {
    const meta = entry.metadata || {};
    return `
      <article class="message entry">
        <div class="message-head"><span>tools changed</span><span>${escapeHTML(formatTime(entry.created_at))}</span></div>
        <div class="message-text">${escapeHTML(meta.previous_tools || "none")} -> ${escapeHTML(meta.selected_tools || "none")}</div>
        ${meta.reason ? `<div class="muted">${escapeHTML(meta.reason)}</div>` : ""}
      </article>
    `;
  }
  if (entry.type === "run_failure") {
    return `
      <article class="message entry">
        <div class="message-head"><span>run failure</span><span>${escapeHTML(entry.turn_id || "")}</span></div>
        <pre>${escapeHTML(entry.error || "")}</pre>
      </article>
    `;
  }
  const msg = entry.message || {};
  const role = msg.role || (entry.type === "tool_call" ? "assistant" : "entry");
  const title = entry.type === "tool_call" ? `tool call · ${msg.tool_name || ""}` : entry.type === "tool_result" ? `tool result · ${msg.tool_name || ""}` : role;
  const body = entry.type === "tool_call" ? msg.tool_args || msg.content : msg.content;
  return `
    <article class="message ${escapeHTML(role)}">
      <div class="message-head"><span>${escapeHTML(title)}</span><span>${escapeHTML(entry.turn_id || "")}</span></div>
      ${msg.reasoning ? `<pre class="code-block">${escapeHTML(msg.reasoning)}</pre>` : ""}
      <div class="message-text">${escapeHTML(body || "")}</div>
    </article>
  `;
}

function appendDisabledReason(session) {
  if (state.running) return "A request is running.";
  if (!session?.can_append_message) return `This session is ${session?.status || "not ready"} and cannot accept another message.`;
  return "This message will use the current session tools.";
}
