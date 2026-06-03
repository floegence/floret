import { escapeHTML, formatLocalTime, profileLabel, relativeTime, shortID, state, toolLabelList, totalTokens } from "../state.js";
import { bindInspector, renderInspector } from "./inspector.js";

const copyPayloads = new Map();

export function renderSessionWorkspace({ sessions, activeSession, result, tools, inspectorTab }) {
  copyPayloads.clear();
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
  root.querySelectorAll("[data-copy-key]").forEach((button) => {
    button.addEventListener("click", () => handlers.onCopy(copyPayloads.get(button.dataset.copyKey || "") || "", button.dataset.copyLabel || "Copied"));
  });
  root.querySelectorAll("[data-delete-session]").forEach((button) => {
    button.addEventListener("click", () => handlers.onDelete(button.dataset.deleteSession || ""));
  });
  root.querySelector("[data-append-form]")?.addEventListener("submit", (event) => {
    event.preventDefault();
    const message = event.currentTarget.elements.message.value.trim();
    handlers.onComposerDraft(state.activeSession?.id || "", event.currentTarget.elements.message.value);
    if (message) handlers.onAppend(message);
  });
  root.querySelector("[data-append-form] textarea[name=\"message\"]")?.addEventListener("input", (event) => {
    handlers.onComposerDraft(state.activeSession?.id || "", event.target.value);
  });
  bindInspector(root, {
    tools: handlers.tools,
    onEditTools: handlers.onEditTools,
    onToolEditDraft: handlers.onToolEditDraft,
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
        <button class="small ${state.action === "refresh-sessions" ? "is-pending" : ""}" type="button" data-refresh-sessions ${state.action === "refresh-sessions" ? "disabled" : ""}>${state.action === "refresh-sessions" ? "Refreshing..." : "Refresh"}</button>
      </div>
      <div class="session-list">
        ${filtered.length ? filtered.map((session) => renderSessionRow(session, activeSession?.id)).join("") : `<div class="section muted">No sessions match the filter.</div>`}
      </div>
    </aside>
  `;
}

function renderSessionRow(session, activeID) {
  const turns = session.turns?.length || 0;
  const exactTime = formatLocalTime(session.updated_at);
  return `
    <article class="session-row ${session.id === activeID ? "active" : ""}">
      <button class="session-select" type="button" data-session-id="${escapeHTML(session.id)}">
        <strong>${escapeHTML(shortID(session.id))}</strong>
        <span class="row-meta">${escapeHTML(session.status || "idle")} · ${turns} turn${turns === 1 ? "" : "s"} · ${escapeHTML(session.profile?.model || "model")}</span>
        <span class="row-pills">
          <span class="tiny-pill">${escapeHTML((session.selected_tools || []).length)} tools</span>
          <span class="tiny-pill" title="${escapeHTML(exactTime)}">${escapeHTML(relativeTime(session.updated_at))}</span>
        </span>
      </button>
      <div class="row-actions">
        ${copyButton(session.id, "Copy ID", "Session id copied", "small ghost")}
        <button class="small danger" type="button" data-delete-session="${escapeHTML(session.id)}">Delete</button>
      </div>
    </article>
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
  const isSending = state.action === "append-turn";
  const canAppend = session.can_append_message && !state.running;
  const composerDraft = state.composerDrafts[session.id] || "";
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
          ${copyButton(session.id, "Copy ID", "Session id copied", "small ghost")}
          <button class="small danger" type="button" data-delete-session="${escapeHTML(session.id)}">Delete</button>
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
        <textarea name="message" placeholder="${canAppend ? "Send the next message to this session" : appendDisabledReason(session)}" ${canAppend ? "" : "disabled"}>${escapeHTML(composerDraft)}</textarea>
        <div class="composer-actions">
          <span class="muted">${escapeHTML(appendDisabledReason(session))}</span>
          <button class="primary ${isSending ? "is-pending" : ""}" type="submit" ${canAppend ? "" : "disabled"}>${isSending ? "Sending..." : "Send"}</button>
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
    const payload = `turn ${entry.turn_status || ""} ${entry.turn_id || ""}`.trim();
    return `
      <article class="message entry">
        <div class="message-head"><span>turn ${escapeHTML(entry.turn_status || "")}</span><span>${escapeHTML(entry.turn_id || "")}</span>${copyButton(payload)}</div>
      </article>
    `;
  }
  if (entry.type === "active_tools_change") {
    const meta = entry.metadata || {};
    const body = `${meta.previous_tools || "none"} -> ${meta.selected_tools || "none"}${meta.reason ? `\n${meta.reason}` : ""}`;
    return `
      <article class="message entry">
        <div class="message-head"><span>tools changed</span><span>${escapeHTML(formatLocalTime(entry.created_at))}</span>${copyButton(body)}</div>
        ${renderMessageBody(body, "tools changed")}
        ${meta.reason ? `<div class="muted">${escapeHTML(meta.reason)}</div>` : ""}
      </article>
    `;
  }
  if (entry.type === "run_failure") {
    const body = entry.error || "";
    const copyPayload = body || `run failure\nturn_id: ${entry.turn_id || "-"}`;
    return `
      <article class="message entry">
        <div class="message-head"><span>run failure</span><span>${escapeHTML(entry.turn_id || "")}</span>${copyButton(copyPayload)}</div>
        ${renderMessageBody(body, "run failure")}
      </article>
    `;
  }
  const msg = entry.message || {};
  const role = msg.role || (entry.type === "tool_call" ? "assistant" : "entry");
  const title = entry.type === "tool_call" ? `tool call · ${msg.tool_name || ""}` : entry.type === "tool_result" ? `tool result · ${msg.tool_name || ""}` : role;
  const body = entry.type === "tool_call" ? msg.tool_args || msg.content : msg.content;
  const copyPayload = [msg.reasoning, body].filter(Boolean).join("\n\n") || structuredEntryCopy(entry, title, msg);
  return `
    <article class="message ${escapeHTML(role)}">
      <div class="message-head"><span>${escapeHTML(title)}</span><span>${escapeHTML(entry.turn_id || "")}</span>${copyButton(copyPayload)}</div>
      ${msg.reasoning ? `<pre class="code-block">${escapeHTML(msg.reasoning)}</pre>` : ""}
      ${renderMessageBody(body || "", title)}
    </article>
  `;
}

function structuredEntryCopy(entry, title, msg) {
  return [
    title,
    `entry_type: ${entry.type || "-"}`,
    `turn_id: ${entry.turn_id || "-"}`,
    `role: ${msg.role || "-"}`,
    msg.tool_name ? `tool_name: ${msg.tool_name}` : "",
    msg.tool_call_id ? `tool_call_id: ${msg.tool_call_id}` : "",
  ].filter(Boolean).join("\n");
}

function copyButton(payload, text = "Copy", label = "Message copied", className = "copy-inline") {
  const key = `copy-${copyPayloads.size + 1}`;
  copyPayloads.set(key, String(payload || ""));
  return `<button class="${escapeHTML(className)}" type="button" data-copy-key="${escapeHTML(key)}" data-copy-label="${escapeHTML(label)}">${escapeHTML(text)}</button>`;
}

function renderMessageBody(body, label) {
  const text = String(body || "");
  const lineCount = text.split("\n").length;
  const long = text.length > 1200 || lineCount > 12;
  if (!long) {
    return `<div class="message-text">${escapeHTML(text)}</div>`;
  }
  const preview = text.slice(0, 180).replace(/\s+/g, " ").trim();
  return `
    <details class="message-fold">
      <summary>${escapeHTML(label)} · ${text.length} chars · ${lineCount} lines · ${escapeHTML(preview)}${text.length > 180 ? "..." : ""}</summary>
      <div class="message-text">${escapeHTML(text)}</div>
    </details>
  `;
}

function appendDisabledReason(session) {
  if (state.running) return "A request is running.";
  if (!session?.can_append_message) return `This session is ${session?.status || "not ready"} and cannot accept another message.`;
  return "This message will use the current session tools.";
}
