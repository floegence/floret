import { escapeHTML, formatLocalTime, profileLabel, relativeTime, shortID, state, toolLabelList, totalTokens } from "../state.js";
import {
  compactionEventsFor,
  compactionTitle,
  compactionTokenLabel,
  latestContextStatus,
  renderCompactionEventRow,
  renderContextMeter,
  renderContextStatusRow,
} from "../contextStatus.js";
import { bindInspector, renderInspector } from "./inspector.js";

const copyPayloads = new Map();
const emptyToolArguments = "No arguments were captured for this tool call.";
const emptyToolResult = "No result body was captured for this tool run.";
const pendingToolResult = "Waiting for tool result...";

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
  root.querySelectorAll("[data-session-select-id]").forEach((button) => {
    button.addEventListener("click", () => handlers.onSelect(button.dataset.sessionSelectId || ""));
  });
  root.querySelector("[data-context-meter]")?.addEventListener("click", () => {
    handlers.onInspectorTab("context");
    if (window.matchMedia("(max-width: 820px)").matches) {
      handlers.onMobilePanel("inspector");
    }
  });
  root.querySelectorAll("[data-copy-key]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      handlers.onCopy(copyPayloads.get(button.dataset.copyKey || "") || "", button.dataset.copyLabel || "Copied");
    });
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
    if (event.isComposing) return;
    handlers.onComposerDraft(state.activeSession?.id || "", event.target.value);
  });
  root.querySelectorAll(".conversation details[data-expand-key]").forEach((details) => {
    details.addEventListener("toggle", () => {
      state.timelineExpanded[details.dataset.expandKey || ""] = details.open;
    });
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
      <button class="session-select" type="button" data-session-select-id="${escapeHTML(session.id)}">
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
  const contextStatus = latestContextStatus(session, result);
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
        <div class="workspace-context-meter">
          ${renderContextMeter(contextStatus)}
        </div>
      </header>
      <div class="conversation" data-session-id="${escapeHTML(session.id)}">
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
  const entries = mergeTimelineEntries(session);
  const compactions = compactionEventsFor(session, result);
  if (!entries.length && !compactions.length && !activeLiveTurn(session)) return `<div class="message entry"><div class="message-text muted">No durable messages yet.</div></div>`;
  const visibleEntries = entries.filter((entry) => {
    return ["user_message", "assistant_message", "tool_call", "tool_result", "active_tools_change", "run_failure", "turn_marker"].includes(entry.type);
  });
  return `${timelineItems(timelineRows(visibleEntries, compactions)).map(renderTimelineItem).join("")}${renderLiveTurn(session)}`;
}

function timelineRows(entries, compactions) {
  const rows = [
    ...entries.map((entry) => ({ type: "entry", entry, at: entry.created_at || "" })),
    ...compactions.map((compaction) => ({ type: "compaction", compaction, at: compaction.observed_at || "" })),
  ];
  return rows.sort((a, b) => {
    const at = Date.parse(a.at || "");
    const bt = Date.parse(b.at || "");
    if (Number.isFinite(at) && Number.isFinite(bt) && at !== bt) return at - bt;
    return 0;
  });
}

function timelineItems(rows) {
  const items = [];
  const pendingToolRuns = new Map();
  for (const row of rows) {
    if (row.type === "compaction") {
      items.push(row);
      continue;
    }
    const entry = row.entry || row;
    if (entry.type !== "tool_call" && entry.type !== "tool_result") {
      items.push({ type: "entry", entry });
      continue;
    }
    const key = toolRunKey(entry);
    if (!key) {
      items.push({ type: "entry", entry });
      continue;
    }
    const existing = pendingToolRuns.get(key);
    if (existing) {
      if (entry.type === "tool_call") existing.call = entry;
      if (entry.type === "tool_result") existing.result = entry;
      continue;
    }
    const run = { type: "tool_run", key, call: entry.type === "tool_call" ? entry : null, result: entry.type === "tool_result" ? entry : null };
    pendingToolRuns.set(key, run);
    items.push(run);
  }
  return items;
}

function renderTimelineItem(item) {
  if (item.type === "tool_run") return renderToolRun(item);
  if (item.type === "compaction") return renderCompactionTimelineItem(item.compaction);
  return renderEntry(item.entry);
}

function renderCompactionTimelineItem(compaction) {
  const copyPayload = [
    `context compaction ${compaction?.phase || ""}`.trim(),
    compaction?.trigger ? `trigger: ${compaction.trigger}` : "",
    compaction?.reason ? `reason: ${compaction.reason}` : "",
    compactionTokenLabel(compaction),
    compaction?.compaction_id ? `compaction_id: ${compaction.compaction_id}` : "",
    compaction?.summary ? `summary:\n${compaction.summary}` : "",
    compaction?.error ? `error: ${compaction.error}` : "",
  ].filter(Boolean).join("\n");
  const title = compaction?.phase === "complete" ? "context compacted" : "context compaction";
  return `
    <article class="message entry context-compact-item" title="${escapeHTML(compactionTitle(compaction))}">
      <div class="message-head">
        <span>${escapeHTML(title)}</span>
        <span>${escapeHTML(formatLocalTime(compaction?.observed_at))}</span>
        ${copyButton(copyPayload, "Copy", "Compaction copied")}
      </div>
      ${renderCompactionEventRow(compaction)}
    </article>
  `;
}

function toolRunKey(entry) {
  const msg = entry.message || {};
  if (!msg.tool_call_id) return "";
  return `${entry.turn_id || ""}:${msg.tool_call_id}`;
}

function mergeTimelineEntries(session) {
  const entries = [];
  const seen = new Set();
  const append = (entry) => {
    if (!entry) return;
    const key = timelineEntryKey(entry);
    if (seen.has(key)) return;
    seen.add(key);
    entries.push(entry);
  };
  (session.path_entries || []).forEach(append);
  const live = activeLiveTurn(session);
  (live?.entries || []).forEach(append);
  return entries;
}

function timelineEntryKey(entry) {
  if (entry.id) return `id:${entry.id}`;
  const msg = entry.message || {};
  if (msg.tool_call_id) return `tool:${entry.type || ""}:${entry.turn_id || ""}:${msg.tool_call_id}`;
  return `${entry.type || "entry"}:${entry.turn_id || ""}:${entry.created_at || ""}:${msg.role || ""}:${String(msg.content || "").slice(0, 80)}`;
}

function activeLiveTurn(session) {
  if (!session?.id || !state.liveTurn) return null;
  return state.liveTurn.session_id === session.id ? state.liveTurn : null;
}

function renderLiveTurn(session) {
  const live = activeLiveTurn(session);
  if (!live) return "";
  const durableUser = (session.path_entries || []).some((entry) => entry.type === "user_message" && entry.turn_id === live.turn_id);
  const userEcho = !durableUser && live.user_message ? `
    <article class="message user pending">
      <div class="message-head"><span>user</span><span>pending</span>${copyButton(live.user_message)}</div>
      ${renderExpandableBody(live.user_message, { label: "user", mode: "user", expandKey: liveExpandKey(session, live, "user") })}
    </article>
  ` : "";
  const existingAssistant = (session.path_entries || []).some((entry) => entry.type === "assistant_message" && entry.turn_id === live.turn_id);
  const assistant = live.assistant_delta && !existingAssistant ? `
    <article class="message assistant streaming">
      <div class="message-head"><span>assistant final</span><span>streaming</span>${copyButton(live.assistant_delta)}</div>
      ${renderExpandableBody(live.assistant_delta, { label: "assistant final", mode: "streaming-answer", caret: true })}
      ${renderReasoningBlock(live.reasoning_delta, "live", liveExpandKey(session, live, "reasoning"))}
    </article>
  ` : "";
  const visibleLiveEntries = (live.entries || []).filter((entry) => ["user_message", "assistant_message", "tool_call", "tool_result", "run_failure", "turn_marker"].includes(entry.type)).length;
  const activityLabel = visibleLiveEntries ? `${visibleLiveEntries} timeline update${visibleLiveEntries === 1 ? "" : "s"}` : `${live.events?.length || 0} event${live.events?.length === 1 ? "" : "s"}`;
  const liveContext = renderLiveContextActivity(session, live);
  const activity = live.events?.length ? `<div class="stream-status">Live turn · ${activityLabel}</div>` : `<div class="stream-status">Live turn started</div>`;
  return `${userEcho}${assistant}${liveContext}${activity}`;
}

function renderLiveContextActivity(session, live) {
  const statuses = (live.context_statuses || []).slice(-4);
  const compactions = (live.compactions || []).slice(-3);
  if (!statuses.length && !compactions.length) return "";
  return `
    <article class="message entry context-live-item">
      <div class="message-head"><span>context</span><span>live</span></div>
      <div class="context-event-list">
        ${statuses.map(renderContextStatusRow).join("")}
        ${compactions.map(renderCompactionEventRow).join("")}
      </div>
    </article>
  `;
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
        ${renderExpandableBody(body, { label: "tools changed", mode: "audit", expandKey: entryExpandKey(entry, "body") })}
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
        ${renderExpandableBody(body, { label: "run failure", mode: "error", expandKey: entryExpandKey(entry, "body") })}
      </article>
    `;
  }
  const msg = entry.message || {};
  if (entry.type === "assistant_message") return renderAssistantMessage(entry, msg);
  if (entry.type === "tool_call" || entry.type === "tool_result") return renderToolActivity(entry, msg);
  const role = msg.role || "entry";
  const title = role;
  const body = msg.content || "";
  const copyPayload = body || structuredEntryCopy(entry, title, msg);
  return `
    <article class="message ${escapeHTML(role)}">
      <div class="message-head"><span>${escapeHTML(title)}</span><span>${escapeHTML(entry.turn_id || "")}</span>${copyButton(copyPayload)}</div>
      ${renderExpandableBody(body, { label: title, mode: role === "user" ? "user" : "audit", expandKey: entryExpandKey(entry, "body") })}
    </article>
  `;
}

function renderToolRun(run) {
  const call = run.call;
  const result = run.result;
  const sourceEntry = call || result || {};
  const callMsg = call?.message || {};
  const resultMsg = result?.message || {};
  const toolName = callMsg.tool_name || resultMsg.tool_name || "tool";
  const callID = callMsg.tool_call_id || resultMsg.tool_call_id || "";
  const args = callMsg.tool_args || (callMsg.content && callMsg.content !== "tool_call" ? callMsg.content : "");
  const output = resultMsg.content || "";
  const status = result ? toolActivityStatus(result, resultMsg) : "called";
  const preview = toolRunPreview(args, output, status);
  const copyPayload = toolRunCopyPayload({ toolName, callID, args, output, status });
  const metadata = toolRunMetadata(call, result);
  const projection = renderToolProjection(resultMsg);
  return `
    <article class="message tool ${status === "error" ? "tool-error" : ""}">
      <details class="tool-activity"${detailStateAttributes(entryExpandKey(sourceEntry, "activity"), false)}>
        <summary>
          <span class="tool-summary-main">
            <span class="tool-kind">tool run</span>
            <span class="tool-name-pill">${escapeHTML(toolName)}</span>
            <span class="tiny-pill">${escapeHTML(sourceEntry.turn_id || "turn")}</span>
            <span class="tiny-pill ${status === "error" ? "danger-pill" : ""}">${escapeHTML(status)}</span>
            ${toolRunMetrics(call, result)}
          </span>
          <span class="tool-summary-preview">${escapeHTML(preview)}</span>
          ${copyButton(copyPayload)}
        </summary>
        <div class="tool-activity-body">
          ${renderReasoningBlock(callMsg.reasoning || resultMsg.reasoning, "tool", entryExpandKey(sourceEntry, "reasoning"))}
          ${callID ? `<div class="key-value"><span>Call ID</span><span>${escapeHTML(callID)}</span></div>` : ""}
          ${renderToolDetailSection("Arguments", args, emptyToolArguments)}
          ${renderToolDetailSection("Result", output, result ? emptyToolResult : pendingToolResult)}
          ${projection}
          ${metadata}
        </div>
      </details>
    </article>
  `;
}

function renderAssistantMessage(entry, msg) {
  const body = msg.content || "";
  const copyPayload = body || structuredEntryCopy(entry, "assistant final", msg);
  return `
    <article class="message assistant">
      <div class="message-head"><span>assistant final</span><span>${escapeHTML(entry.turn_id || "")}</span>${copyButton(copyPayload)}</div>
      ${renderExpandableBody(body, { label: "assistant final", mode: "final-answer", expandKey: entryExpandKey(entry, "answer") })}
      ${renderReasoningBlock(msg.reasoning, entry.turn_id || "", entryExpandKey(entry, "reasoning"))}
    </article>
  `;
}

function renderReasoningBlock(reasoning, scope, expandKey = "") {
  const text = String(reasoning || "");
  if (!text.trim()) return "";
  return `
    <details class="reasoning-fold"${detailStateAttributes(expandKey, false)}>
      <summary>
        <span>Reasoning</span>
        <span>${escapeHTML(scope || "hidden")} · ${text.length} chars · ${lineCount(text)} lines</span>
        ${copyButton(text, "Copy reasoning", "Reasoning copied", "copy-inline subtle-copy")}
      </summary>
      <pre class="code-block">${escapeHTML(text)}</pre>
    </details>
  `;
}

function renderToolActivity(entry, msg) {
  const isResult = entry.type === "tool_result";
  const title = `${isResult ? "tool result" : "tool call"} · ${msg.tool_name || "tool"}`;
  const body = isResult ? msg.content || "" : msg.tool_args || msg.content || "";
  const status = toolActivityStatus(entry, msg);
  const preview = toolPreview(body, msg, isResult);
  const copyPayload = body || structuredEntryCopy(entry, title, msg);
  const metadata = renderMetadataBlock(entry.metadata);
  const projection = renderToolProjection(msg);
  return `
    <article class="message tool ${status === "error" ? "tool-error" : ""}">
      <details class="tool-activity"${detailStateAttributes(entryExpandKey(entry, "activity"), false)}>
        <summary>
          <span class="tool-summary-main">
            <span class="tool-kind">${escapeHTML(isResult ? "tool result" : "tool call")}</span>
            <span class="tool-name-pill">${escapeHTML(msg.tool_name || "tool")}</span>
            <span class="tiny-pill">${escapeHTML(entry.turn_id || "turn")}</span>
            <span class="tiny-pill ${status === "error" ? "danger-pill" : ""}">${escapeHTML(status)}</span>
            ${toolActivityMetrics(entry)}
          </span>
          <span class="tool-summary-preview">${escapeHTML(preview)}</span>
          ${copyButton(copyPayload)}
        </summary>
        <div class="tool-activity-body">
          ${renderReasoningBlock(msg.reasoning, "tool", entryExpandKey(entry, "reasoning"))}
          ${msg.tool_call_id ? `<div class="key-value"><span>Call ID</span><span>${escapeHTML(msg.tool_call_id)}</span></div>` : ""}
          ${renderExpandableBody(body, { label: isResult ? "Tool result" : "Tool arguments", mode: "tool", forceOpen: true })}
          ${projection}
          ${metadata}
        </div>
      </details>
    </article>
  `;
}

function renderToolDetailSection(label, body, emptyText) {
  if (String(body || "").trim()) {
    return `
      <div class="tool-detail-section">
        <strong>${escapeHTML(label)}</strong>
        ${renderExpandableBody(body, { label, mode: "tool", forceOpen: true })}
      </div>
    `;
  }
  return `
    <div class="tool-detail-section empty">
      <strong>${escapeHTML(label)}</strong>
      <p class="muted">${escapeHTML(emptyText)}</p>
    </div>
  `;
}

function renderToolProjection(msg) {
  const view = msg?.tool_result;
  if (!view) return "";
  const ref = view.full_output;
  const rows = [
    ["Projection", view.truncated ? "truncated" : "full"],
    ["Strategy", view.strategy || ""],
    ["Visible", formatBytes(view.visible_bytes)],
    ["Original", formatBytes(view.original_bytes)],
    ["Hash", shortHash(view.content_sha256 || "")],
  ].filter(([, value]) => value || value === 0);
  return `
    <div class="tool-detail-section">
      <strong>Model-visible output</strong>
      <div class="tool-projection-grid">
        ${rows.map(([key, value]) => `<div class="key-value"><span>${escapeHTML(key)}</span><span>${escapeHTML(value)}</span></div>`).join("")}
      </div>
      ${ref ? `
        <div class="artifact-link-row">
          <span>${escapeHTML(ref.safe_label || ref.id || "tool output artifact")} · ${escapeHTML(formatBytes(ref.size_bytes))}</span>
          <a class="button small ghost" href="${escapeHTML(ref.url || "#")}" target="_blank" rel="noreferrer">Open artifact</a>
        </div>
      ` : ""}
    </div>
  `;
}

function renderMetadataBlock(metadata) {
  if (!metadata || !Object.keys(metadata).length) return "";
  return `
    <div class="tool-detail-section metadata">
      <strong>Metadata</strong>
      <pre class="json-block">${escapeHTML(JSON.stringify(metadata, null, 2))}</pre>
    </div>
  `;
}

function toolRunMetadata(...entries) {
  const merged = {};
  for (const entry of entries) {
    for (const [key, value] of Object.entries(entry?.metadata || {})) {
      merged[key] = value;
    }
  }
  return renderMetadataBlock(merged);
}

function toolRunMetrics(...entries) {
  const merged = {};
  for (const entry of entries) {
    Object.assign(merged, entry?.metadata || {});
  }
  return toolActivityMetrics({ metadata: merged });
}

function toolRunPreview(args, output, status) {
  const text = args || output;
  if (text) return toolPreview(text, { tool_args: args, content: output }, !args);
  return status === "called" ? "waiting for result" : "no result body captured";
}

function toolRunCopyPayload({ toolName, callID, args, output, status }) {
  const parts = [
    `tool: ${toolName || "-"}`,
    `status: ${status || "-"}`,
    callID ? `call_id: ${callID}` : "",
    args ? `arguments:\n${formatStructuredBody(args)}` : `arguments: ${emptyToolArguments}`,
    output ? `result:\n${formatStructuredBody(output)}` : `result: ${status === "called" ? pendingToolResult : emptyToolResult}`,
  ];
  return parts.filter(Boolean).join("\n\n");
}

function toolActivityMetrics(entry) {
  const meta = entry.metadata || {};
  const parts = [
    meta.duration_ms ? `${meta.duration_ms} ms` : "",
    meta.byte_length ? `${meta.byte_length} bytes` : "",
    meta.bytes ? `${meta.bytes} bytes` : "",
    meta.output_bytes ? `${meta.output_bytes} bytes` : "",
  ].filter(Boolean);
  return parts.map((part) => `<span class="tiny-pill">${escapeHTML(part)}</span>`).join("");
}

function formatBytes(value) {
	const n = Number(value || 0);
	if (!n) return "";
	if (n < 1024) return `${n} B`;
	if (n < 1024 * 1024) return `${(n / 1024).toFixed(n < 10 * 1024 ? 1 : 0)} KB`;
	return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

function shortHash(value) {
  const text = String(value || "");
  if (!text) return "";
  if (text.length <= 16) return text;
  return `${text.slice(0, 8)}...${text.slice(-6)}`;
}

function toolActivityStatus(entry, msg) {
  const text = `${entry.error || ""}\n${msg.content || ""}`.trim();
  if (entry.error || /^ERROR:/i.test(text)) return "error";
  if (entry.type === "tool_call") return "called";
  return "success";
}

function toolPreview(body, msg, isResult) {
  if (!isResult && msg.tool_args) {
    const parsed = parseJSONMaybe(msg.tool_args);
    if (parsed && typeof parsed === "object") {
      if (parsed.patch) return truncateOneLine(`patch: ${patchOperationPreview(parsed.patch)}`, 120);
      for (const key of ["query", "command", "url", "path", "pattern"]) {
        if (parsed[key]) return truncateOneLine(`${key}: ${JSON.stringify(parsed[key])}`, 120);
      }
    }
  }
  const text = String(body || msg.content || "").split("\n").find((line) => line.trim()) || (isResult ? "result captured" : "arguments captured");
  return truncateOneLine(text, 120);
}

function patchOperationPreview(patch) {
  const ops = String(patch || "").split("\n").map((line) => line.trim()).filter((line) => {
    return line.startsWith("*** Add File:") || line.startsWith("*** Update File:") || line.startsWith("*** Delete File:") || line.startsWith("*** Move to:");
  });
  return ops.length ? ops.join(" · ") : "structured patch";
}

function parseJSONMaybe(value) {
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function isLikelyJSON(value) {
  const text = String(value || "").trim();
  if (!text) return false;
  return (text.startsWith("{") && text.endsWith("}")) || (text.startsWith("[") && text.endsWith("]"));
}

function formatStructuredBody(value) {
  const text = String(value || "");
  const parsed = parseJSONMaybe(text);
  return parsed ? JSON.stringify(parsed, null, 2) : text;
}

function truncateOneLine(value, limit) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (text.length <= limit) return text;
  return `${text.slice(0, Math.max(0, limit - 3))}...`;
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

function renderExpandableBody(body, options = {}) {
  const {
    label = "message",
    mode = "audit",
    caret = false,
    forceOpen = false,
    expandKey = "",
  } = options;
  const text = String(body || "");
  const lines = lineCount(text);
  const long = text.length > 1200 || lines > 12;
  const escaped = escapeHTML(mode === "tool" && isLikelyJSON(text) ? formatStructuredBody(text) : text);
  if (mode === "final-answer" || mode === "streaming-answer") {
    if (mode === "streaming-answer") {
      return `<div class="message-text final-answer">${escaped}${caret ? `<span class="stream-caret" aria-hidden="true"></span>` : ""}</div>`;
    }
    if (lines > 80 || text.length > 12000) {
      const preview = firstLines(text, 60);
      return `
        <div class="message-text final-answer answer-preview">${escapeHTML(preview)}</div>
        <details class="answer-expand"${detailStateAttributes(expandKey, false)}>
          <summary>${escapeHTML(label)} · ${text.length} chars · ${lines} lines · Show full answer</summary>
          <div class="message-text final-answer">${escaped}</div>
        </details>
      `;
    }
    return `<div class="message-text final-answer">${escaped}</div>`;
  }
  if (!long && !forceOpen) {
    return `<div class="message-text">${escaped}</div>`;
  }
  if (forceOpen) {
    return `<pre class="${isLikelyJSON(text) ? "json-block" : "code-block"}">${escaped}</pre>`;
  }
  const preview = previewText(text);
  return `
    <details class="message-fold"${detailStateAttributes(expandKey, false)}>
      <summary>${escapeHTML(label)} · ${text.length} chars · ${lines} lines · ${escapeHTML(preview)}</summary>
      <div class="message-text">${escaped}</div>
    </details>
  `;
}

function detailStateAttributes(expandKey, defaultOpen = false) {
  if (!expandKey) return defaultOpen ? " open" : "";
  const hasSaved = Object.prototype.hasOwnProperty.call(state.timelineExpanded, expandKey);
  const open = hasSaved ? state.timelineExpanded[expandKey] : defaultOpen;
  return ` data-expand-key="${escapeHTML(expandKey)}"${open ? " open" : ""}`;
}

function entryExpandKey(entry, part) {
  const stable = entry.id ? `id:${entry.id}` : timelineEntryKey(entry);
  return `session:${entry.thread_id || state.activeSession?.id || "unknown"}:${stable}:${part}`;
}

function liveExpandKey(session, live, part) {
  const stable = live.turn_id || live.sequence || "pending";
  return `session:${session?.id || live.session_id || "unknown"}:live:${stable}:${part}`;
}

function renderMessageBody(body, label) {
  return renderExpandableBody(body, { label, mode: "audit" });
}

function lineCount(text) {
  return String(text || "").split("\n").length;
}

function previewText(value, limit = 180) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (text.length <= limit) return text;
  return `${text.slice(0, Math.max(0, limit - 3))}...`;
}

function firstLines(value, limit) {
  const lines = String(value || "").split("\n");
  if (lines.length <= limit) return String(value || "");
  return `${lines.slice(0, limit).join("\n")}\n...`;
}

function appendDisabledReason(session) {
  if (state.running) return "A request is running.";
  if (session?.status === "interrupted") return "This session has an interrupted turn. Inspect or recover it before appending another message.";
  if (!session?.can_append_message) return `This session is ${session?.status || "not ready"} and cannot accept another message.`;
  return "This message will use the current session tools.";
}
