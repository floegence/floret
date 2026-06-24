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
  root.querySelector("[data-subagent-spawn-form]")?.addEventListener("submit", (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const payload = {
      task_name: form.elements.task_name.value.trim(),
      host_profile_ref: form.elements.host_profile_ref.value.trim(),
      fork_mode: form.elements.fork_mode.value,
      message: form.elements.message.value.trim(),
    };
    handlers.onSubagentSpawnDraft(state.activeSession?.id || "", payload);
    if (payload.task_name && payload.message) handlers.onSubagentSpawn(payload);
  });
  root.querySelector("[data-subagent-spawn-form]")?.addEventListener("input", (event) => {
    if (event.isComposing) return;
    const form = event.currentTarget;
    handlers.onSubagentSpawnDraft(state.activeSession?.id || "", {
      task_name: form.elements.task_name.value,
      host_profile_ref: form.elements.host_profile_ref.value,
      fork_mode: form.elements.fork_mode.value,
      message: form.elements.message.value,
    });
  });
  root.querySelectorAll("[data-subagent-input-form]").forEach((form) => {
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      const target = form.dataset.subagentInputForm || "";
      const payload = {
        message: form.elements.message.value.trim(),
        interrupt: Boolean(form.elements.interrupt?.checked),
      };
      handlers.onSubagentInputDraft(state.activeSession?.id || "", target, payload);
      if (target && payload.message) handlers.onSubagentInput(target, payload);
    });
    form.addEventListener("input", (event) => {
      if (event.isComposing) return;
      const target = form.dataset.subagentInputForm || "";
      handlers.onSubagentInputDraft(state.activeSession?.id || "", target, {
        message: form.elements.message.value,
        interrupt: Boolean(form.elements.interrupt?.checked),
      });
    });
    form.addEventListener("change", () => {
      const target = form.dataset.subagentInputForm || "";
      handlers.onSubagentInputDraft(state.activeSession?.id || "", target, {
        message: form.elements.message.value,
        interrupt: Boolean(form.elements.interrupt?.checked),
      });
    });
  });
  root.querySelectorAll("[data-subagent-wait]").forEach((button) => {
    button.addEventListener("click", () => handlers.onSubagentWait(button.dataset.subagentWait || ""));
  });
  root.querySelectorAll("[data-subagent-detail]").forEach((button) => {
    button.addEventListener("click", () => handlers.onSubagentDetail(button.dataset.subagentDetail || ""));
  });
  root.querySelectorAll("[data-subagent-detail-more]").forEach((button) => {
    button.addEventListener("click", () => handlers.onSubagentDetailMore?.(button.dataset.subagentDetailMore || ""));
  });
  root.querySelectorAll("[data-subagent-close]").forEach((button) => {
    button.addEventListener("click", () => handlers.onSubagentClose(button.dataset.subagentClose || ""));
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
    return [session.title, session.id, session.status, session.profile?.model, (session.selected_tools || []).join(" ")].join(" ").toLowerCase().includes(filter);
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
  const title = sessionTitle(session);
  return `
    <article class="session-row ${session.id === activeID ? "active" : ""}">
      <button class="session-select" type="button" data-session-select-id="${escapeHTML(session.id)}">
        <strong>${escapeHTML(title)}</strong>
        <span class="row-meta">${escapeHTML(session.status || "idle")} · ${turns} turn${turns === 1 ? "" : "s"} · ${escapeHTML(session.profile?.model || "model")}</span>
        <span class="row-pills">
          <span class="tiny-pill" title="${escapeHTML(session.id)}">${escapeHTML(shortID(session.id))}</span>
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
  const title = sessionTitle(session);
  return `
    <section class="workspace">
      <header class="workspace-head">
        <div class="workspace-title">
          <h1>${escapeHTML(title)}</h1>
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
      ${renderSubagentPanel(session)}
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

function sessionTitle(session) {
  const title = (session?.title || "").trim();
  return title || shortID(session?.id || "");
}

function renderSubagentPanel(session) {
  const subagents = session.subagents || [];
  const spawnDraft = state.subagentSpawnDrafts[session.id] || {};
  const isSpawning = state.action === "subagent-spawn" && state.actionTarget === session.id;
  const busy = subagentActionBusy(session);
  return `
    <section class="subagent-panel" aria-label="Subagents">
      <div class="subagent-panel-head">
        <div>
          <h2>Subagents</h2>
          <p>${subagents.length ? `${subagents.length} child thread${subagents.length === 1 ? "" : "s"}` : "No child threads yet"}</p>
        </div>
        <span class="tiny-pill">${subagents.filter((item) => ["running", "waiting"].includes(String(item.status || ""))).length} active</span>
      </div>
      <form class="subagent-spawn-form" data-subagent-spawn-form>
        <input name="task_name" placeholder="task name" value="${escapeHTML(spawnDraft.task_name || "")}" ${busy ? "disabled" : ""} />
        <select name="host_profile_ref" ${busy ? "disabled" : ""}>
          ${["host-ref:explore", "host-ref:work", "host-ref:review"].map((ref) => `<option value="${ref}" ${String(spawnDraft.host_profile_ref || "host-ref:work") === ref ? "selected" : ""}>${ref}</option>`).join("")}
        </select>
        <select name="fork_mode" ${busy ? "disabled" : ""}>
          ${["full_path", "none"].map((mode) => `<option value="${mode}" ${String(spawnDraft.fork_mode || "full_path") === mode ? "selected" : ""}>${mode}</option>`).join("")}
        </select>
        <textarea name="message" placeholder="Give the child thread a concrete task" ${busy ? "disabled" : ""}>${escapeHTML(spawnDraft.message || "")}</textarea>
        <button class="primary ${isSpawning ? "is-pending" : ""}" type="submit" ${busy ? "disabled" : ""}>${isSpawning ? "Starting..." : "Start"}</button>
      </form>
      <div class="subagent-list">
        ${subagents.length ? subagents.map((item) => renderSubagentCard(session, item)).join("") : `<div class="subagent-empty">Start a child thread to inspect lifecycle, prompt scope, and handoff behavior.</div>`}
      </div>
    </section>
  `;
}

function renderSubagentCard(session, item) {
  const target = item.thread_id || "";
  const draft = state.subagentInputDrafts[`${session.id}\u0000${target}`] || {};
  const status = String(item.status || "idle");
  const running = ["running", "waiting"].includes(status);
  const canSend = Boolean(item.can_send_input);
  const isWaiting = state.action === "subagent-wait" && state.actionTarget === target;
  const isDetail = state.action === "subagent-detail" && state.actionTarget === target;
  const isInput = state.action === "subagent-input" && state.actionTarget === target;
  const isClosing = state.action === "subagent-close" && state.actionTarget === target;
  const busy = subagentActionBusy(session);
  const detail = state.subagentDetails[`${session.id}\u0000${target}`] || null;
  const payload = [
    `subagent: ${item.path || item.task_name || item.thread_id || ""}`,
    `thread_id: ${item.thread_id || ""}`,
    `status: ${status}`,
    item.host_profile_ref ? `host_profile_ref: ${item.host_profile_ref}` : "",
    item.latest_turn_id ? `latest_turn_id: ${item.latest_turn_id}` : "",
    item.waiting_prompt ? `waiting_prompt:\n${item.waiting_prompt}` : "",
    item.last_message ? `last_message:\n${item.last_message}` : "",
  ].filter(Boolean).join("\n");
  return `
    <article class="subagent-card ${running ? "active" : ""}">
      <div class="subagent-card-main">
        <div>
          <strong>${escapeHTML(item.task_name || item.path || "subagent")}</strong>
          <span>${escapeHTML(item.path || item.thread_id || "")}</span>
        </div>
        <div class="subagent-pills">
          <span class="status-pill ${escapeHTML(status)}">${escapeHTML(status)}</span>
          ${item.host_profile_ref ? `<span class="tiny-pill">host ${escapeHTML(item.host_profile_ref)}</span>` : ""}
          ${item.queued_inputs ? `<span class="tiny-pill">${escapeHTML(item.queued_inputs)} queued</span>` : ""}
          ${item.latest_turn_id ? `<span class="tiny-pill">${escapeHTML(shortID(item.latest_turn_id))}</span>` : ""}
          ${copyButton(payload, "Copy", "Subagent copied", "copy-inline subtle-copy")}
        </div>
      </div>
      ${item.waiting_prompt ? `<div class="subagent-last waiting">${escapeHTML(item.waiting_prompt)}</div>` : ""}
      ${item.last_message ? `<div class="subagent-last">${escapeHTML(item.last_message)}</div>` : ""}
      <form class="subagent-input-form" data-subagent-input-form="${escapeHTML(target)}">
        <input name="message" placeholder="${canSend ? "send input to child thread" : "child thread is closed"}" value="${escapeHTML(draft.message || "")}" ${target && canSend && !busy ? "" : "disabled"} />
        <label class="inline-check"><input type="checkbox" name="interrupt" ${draft.interrupt ? "checked" : ""} ${target && canSend && !busy ? "" : "disabled"} /> interrupt</label>
        <button class="${isInput ? "is-pending" : ""}" type="submit" ${target && canSend && !busy ? "" : "disabled"}>${isInput ? "Sending..." : "Send"}</button>
      </form>
      <div class="subagent-actions">
        <button class="small ${isDetail ? "is-pending" : ""}" type="button" data-subagent-detail="${escapeHTML(target)}" ${target ? "" : "disabled"}>${isDetail ? "Loading..." : "Detail"}</button>
        <button class="small ${isWaiting ? "is-pending" : ""}" type="button" data-subagent-wait="${escapeHTML(target)}" ${target && !busy ? "" : "disabled"}>${isWaiting ? "Waiting..." : "Wait"}</button>
        <button class="small danger ${isClosing ? "is-pending" : ""}" type="button" data-subagent-close="${escapeHTML(target)}" ${target && item.can_close && !busy ? "" : "disabled"}>${isClosing ? "Closing..." : "Close"}</button>
      </div>
      ${detail ? renderSubagentDetail(session, target, detail) : ""}
    </article>
  `;
}

function subagentActionBusy(session) {
  return Boolean(state.running || session?.status === "running" || session?.phase === "turn");
}

function renderSubagentDetail(session, target, detail) {
  const snapshot = detail.snapshot || {};
  const events = Array.isArray(detail.events) ? detail.events : [];
  const generated = detail.generated_at ? formatLocalTime(detail.generated_at) : "";
  const hasMore = Boolean(detail.has_more);
  const isLoadingMore = state.action === "subagent-detail" && state.actionTarget === target;
  return `
    <section class="subagent-detail" aria-label="Subagent detail">
      <div class="subagent-detail-head">
        <div>
          <strong>${escapeHTML(snapshot.task_name || snapshot.path || target || "subagent")}</strong>
          <span>${escapeHTML(snapshot.thread_id || target)} · ${events.length} event${events.length === 1 ? "" : "s"}</span>
        </div>
        <span class="tiny-pill" title="${escapeHTML(generated)}">ordinal ${escapeHTML(detail.next_ordinal || 0)}</span>
      </div>
      <div class="subagent-detail-timeline">
        ${events.length ? events.map((event) => renderSubagentDetailEvent(session, event)).join("") : `<div class="subagent-detail-empty">No detail events are available for this child thread yet.</div>`}
      </div>
      ${hasMore ? `<div class="subagent-detail-more"><button class="small" type="button" data-subagent-detail-more="${escapeHTML(target)}" ${isLoadingMore ? "disabled" : ""}>${isLoadingMore ? "Loading..." : "Load more"}</button></div>` : ""}
    </section>
  `;
}

function renderSubagentDetailEvent(session, event) {
  const kind = String(event?.kind || event?.type || "event");
  const title = subagentDetailTitle(event);
  const meta = [
    event?.ordinal ? `#${event.ordinal}` : "",
    event?.turn_id ? shortID(event.turn_id) : "",
    event?.created_at ? formatLocalTime(event.created_at) : "",
  ].filter(Boolean).join(" · ");
  const body = subagentDetailBody(event);
  const copyPayload = subagentDetailCopyPayload(event, title, body);
  const expandKey = `session:${session?.id || "unknown"}:subagent:${event?.thread_id || "unknown"}:${event?.ordinal || event?.id || title}:body`;
  return `
    <article class="subagent-detail-event kind-${escapeHTML(kind)}">
      <div class="subagent-detail-event-head">
        <span>${escapeHTML(title)}</span>
        <span>${escapeHTML(meta)}</span>
        ${copyButton(copyPayload, "Copy", "Subagent event copied", "copy-inline subtle-copy")}
      </div>
      ${body ? renderExpandableBody(body, { label: title, mode: kind === "error" ? "error" : "audit", expandKey }) : renderSubagentDetailFacts(event)}
    </article>
  `;
}

function subagentDetailTitle(event) {
  switch (event?.kind) {
    case "input":
      return "delegated input";
    case "user_message":
      return "user";
    case "assistant_message":
      return "assistant";
    case "tool_call":
      return `tool call ${event.tool_call?.name || ""}`.trim();
    case "tool_result":
      return `tool result ${event.tool_result?.tool_name || ""}`.trim();
    case "approval":
      return `approval ${event.approval?.state || ""}`.trim();
    case "turn_marker":
      return `turn ${event.turn_marker?.status || ""}`.trim();
    case "compaction":
      return "context compaction";
    case "error":
      return "run failure";
    default:
      return event?.type || event?.kind || "event";
  }
}

function subagentDetailBody(event) {
  switch (event?.kind) {
    case "input":
    case "user_message":
    case "assistant_message":
      return [event.message?.content || event.message?.preview || "", event.message?.reasoning ? `\nReasoning:\n${event.message.reasoning}` : ""].join("").trim();
    case "tool_call":
      return [
        event.tool_call?.name ? `tool: ${event.tool_call.name}` : "",
        event.tool_call?.id ? `call_id: ${event.tool_call.id}` : "",
        event.tool_call?.args_hash ? `args_hash: ${event.tool_call.args_hash}` : "",
        event.tool_call?.args_preview && !event.tool_call?.args_json ? `args_preview:\n${event.tool_call.args_preview}` : "",
        event.tool_call?.args_json ? `args:\n${event.tool_call.args_json}` : "",
      ].filter(Boolean).join("\n");
    case "tool_result": {
      const result = event.tool_result || {};
      return [
        result.tool_name ? `tool: ${result.tool_name}` : "",
        result.call_id ? `call_id: ${result.call_id}` : "",
        result.truncated ? `truncated: ${result.visible_bytes || 0}/${result.original_bytes || 0} bytes` : "",
        result.content_sha256 ? `sha256: ${result.content_sha256}` : "",
        result.content ? `content:\n${result.content}` : "",
        !result.content && result.preview ? `preview:\n${result.preview}` : "",
      ].filter(Boolean).join("\n");
    }
    case "approval": {
      const approval = event.approval || {};
      return [
        approval.state ? `state: ${approval.state}` : "",
        approval.tool_name ? `tool: ${approval.tool_name}` : "",
        approval.tool_id ? `tool_id: ${approval.tool_id}` : "",
        approval.args_hash ? `args_hash: ${approval.args_hash}` : "",
        approval.reason ? `reason: ${approval.reason}` : "",
      ].filter(Boolean).join("\n");
    }
    case "turn_marker":
      return event.turn_marker?.status || "";
    case "compaction": {
      const compaction = event.compaction || {};
      return [
        compaction.phase ? `phase: ${compaction.phase}` : "",
        compaction.trigger ? `trigger: ${compaction.trigger}` : "",
        compaction.reason ? `reason: ${compaction.reason}` : "",
        compaction.summary ? `summary:\n${compaction.summary}` : "",
      ].filter(Boolean).join("\n");
    }
    case "error":
      return event.error || "";
    default:
      return event.message?.content || event.message?.preview || event.error || "";
  }
}

function renderSubagentDetailFacts(event) {
  const facts = [
    event?.type ? ["type", event.type] : null,
    event?.thread_id ? ["thread", shortID(event.thread_id)] : null,
    event?.parent_id ? ["parent", shortID(event.parent_id)] : null,
  ].filter(Boolean);
  if (!facts.length) return "";
  return `<div class="subagent-detail-facts">${facts.map(([key, value]) => `<span><strong>${escapeHTML(key)}</strong>${escapeHTML(value)}</span>`).join("")}</div>`;
}

function subagentDetailCopyPayload(event, title, body) {
  return [
    title,
    `kind: ${event?.kind || "-"}`,
    `type: ${event?.type || "-"}`,
    `ordinal: ${event?.ordinal || 0}`,
    event?.turn_id ? `turn_id: ${event.turn_id}` : "",
    body,
  ].filter(Boolean).join("\n");
}

function renderTimeline(session, result) {
  const entries = mergeTimelineEntries(session);
  const compactions = compactionEventsFor(session, result);
  const hasActivity = Boolean(activityTimelineForSession(session, result));
  if (!entries.length && !compactions.length && !hasActivity && !activeLiveTurn(session)) return `<div class="message entry"><div class="message-text muted">No durable messages yet.</div></div>`;
  const visibleEntries = entries.filter((entry) => {
    return ["user_message", "assistant_message", "active_tools_change", "run_failure", "turn_marker"].includes(entry.type);
  });
  return `${timelineRows(visibleEntries, compactions).map(renderTimelineItem).join("")}${renderActivityPanel(session, result)}${renderLiveTurn(session)}`;
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

function renderTimelineItem(item) {
  if (item.type === "compaction") return renderCompactionTimelineItem(item.compaction);
  return renderEntry(item.entry || item);
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
  const visibleLiveEntries = (live.entries || []).filter((entry) => ["user_message", "assistant_message", "run_failure", "turn_marker"].includes(entry.type)).length;
  const activityLabel = visibleLiveEntries ? `${visibleLiveEntries} timeline update${visibleLiveEntries === 1 ? "" : "s"}` : `${live.events?.length || 0} event${live.events?.length === 1 ? "" : "s"}`;
  const liveContext = renderLiveContextActivity(session, live);
  const liveActivity = renderActivityPanel(session, state.liveTurn?.result, { live: true });
  const activity = live.events?.length ? `<div class="stream-status">Live turn · ${activityLabel}</div>` : `<div class="stream-status">Live turn started</div>`;
  return `${userEcho}${assistant}${liveContext}${liveActivity}${activity}`;
}

function renderActivityPanel(session, result, options = {}) {
  const timeline = activityTimelineForSession(session, result, options.live);
  if (!timeline || Number(timeline.schema_version || 0) !== 1 || !timeline.summary) return "";
  const total = Number(timeline.summary.total_items || 0);
  if (!total && !timeline.summary.needs_attention) return "";
  const key = activityExpandKey(session, timeline, options.live);
  const defaultOpen = Boolean(timeline.summary.needs_attention);
  const status = String(timeline.summary.status || "pending");
  const severity = String(timeline.summary.severity || "quiet");
  const items = Array.isArray(timeline.items) ? timeline.items : [];
  const reasons = (timeline.summary.attention_reasons || []).join(", ");
  const copyPayload = activityCopyPayload(timeline);
  return `
    <article class="message entry activity-digest ${timeline.summary.needs_attention ? "needs-attention" : ""}">
      <details class="activity-panel"${detailStateAttributes(key, defaultOpen)}>
        <summary>
          <span class="activity-summary-main">
            <span class="tool-kind">activity</span>
            <span class="activity-status ${escapeHTML(status)}">${escapeHTML(status)}</span>
            <span class="tiny-pill">${total} item${total === 1 ? "" : "s"}</span>
            ${timeline.summary.duration_ms ? `<span class="tiny-pill">${escapeHTML(formatDuration(timeline.summary.duration_ms))}</span>` : ""}
            <span class="tiny-pill severity-${escapeHTML(severity)}">${escapeHTML(severity)}</span>
          </span>
          <span class="activity-summary-preview">${escapeHTML(reasons || activityCountsLabel(timeline.summary.counts || {}))}</span>
          ${copyButton(copyPayload, "Copy", "Activity copied")}
        </summary>
        <div class="activity-body">
          <div class="activity-count-grid">
            ${activityCountCells(timeline.summary.counts || {})}
          </div>
          <div class="activity-items">
            ${items.map(renderActivityItem).join("")}
          </div>
        </div>
      </details>
    </article>
  `;
}

function activityTimelineForSession(session, result, liveOnly = false) {
  const live = activeLiveTurn(session);
  if (liveOnly) return live?.activity_timeline || live?.result?.activity_timeline || live?.result?.observation?.activity_timeline || null;
  if (result?.session_id === session?.id && result?.activity_timeline) return result.activity_timeline;
  if (result?.session_id === session?.id && result?.observation?.activity_timeline) return result.observation.activity_timeline;
  return session?.activity_timeline || session?.observation?.activity_timeline || null;
}

function renderActivityItem(item) {
  const status = String(item?.status || "pending");
  const severity = String(item?.severity || "quiet");
  const name = item?.tool_name || activityKindLabel(item?.kind || "");
  const meta = activityItemMeta(item);
  return `
    <div class="activity-item ${item?.needs_attention ? "needs-attention" : ""}">
      <span class="activity-dot severity-${escapeHTML(severity)}" aria-hidden="true"></span>
      <span class="activity-item-main">
        <strong>${escapeHTML(name || "activity")}</strong>
        <span>${escapeHTML(activityKindLabel(item?.kind || ""))}</span>
      </span>
      <span class="tiny-pill ${status === "error" ? "danger-pill" : ""}">${escapeHTML(status)}</span>
      ${item?.requires_approval ? `<span class="tiny-pill severity-blocking">${escapeHTML(item.approval_state || "approval")}</span>` : ""}
      ${meta ? `<span class="activity-item-meta">${escapeHTML(meta)}</span>` : ""}
    </div>
  `;
}

function activityItemMeta(item) {
  const meta = item?.metadata || {};
  const parts = [
    meta.duration_ms ? formatDuration(meta.duration_ms) : "",
    meta.result_count ? `${meta.result_count} results` : "",
    meta.visible_bytes ? `${formatBytes(meta.visible_bytes)} visible` : "",
    meta.batch_size && meta.batch_size !== "1" ? `batch ${Number(meta.batch_index || 0) + 1}/${meta.batch_size}` : "",
  ];
  return parts.filter(Boolean).join(" · ");
}

function activityCountCells(counts) {
  const cells = [
    ["running", counts.running],
    ["waiting", counts.waiting],
    ["success", counts.success],
    ["error", counts.error],
    ["approval", counts.approval],
  ].filter(([, value]) => Number(value || 0) > 0);
  return cells.length ? cells.map(([label, value]) => `<span><strong>${escapeHTML(value)}</strong>${escapeHTML(label)}</span>`).join("") : `<span><strong>0</strong>items</span>`;
}

function activityCountsLabel(counts) {
  return [
    counts.running ? `${counts.running} running` : "",
    counts.waiting ? `${counts.waiting} waiting` : "",
    counts.success ? `${counts.success} completed` : "",
    counts.error ? `${counts.error} failed` : "",
    counts.approval ? `${counts.approval} approval` : "",
  ].filter(Boolean).join(" · ") || "no activity";
}

function activityCopyPayload(timeline) {
  const summary = timeline.summary || {};
  const lines = [
    `activity: ${summary.status || "pending"}`,
    `items: ${summary.total_items || 0}`,
    `attention: ${summary.needs_attention ? "yes" : "no"}`,
    summary.attention_reasons?.length ? `reasons: ${summary.attention_reasons.join(", ")}` : "",
    ...(timeline.items || []).map((item) => `${item.status || "pending"} · ${item.tool_name || activityKindLabel(item.kind || "")} · ${item.kind || "activity"}`),
  ];
  return lines.filter(Boolean).join("\n");
}

function activityKindLabel(kind) {
  switch (kind) {
    case "hosted_tool":
      return "hosted tool";
    case "approval":
      return "approval";
    case "control":
      return "control";
    case "budget":
      return "budget";
    default:
      return "tool";
  }
}

function activityExpandKey(session, timeline, live) {
  const runID = timeline?.run_id || timeline?.turn_id || (live ? state.liveTurn?.turn_id : "") || "latest";
  const status = timeline?.summary?.status || "pending";
  return `session:${session?.id || state.activeSession?.id || "unknown"}:activity:${runID}:${status}:${live ? "live" : "final"}`;
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

function formatBytes(value) {
	const n = Number(value || 0);
	if (!n) return "";
	if (n < 1024) return `${n} B`;
	if (n < 1024 * 1024) return `${(n / 1024).toFixed(n < 10 * 1024 ? 1 : 0)} KB`;
	return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

function formatDuration(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n <= 0) return "";
  if (n < 1000) return `${Math.round(n)} ms`;
  if (n < 60000) return `${(n / 1000).toFixed(n < 10000 ? 1 : 0)} s`;
  return `${Math.floor(n / 60000)}m ${Math.round((n % 60000) / 1000)}s`;
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
  const escaped = escapeHTML(text);
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
    return `<pre class="code-block">${escaped}</pre>`;
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
