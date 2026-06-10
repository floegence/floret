import { escapeHTML, state } from "./state.js";

export function contextStatusesFor(session, result) {
  const live = stateLiveFor(session);
  const out = [
    ...(session?.context_statuses || []),
    ...(session?.observation?.context_statuses || []),
    ...(result?.session_id === session?.id ? result?.observation?.context_statuses || [] : []),
    ...(live?.context_statuses || []),
  ];
  return dedupeContextStatuses(out).sort(compareContextStatus);
}

export function latestContextStatus(session, result) {
  const statuses = contextStatusesFor(session, result);
  return statuses.length ? statuses[statuses.length - 1] : null;
}

export function contextStatusForRequest(statuses, request) {
  const requestID = request?.id || request?.request_id || `${request?.run_id || ""}:req:${request?.step || ""}`;
  const logicalID = request?.logical_request_id || "";
  const candidates = (statuses || []).filter((status) => {
    if (!status || status.phase !== "projected_request") return false;
    if (requestID && status.request_id === requestID) return true;
    if (logicalID && status.logical_request_id === logicalID && Number(status.attempt || 0) === Number(request?.attempt || 0)) return true;
    return status.run_id === request?.run_id && Number(status.step || 0) === Number(request?.step || 0);
  });
  return candidates.length ? candidates[candidates.length - 1] : null;
}

export function renderContextMeter(status, options = {}) {
  const label = contextTokenLabel(status);
  const percent = formatPercent(status?.used_ratio);
  const threshold = Number(status?.threshold_ratio || 0);
  const used = clamp01(Number(status?.used_ratio || 0));
  const title = contextStatusTitle(status);
  const tone = contextStatusTone(status);
  const thresholdStyle = threshold > 0 ? ` style="left:${escapeHTML(String(Math.min(100, Math.max(0, threshold * 100))))}%"` : "";
  const valueNow = Math.round(used * 100);
  const interactive = options.interactive !== false;
  const tag = interactive ? "button" : "div";
  const attrs = interactive ? ` type="button" data-context-meter` : "";
  return `
    <${tag} class="context-meter context-${escapeHTML(tone)}" title="${escapeHTML(title)}"${attrs}>
      <span class="context-meter-main">
        <strong>${escapeHTML(label)}</strong>
        <span>${escapeHTML(contextStatusMeta(status))}</span>
      </span>
      <span class="context-meter-bar" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${escapeHTML(String(valueNow))}" aria-valuetext="${escapeHTML(`${percent} context used`)}">
        <span class="context-meter-fill" style="width:${escapeHTML(String(Math.min(100, Math.max(0, used * 100))))}%"></span>
        ${threshold > 0 ? `<span class="context-meter-threshold"${thresholdStyle}></span>` : ""}
      </span>
      <span class="context-status-pill">${escapeHTML(`${percent} · ${statusLabel(status)} · ${phaseLabel(status?.phase)}`)}</span>
    </${tag}>
  `;
}

export function renderContextStatusRow(status) {
  const label = contextTokenLabel(status);
  const meta = [
    status?.step ? `step ${status.step}` : "",
    status?.attempt ? `attempt ${status.attempt}` : "",
    phaseLabel(status?.phase),
    statusLabel(status),
  ].filter(Boolean).join(" · ");
  return `
    <div class="context-event-row context-${escapeHTML(contextStatusTone(status))}" title="${escapeHTML(contextStatusTitle(status))}">
      <strong>${escapeHTML(meta || "context")}</strong>
      <span>${escapeHTML(label)}</span>
      <span>${escapeHTML(contextStatusMeta(status))}</span>
    </div>
  `;
}

export function renderCompactionEventRow(compaction) {
  if (!compaction) return "";
  const label = compaction.status === "failed" || compaction.phase === "failed"
    ? "compaction failed"
    : compaction.phase === "complete" ? "compaction complete" : "compaction started";
  const tokenText = compactionTokenLabel(compaction);
  const reason = [compaction.trigger, compaction.reason].filter(Boolean).join(" · ");
  const preview = compaction.summary_preview || compaction.error || "";
  return `
    <div class="context-event-row context-compact" title="${escapeHTML(compactionTitle(compaction))}">
      <strong>${escapeHTML(label)}</strong>
      <span>${escapeHTML(tokenText)}</span>
      <span>${escapeHTML([reason, preview].filter(Boolean).join(" · ") || compaction.status || "compaction")}</span>
    </div>
  `;
}

export function compactionEventsFor(session, result) {
  const live = stateLiveFor(session);
  const out = [
    ...(session?.compaction_events || []),
    ...(session?.observation?.compaction_events || []),
    ...(result?.session_id === session?.id ? result?.observation?.compaction_events || [] : []),
    ...(live?.compactions || []),
  ];
  return dedupeCompactions(out);
}

export function compactionEventKey(compaction) {
  const id = compaction?.compaction_id || "";
  if (id) return ["id", id, compaction?.phase || ""].join(":");
  return [
    "event",
    compaction?.phase || "",
    compaction?.step || "",
    compaction?.observed_at || "",
  ].join(":");
}

export function formatContextCount(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "0";
  return num.toLocaleString();
}

export function formatPercent(value) {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) return "0%";
  const pct = num * 100;
  return `${pct >= 10 ? pct.toFixed(0) : pct.toFixed(1)}%`;
}

export function contextCurrentTokens(status) {
  const pressure = status?.context_pressure || {};
  const usage = status?.usage || {};
  return Number(pressure.window_input_tokens || pressure.WindowInputTokens || usage.window_input_tokens || usage.WindowInputTokens || pressure.projected_input_tokens || pressure.ProjectedInputTokens || 0);
}

export function contextWindowTokens(status) {
  const pressure = status?.context_pressure || {};
  return Number(pressure.context_window_tokens || pressure.ContextWindowTokens || 0);
}

export function contextTokenLabel(status) {
  if (!status) return "Context n/a";
  const current = contextCurrentTokens(status);
  const window = contextWindowTokens(status);
  if (!window) return `Context ${formatContextCount(current)} / n/a`;
  return `Context ${formatContextCount(current)} / ${formatContextCount(window)}`;
}

export function statusLabel(status) {
  const value = String(status?.status || "stable").replace(/_/g, " ");
  return value || "stable";
}

export function phaseLabel(phase) {
  switch (phase) {
    case "projected_request":
      return "projected";
    case "provider_usage":
      return "observed";
    default:
      return phase ? String(phase).replace(/_/g, " ") : "context";
  }
}

export function contextStatusTone(status) {
  switch (status?.status) {
    case "hard_limit":
      return "hard";
    case "will_compact":
      return "compact";
    case "near_threshold":
      return "near";
    case "estimated":
      return "estimated";
    default:
      return "stable";
  }
}

export function contextStatusMeta(status) {
  if (!status) return "No provider request captured yet";
  const pressure = status.context_pressure || {};
  const estimate = status.request_estimate || {};
  const source = pressure.pressure_source || pressure.Source || estimate.source || estimate.Source || status.usage?.source || "";
  const method = pressure.estimate_method || pressure.EstimateMethod || estimate.method || estimate.Method || "";
  const confidence = pressure.confidence || pressure.Confidence || estimate.confidence || estimate.Confidence || "";
  return [source, method, confidence].filter(Boolean).join(" · ") || "context status";
}

export function compactionTokenLabel(compaction) {
  const before = Number(compaction?.tokens_before || 0);
  const after = Number(compaction?.tokens_after_estimate || 0);
  if (before && after) return `${formatContextCount(before)} -> ${formatContextCount(after)} tokens`;
  if (before) return `${formatContextCount(before)} tokens before`;
  if (after) return `${formatContextCount(after)} tokens after`;
  return "token delta n/a";
}

export function compactionTitle(compaction) {
  if (!compaction) return "No compaction event captured.";
  const parts = [
    compaction.phase || "",
    compaction.status || "",
    compaction.trigger || "",
    compaction.reason || "",
    compactionTokenLabel(compaction),
    compaction.compaction_id ? `id ${compaction.compaction_id}` : "",
    compaction.compaction_window_id ? `window ${compaction.compaction_window_id}` : "",
    compaction.error || "",
  ];
  return parts.filter(Boolean).join(" · ");
}

export function contextStatusTitle(status) {
  if (!status) return "No context status has been captured yet.";
  const pressure = status.context_pressure || {};
  const usage = status.usage || {};
  return [
    contextTokenLabel(status),
    `${formatPercent(status.used_ratio)} used`,
    pressure.threshold_tokens || pressure.ThresholdTokens ? `threshold ${formatContextCount(pressure.threshold_tokens || pressure.ThresholdTokens)}` : "",
    pressure.request_safe_limit_tokens || pressure.RequestSafeLimit ? `safe limit ${formatContextCount(pressure.request_safe_limit_tokens || pressure.RequestSafeLimit)}` : "",
    usage.available === false ? "usage unavailable" : "",
    contextStatusMeta(status),
  ].filter(Boolean).join(" · ");
}

function dedupeContextStatuses(statuses) {
  const seen = new Set();
  const out = [];
  for (const status of statuses || []) {
    if (!status) continue;
    const key = [
      status.phase || "",
      status.request_id || status.logical_request_id || "",
      status.step || "",
      status.attempt || "",
      status.observed_at || "",
    ].join(":");
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(status);
  }
  return out;
}

function dedupeCompactions(compactions) {
  const seen = new Set();
  const out = [];
  for (const item of compactions || []) {
    if (!item) continue;
    const key = compactionEventKey(item);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out.sort((a, b) => Date.parse(a.observed_at || "") - Date.parse(b.observed_at || ""));
}

function compareContextStatus(a, b) {
  const at = Date.parse(a?.observed_at || "");
  const bt = Date.parse(b?.observed_at || "");
  if (Number.isFinite(at) && Number.isFinite(bt) && at !== bt) return at - bt;
  if ((a?.step || 0) !== (b?.step || 0)) return (a?.step || 0) - (b?.step || 0);
  return phaseOrder(a?.phase) - phaseOrder(b?.phase);
}

function phaseOrder(phase) {
  if (phase === "projected_request") return 1;
  if (phase === "provider_usage") return 2;
  return 9;
}

function stateLiveFor(session) {
  return state.liveTurn?.session_id === session?.id ? state.liveTurn : null;
}

function clamp01(value) {
  if (!Number.isFinite(value) || value < 0) return 0;
  if (value > 1) return 1;
  return value;
}
