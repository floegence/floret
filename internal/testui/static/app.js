import { api } from "./api.js";
import { compactionEventKey } from "./contextStatus.js";
import { clone, contextPolicyForProfile, defaultProfile, defaultWebSearchForProvider, normalizePath, providerByID, providerDefaultBaseURL, providerDefaultModel, routePath, state, toolsForProfile } from "./state.js";
import { bindNewSession, renderNewSession } from "./views/newSession.js";
import { bindSessionWorkspace, renderSessionWorkspace } from "./views/sessionWorkspace.js";
import { bindSkills, landingDemoDraft, readSkillInstallDraft, renderSkills } from "./views/skills.js";
import { bindSettings, readSettingsDraft, renderSettings } from "./views/settings.js";

const appView = document.getElementById("appView");
const envStatus = document.getElementById("envStatus");
const globalStatus = document.getElementById("globalStatus");
const toastRegion = document.getElementById("toastRegion");
let autoRefreshTimer = 0;

// IMPORTANT: IME composition owns the editable DOM node until compositionend;
// background refresh or streaming renders must not replace it mid-candidate.
document.addEventListener("compositionstart", (event) => {
  const target = event.target;
  if (!(target instanceof Element)) return;
  if (!appView.contains(target) || !isEditableControl(target)) return;
  state.imeComposition = {
    active: true,
    selector: focusSelectorFor(target),
    pendingRender: false,
    pendingRefresh: false,
  };
  scheduleAutoRefresh();
});

document.addEventListener("compositionend", (event) => {
  const target = event.target;
  const wasActive = state.imeComposition.active;
  const pendingRender = state.imeComposition.pendingRender;
  const pendingRefresh = state.imeComposition.pendingRefresh;
  state.imeComposition = { active: false, selector: "", pendingRender: false, pendingRefresh: false };
  if (target instanceof Element && appView.contains(target) && isEditableControl(target)) {
    persistEditableDraft(target);
  }
  if (!wasActive) return;
  if (pendingRender) {
    render({ preserveFocus: true });
    return;
  }
  if (pendingRefresh) {
    scheduleAutoRefresh();
  }
});

document.addEventListener("click", (event) => {
  const toastClose = event.target.closest("[data-toast-close]");
  if (toastClose) {
    dismissToast(toastClose.dataset.toastClose || "");
    return;
  }
  const link = event.target.closest("[data-link]");
  if (!link) return;
  const url = new URL(link.href, window.location.href);
  if (url.origin !== window.location.origin) return;
  event.preventDefault();
  captureActiveDrafts();
  navigate(normalizePath(url.pathname));
});

window.addEventListener("popstate", () => {
  captureActiveDrafts();
  state.route = normalizePath();
  render();
});

document.addEventListener("visibilitychange", scheduleAutoRefresh);
document.addEventListener("selectionchange", flushDeferredRenderAfterSelection);

boot();

async function boot() {
  setStatus("loading");
  try {
    state.route = normalizePath();
    state.config = await api.config();
    if (!state.config.profiles?.length) {
      state.config.profiles = [defaultProfile()];
      state.config.active_profile_id = state.config.profiles[0].id;
    }
    await refreshSessions({ selectRoute: true });
    setStatus("idle");
    render();
  } catch (error) {
    setStatus("error");
    appView.innerHTML = `<div class="empty-state"><h1>Could not load test UI</h1><p>${escapeText(error.message)}</p></div>`;
  }
}

async function refreshSessions({ selectRoute = false } = {}) {
  state.sessions = await api.sessions();
  if (selectRoute && state.route.id) {
    try {
      setActiveSessionSnapshot(await fetchSessionSnapshot(state.route.id), { force: true });
    } catch (error) {
      if (error.status !== 404) throw error;
      if (state.sessions.length) {
        setActiveSessionSnapshot(await fetchSessionSnapshot(state.sessions[0].id), { force: true });
        replaceRoute({ name: "sessions", id: state.activeSession.id });
      } else {
        state.activeSession = null;
        replaceRoute({ name: "sessions", id: "" });
      }
    }
    return;
  }
  if (state.activeSession?.id) {
    const fresh = state.sessions.find((session) => session.id === state.activeSession.id);
    if (fresh) {
      setActiveSessionSnapshot(await fetchSessionSnapshot(fresh.id));
      return;
    }
    if (state.sessions.length) {
      setActiveSessionSnapshot(await fetchSessionSnapshot(state.sessions[0].id), { force: true });
      if (state.route.name === "sessions") {
        replaceRoute({ name: "sessions", id: state.activeSession.id });
      }
      return;
    }
    state.activeSession = null;
    if (state.route.name === "sessions") {
      replaceRoute({ name: "sessions", id: "" });
    }
    return;
  }
  if (state.sessions.length) {
    setActiveSessionSnapshot(await fetchSessionSnapshot(state.sessions[0].id), { force: true });
    if (state.route.name === "sessions" && !state.route.id) {
      replaceRoute({ name: "sessions", id: state.activeSession.id });
    }
  }
}

function render(options = {}) {
  if (state.imeComposition.active && !options.force) {
    state.imeComposition.pendingRender = true;
    if (options.scheduleRefresh) state.imeComposition.pendingRefresh = true;
    return;
  }
  if (options.deferForTextSelection && hasActiveAppTextSelection()) {
    state.deferredRender = { preserveFocus: Boolean(options.preserveFocus), scheduleRefresh: Boolean(options.scheduleRefresh) };
    if (options.scheduleRefresh) scheduleAutoRefresh();
    return;
  }
  state.deferredRender = null;
  const focusState = options.preserveFocus ? captureFocusState() : null;
  const viewportState = captureSessionViewportState();
  renderTopbar();
  renderToasts();
  switch (state.route.name) {
    case "new":
      appView.innerHTML = renderNewSession();
      bindNewSession(appView, {
        onCreate: createSession,
        onProbe: runProbe,
        onSwitchProfile: switchNewSessionProfile,
      });
      break;
    case "settings":
      appView.innerHTML = renderSettings();
      bindSettings(appView, {
        onSwitchProfile: switchSettingsProfile,
        onSwitchProvider: switchSettingsProvider,
        onSwitchSearchMode: () => render(),
        onSave: saveSettings,
        onDuplicate: duplicateProfile,
        onRunCheck: runCheck,
      });
      break;
    case "skills":
      appView.innerHTML = renderSkills();
      bindSkills(appView, {
        onPreview: previewSkill,
        onInstall: installSkill,
        onLandingDemo: useLandingDemo,
      });
      break;
    case "sessions":
    default:
      const sessionTools = toolsForProfile(state.activeSession?.profile);
      appView.innerHTML = renderSessionWorkspace({
        sessions: state.sessions,
        activeSession: state.activeSession,
        result: state.lastResult,
        tools: sessionTools,
        inspectorTab: state.inspectorTab,
      });
      bindSessionWorkspace(appView, {
        tools: sessionTools,
        onFilter: (value) => {
          state.sessionFilter = value;
          render({ restoreFilterFocus: true });
        },
        onRefresh: async () => {
          await runWithStatus({ status: "loading", action: "refresh-sessions", successMessage: "Sessions refreshed" }, async () => {
            await refreshSessions();
          });
        },
        onSelect: selectSession,
        onAppend: appendTurn,
        onCopy: copyText,
        onDelete: deleteSession,
        onComposerDraft: updateComposerDraft,
        onEditTools: updateSessionTools,
        onToolEditDraft: updateToolEditDraft,
        onInspectorTab: (tab) => {
          state.inspectorTab = tab;
          render();
        },
        onMobilePanel: (panel) => {
          state.mobilePanel = state.mobilePanel === panel ? "" : panel;
          render();
        },
      });
      break;
  }
  if (options.restoreFilterFocus) {
    restoreSessionFilterFocus();
  }
  if (focusState) {
    restoreFocusState(focusState);
  }
  restoreSessionViewportState(viewportState);
  scheduleAutoRefresh();
}

function renderTopbar() {
  const file = state.config?.env_file_found ? ".env.local" : "defaults";
  const profiles = state.config?.profiles?.length || 0;
  const offset = state.config?.local_time?.offset_label || "local time";
  envStatus.textContent = `${file} · ${profiles} profile(s) · ${offset}`;
  document.querySelectorAll("[data-route]").forEach((link) => {
    const route = link.dataset.route;
    const active = route === state.route.name || (route === "sessions" && state.route.name === "sessions");
    link.classList.toggle("active", active);
  });
}

function setActiveSessionSnapshot(session, options = {}) {
  if (!session) {
    state.activeSession = null;
    return true;
  }
  if (!options.force && !shouldAcceptSessionSnapshot(state.activeSession, session, options)) {
    return false;
  }
  state.activeSession = session;
  return true;
}

function fetchSessionSnapshot(id) {
  return api.session(id);
}

function shouldAcceptSessionSnapshot(current, next, options = {}) {
  if (!current?.id || current.id !== next?.id) return true;
  if (options.allowRunningOverlay) {
    return current.status === "running" || current.phase === "turn";
  }
  const currentTime = Date.parse(current.updated_at || "");
  const nextTime = Date.parse(next.updated_at || "");
  if (Number.isFinite(currentTime) && Number.isFinite(nextTime) && nextTime < currentTime) {
    return false;
  }
  if (current.can_append_message && current.status !== "running" && next.status === "running") {
    return false;
  }
  return true;
}

async function selectSession(id) {
  if (!id) return;
  if (state.activeSession?.id === id) return;
  captureActiveDrafts();
  captureSessionViewportState();
  const token = ++state.selectionToken;
  await runWithStatus({ status: "loading", action: "select-session", renderStart: false }, async () => {
    const session = await fetchSessionSnapshot(id);
    if (token !== state.selectionToken) return;
    setActiveSessionSnapshot(session, { force: true });
    state.lastResult = null;
    replaceRoute({ name: "sessions", id });
  });
}

async function createSession(payload) {
  state.newSessionDraft = payload;
  const request = newSessionRequestPayload(payload);
  const token = ++state.mutationToken;
  await runWithStatus({ status: "running", action: "create-session", successMessage: "Session created and opened" }, async () => {
    const session = await api.createSession(request);
    if (token !== state.mutationToken) return false;
    activateSessionSnapshot(session);
    state.lastResult = null;
    if (token === state.mutationToken) state.newSessionDraft = null;
    await refreshSessionsNonBlocking(token);
    void queueInitialTurn(session.id, payload.message, token);
    return true;
  });
}

function newSessionRequestPayload(draft) {
  const customPrompt = Boolean(draft.custom_prompt);
  const systemPrompt = customPrompt ? String(draft.system_prompt || "").trim() : "";
  if (customPrompt && !systemPrompt) {
    throw new Error("Custom prompt is enabled, so enter a system prompt or turn off customization.");
  }
  const payload = {
    profile_id: draft.profile_id || "",
    agent_profile: customPrompt ? {} : {},
    prompt_identity: customPrompt ? { source: "system_prompt_override" } : draft.prompt_identity || state.config?.prompt_identity || {},
    message: draft.message || "",
    system_prompt: systemPrompt,
    selected_tools: draft.selected_tools || [],
    context_policy: draft.context_policy || {},
  };
  if (!payload.system_prompt) delete payload.system_prompt;
  delete payload.agent_profile;
  if (!payload.prompt_identity?.source) delete payload.prompt_identity;
  return payload;
}

async function queueInitialTurn(sessionID, message, token) {
  if (!sessionID || !message || token !== state.mutationToken) return;
  await runStreamingTurn(sessionID, message, token, "Initial task completed");
}

async function appendTurn(message) {
  if (!state.activeSession?.id) return;
  const sessionID = state.activeSession.id;
  const token = ++state.mutationToken;
  await runStreamingTurn(sessionID, message, token, "Message sent");
}

async function runStreamingTurn(sessionID, message, token, successMessage) {
  const trimmed = String(message || "").trim();
  if (!sessionID || !trimmed) return;
  delete state.composerDrafts[sessionID];
  state.liveTurn = createLiveTurn(sessionID, trimmed);
  state.inspectorTab = "events";
  await runWithStatus({ status: "running", action: "append-turn", target: sessionID, successMessage }, async () => {
    let finalSession = null;
    let streamError = null;
    try {
      await api.streamTurn(sessionID, { message: trimmed }, (event) => {
        if (token !== state.mutationToken || state.liveTurn?.session_id !== sessionID) return;
        applyStreamEvent(event);
        render({ preserveFocus: true, scheduleRefresh: true });
      });
    } catch (error) {
      streamError = error;
      if (token === state.mutationToken && state.liveTurn?.session_id === sessionID) {
        state.liveTurn.failed = true;
      }
    } finally {
      if (token === state.mutationToken && state.activeSession?.id === sessionID) {
        try {
          finalSession = await fetchSessionSnapshot(sessionID);
          setActiveSessionSnapshot(finalSession);
        } catch {
          // The toast path in runWithStatus will surface the stream failure.
        }
      }
    }
    if (token !== state.mutationToken) return false;
    const result = state.liveTurn?.result || null;
    if (result) {
      state.lastResult = result;
      if (finalSession) result.session = finalSession;
    }
    state.liveTurn = null;
    await refreshSessionsNonBlocking(token);
    if (streamError) throw streamError;
    return true;
  });
  if (token === state.mutationToken && state.liveTurn?.session_id === sessionID) {
    state.liveTurn = null;
    await refreshSessionsNonBlocking(token);
    render({ preserveFocus: true, scheduleRefresh: true });
  }
}

async function updateSessionTools(selectedTools, reason) {
  if (!state.activeSession?.id) return;
  const sessionID = state.activeSession.id;
  state.toolEditDrafts[sessionID] = { selected_tools: selectedTools, reason };
  const token = ++state.mutationToken;
  await runWithStatus({ status: "loading", action: "update-tools", successMessage: "Session tools updated" }, async () => {
    const snapshot = await api.updateTools(sessionID, { selected_tools: selectedTools, reason });
    if (token !== state.mutationToken) return false;
    if (state.activeSession?.id === sessionID || state.route.id === sessionID) {
      setActiveSessionSnapshot(snapshot, { force: true });
    }
    delete state.toolEditDrafts[sessionID];
    state.lastResult = null;
    await refreshSessionsNonBlocking(token);
    return true;
  });
}

function createLiveTurn(sessionID, message) {
  return {
    session_id: sessionID,
    turn_id: "",
    sequence: 0,
    user_message: message,
    assistant_delta: "",
    reasoning_delta: "",
    events: [],
    provider_events: [],
    provider_requests: [],
    context_statuses: [],
    compactions: [],
    entries: [],
    result: null,
    failed: false,
  };
}

function applyStreamEvent(event) {
  if (!event || !state.liveTurn) return;
  if (Number(event.sequence || 0) <= Number(state.liveTurn.sequence || 0)) return;
  state.liveTurn.sequence = Number(event.sequence || 0);
  if (event.turn_id) state.liveTurn.turn_id = event.turn_id;
  state.liveTurn.events.push(event);
  ensureStreamingResult();
  switch (event.type) {
    case "session_snapshot":
      if (event.session_snapshot) {
        setActiveSessionSnapshot(event.session_snapshot);
        state.liveTurn.result.session = event.session_snapshot;
      }
      break;
    case "provider_request":
      if (event.provider_request) {
        state.liveTurn.provider_requests.push(event.provider_request);
        state.liveTurn.result.observation.provider_requests = state.liveTurn.provider_requests;
      }
      break;
    case "context_status":
      if (event.context_status) {
        upsertLiveContextStatus(event.context_status);
      }
      if (event.engine_event) {
        state.liveTurn.result.events.push(event.engine_event);
      }
      break;
    case "context_compaction":
      if (event.compaction) {
        upsertLiveCompaction(event.compaction);
      }
      if (event.engine_event) {
        state.liveTurn.result.events.push(event.engine_event);
      }
      if (event.error) state.liveTurn.failed = true;
      break;
    case "provider_delta":
      applyProviderDelta(event);
      break;
    case "tool_call":
    case "tool_result":
    case "assistant_message_appended":
    case "user_message_appended":
    case "turn_save_point":
      if (event.entry) {
        upsertLiveEntry(event.entry);
      }
      if (event.engine_event) {
        state.liveTurn.result.events.push(event.engine_event);
      }
      if (event.provider_event) {
        state.liveTurn.provider_events.push(event.provider_event);
        state.liveTurn.result.observation.provider_events = state.liveTurn.provider_events;
      }
      if (event.error) state.liveTurn.failed = true;
      break;
    case "turn_completed":
    case "turn_failed":
      if (event.result) {
        state.liveTurn.result = event.result;
        state.lastResult = event.result;
        if (event.result.session) setActiveSessionSnapshot(event.result.session);
      }
      if (event.entry) {
        upsertLiveEntry(event.entry);
      }
      if (event.error) state.liveTurn.failed = true;
      break;
    default:
      if (event.engine_event) {
        state.liveTurn.result.events.push(event.engine_event);
      }
  }
  if (state.liveTurn?.result) {
    state.lastResult = state.liveTurn.result;
  }
}

function ensureStreamingResult() {
  if (!state.liveTurn.result) {
    state.liveTurn.result = {
      session_id: state.liveTurn.session_id,
      turn_id: state.liveTurn.turn_id,
      status: "running",
      summary: "Turn is running.",
      events: [],
      harness_events: [],
      observation: {
        provider_requests: state.liveTurn.provider_requests,
        provider_events: state.liveTurn.provider_events,
        context_statuses: state.liveTurn.context_statuses,
        compaction_events: state.liveTurn.compactions,
        path_entries: state.liveTurn.entries,
      },
      session: state.activeSession,
    };
  }
  if (!state.liveTurn.result.observation) {
    state.liveTurn.result.observation = {};
  }
  state.liveTurn.result.observation.provider_requests = state.liveTurn.provider_requests;
  state.liveTurn.result.observation.provider_events = state.liveTurn.provider_events;
  state.liveTurn.result.observation.context_statuses = state.liveTurn.context_statuses;
  state.liveTurn.result.observation.compaction_events = state.liveTurn.compactions;
  state.liveTurn.result.observation.path_entries = state.liveTurn.entries;
}

function applyProviderDelta(event) {
  const engineEvent = event.engine_event || null;
  const providerEvent = event.provider_event || null;
  if (engineEvent) {
    state.liveTurn.result.events.push(engineEvent);
    if (engineEvent.type === "provider_delta") {
      state.liveTurn.assistant_delta += engineEvent.message || "";
    }
    if (engineEvent.type === "provider_reasoning") {
      state.liveTurn.reasoning_delta += engineEvent.message || "";
    }
  }
  if (providerEvent) {
    state.liveTurn.provider_events.push(providerEvent);
    state.liveTurn.result.observation.provider_events = state.liveTurn.provider_events;
    if (providerEvent.type === "delta") {
      state.liveTurn.assistant_delta += providerEvent.text || "";
    }
  }
}

function upsertLiveContextStatus(status) {
  const next = upsertByKey(state.liveTurn.context_statuses, status, contextStatusKey);
  state.liveTurn.context_statuses = next;
  state.liveTurn.result.observation.context_statuses = next;
  overlayActiveSessionContext({
    contextStatuses: next,
  });
}

function upsertLiveCompaction(compaction) {
  const next = upsertByKey(state.liveTurn.compactions, compaction, compactionKey);
  state.liveTurn.compactions = next;
  state.liveTurn.result.observation.compaction_events = next;
  overlayActiveSessionContext({
    compactions: next,
  });
}

function overlayActiveSessionContext({ contextStatuses, compactions }) {
  if (state.activeSession?.id !== state.liveTurn?.session_id) return;
  const observation = { ...(state.activeSession.observation || {}) };
  const patch = {
    ...state.activeSession,
    observation,
    status: "running",
    phase: "turn",
  };
  if (contextStatuses) {
    patch.context_statuses = mergeByKey(state.activeSession.context_statuses, contextStatuses, contextStatusKey);
    observation.context_statuses = mergeByKey(observation.context_statuses, contextStatuses, contextStatusKey);
  }
  if (compactions) {
    patch.compaction_events = mergeByKey(state.activeSession.compaction_events, compactions, compactionKey);
    observation.compaction_events = mergeByKey(observation.compaction_events, compactions, compactionKey);
  }
  setActiveSessionSnapshot(patch, { allowRunningOverlay: true });
  state.liveTurn.result.session = state.activeSession;
}

function upsertByKey(items, item, keyFor) {
  if (!item) return items || [];
  return mergeByKey(items, [item], keyFor);
}

function mergeByKey(base, additions, keyFor) {
  const out = [...(base || [])];
  for (const item of additions || []) {
    if (!item) continue;
    const key = keyFor(item);
    const index = out.findIndex((existing) => keyFor(existing) === key);
    if (index >= 0) {
      out[index] = item;
    } else {
      out.push(item);
    }
  }
  return out;
}

function contextStatusKey(status) {
  return [
    status?.phase || "",
    status?.request_id || status?.logical_request_id || "",
    status?.step || "",
    status?.attempt || "",
    status?.observed_at || "",
  ].join(":");
}

function compactionKey(compaction) {
  return compactionEventKey(compaction);
}

function upsertLiveEntry(entry) {
  if (!entry?.id) return;
  const index = state.liveTurn.entries.findIndex((item) => item.id === entry.id);
  if (index >= 0) {
    state.liveTurn.entries[index] = entry;
  } else {
    state.liveTurn.entries.push(entry);
  }
  if (state.activeSession?.id === state.liveTurn.session_id) {
    const entries = state.activeSession.path_entries || [];
    const existing = entries.findIndex((item) => item.id === entry.id);
    const nextEntries = existing >= 0 ? entries.map((item, idx) => (idx === existing ? entry : item)) : [...entries, entry];
    setActiveSessionSnapshot({ ...state.activeSession, path_entries: nextEntries, status: "running", phase: "turn" }, { allowRunningOverlay: true });
    state.liveTurn.result.session = state.activeSession;
  }
}

async function deleteSession(sessionID) {
  if (!sessionID) return;
  const confirmed = window.confirm(`Delete session ${sessionID}? This removes the test UI session, tree, and prompt-cache data.`);
  if (!confirmed) return;
  const token = ++state.mutationToken;
  await runWithStatus({ status: "loading", action: "delete-session", target: sessionID, successMessage: "Session deleted" }, async () => {
    await api.deleteSession(sessionID);
    if (token !== state.mutationToken) return false;
    clearSessionViewportState(sessionID);
    if (state.activeSession?.id === sessionID) {
      state.activeSession = null;
      state.lastResult = null;
      delete state.composerDrafts[sessionID];
      delete state.toolEditDrafts[sessionID];
      replaceRoute({ name: "sessions", id: "" });
    }
    await refreshSessions();
    return true;
  });
}

async function copyText(text, label = "Copied") {
  const value = String(text || "");
  if (!value) {
    addToast("error", "Nothing to copy");
    return;
  }
  try {
    await navigator.clipboard.writeText(value);
    addToast("success", label);
  } catch {
    addToast("error", "Clipboard copy failed");
  }
}

async function runProbe(selectedTools) {
  state.probeResult = "Running tool contract probe...";
  render();
  await runWithStatus({ status: "running", action: "run-probe", successMessage: "Tool contract probe completed" }, async () => {
    const result = await api.interfaceProbe({
      profile_id: state.newSessionDraft?.profile_id || state.config?.active_profile_id || "",
      selected_tools: selectedTools,
    });
    state.lastResult = result;
    state.probeResult = result.summary || result.output || "Probe completed.";
  });
}

function switchNewSessionProfile(profileID) {
  const profiles = state.config?.profiles || [];
  const profile = profiles.find((item) => item.id === profileID) || profiles[0] || defaultProfile();
  state.newSessionDraft = {
    ...(state.newSessionDraft || {}),
    profile_id: profileID,
    context_policy: contextPolicyForProfile(profile),
  };
  render();
}

function switchSettingsProfile(profileID) {
  ensureSettingsDraft();
  state.settingsDraft.active_profile_id = profileID;
  render();
}

function switchSettingsProvider(providerID) {
  ensureSettingsDraft();
  const profiles = clone(state.settingsDraft.profiles || state.config?.profiles || [defaultProfile()]);
  const activeID = state.settingsDraft.active_profile_id || state.config?.active_profile_id || profiles[0]?.id;
  const index = profiles.findIndex((profile) => profile.id === activeID);
  if (index < 0) return;
  const provider = providerByID(providerID);
  profiles[index] = {
    ...profiles[index],
    provider: providerID,
    model: providerDefaultModel(provider) || profiles[index].model || "",
    base_url: providerDefaultBaseURL(provider),
    web_search: defaultWebSearchForProvider(provider),
  };
  state.settingsDraft.profiles = profiles;
  render();
}

async function saveSettings(payload) {
  const token = ++state.mutationToken;
  state.settingsDraft = payload;
  state.settingsError = null;
  await runWithStatus({
    status: "loading",
    action: "save-settings",
    successMessage: "Settings saved",
    onError: (error) => {
      const activeProfile = (payload.profiles || []).find((profile) => profile.id === payload.active_profile_id) || payload.profiles?.[0] || {};
      state.settingsError = {
        message: error.message || "Settings save failed",
        profile_id: activeProfile.id || "",
        source: activeProfile.web_search?.source || "disabled",
        wire_shape: activeProfile.web_search?.hosted?.wire_shape || "",
      };
    },
  }, async () => {
    state.config = await api.saveConfig(payload);
    if (token !== state.mutationToken) return false;
    if (token === state.mutationToken && state.settingsDraft === payload) {
      state.settingsDraft = null;
    }
    state.settingsError = null;
    return true;
  });
}

function duplicateProfile() {
  ensureSettingsDraft();
  const profiles = clone(state.settingsDraft.profiles || state.config?.profiles || [defaultProfile()]);
  const activeID = state.settingsDraft.active_profile_id || state.config?.active_profile_id || profiles[0]?.id;
  const active = profiles.find((profile) => profile.id === activeID) || profiles[0] || defaultProfile();
  const copy = {
    ...active,
    id: `${active.id || "profile"}-${Date.now().toString(36)}`,
    name: `${active.name || active.id || "Profile"} copy`,
    api_key: "",
    api_key_set: active.api_key_set,
  };
  profiles.push(copy);
  state.settingsDraft.profiles = profiles;
  state.settingsDraft.active_profile_id = copy.id;
  addToast("info", "Profile duplicated");
  render();
}

async function runCheck(target) {
  state.checkResult = `Running ${target}...`;
  render();
  await runWithStatus({ status: "running", action: "run-check", target, successMessage: `${target} completed` }, async () => {
    const payload = target === "live-tool-scenarios" ? { profile_id: state.config?.active_profile_id || "" } : {};
    const result = await api.runCheck(target, payload);
    state.checkResult = JSON.stringify(result, null, 2);
  });
}

async function previewSkill(draft) {
  state.skillsInstallDraft = draft;
  await runWithStatus({ status: "loading", action: "skill-preview", successMessage: "Skill preview ready" }, async () => {
    state.skillsPreview = await api.previewSkill({ url: draft.url });
    state.skillsInstallDraft = { url: draft.url, replace: false };
  });
}

async function installSkill(draft) {
  if (!state.skillsPreview?.preview_token) {
    addToast("error", "Preview the skill before installing it");
    return;
  }
  if (String(state.skillsPreview.url || "").trim() !== String(draft.url || "").trim()) {
    addToast("error", "Preview the current skill URL before installing it");
    return;
  }
  if (state.skillsPreview.requires_replace && !draft.replace) {
    addToast("error", "Confirm replacement before installing over the existing skill");
    return;
  }
  state.skillsInstallDraft = draft;
  await runWithStatus({ status: "loading", action: "skill-install", successMessage: state.skillsPreview.requires_replace ? "Skill replaced" : "Skill installed" }, async () => {
    const response = await api.installSkill({
      url: draft.url,
      preview_token: state.skillsPreview.preview_token,
      replace: Boolean(draft.replace),
    });
    state.config.capabilities = response.capabilities;
    state.skillsPreview = null;
    state.skillsInstallDraft = { url: draft.url, replace: false };
  });
}

function useLandingDemo() {
  state.newSessionDraft = landingDemoDraft(toolsForProfile(currentLandingDemoProfile()));
  state.inspectorTab = "events";
  navigate({ name: "new", id: "" });
  addToast("info", "Landing demo loaded into New Session");
}

function currentLandingDemoProfile() {
  const profiles = state.config?.profiles || [];
  return profiles.find((profile) => profile.id === state.config?.active_profile_id) || profiles[0] || defaultProfile();
}

function navigate(route, options = {}) {
  state.route = route;
  const path = routePath(route);
  if (options.replace) {
    window.history.replaceState({}, "", path);
  } else if (window.location.pathname !== path) {
    window.history.pushState({}, "", path);
  }
  render();
}

function replaceRoute(route) {
  state.route = route;
  window.history.replaceState({}, "", routePath(route));
}

async function activateSession(result, token) {
  if (token && token !== state.mutationToken) return;
  state.lastResult = result;
  activateSessionSnapshot(result.session);
  await refreshSessionsNonBlocking(token);
}

function activateSessionSnapshot(session) {
  setActiveSessionSnapshot(session, { force: true });
  state.mobilePanel = "";
  state.inspectorTab = "requests";
  replaceRoute({ name: "sessions", id: session.id });
}

function restoreSessionFilterFocus() {
  const input = appView.querySelector("[data-session-filter]");
  if (!input) return;
  input.focus();
  const end = input.value.length;
  input.setSelectionRange(end, end);
}

function captureSessionViewportState() {
  if (state.route.name !== "sessions" || !state.activeSession?.id) return null;
  captureConversationScroll(state.activeSession.id);
  captureTimelineExpanded();
  return { sessionID: state.activeSession.id };
}

function restoreSessionViewportState(viewportState) {
	if (state.route.name !== "sessions" || !state.activeSession?.id) return;
	if (viewportState === null) return;
	restoreConversationScroll(state.activeSession.id);
}

function captureConversationScroll(sessionID) {
	const conversation = appView.querySelector(".conversation");
	const renderedSessionID = conversation?.dataset.sessionId || sessionID;
	if (!conversation || !renderedSessionID) return;
	const bottomGap = conversation.scrollHeight - conversation.clientHeight - conversation.scrollTop;
	state.conversationScroll[renderedSessionID] = {
		top: conversation.scrollTop,
		bottomPinned: bottomGap <= 16,
	};
}

function restoreConversationScroll(sessionID) {
  const conversation = appView.querySelector(".conversation");
  const saved = state.conversationScroll[sessionID];
  if (!conversation || !saved) return;
  if (saved.bottomPinned) {
    conversation.scrollTop = Math.max(0, conversation.scrollHeight - conversation.clientHeight);
    return;
  }
  conversation.scrollTop = Math.min(saved.top || 0, Math.max(0, conversation.scrollHeight - conversation.clientHeight));
}

function captureTimelineExpanded() {
  appView.querySelectorAll("details[data-expand-key]").forEach((details) => {
    state.timelineExpanded[details.dataset.expandKey || ""] = details.open;
  });
}

function clearSessionViewportState(sessionID) {
  delete state.conversationScroll[sessionID];
  const prefix = `session:${sessionID}:`;
  for (const key of Object.keys(state.timelineExpanded)) {
    if (key.startsWith(prefix)) delete state.timelineExpanded[key];
  }
}

async function runWithStatus({ status = "loading", action = "", target = "", successMessage = "", renderStart = true, onError = null }, fn) {
  const token = ++state.actionToken;
  state.action = action;
  state.actionTarget = target;
  state.running = Boolean(action);
  setStatus(status);
  if (renderStart) render();
  let finalStatus = "idle";
  try {
    const result = await fn();
    if (result !== false && successMessage) addToast("success", successMessage);
  } catch (error) {
    finalStatus = "error";
    onError?.(error);
    addToast("error", error.message || "Action failed");
  } finally {
    if (token === state.actionToken) {
      state.action = "";
      state.actionTarget = "";
      state.running = false;
      setStatus(finalStatus);
    }
    render();
  }
}

function setStatus(status) {
  globalStatus.className = `global-status ${status}`;
  const label = state.action ? `${status} · ${actionLabel(state.action)}` : status;
  globalStatus.textContent = label;
}

function addToast(kind, message) {
  const id = `toast-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`;
  state.toasts = [...state.toasts.slice(-4), { id, kind, message }];
  renderToasts();
  const timeout = kind === "error" ? 9000 : 3600;
  window.setTimeout(() => dismissToast(id), timeout);
}

function dismissToast(id) {
  if (!id) return;
  const next = state.toasts.filter((toast) => toast.id !== id);
  if (next.length === state.toasts.length) return;
  state.toasts = next;
  renderToasts();
}

function renderToasts() {
  if (!toastRegion) return;
  toastRegion.innerHTML = state.toasts.map((toast) => `
    <article class="toast ${escapeText(toast.kind)}" role="${toast.kind === "error" ? "alert" : "status"}">
      <span>${escapeText(toast.message)}</span>
      <button type="button" class="toast-close" data-toast-close="${escapeText(toast.id)}" aria-label="Dismiss notification">&times;</button>
    </article>
  `).join("");
}

function actionLabel(action) {
  switch (action) {
    case "create-session":
      return "creating";
    case "append-turn":
      return "sending";
    case "refresh-sessions":
      return "refreshing";
    case "save-settings":
      return "saving";
    case "update-tools":
      return "updating tools";
    case "run-probe":
      return "validating";
    case "run-check":
      return "running check";
    case "skill-preview":
      return "previewing skill";
    case "skill-install":
      return "installing skill";
    case "select-session":
      return "opening";
    case "delete-session":
      return "deleting";
    default:
      return action || "working";
  }
}

function scheduleAutoRefresh() {
  window.clearTimeout(autoRefreshTimer);
  autoRefreshTimer = 0;
  if (document.hidden || state.route.name !== "sessions" || !state.activeSession?.id) return;
  if (state.liveTurn?.session_id === state.activeSession.id) return;
  if (state.imeComposition.active) {
    state.imeComposition.pendingRefresh = true;
    return;
  }
  if (state.activeSession.status !== "running" && state.activeSession.phase !== "turn") return;
  const delay = 1000;
  autoRefreshTimer = window.setTimeout(refreshActiveSessionSnapshot, delay);
}

async function refreshActiveSessionSnapshot() {
  if (document.hidden || state.route.name !== "sessions" || !state.activeSession?.id) return;
  if (state.liveTurn?.session_id === state.activeSession.id) return;
  if (state.imeComposition.active) {
    state.imeComposition.pendingRefresh = true;
    return;
  }
  if (state.action === "select-session" || state.action === "delete-session") return scheduleAutoRefresh();
  const sessionID = state.activeSession.id;
  captureActiveDrafts();
  try {
    const [sessions, session] = await Promise.all([api.sessions(), fetchSessionSnapshot(sessionID)]);
    if (state.route.name !== "sessions" || state.activeSession?.id !== sessionID) return;
    state.sessions = sessions;
    setActiveSessionSnapshot(session);
    render({ preserveFocus: true, deferForTextSelection: true, scheduleRefresh: true });
  } catch (error) {
    if (error.status === 404 && state.activeSession?.id === sessionID) {
      state.activeSession = null;
      state.lastResult = null;
      replaceRoute({ name: "sessions", id: "" });
      await refreshSessions();
      render();
      return;
    }
    scheduleAutoRefresh();
  }
}

function hasActiveAppTextSelection() {
  const selection = window.getSelection?.();
  if (!selection || selection.isCollapsed || !selection.rangeCount) return false;
  for (let i = 0; i < selection.rangeCount; i += 1) {
    const range = selection.getRangeAt(i);
    if (range.intersectsNode?.(appView) || nodeInsideApp(range.commonAncestorContainer)) return true;
  }
  return false;
}

function nodeInsideApp(node) {
  if (!node) return false;
  const element = node.nodeType === Node.ELEMENT_NODE ? node : node.parentElement;
  return Boolean(element && appView.contains(element));
}

function flushDeferredRenderAfterSelection() {
  if (!state.deferredRender || hasActiveAppTextSelection()) return;
  const options = state.deferredRender;
  state.deferredRender = null;
  window.setTimeout(() => {
    if (state.deferredRender || hasActiveAppTextSelection()) return;
    render(options);
  }, 0);
}

function captureFocusState() {
  const active = document.activeElement;
  if (!active || !appView.contains(active) || !isEditableControl(active)) return null;
  const selector = focusSelectorFor(active);
  if (!selector) return null;
  return {
    selector,
    value: "value" in active ? active.value : "",
    selectionStart: typeof active.selectionStart === "number" ? active.selectionStart : null,
    selectionEnd: typeof active.selectionEnd === "number" ? active.selectionEnd : null,
    scrollTop: typeof active.scrollTop === "number" ? active.scrollTop : 0,
  };
}

function restoreFocusState(focusState) {
  if (!focusState?.selector) return;
  const next = appView.querySelector(focusState.selector);
  if (!next || !isEditableControl(next) || next.disabled) return;
  next.focus({ preventScroll: true });
  if ("value" in next && next.value !== focusState.value) {
    next.value = focusState.value;
  }
  if (typeof next.setSelectionRange === "function" && focusState.selectionStart !== null && focusState.selectionEnd !== null) {
    const max = String(next.value || "").length;
    next.setSelectionRange(Math.min(focusState.selectionStart, max), Math.min(focusState.selectionEnd, max));
  }
  if (typeof next.scrollTop === "number") {
    next.scrollTop = focusState.scrollTop || 0;
  }
}

function isEditableControl(element) {
  if (!(element instanceof HTMLInputElement || element instanceof HTMLTextAreaElement || element instanceof HTMLSelectElement)) return false;
  if (element.type === "button" || element.type === "submit" || element.type === "checkbox" || element.type === "radio") return false;
  return !element.readOnly;
}

function focusSelectorFor(element) {
  if (element.matches("[data-session-filter]")) return "[data-session-filter]";
  const name = element.getAttribute("name");
  if (!name) return "";
  if (element.closest("[data-append-form]")) return `[data-append-form] [name="${cssEscape(name)}"]`;
  if (element.closest("[data-tool-edit-form]")) return `[data-tool-edit-form] [name="${cssEscape(name)}"]`;
  if (element.closest("[data-new-session-form]")) return `[data-new-session-form] [name="${cssEscape(name)}"]`;
  if (element.closest("[data-skill-install-form]")) return `[data-skill-install-form] [name="${cssEscape(name)}"]`;
  if (element.closest("[data-settings-form]")) return `[data-settings-form] [name="${cssEscape(name)}"]`;
  return "";
}

function cssEscape(value) {
  if (window.CSS?.escape) return window.CSS.escape(value);
  return String(value).replaceAll("\\", "\\\\").replaceAll('"', '\\"');
}

function escapeText(value) {
  return String(value ?? "").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

async function refreshSessionsNonBlocking(mutationToken = 0) {
  const refreshToken = ++state.refreshToken;
  try {
    const sessions = await api.sessions();
    if (refreshToken !== state.refreshToken) return;
    if (mutationToken && mutationToken !== state.mutationToken) return;
    state.sessions = sessions;
  } catch (error) {
    addToast("error", `Action completed, but the session list could not refresh: ${error.message || "refresh failed"}`);
  }
}

function updateComposerDraft(sessionID, message) {
  if (!sessionID) return;
  state.composerDrafts[sessionID] = message;
}

function updateToolEditDraft(sessionID, draft) {
  if (!sessionID) return;
  state.toolEditDrafts[sessionID] = draft;
}

function persistEditableDraft(element) {
  if (!element || !isEditableControl(element)) return;
  if (element.closest("[data-append-form]") && state.activeSession?.id) {
    state.composerDrafts[state.activeSession.id] = element.value || "";
    return;
  }
  const newSessionForm = element.closest("[data-new-session-form]");
  if (newSessionForm) {
    const toolArea = appView.querySelector("[data-new-tools]");
    if (toolArea) state.newSessionDraft = readNewSessionDraft(newSessionForm, toolArea);
    return;
  }
  const settingsForm = element.closest("[data-settings-form]");
  if (settingsForm) {
    state.settingsDraft = readSettingsDraft(settingsForm);
    return;
  }
  const skillsForm = element.closest("[data-skill-install-form]");
  if (skillsForm) {
    state.skillsInstallDraft = readSkillInstallDraft(skillsForm);
    return;
  }
  const toolForm = element.closest("[data-tool-edit-form]");
  if (toolForm && state.activeSession?.id) {
    state.toolEditDrafts[state.activeSession.id] = readToolEditDraft(toolForm);
  }
}

function ensureSettingsDraft() {
  if (state.settingsDraft) return;
  state.settingsDraft = {
    active_profile_id: state.config?.active_profile_id || "",
    profiles: clone(state.config?.profiles || [defaultProfile()]),
    search_provider: {
      provider: state.config?.search_provider?.provider || "brave",
      api_key: "",
      endpoint: state.config?.search_provider?.endpoint || "",
    },
  };
}

function captureActiveDrafts() {
  if (state.route.name === "new") {
    const form = appView.querySelector("[data-new-session-form]");
    const toolArea = appView.querySelector("[data-new-tools]");
    if (form && toolArea) {
      state.newSessionDraft = readNewSessionDraft(form, toolArea);
    }
  }
  if (state.route.name === "settings") {
    const form = appView.querySelector("[data-settings-form]");
    if (form) {
      state.settingsDraft = readSettingsDraft(form);
    }
  }
  if (state.route.name === "skills") {
    const form = appView.querySelector("[data-skill-install-form]");
    if (form) {
      state.skillsInstallDraft = readSkillInstallDraft(form);
    }
  }
  const appendForm = appView.querySelector("[data-append-form]");
  if (appendForm && state.activeSession?.id) {
    state.composerDrafts[state.activeSession.id] = appendForm.elements.message?.value || "";
  }
  const toolForm = appView.querySelector("[data-tool-edit-form]");
  if (toolForm && state.activeSession?.id) {
    state.toolEditDrafts[state.activeSession.id] = readToolEditDraft(toolForm);
  }
}

function readNewSessionDraft(form, toolArea) {
  const data = new FormData(form);
  const customPrompt = data.get("custom_prompt") === "on";
  return {
    profile_id: String(data.get("profile_id") || ""),
    message: String(data.get("message") || ""),
    agent_profile: state.config?.agent_profile || {},
    prompt_identity: state.config?.prompt_identity || {},
    custom_prompt: customPrompt,
    system_prompt: customPrompt ? String(data.get("system_prompt") || "") : "",
    selected_tools: Array.from(toolArea.querySelectorAll('input[name="new-tools"]:checked')).map((input) => input.value),
    context_policy: {
      context_window_tokens: Number(data.get("context_window_tokens") || 0),
      max_output_tokens: Number(data.get("max_output_tokens") || 0),
      recent_tail_tokens: Number(data.get("recent_tail_tokens") || 0),
    },
  };
}

function readToolEditDraft(form) {
  return {
    selected_tools: Array.from(form.querySelectorAll('input[name="session-tools"]:checked')).map((input) => input.value),
    reason: form.elements.reason?.value || "",
  };
}
