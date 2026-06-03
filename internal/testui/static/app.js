import { api } from "./api.js";
import { clone, defaultProfile, normalizePath, providerByID, providerDefaultBaseURL, providerDefaultModel, routePath, state } from "./state.js";
import { bindNewSession, renderNewSession } from "./views/newSession.js";
import { bindSessionWorkspace, renderSessionWorkspace } from "./views/sessionWorkspace.js";
import { bindSettings, readSettingsDraft, renderSettings } from "./views/settings.js";

const appView = document.getElementById("appView");
const envStatus = document.getElementById("envStatus");
const globalStatus = document.getElementById("globalStatus");
const toastRegion = document.getElementById("toastRegion");
let autoRefreshTimer = 0;

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
      state.activeSession = await api.session(state.route.id);
    } catch (error) {
      if (error.status !== 404) throw error;
      if (state.sessions.length) {
        state.activeSession = await api.session(state.sessions[0].id);
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
      state.activeSession = await api.session(fresh.id);
      return;
    }
    if (state.sessions.length) {
      state.activeSession = await api.session(state.sessions[0].id);
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
    state.activeSession = await api.session(state.sessions[0].id);
    if (state.route.name === "sessions" && !state.route.id) {
      replaceRoute({ name: "sessions", id: state.activeSession.id });
    }
  }
}

function render(options = {}) {
  const focusState = options.preserveFocus ? captureFocusState() : null;
  renderTopbar();
  renderToasts();
  switch (state.route.name) {
    case "new":
      appView.innerHTML = renderNewSession();
      bindNewSession(appView, {
        onCreate: createSession,
        onProbe: runProbe,
      });
      break;
    case "settings":
      appView.innerHTML = renderSettings();
      bindSettings(appView, {
        onSwitchProfile: switchSettingsProfile,
        onSwitchProvider: switchSettingsProvider,
        onSave: saveSettings,
        onDuplicate: duplicateProfile,
        onRunCheck: runCheck,
      });
      break;
    case "sessions":
    default:
      appView.innerHTML = renderSessionWorkspace({
        sessions: state.sessions,
        activeSession: state.activeSession,
        result: state.lastResult,
        tools: state.config?.tools || [],
        inspectorTab: state.inspectorTab,
      });
      bindSessionWorkspace(appView, {
        tools: state.config?.tools || [],
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

async function selectSession(id) {
  if (!id) return;
  captureActiveDrafts();
  const token = ++state.selectionToken;
  await runWithStatus({ status: "loading", action: "select-session", renderStart: false }, async () => {
    const session = await api.session(id);
    if (token !== state.selectionToken) return;
    state.activeSession = session;
    state.lastResult = null;
    replaceRoute({ name: "sessions", id });
  });
}

async function createSession(payload) {
  state.newSessionDraft = payload;
  const token = ++state.mutationToken;
  await runWithStatus({ status: "running", action: "create-session", successMessage: "Session created and opened" }, async () => {
    const session = await api.createSession(payload);
    if (token !== state.mutationToken) return false;
    activateSessionSnapshot(session);
    state.lastResult = null;
    if (token === state.mutationToken) state.newSessionDraft = null;
    await refreshSessionsNonBlocking(token);
    void queueInitialTurn(session.id, payload.message, token);
    return true;
  });
}

async function queueInitialTurn(sessionID, message, token) {
  if (!sessionID || !message || token !== state.mutationToken) return;
  await runWithStatus({ status: "running", action: "append-turn", target: sessionID, successMessage: "Initial task completed" }, async () => {
    const result = await api.appendTurn(sessionID, { message });
    if (token !== state.mutationToken) return false;
    state.lastResult = result;
    if (state.activeSession?.id === sessionID || state.route.id === sessionID) {
      state.activeSession = result.session;
    }
    await refreshSessionsNonBlocking(token);
    return true;
  });
}

async function appendTurn(message) {
  if (!state.activeSession?.id) return;
  const sessionID = state.activeSession.id;
  const token = ++state.mutationToken;
  await runWithStatus({ status: "running", action: "append-turn", successMessage: "Message sent" }, async () => {
    const result = await api.appendTurn(sessionID, { message });
    if (token !== state.mutationToken) return false;
    state.lastResult = result;
    if (state.activeSession?.id === sessionID || state.route.id === sessionID) {
      state.activeSession = result.session;
    }
    delete state.composerDrafts[sessionID];
    await refreshSessionsNonBlocking(token);
    return true;
  });
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
      state.activeSession = snapshot;
    }
    delete state.toolEditDrafts[sessionID];
    state.lastResult = null;
    await refreshSessionsNonBlocking(token);
    return true;
  });
}

async function deleteSession(sessionID) {
  if (!sessionID) return;
  const confirmed = window.confirm(`Delete session ${sessionID}? This removes the test UI session, tree, and prompt-cache data.`);
  if (!confirmed) return;
  const token = ++state.mutationToken;
  await runWithStatus({ status: "loading", action: "delete-session", target: sessionID, successMessage: "Session deleted" }, async () => {
    await api.deleteSession(sessionID);
    if (token !== state.mutationToken) return false;
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
    const result = await api.interfaceProbe({ selected_tools: selectedTools });
    state.lastResult = result;
    state.probeResult = result.summary || result.output || "Probe completed.";
  });
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
    base_url: providerDefaultBaseURL(provider) || profiles[index].base_url || "",
  };
  state.settingsDraft.profiles = profiles;
  render();
}

async function saveSettings(payload) {
  const token = ++state.mutationToken;
  state.settingsDraft = payload;
  await runWithStatus({ status: "loading", action: "save-settings", successMessage: "Settings saved" }, async () => {
    state.config = await api.saveConfig(payload);
    if (token !== state.mutationToken) return false;
    if (token === state.mutationToken && state.settingsDraft === payload) {
      state.settingsDraft = null;
    }
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
    const result = await api.runCheck(target);
    state.checkResult = JSON.stringify(result, null, 2);
  });
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
  state.activeSession = session;
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

async function runWithStatus({ status = "loading", action = "", target = "", successMessage = "", renderStart = true }, fn) {
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
  const delay = state.activeSession.status === "running" || state.activeSession.phase === "turn" ? 1000 : 2000;
  autoRefreshTimer = window.setTimeout(refreshActiveSessionSnapshot, delay);
}

async function refreshActiveSessionSnapshot() {
  if (document.hidden || state.route.name !== "sessions" || !state.activeSession?.id) return;
  if (state.action === "select-session" || state.action === "delete-session") return scheduleAutoRefresh();
  const sessionID = state.activeSession.id;
  captureActiveDrafts();
  try {
    const [sessions, session] = await Promise.all([api.sessions(), api.session(sessionID)]);
    if (state.route.name !== "sessions" || state.activeSession?.id !== sessionID) return;
    state.sessions = sessions;
    state.activeSession = session;
    render({ preserveFocus: true });
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
  return {
    profile_id: String(data.get("profile_id") || ""),
    message: String(data.get("message") || ""),
    system_prompt: String(data.get("system_prompt") || ""),
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
