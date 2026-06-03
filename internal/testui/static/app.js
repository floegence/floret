import { api } from "./api.js";
import { clone, defaultProfile, normalizePath, providerByID, providerDefaultBaseURL, providerDefaultModel, routePath, state } from "./state.js";
import { bindNewSession, renderNewSession } from "./views/newSession.js";
import { bindSessionWorkspace, renderSessionWorkspace } from "./views/sessionWorkspace.js";
import { bindSettings, renderSettings } from "./views/settings.js";

const appView = document.getElementById("appView");
const envStatus = document.getElementById("envStatus");
const globalStatus = document.getElementById("globalStatus");
const toastRegion = document.getElementById("toastRegion");

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
  navigate(normalizePath(url.pathname));
});

window.addEventListener("popstate", () => {
  state.route = normalizePath();
  render();
});

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
    state.activeSession = fresh ? await api.session(fresh.id) : null;
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
        onEditTools: updateSessionTools,
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
  await runWithStatus({ status: "loading", action: "select-session", renderStart: false }, async () => {
    state.activeSession = await api.session(id);
    state.lastResult = null;
    replaceRoute({ name: "sessions", id });
  });
}

async function createSession(payload) {
  state.newSessionDraft = payload;
  await runWithStatus({ status: "running", action: "create-session", successMessage: "Session created and opened" }, async () => {
    const result = await api.createSession(payload);
    await activateSession(result);
    state.newSessionDraft = null;
  });
}

async function appendTurn(message) {
  if (!state.activeSession?.id) return;
  await runWithStatus({ status: "running", action: "append-turn", successMessage: "Message sent" }, async () => {
    const result = await api.appendTurn(state.activeSession.id, { message });
    state.lastResult = result;
    state.sessions = await api.sessions();
    state.activeSession = result.session;
  });
}

async function updateSessionTools(selectedTools, reason) {
  if (!state.activeSession?.id) return;
  await runWithStatus({ status: "loading", action: "update-tools", successMessage: "Session tools updated" }, async () => {
    const snapshot = await api.updateTools(state.activeSession.id, { selected_tools: selectedTools, reason });
    state.sessions = await api.sessions();
    state.activeSession = snapshot;
    state.lastResult = null;
  });
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
  state.config.active_profile_id = profileID;
  render();
}

function switchSettingsProvider(providerID) {
  const profiles = clone(state.config?.profiles || [defaultProfile()]);
  const activeID = state.config?.active_profile_id || profiles[0]?.id;
  const index = profiles.findIndex((profile) => profile.id === activeID);
  if (index < 0) return;
  const provider = providerByID(providerID);
  profiles[index] = {
    ...profiles[index],
    provider: providerID,
    model: providerDefaultModel(provider) || profiles[index].model || "",
    base_url: providerDefaultBaseURL(provider) || profiles[index].base_url || "",
  };
  state.config.profiles = profiles;
  render();
}

async function saveSettings(payload) {
  await runWithStatus({ status: "loading", action: "save-settings", successMessage: "Settings saved" }, async () => {
    state.config = await api.saveConfig(payload);
  });
}

function duplicateProfile() {
  const profiles = clone(state.config?.profiles || [defaultProfile()]);
  const active = profiles.find((profile) => profile.id === state.config.active_profile_id) || profiles[0] || defaultProfile();
  const copy = {
    ...active,
    id: `${active.id || "profile"}-${Date.now().toString(36)}`,
    name: `${active.name || active.id || "Profile"} copy`,
    api_key: "",
    api_key_set: active.api_key_set,
  };
  profiles.push(copy);
  state.config.profiles = profiles;
  state.config.active_profile_id = copy.id;
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

async function activateSession(result) {
  state.lastResult = result;
  state.sessions = await api.sessions();
  state.activeSession = result.session;
  state.mobilePanel = "";
  state.inspectorTab = "requests";
  replaceRoute({ name: "sessions", id: result.session_id });
}

function restoreSessionFilterFocus() {
  const input = appView.querySelector("[data-session-filter]");
  if (!input) return;
  input.focus();
  const end = input.value.length;
  input.setSelectionRange(end, end);
}

async function runWithStatus({ status = "loading", action = "", target = "", successMessage = "", renderStart = true }, fn) {
  state.action = action;
  state.actionTarget = target;
  state.running = Boolean(action);
  setStatus(status);
  if (renderStart) render();
  let finalStatus = "idle";
  try {
    await fn();
    if (successMessage) addToast("success", successMessage);
  } catch (error) {
    finalStatus = "error";
    addToast("error", error.message || "Action failed");
  } finally {
    state.action = "";
    state.actionTarget = "";
    state.running = false;
    setStatus(finalStatus);
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
    <article class="toast ${escapeText(toast.kind)}" role="status">
      <span>${escapeText(toast.message)}</span>
      <button type="button" class="toast-close" data-toast-close="${escapeText(toast.id)}" aria-label="Dismiss notification">x</button>
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
    default:
      return action || "working";
  }
}

function escapeText(value) {
  return String(value ?? "").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}
