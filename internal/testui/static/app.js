import { api } from "./api.js";
import { clone, defaultProfile, normalizePath, providerByID, providerDefaultBaseURL, providerDefaultModel, routePath, state } from "./state.js";
import { bindNewSession, renderNewSession } from "./views/newSession.js";
import { bindSessionWorkspace, renderSessionWorkspace } from "./views/sessionWorkspace.js";
import { bindSettings, renderSettings } from "./views/settings.js";

const appView = document.getElementById("appView");
const envStatus = document.getElementById("envStatus");
const globalStatus = document.getElementById("globalStatus");

document.addEventListener("click", (event) => {
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
          await runWithStatus("loading", async () => {
            await refreshSessions();
            render();
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
  envStatus.textContent = `${file} · ${profiles} profile(s)`;
  document.querySelectorAll("[data-route]").forEach((link) => {
    const route = link.dataset.route;
    const active = route === state.route.name || (route === "sessions" && state.route.name === "sessions");
    link.classList.toggle("active", active);
  });
}

async function selectSession(id) {
  if (!id) return;
  await runWithStatus("loading", async () => {
    state.activeSession = await api.session(id);
    state.lastResult = null;
    navigate({ name: "sessions", id }, { replace: true });
  });
}

async function createSession(payload) {
  await runWithStatus("running", async () => {
    const result = await api.createSession(payload);
    state.lastResult = result;
    state.sessions = await api.sessions();
    state.activeSession = result.session;
    navigate({ name: "sessions", id: result.session_id }, { replace: true });
  });
}

async function appendTurn(message) {
  if (!state.activeSession?.id) return;
  await runWithStatus("running", async () => {
    const result = await api.appendTurn(state.activeSession.id, { message });
    state.lastResult = result;
    state.sessions = await api.sessions();
    state.activeSession = result.session;
    render();
  });
}

async function updateSessionTools(selectedTools, reason) {
  if (!state.activeSession?.id) return;
  await runWithStatus("loading", async () => {
    const snapshot = await api.updateTools(state.activeSession.id, { selected_tools: selectedTools, reason });
    state.sessions = await api.sessions();
    state.activeSession = snapshot;
    state.lastResult = null;
    render();
  });
}

async function runProbe(selectedTools) {
  state.probeResult = "Running tool contract probe...";
  render();
  await runWithStatus("running", async () => {
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
  await runWithStatus("loading", async () => {
    state.config = await api.saveConfig(payload);
    render();
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
  render();
}

async function runCheck(target) {
  state.checkResult = `Running ${target}...`;
  render();
  await runWithStatus("running", async () => {
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

function restoreSessionFilterFocus() {
  const input = appView.querySelector("[data-session-filter]");
  if (!input) return;
  input.focus();
  const end = input.value.length;
  input.setSelectionRange(end, end);
}

async function runWithStatus(status, fn) {
  setStatus(status);
  state.running = status === "running";
  renderTopbar();
  try {
    await fn();
    setStatus("idle");
  } catch (error) {
    setStatus("error");
    window.alert(error.message);
  } finally {
    state.running = false;
    renderTopbar();
    render();
  }
}

function setStatus(status) {
  globalStatus.className = `global-status ${status}`;
  globalStatus.textContent = status;
}

function escapeText(value) {
  return String(value ?? "").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}
