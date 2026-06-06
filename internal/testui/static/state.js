export const state = {
  config: null,
  sessions: [],
  activeSession: null,
  lastResult: null,
  route: { name: "sessions", id: "" },
  running: false,
  action: "",
  actionTarget: "",
  actionToken: 0,
  mutationToken: 0,
  refreshToken: 0,
  selectionToken: 0,
  toasts: [],
  inspectorTab: "tools",
  selectedRequest: 0,
  sessionFilter: "",
  probeResult: "",
  checkResult: "",
  mobilePanel: "",
  liveTurn: null,
  imeComposition: { active: false, selector: "", pendingRender: false, pendingRefresh: false },
  deferredRender: null,
  conversationScroll: {},
  timelineExpanded: {},
  newSessionDraft: null,
  skillsInstallDraft: null,
  skillsPreview: null,
  composerDrafts: {},
  settingsDraft: null,
  toolEditDrafts: {},
};

export function currentProfile() {
  const profiles = state.config?.profiles || [];
  return profiles.find((profile) => profile.id === state.config?.active_profile_id) || profiles[0] || defaultProfile();
}

export function defaultProfile() {
  const provider = providerCatalog()[0] || null;
  const providerID = provider?.id || "fake";
  return {
    id: "fake",
    name: provider?.name || "Fake",
    provider: providerID,
    model: providerDefaultModel(provider) || "fake-model",
    base_url: providerDefaultBaseURL(provider),
    fake_response: "floret local provider ok",
  };
}

export function defaultContextPolicy() {
  return {
    ...baseContextPolicyDefaults(),
    ...(state.config?.context_policy_defaults || {}),
  };
}

function baseContextPolicyDefaults() {
  return {
    context_window_tokens: 128000,
    max_output_tokens: 0,
    reserved_output_tokens: 4096,
    reserved_summary_tokens: 20000,
    recent_tail_tokens: 12000,
    recent_user_tokens: 15000,
    estimator_source: "chars_per_token",
    max_compaction_failures: 2,
    microcompact_tool_tokens: 4096,
  };
}

export function normalizePath(pathname = window.location.pathname) {
  if (pathname === "/" || pathname === "") return { name: "sessions", id: "" };
  if (pathname === "/sessions/new") return { name: "new", id: "" };
  if (pathname === "/skills") return { name: "skills", id: "" };
  if (pathname === "/settings") return { name: "settings", id: "" };
  if (pathname.startsWith("/sessions/")) return { name: "sessions", id: decodeURIComponent(pathname.slice("/sessions/".length)) };
  return { name: "sessions", id: "" };
}

export function routePath(route) {
  if (route.name === "new") return "/sessions/new";
  if (route.name === "skills") return "/skills";
  if (route.name === "settings") return "/settings";
  if (route.id) return `/sessions/${encodeURIComponent(route.id)}`;
  return "/sessions";
}

export function toolNamesForPreset(preset, tools) {
  const availableTools = (tools || []).filter((tool) => tool.available !== false);
  const byGroup = (groups) => {
    const wanted = new Set(groups);
    return availableTools.filter((tool) => wanted.has(tool.group)).map((tool) => tool.name);
  };
  const read = byGroup(["workspace_read"]);
  const write = byGroup(["workspace_write"]);
  const shell = byGroup(["execution"]);
  switch (preset) {
    case "read":
      return read;
    case "coding":
      return [...read, ...write];
    case "shell":
      return [...read, ...write, ...shell];
    case "all":
      return availableTools.map((tool) => tool.name);
    default:
      return [];
  }
}

export function groupTools(tools) {
  const groups = [];
  for (const tool of tools || []) {
    const sourceGroup = toolMatrixGroup(tool);
    const id = sourceGroup.id;
    let group = groups.find((item) => item.id === id);
    if (!group) {
      group = { id, title: sourceGroup.title, tools: [] };
      groups.push(group);
    }
    group.tools.push(tool);
  }
  return groups;
}

function toolMatrixGroup(tool) {
  if (tool.kind === "mcp" || tool.source === "mcp" || tool.annotations?.source === "mcp") {
    return { id: "mcp_client_tools", title: "MCP client tools" };
  }
  if (tool.kind === "skill" || tool.source === "skill" || tool.annotations?.source === "skill") {
    return { id: "agent_skills", title: "Agent skills" };
  }
  if (tool.name === "web_search") {
    if (tool.available === false) {
      return { id: "unavailable_capabilities", title: "Disabled / unavailable capabilities" };
    }
    if (tool.source === "provider-hosted") {
      return { id: "provider_hosted_capabilities", title: "Provider-hosted capabilities" };
    }
    if (tool.source?.startsWith("client:")) {
      return { id: "local_client_tools", title: "Local client tools" };
    }
  }
  return { id: tool.group || "tools", title: tool.group_title || tool.group || "Tools" };
}

export function providerCatalog() {
  return state.config?.catalog || [];
}

export function providerByID(providerID) {
  return providerCatalog().find((provider) => provider.id === providerID) || null;
}

export function providerDefaultModel(provider) {
  if (!provider) return "";
  return provider.default_model || provider.models?.find((model) => model.default)?.id || provider.models?.[0]?.id || "";
}

export function providerDefaultBaseURL(provider) {
  return provider?.default_base_url || "";
}

export function providerModel(provider, modelID) {
  if (!provider || !modelID) return null;
  return provider.models?.find((model) => model.id === modelID) || null;
}

export function contextPolicyForProfile(profile) {
  const defaults = defaultContextPolicy();
  const baseDefaults = baseContextPolicyDefaults();
  const provider = providerByID(profile?.provider);
  const model = providerModel(provider, profile?.model || providerDefaultModel(provider));
  const maxOutput = Number(model?.max_tokens ?? baseDefaults.max_output_tokens ?? 0);
  return {
    ...defaults,
    context_window_tokens: Number(model?.context_window ?? defaults.context_window_tokens ?? baseDefaults.context_window_tokens ?? 0),
    max_output_tokens: maxOutput,
    reserved_output_tokens: Number(defaults.reserved_output_tokens ?? 4096),
  };
}

export function shortID(id) {
  if (!id) return "none";
  if (id.length <= 18) return id;
  return `${id.slice(0, 10)}...${id.slice(-6)}`;
}

export function profileLabel(profile) {
  if (!profile) return "No profile";
  const name = profile.name || profile.id || profile.provider || "profile";
  const model = profile.model || "model";
  if (name.includes(model)) return name;
  return `${name} / ${model}`;
}

export function toolLabelList(names) {
  const clean = (names || []).filter(Boolean);
  if (!clean.length) return "none";
  if (clean.length <= 4) return clean.join(", ");
  return `${clean.slice(0, 4).join(", ")} +${clean.length - 4} more`;
}

export function formatLocalTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  const offsetMinutes = Number(state.config?.local_time?.offset_minutes || 0);
  const shifted = new Date(date.getTime() + offsetMinutes * 60 * 1000);
  const yyyy = shifted.getUTCFullYear();
  const mm = pad2(shifted.getUTCMonth() + 1);
  const dd = pad2(shifted.getUTCDate());
  const hh = pad2(shifted.getUTCHours());
  const min = pad2(shifted.getUTCMinutes());
  const sec = pad2(shifted.getUTCSeconds());
  const label = state.config?.local_time?.offset_label || offsetLabel(offsetMinutes);
  return `${yyyy}-${mm}-${dd} ${hh}:${min}:${sec} ${label}`;
}

export function relativeTime(value) {
  if (!value) return "updated -";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "updated -";
  const diffSeconds = Math.max(0, Math.round((Date.now() - date.getTime()) / 1000));
  if (diffSeconds < 45) return "updated just now";
  const minutes = Math.round(diffSeconds / 60);
  if (minutes < 60) return `updated ${minutes} min ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `updated ${hours} h ago`;
  const days = Math.round(hours / 24);
  if (days < 30) return `updated ${days} d ago`;
  return `updated ${formatLocalTime(value)}`;
}

export function formatDuration(ms) {
  if (!ms && ms !== 0) return "-";
  if (ms < 1000) return `${ms} ms`;
  return `${(ms / 1000).toFixed(1)} s`;
}

export function totalTokens(usage) {
  if (!usage) return 0;
  return usage.TotalTokens || usage.total_tokens || usage.InputTokens + usage.OutputTokens + usage.ReasoningTokens || 0;
}

export function escapeHTML(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

export function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function pad2(value) {
  return String(value).padStart(2, "0");
}

function offsetLabel(offsetMinutes) {
  const sign = offsetMinutes < 0 ? "-" : "+";
  const abs = Math.abs(offsetMinutes);
  return `UTC${sign}${pad2(Math.floor(abs / 60))}:${pad2(abs % 60)}`;
}
