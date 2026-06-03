export const state = {
  config: null,
  sessions: [],
  activeSession: null,
  lastResult: null,
  route: { name: "sessions", id: "" },
  running: false,
  inspectorTab: "tools",
  selectedRequest: 0,
  sessionFilter: "",
  probeResult: "",
  checkResult: "",
  mobilePanel: "",
};

export function currentProfile() {
  const profiles = state.config?.profiles || [];
  return profiles.find((profile) => profile.id === state.config?.active_profile_id) || profiles[0] || defaultProfile();
}

export function defaultProfile() {
  return {
    id: "fake",
    name: "Fake",
    provider: "fake",
    model: "fake-model",
    fake_response: "floret local provider ok",
  };
}

export function defaultContextPolicy() {
  return {
    context_window_tokens: 128000,
    max_output_tokens: 4096,
    recent_tail_tokens: 4096,
  };
}

export function normalizePath(pathname = window.location.pathname) {
  if (pathname === "/" || pathname === "") return { name: "sessions", id: "" };
  if (pathname === "/sessions/new") return { name: "new", id: "" };
  if (pathname === "/settings") return { name: "settings", id: "" };
  if (pathname.startsWith("/sessions/")) return { name: "sessions", id: decodeURIComponent(pathname.slice("/sessions/".length)) };
  return { name: "sessions", id: "" };
}

export function routePath(route) {
  if (route.name === "new") return "/sessions/new";
  if (route.name === "settings") return "/settings";
  if (route.id) return `/sessions/${encodeURIComponent(route.id)}`;
  return "/sessions";
}

export function toolNamesForPreset(preset, tools) {
  const byGroup = (groups) => {
    const wanted = new Set(groups);
    return (tools || []).filter((tool) => wanted.has(tool.group)).map((tool) => tool.name);
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
      return (tools || []).map((tool) => tool.name);
    default:
      return [];
  }
}

export function groupTools(tools) {
  const groups = [];
  for (const tool of tools || []) {
    const id = tool.group || "tools";
    let group = groups.find((item) => item.id === id);
    if (!group) {
      group = { id, title: tool.group_title || id, tools: [] };
      groups.push(group);
    }
    group.tools.push(tool);
  }
  return groups;
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

export function formatTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleString();
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
