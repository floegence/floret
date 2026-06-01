const state = {
  config: null,
  catalog: [],
  activeProfileId: "",
  running: false,
  lastResult: null,
};

const $ = (id) => document.getElementById(id);

const el = {
  envStatus: $("envStatus"),
  profileSelect: $("profileSelect"),
  profileName: $("profileName"),
  providerType: $("providerType"),
  modelName: $("modelName"),
  modelSelect: $("modelSelect"),
  modelDetails: $("modelDetails"),
  baseURL: $("baseURL"),
  apiKey: $("apiKey"),
  fakeResponse: $("fakeResponse"),
  addProfile: $("addProfile"),
  duplicateProfile: $("duplicateProfile"),
  saveConfig: $("saveConfig"),
  systemPrompt: $("systemPrompt"),
  userMessage: $("userMessage"),
  maxSteps: $("maxSteps"),
  maxContextMessages: $("maxContextMessages"),
  runAgent: $("runAgent"),
  runStatus: $("runStatus"),
  runTitle: $("runTitle"),
  summaryText: $("summaryText"),
  metricStatus: $("metricStatus"),
  metricSteps: $("metricSteps"),
  metricRequests: $("metricRequests"),
  metricTools: $("metricTools"),
  metricTokens: $("metricTokens"),
  metricDuration: $("metricDuration"),
  transitions: $("transitions"),
  transitionCount: $("transitionCount"),
  finalOutput: $("finalOutput"),
  outputMeta: $("outputMeta"),
  stepInspector: $("stepInspector"),
  stepCount: $("stepCount"),
  sessionMessages: $("sessionMessages"),
  messageCount: $("messageCount"),
  rawJson: $("rawJson"),
  copyRaw: $("copyRaw"),
  checkStatus: $("checkStatus"),
};

el.profileSelect.addEventListener("change", () => {
  syncCurrentProfileFromForm();
  state.activeProfileId = el.profileSelect.value;
  renderProfileForm();
});
["input", "change"].forEach((eventName) => {
  [el.profileName, el.providerType, el.modelName, el.baseURL, el.apiKey, el.fakeResponse].forEach((input) => {
    input.addEventListener(eventName, () => {
      syncCurrentProfileFromForm();
      if (input === el.providerType) applyProviderPreset();
      renderProviderFields();
      renderProfileOptions();
    });
  });
});
el.modelSelect.addEventListener("change", () => {
  el.modelName.value = el.modelSelect.value;
  syncCurrentProfileFromForm();
  renderModelDetails();
  renderProfileOptions();
});
el.addProfile.addEventListener("click", addProfile);
el.duplicateProfile.addEventListener("click", duplicateProfile);
el.saveConfig.addEventListener("click", saveConfig);
el.runAgent.addEventListener("click", runAgent);
el.copyRaw.addEventListener("click", copyRaw);
document.querySelectorAll(".check-button").forEach((button) => {
  button.addEventListener("click", () => runCheck(button.dataset.target));
});

loadConfig();

async function loadConfig() {
  const response = await fetch("/api/config");
  if (!response.ok) {
    el.envStatus.textContent = "Configuration could not be loaded.";
    return;
  }
  state.config = await response.json();
  state.catalog = state.config.catalog || [];
  state.activeProfileId = state.config.active_profile_id || state.config.profiles?.[0]?.id || "";
  if (!state.config.profiles || state.config.profiles.length === 0) {
    state.config.profiles = [defaultProfile()];
    state.activeProfileId = state.config.profiles[0].id;
  }
  renderConfig();
}

function renderConfig() {
  const file = state.config.env_file_found ? ".env.local" : "defaults";
  el.envStatus.textContent = `${file} · ${state.config.profiles.length} profile(s)`;
  renderProviderOptions();
  renderProfileOptions();
  renderProfileForm();
}

function renderProviderOptions() {
  const current = el.providerType.value || activeProfile()?.provider || "fake";
  el.providerType.innerHTML = "";
  providerCatalog().forEach((provider) => {
    const option = document.createElement("option");
    option.value = provider.id;
    option.textContent = provider.name || provider.id;
    el.providerType.appendChild(option);
  });
  el.providerType.value = current;
}

function renderProfileOptions() {
  const current = state.activeProfileId;
  el.profileSelect.innerHTML = "";
  state.config.profiles.forEach((profile) => {
    const option = document.createElement("option");
    option.value = profile.id;
    option.textContent = profileLabel(profile);
    el.profileSelect.appendChild(option);
  });
  el.profileSelect.value = current;
}

function renderProfileForm() {
  const profile = activeProfile();
  if (!profile) return;
  el.profileName.value = profile.name || "";
  el.providerType.value = profile.provider || "fake";
  el.modelName.value = profile.model || "";
  el.baseURL.value = profile.base_url || "";
  el.apiKey.value = "";
  el.apiKey.placeholder = profile.api_key_set ? "saved key retained if left empty" : "required for live providers";
  el.fakeResponse.value = profile.fake_response || "";
  renderProviderFields();
}

function renderProviderFields() {
  const live = el.providerType.value !== "fake";
  renderModelOptions();
  renderModelDetails();
  document.querySelectorAll(".provider-only").forEach((node) => node.classList.toggle("hidden", !live));
  document.querySelectorAll(".fake-only").forEach((node) => node.classList.toggle("hidden", live));
}

function renderModelOptions() {
  const provider = providerByID(el.providerType.value);
  const models = provider?.models || [];
  const current = el.modelName.value || provider?.default_model || models[0]?.id || "";
  el.modelSelect.innerHTML = "";
  models.forEach((model) => {
    const option = document.createElement("option");
    option.value = model.id;
    option.textContent = `${model.name || model.id} · ${model.id}`;
    el.modelSelect.appendChild(option);
  });
  const customAllowed = !provider || provider.custom || provider.id === "openai-compatible";
  if (customAllowed && current && !models.some((model) => model.id === current)) {
    const option = document.createElement("option");
    option.value = current;
    option.textContent = `${current} · custom`;
    el.modelSelect.appendChild(option);
  }
  el.modelSelect.value = current;
  if (!el.modelSelect.value && models[0]) {
    el.modelSelect.value = models[0].id;
  }
  el.modelName.value = el.modelSelect.value || current;
  el.modelSelect.classList.toggle("hidden", models.length === 0 && customAllowed);
  el.modelName.classList.toggle("hidden", !(models.length === 0 && customAllowed));
}

function renderModelDetails() {
  const provider = providerByID(el.providerType.value);
  const model = modelByID(provider, el.modelName.value || el.modelSelect.value);
  if (!provider) {
    el.modelDetails.textContent = "Unknown provider.";
    return;
  }
  const pieces = [
    provider.api || "api unknown",
    provider.default_base_url ? provider.default_base_url : "custom endpoint",
  ];
  if (model) {
    pieces.push(`${formatTokens(model.context_window)} context`);
    pieces.push(`${formatTokens(model.max_tokens)} max output`);
    pieces.push((model.input || []).includes("image") ? "text + image" : "text only");
    pieces.push(model.reasoning ? "reasoning" : "no reasoning flag");
  } else {
    pieces.push("custom model metadata");
  }
  el.modelDetails.textContent = pieces.filter(Boolean).join(" · ");
}

function applyProviderPreset() {
  const provider = providerByID(el.providerType.value);
  if (!provider) return;
  if (!el.profileName.value.trim() || el.profileName.value === "New provider") {
    el.profileName.value = provider.name || provider.id;
  }
  if (provider.default_base_url) {
    el.baseURL.value = provider.default_base_url;
  }
  const defaultModel = provider.default_model || provider.models?.[0]?.id || "";
  if (defaultModel) {
    el.modelName.value = defaultModel;
  }
  renderModelOptions();
  renderModelDetails();
  syncCurrentProfileFromForm();
}

function syncCurrentProfileFromForm() {
  if (!state.config) return;
  const index = state.config.profiles.findIndex((profile) => profile.id === state.activeProfileId);
  if (index < 0) return;
  const existing = state.config.profiles[index];
  const provider = el.providerType.value;
  const model = el.modelName.value.trim();
  state.config.profiles[index] = {
    ...existing,
    name: profileLabel({ provider, model }),
    provider,
    model,
    base_url: el.baseURL.value.trim(),
    api_key: el.apiKey.value.trim(),
    api_key_set: existing.api_key_set || Boolean(el.apiKey.value.trim()),
    fake_response: el.fakeResponse.value.trim(),
  };
}

function addProfile() {
  syncCurrentProfileFromForm();
  const profile = defaultProfile();
  profile.id = uniqueID("profile");
  profile.name = profileLabel(profile);
  state.config.profiles.push(profile);
  state.activeProfileId = profile.id;
  renderConfig();
}

function duplicateProfile() {
  syncCurrentProfileFromForm();
  const source = activeProfile() || defaultProfile();
  const copy = { ...source, id: uniqueID(source.id || "profile"), name: `${profileLabel(source)} copy`, api_key: "" };
  state.config.profiles.push(copy);
  state.activeProfileId = copy.id;
  renderConfig();
}

async function saveConfig() {
  syncCurrentProfileFromForm();
  el.saveConfig.disabled = true;
  try {
    const response = await fetch("/api/config", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        active_profile_id: state.activeProfileId,
        profiles: state.config.profiles,
      }),
    });
    const body = await response.json();
    if (!response.ok) throw new Error(body.error || "save failed");
    state.config = body;
    state.activeProfileId = body.active_profile_id;
    renderConfig();
    el.envStatus.textContent = `.env.local saved · ${state.config.profiles.length} profile(s)`;
  } catch (error) {
    el.envStatus.textContent = error.message;
  } finally {
    el.saveConfig.disabled = false;
  }
}

async function runAgent() {
  syncCurrentProfileFromForm();
  const message = el.userMessage.value.trim();
  if (!message) {
    renderError("First user message is required.");
    return;
  }
  setRunning(true);
  resetRunView("running");
  try {
    const response = await fetch("/api/agent/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        profile_id: state.activeProfileId,
        message,
        system_prompt: el.systemPrompt.value,
        max_steps: Number(el.maxSteps.value || 8),
        max_context_messages: Number(el.maxContextMessages.value || 32),
      }),
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || "agent run failed");
    state.lastResult = result;
    renderAgentResult(result);
  } catch (error) {
    renderError(error.message);
  } finally {
    setRunning(false);
  }
}

async function runCheck(target) {
  el.checkStatus.textContent = `Running ${target}...`;
  try {
    const response = await fetch("/api/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ target }),
    });
    const result = await response.json();
    el.checkStatus.textContent = `${result.title}: ${result.status} · ${result.summary}`;
  } catch (error) {
    el.checkStatus.textContent = error.message;
  }
}

function renderAgentResult(result) {
  setStatus(result.status);
  el.runTitle.textContent = `${result.profile.name} / ${result.profile.model}`;
  el.summaryText.textContent = result.summary || "";
  el.metricStatus.textContent = result.status;
  el.metricSteps.textContent = String(result.metrics.steps || 0);
  el.metricRequests.textContent = String(result.metrics.llm_requests || 0);
  el.metricTools.textContent = String(result.metrics.tool_calls || 0);
  el.metricTokens.textContent = String(totalTokens(result.metrics.usage));
  el.metricDuration.textContent = formatDuration(result.duration_ms);
  el.finalOutput.textContent = result.output || result.error || "(empty output)";
  el.outputMeta.textContent = result.error ? "error" : "captured";
  renderTransitions(result.observation.transitions || []);
  renderSteps(result);
  renderSessionMessages(result.observation.session_messages || []);
  el.rawJson.textContent = JSON.stringify(result, null, 2);
}

function renderTransitions(transitions) {
  el.transitionCount.textContent = String(transitions.length);
  el.transitions.classList.toggle("empty", transitions.length === 0);
  el.transitions.innerHTML = "";
  if (transitions.length === 0) {
    el.transitions.textContent = "No transitions yet.";
    return;
  }
  transitions.forEach((item) => {
    const row = document.createElement("div");
    row.className = `transition ${transitionClass(item.to)}`;
    row.innerHTML = `
      <span class="dot"></span>
      <div>
        <strong>${escapeHTML(item.from)} → ${escapeHTML(item.to)}</strong>
        <small>step ${item.step || "-"} · ${escapeHTML(item.reason || "")}</small>
        <p>${escapeHTML(item.details || "")}</p>
      </div>
    `;
    el.transitions.appendChild(row);
  });
}

function renderSteps(result) {
  const requests = result.observation.provider_requests || [];
  const providerEvents = result.observation.provider_events || [];
  el.stepCount.textContent = `${requests.length} steps`;
  el.stepInspector.classList.toggle("empty", requests.length === 0);
  el.stepInspector.innerHTML = "";
  if (requests.length === 0) {
    el.stepInspector.textContent = "No provider steps yet.";
    return;
  }
  requests.forEach((request) => {
    const events = providerEvents.filter((event) => event.step === request.step);
    const card = document.createElement("article");
    card.className = "step-card";
    card.innerHTML = `
      <header>
        <span class="step-badge">Step ${request.step}</span>
        <strong>${escapeHTML(request.provider)} / ${escapeHTML(request.model)}</strong>
        <small>${request.messages.length} messages · ${request.tools.length} tools · ${events.length} stream events</small>
      </header>
      <div class="step-columns">
        <div>
          <h4>Provider Request</h4>
          ${request.messages.map(renderMessageMini).join("")}
          <div class="tool-list">${request.tools.map((tool) => `<code>${escapeHTML(tool.name || tool.Name)}</code>`).join("")}</div>
        </div>
        <div>
          <h4>Provider Stream</h4>
          ${events.map(renderProviderEvent).join("")}
        </div>
      </div>
    `;
    el.stepInspector.appendChild(card);
  });
}

function renderSessionMessages(messages) {
  el.messageCount.textContent = String(messages.length);
  el.sessionMessages.classList.toggle("empty", messages.length === 0);
  el.sessionMessages.innerHTML = "";
  if (messages.length === 0) {
    el.sessionMessages.textContent = "No session messages yet.";
    return;
  }
  messages.forEach((message) => {
    const row = document.createElement("div");
    row.className = `message-row role-${escapeAttr(message.role)}`;
    row.innerHTML = `
      <strong>${escapeHTML(message.role)}</strong>
      <p>${escapeHTML(message.content || message.tool_args || "")}</p>
      ${message.tool_name ? `<small>${escapeHTML(message.tool_name)} · ${escapeHTML(message.tool_call_id || "")}</small>` : ""}
    `;
    el.sessionMessages.appendChild(row);
  });
}

function renderMessageMini(message) {
  const content = message.content || message.tool_args || "";
  return `
    <div class="mini-message role-${escapeAttr(message.role)}">
      <strong>${escapeHTML(message.role)}</strong>
      <span>${escapeHTML(content || message.tool_name || "")}</span>
    </div>
  `;
}

function renderProviderEvent(event) {
  const toolCalls = event.tool_calls || event.ToolCalls || [];
  const usage = event.usage || event.Usage || {};
  const body = [
    event.text || event.Text || "",
    event.reason || event.Reason || "",
    toolCalls.length ? toolCalls.map((call) => `${call.Name || call.name}(${call.Args || call.args || ""})`).join(", ") : "",
    totalTokens(usage) ? `${totalTokens(usage)} tokens` : "",
  ].filter(Boolean).join(" · ");
  return `
    <div class="stream-event">
      <strong>${escapeHTML(event.type || event.Type)}</strong>
      <span>${escapeHTML(body || "(empty)")}</span>
    </div>
  `;
}

function resetRunView(status) {
  setStatus(status);
  el.runTitle.textContent = "Running agent...";
  el.summaryText.textContent = "Waiting for engine events and provider stream.";
  ["metricStatus", "metricSteps", "metricRequests", "metricTools", "metricTokens", "metricDuration"].forEach((key) => {
    el[key].textContent = "-";
  });
  el.transitions.textContent = "No transitions yet.";
  el.transitions.className = "scroll empty";
  el.stepInspector.textContent = "No provider steps yet.";
  el.stepInspector.className = "steps empty";
  el.sessionMessages.textContent = "No session messages yet.";
  el.sessionMessages.className = "scroll empty";
  el.finalOutput.textContent = "The agent output will appear here.";
  el.rawJson.textContent = "{}";
}

function renderError(message) {
  setStatus("error");
  el.summaryText.textContent = message;
  el.finalOutput.textContent = message;
  el.outputMeta.textContent = "error";
}

function setRunning(value) {
  state.running = value;
  el.runAgent.disabled = value;
  el.runAgent.textContent = value ? "Running..." : "Run Agent";
}

function setStatus(status) {
  el.runStatus.className = `status ${escapeAttr(status || "idle")}`;
  el.runStatus.textContent = status || "idle";
}

function activeProfile() {
  return state.config?.profiles?.find((profile) => profile.id === state.activeProfileId);
}

function defaultProfile() {
  const fake = providerByID("fake");
  const model = fake?.default_model || "fake-model";
  return {
    id: "local",
    name: "fake / fake-model",
    provider: "fake",
    model,
    fake_response: "floret local provider ok",
  };
}

function profileLabel(profile) {
  const provider = providerByID(profile?.provider);
  const providerName = provider?.name || profile?.provider || "Provider";
  const model = profile?.model || provider?.default_model || "model";
  return `${providerName} / ${model}`;
}

function providerCatalog() {
  return state.catalog?.length ? state.catalog : [{ id: "fake", name: "Fake", default_model: "fake-model", models: [{ id: "fake-model", name: "Fake model" }] }];
}

function providerByID(id) {
  return providerCatalog().find((provider) => provider.id === id);
}

function modelByID(provider, id) {
  return provider?.models?.find((model) => model.id === id);
}

function formatTokens(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n <= 0) return "unknown";
  if (n >= 1000000) return `${(n / 1000000).toFixed(n % 1000000 === 0 ? 0 : 1)}M`;
  if (n >= 1000) return `${Math.round(n / 1000)}K`;
  return String(n);
}

function uniqueID(prefix) {
  const base = slug(prefix || "profile") || "profile";
  let id = base;
  let i = 2;
  const ids = new Set(state.config.profiles.map((profile) => profile.id));
  while (ids.has(id)) {
    id = `${base}-${i}`;
    i += 1;
  }
  return id;
}

function slug(value) {
  return String(value).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 48);
}

function totalTokens(usage) {
  usage = usage || {};
  return usage.total_tokens || usage.TotalTokens || ["input_tokens", "output_tokens", "reasoning_tokens", "cache_read_tokens", "cache_write_tokens"].reduce((sum, key) => sum + (usage[key] || 0), 0) || ["InputTokens", "OutputTokens", "ReasoningTokens", "CacheReadTokens", "CacheWriteTokens"].reduce((sum, key) => sum + (usage[key] || 0), 0);
}

function transitionClass(status) {
  if (["completed", "step_finished", "tool_result_received"].includes(status)) return "ok";
  if (["failed", "error", "budget_exceeded", "cancelled"].includes(status)) return "bad";
  if (["waiting", "provider_waiting", "tool_calling"].includes(status)) return "active";
  return "";
}

function formatDuration(ms) {
  if (!ms) return "0 ms";
  if (ms < 1000) return `${ms} ms`;
  return `${(ms / 1000).toFixed(1)} s`;
}

async function copyRaw() {
  await navigator.clipboard.writeText(el.rawJson.textContent);
  el.copyRaw.textContent = "Copied";
  setTimeout(() => { el.copyRaw.textContent = "Copy"; }, 1200);
}

function escapeHTML(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function escapeAttr(value) {
  return String(value ?? "").replace(/[^a-z0-9_-]/gi, "");
}
