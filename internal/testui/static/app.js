const state = {
  config: null,
  catalog: [],
  activeProfileId: "",
  running: false,
  lastResult: null,
  selectedStep: null,
  activeTab: "overview",
  detailFocus: null,
  streamFilter: "all",
  traceFilter: "",
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
  flowHealth: $("flowHealth"),
  eventCount: $("eventCount"),
  transitions: $("transitions"),
  transitionCount: $("transitionCount"),
  finalOutput: $("finalOutput"),
  outputMeta: $("outputMeta"),
  stepList: $("stepList"),
  stepCount: $("stepCount"),
  detailTitle: $("detailTitle"),
  detailMeta: $("detailMeta"),
  stepDetail: $("stepDetail"),
  tabs: Array.from(document.querySelectorAll(".tab")),
  sessionMessages: $("sessionMessages"),
  messageCount: $("messageCount"),
  rawJson: $("rawJson"),
  traceSearch: $("traceSearch"),
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
el.traceSearch.addEventListener("input", () => {
  state.traceFilter = el.traceSearch.value.trim();
  renderRawTrace();
});

el.tabs.forEach((button) => {
  button.addEventListener("click", () => {
    state.activeTab = button.dataset.tab || "overview";
    state.detailFocus = null;
    if (state.activeTab !== "stream") state.streamFilter = "all";
    renderTabs();
    renderStepDetail();
  });
});

document.querySelectorAll(".check-button").forEach((button) => {
  button.addEventListener("click", () => runCheck(button.dataset.target));
});

document.addEventListener("click", (event) => {
  const tabTarget = event.target.closest("[data-open-tab]");
  if (tabTarget) {
    openStepDetail(tabTarget.dataset.selectStep, tabTarget.dataset.openTab, tabTarget.dataset.detailFocus);
    return;
  }

  const stepTarget = event.target.closest("[data-select-step]");
  if (stepTarget) {
    openStepDetail(stepTarget.dataset.selectStep, stepTarget.dataset.openTab || state.activeTab, stepTarget.dataset.detailFocus);
    return;
  }

  const filterTarget = event.target.closest("[data-stream-filter]");
  if (filterTarget) {
    state.streamFilter = filterTarget.dataset.streamFilter || "all";
    renderStepDetail();
    return;
  }

  const copyTarget = event.target.closest("[data-copy]");
  if (copyTarget) {
    copyDynamic(copyTarget);
  }
});

document.addEventListener("keydown", (event) => {
  if (!["Enter", " "].includes(event.key)) return;
  if (event.target.closest("button, input, select, textarea, summary")) return;
  const target = event.target.closest("[role='button'][data-select-step]");
  if (!target) return;
  event.preventDefault();
  openStepDetail(target.dataset.selectStep, target.dataset.openTab || state.activeTab, target.dataset.detailFocus);
});

loadConfig();
resetRunView("idle");

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
    if (model.cache?.prompt_cache_key || model.cache?.anthropic_cache_control) pieces.push("prompt cache");
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
        profile: activeProfile(),
        message,
        system_prompt: el.systemPrompt.value,
        max_steps: Number(el.maxSteps.value || 8),
        max_context_messages: Number(el.maxContextMessages.value || 32),
      }),
    });
    const result = await response.json();
    if (!response.ok) {
      if (result && (result.id || result.status || result.observation)) {
        state.lastResult = result;
        renderAgentResult(result);
        return;
      }
      throw new Error(result.error || "agent run failed");
    }
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
  el.runTitle.textContent = profileLabel(result.profile);
  el.summaryText.textContent = result.summary || "";
  el.metricStatus.textContent = result.status;
  el.metricSteps.textContent = String(result.metrics?.steps || 0);
  el.metricRequests.textContent = String(result.metrics?.llm_requests || 0);
  el.metricTools.textContent = String(result.metrics?.tool_calls || 0);
  el.metricTokens.textContent = String(totalTokens(result.metrics?.usage));
  el.metricDuration.textContent = formatDuration(result.duration_ms);
  el.finalOutput.textContent = result.output || result.error || "(empty output)";
  el.outputMeta.textContent = result.error ? "error" : "captured";
  el.eventCount.textContent = `${eventTotal(result)} events`;

  const steps = stepModels(result);
  state.selectedStep = steps[0]?.step || null;
  state.activeTab = "overview";
  state.detailFocus = null;
  state.streamFilter = "all";
  renderRunInspector();
  renderSessionMessages(result.observation?.session_messages || []);
  renderRawTrace();
}

function renderRunInspector() {
  const result = state.lastResult;
  renderFlowHealth(result);
  renderTransitions(result?.observation?.transitions || []);
  renderStepList(result);
  renderTabs();
  renderStepDetail();
}

function openStepDetail(stepValue, tab, focus) {
  const step = Number(stepValue);
  if (Number.isFinite(step) && step > 0) {
    state.selectedStep = step;
  }
  state.activeTab = normalizeTab(tab);
  state.detailFocus = focus || null;
  if (state.activeTab !== "stream") state.streamFilter = "all";
  renderRunInspector();
  requestAnimationFrame(() => {
    const target = el.stepDetail.querySelector(".focus-target") || el.stepDetail;
    target.scrollIntoView({ block: "nearest", behavior: "smooth" });
  });
}

function normalizeTab(tab) {
  return ["overview", "request", "stream", "cache", "raw"].includes(tab) ? tab : "overview";
}

function renderFlowHealth(result) {
  if (!result) {
    el.flowHealth.className = "health-grid empty";
    el.flowHealth.textContent = "No run has completed yet.";
    el.eventCount.textContent = "0 events";
    return;
  }
  const diagnostics = runDiagnostics(result);
  const cache = aggregateCache(result);
  const profile = result.profile || {};
  const finalDecision = runDecisionSummary(result);
  const finalStep = lastStepNumber(result);
  const decisionAttrs = finalStep ? `type="button" data-select-step="${finalStep}" data-open-tab="overview" data-detail-focus="decision"` : "type=\"button\"";
  const cacheAttrs = finalStep ? `type="button" data-select-step="${finalStep}" data-open-tab="cache" data-detail-focus="segments"` : "type=\"button\"";
  const level = diagnostics.some((item) => item.severity === "bad") ? "bad" : diagnostics.some((item) => item.severity === "warn") ? "warn" : "ok";
  el.flowHealth.className = "health-grid";
  el.flowHealth.innerHTML = `
    <button class="health-card action-card decision ${escapeAttr(finalDecision.severity)}" ${decisionAttrs}>
      <span>Agent decision</span>
      <strong>${escapeHTML(finalDecision.title)}</strong>
      <small>${escapeHTML(finalDecision.detail)}</small>
    </button>
    <div class="health-card ${escapeAttr(level)}">
      <span>Diagnostics</span>
      <strong>${diagnostics.length ? `${diagnostics.length} item(s)` : "clear"}</strong>
      <small>${level === "ok" ? "No state-flow warnings" : severityLabel(level)}</small>
    </div>
    <div class="health-card">
      <span>Run snapshot</span>
      <strong>${escapeHTML(profile.provider || "-")}</strong>
      <small>${escapeHTML(profile.model || "-")}</small>
    </div>
    <button class="health-card action-card" ${cacheAttrs}>
      <span>Prompt cache</span>
      <strong>${cache.reusedSegments} reused / ${cache.newSegments} new</strong>
      <small>${cache.cacheReadTokens} read / ${cache.cacheWriteTokens} write tokens</small>
    </button>
    <button class="health-card action-card" ${decisionAttrs}>
      <span>Terminal state</span>
      <strong>${escapeHTML(result.status || "-")}</strong>
      <small>${escapeHTML(finalDecision.terminal || result.error || result.summary || "")}</small>
    </button>
    <div class="diagnostics-list ${diagnostics.length ? "" : "empty-diagnostics"}">
      ${diagnostics.length ? diagnostics.map(renderDiagnostic).join("") : "<span>No warnings detected in the captured events.</span>"}
    </div>
  `;
}

function renderTransitions(transitions) {
  el.transitionCount.textContent = `${transitions.length} transitions`;
  el.transitions.classList.toggle("empty", transitions.length === 0);
  el.transitions.innerHTML = "";
  if (transitions.length === 0) {
    el.transitions.textContent = "No transitions yet.";
    return;
  }
  transitions.forEach((item, index) => {
    const action = transitionAction(item);
    const selected = item.step && item.step === state.selectedStep;
    const row = document.createElement("div");
    row.className = `transition-item ${transitionClass(item.to)} ${item.step ? "interactive" : ""} ${selected ? "selected" : ""}`;
    const attrs = item.step ? `data-select-step="${item.step}" data-open-tab="${escapeAttr(action.tab)}"${action.focus ? ` data-detail-focus="${escapeAttr(action.focus)}"` : ""}` : "";
    const summaryTag = item.step ? "button" : "div";
    const summaryType = item.step ? " type=\"button\"" : "";
    row.innerHTML = `
      <${summaryTag} class="transition-summary"${summaryType} ${attrs}>
        <span class="dot"></span>
        <span class="transition-copy">
          <strong>${escapeHTML(item.from)} -> ${escapeHTML(item.to)}</strong>
          <small>${formatTime(item.at)} · ${item.step ? `step ${item.step}` : `event ${index + 1}`} · ${escapeHTML(item.reason || "")}</small>
          ${item.details ? `<p>${escapeHTML(item.details)}</p>` : ""}
        </span>
      </${summaryTag}>
      ${item.step ? renderTransitionActions(item) : ""}
    `;
    el.transitions.appendChild(row);
  });
}

function renderTransitionActions(item) {
  const step = stepModels(state.lastResult).find((model) => model.step === item.step);
  if (!step) return "";
  const chips = [];
  const messages = step.request.messages.length;
  const segments = step.request.raw_segments?.length || 0;
  const tools = step.request.tools.length;
  const events = step.providerEvents.length;
  switch (item.reason) {
    case "provider_request":
      if (messages > 0) chips.push(flowChip(`${messages} messages`, step.step, "request", "messages"));
      if (segments > 0) chips.push(flowChip(`${segments} raw segments`, step.step, "cache", "segments"));
      if (tools > 0) chips.push(flowChip(`${tools} tools`, step.step, "request", "tools"));
      break;
    case "provider_delta":
    case "provider_finish":
    case "provider_retry":
      if (item.reason === "provider_finish") chips.push(flowChip("finish decision", step.step, "overview", "decision"));
      if (events > 0) chips.push(flowChip(`${events} provider events`, step.step, "stream", "events"));
      break;
    case "context_compact":
    case "context_continue":
      if (segments > 0) chips.push(flowChip(`${segments} raw segments`, step.step, "cache", "segments"));
      chips.push(flowChip("step decision", step.step, "overview", "decision"));
      break;
    case "step_end":
      chips.push(flowChip("decision", step.step, "overview", "decision"));
      if (events > 0) chips.push(flowChip("stream", step.step, "stream", "events"));
      break;
    default:
      chips.push(flowChip("step sequence", step.step, "overview", "sequence"));
      break;
  }
  return chips.length ? `<div class="flow-actions" aria-label="Open captured details">${chips.join("")}</div>` : "";
}

function flowChip(label, step, tab, focus) {
  const active = state.selectedStep === step && state.activeTab === tab && state.detailFocus === focus;
  return `
    <button class="flow-chip ${active ? "active" : ""}" type="button" data-select-step="${step}" data-open-tab="${escapeAttr(tab)}" data-detail-focus="${escapeAttr(focus)}">
      ${escapeHTML(label)}
    </button>
  `;
}

function transitionAction(item) {
  switch (item.reason) {
    case "provider_request":
      return { tab: "request", focus: "messages" };
    case "provider_delta":
    case "provider_retry":
      return { tab: "stream", focus: "events" };
    case "provider_finish":
      return { tab: "overview", focus: "decision" };
    case "context_continue":
      return { tab: "overview", focus: "decision" };
    case "context_compact":
      return { tab: "cache", focus: "segments" };
    case "step_end":
      return { tab: "overview", focus: "decision" };
    case "tool_call":
    case "tool_result":
    case "run_end":
    case "budget_exceeded":
      return { tab: "raw", focus: "events" };
    default:
      return { tab: "overview", focus: "sequence" };
  }
}

function renderStepList(result) {
  const steps = stepModels(result);
  el.stepCount.textContent = `${steps.length} steps`;
  el.stepList.classList.toggle("empty", steps.length === 0);
  el.stepList.innerHTML = "";
  if (steps.length === 0) {
    el.stepList.textContent = "No provider steps yet.";
    return;
  }
  steps.forEach((step) => {
    const selected = step.step === state.selectedStep;
    const decision = step.decision;
    const row = document.createElement("button");
    row.type = "button";
    row.className = `step-row ${selected ? "selected" : ""} ${step.severity}`;
    row.dataset.selectStep = String(step.step);
    row.innerHTML = `
      <span class="step-main">
        <strong>Step ${step.step}</strong>
        <small>${escapeHTML(decision.title)} · ${escapeHTML(step.provider)} / ${escapeHTML(step.model)}</small>
      </span>
      <span class="step-pills">
        <span>${step.request.messages.length} msg</span>
        <span>${step.request.tools.length} tools</span>
        <span>${step.providerEvents.length} stream</span>
        ${decision.finish ? `<span class="decision-pill finish">finish ${escapeHTML(decision.finish)}</span>` : ""}
        ${decision.completion ? `<span class="decision-pill completion">done ${escapeHTML(decision.completion)}</span>` : ""}
        ${decision.continuation ? `<span class="decision-pill continuation">continue ${escapeHTML(decision.continuation)}</span>` : ""}
        <span>${totalTokens(step.usage)} tokens</span>
      </span>
      <span class="step-cache">${step.cache.reused_segments || 0} reused / ${step.cache.new_segments || 0} new</span>
      <span class="state-badge ${step.severity}">${step.label}</span>
    `;
    el.stepList.appendChild(row);
  });
}

function renderTabs() {
  el.tabs.forEach((button) => {
    const active = button.dataset.tab === state.activeTab;
    button.classList.toggle("active", active);
    button.setAttribute("aria-selected", active ? "true" : "false");
  });
}

function renderStepDetail() {
  const step = selectedStepModel();
  if (!step) {
    el.detailTitle.textContent = "Step Detail";
    el.detailMeta.textContent = "select a step";
    el.stepDetail.className = "detail-body empty";
    el.stepDetail.textContent = "Select a provider step to inspect its request, stream, cache, and raw data.";
    return;
  }
  el.detailTitle.textContent = `Step ${step.step}`;
  el.detailMeta.textContent = `${step.provider} / ${step.model}`;
  el.stepDetail.className = "detail-body";
  switch (state.activeTab) {
    case "request":
      el.stepDetail.innerHTML = renderRequestTab(step);
      break;
    case "stream":
      el.stepDetail.innerHTML = renderStreamTab(step);
      break;
    case "cache":
      el.stepDetail.innerHTML = renderCacheTab(step);
      break;
    case "raw":
      el.stepDetail.innerHTML = renderRawTab(step);
      break;
    case "overview":
    default:
      el.stepDetail.innerHTML = renderOverviewTab(step);
      break;
  }
}

function renderOverviewTab(step) {
  const diagnostics = step.diagnostics.length ? step.diagnostics.map(renderDiagnostic).join("") : "<span>No warnings detected for this step.</span>";
  return `
    <section class="decision-board ${escapeAttr(step.decision.severity)} ${focusClass("decision")}">
      <div>
        <span>Step decision</span>
        <strong>${escapeHTML(step.decision.title)}</strong>
        <p>${escapeHTML(step.decision.detail)}</p>
      </div>
      <div class="decision-facts">
        ${decisionFact("Finish", step.decision.finish || "-")}
        ${decisionFact("Raw finish", step.decision.rawFinish || "-")}
        ${decisionFact("Completion", step.decision.completion || "-")}
        ${decisionFact("Continuation", step.decision.continuation || "-")}
        ${decisionFact("Inferred", step.decision.inferred ? "yes" : "no")}
      </div>
      <div class="decision-path">${stepDecisionPath(step)}</div>
      <div class="decision-evidence">
        ${flowChip("Request messages", step.step, "request", "messages")}
        ${flowChip("Provider stream", step.step, "stream", "events")}
        ${flowChip("Prompt segments", step.step, "cache", "segments")}
        ${flowChip("Raw step JSON", step.step, "raw", "events")}
      </div>
    </section>
    ${focusNote("decision", "Opened from State Flow. This panel explains why the selected step completed, continued, waited, or failed.")}
    <div class="detail-summary-grid">
      ${summaryTile("Provider events", step.providerEvents.length)}
      ${summaryTile("Engine events", step.engineEvents.length)}
      ${summaryTile("Tool calls", step.toolCalls.length)}
      ${summaryTile("Tool results", step.toolResults.length)}
      ${summaryTile("Prompt segments", step.request.raw_segments?.length || 0)}
      ${summaryTile("Tokens", totalTokens(step.usage))}
    </div>
    <section class="detail-section">
      <div class="section-label">Step diagnostics</div>
      <div class="diagnostics-list ${step.diagnostics.length ? "" : "empty-diagnostics"}">${diagnostics}</div>
    </section>
    <section class="detail-section ${focusClass("sequence")}">
      <div class="section-label">Event sequence</div>
      ${focusNote("sequence", "Opened from State Flow. This sequence shows the ordered engine and provider events for the selected step.")}
      <div class="sequence-list">
        ${stepSequence(step).map(renderSequenceItem).join("") || "<p class=\"empty-note\">No captured sequence items.</p>"}
      </div>
    </section>
  `;
}

function renderRequestTab(step) {
  return `
    <section class="detail-section ${focusClass("messages")}">
      <div class="section-label split">
        <span>Provider request messages</span>
        <span>${step.request.messages.length} messages</span>
      </div>
      ${focusNote("messages", "Opened from State Flow. These are the exact messages sent to the provider for this request.")}
      <div class="message-stack">${step.request.messages.map((message, index) => renderMessage(message, index, { collapsible: true })).join("") || "<p class=\"empty-note\">No request messages.</p>"}</div>
    </section>
    <section class="detail-section ${focusClass("tools")}">
      <div class="section-label split">
        <span>Tool definitions</span>
        <span>${step.request.tools.length} tools</span>
      </div>
      ${focusNote("tools", "Opened from State Flow. These are the tool definitions exposed to the selected provider call.")}
      <div class="tool-definition-list">${step.request.tools.map(renderToolDefinition).join("") || "<p class=\"empty-note\">No tools were exposed.</p>"}</div>
    </section>
    <section class="detail-section">
      <div class="section-label">Request metadata</div>
      ${renderKeyValueGrid([
        ["Provider", step.provider],
        ["Model", step.model],
        ["Step", step.step],
        ["Messages", step.request.messages.length],
        ["Tools", step.request.tools.length],
        ["Raw segments", step.request.raw_segments?.length || 0],
      ])}
    </section>
  `;
}

function renderStreamTab(step) {
  const allEvents = step.providerEvents.map((event, index) => ({ event, index }));
  const eventTypes = ["all", ...Array.from(new Set(step.providerEvents.map((event) => eventType(event))))];
  const visible = state.streamFilter === "all" ? allEvents : allEvents.filter((item) => eventType(item.event) === state.streamFilter);
  return `
    <section class="detail-section ${focusClass("events")}">
      ${focusNote("events", "Opened from State Flow. These are the provider stream events captured after the selected request.")}
      <div class="stream-toolbar">
        ${eventTypes.map((type) => `
          <button class="filter-chip ${state.streamFilter === type ? "active" : ""}" type="button" data-stream-filter="${escapeAttr(type)}">
            ${escapeHTML(type)}${type === "all" ? ` (${step.providerEvents.length})` : ""}
          </button>
        `).join("")}
      </div>
      <div class="event-list">
        ${visible.map((item) => renderProviderEvent(item.event, item.index)).join("") || "<p class=\"empty-note\">No provider events match this filter.</p>"}
      </div>
    </section>
  `;
}

function renderCacheTab(step) {
  const cache = step.cache || {};
  const segments = step.request.raw_segments || [];
  return `
    <section class="cache-decision-strip ${escapeAttr(step.decision.severity)}">
      ${decisionFact("Continuation", step.decision.continuation || "none")}
      ${decisionFact("Completion", step.decision.completion || "-")}
      ${decisionFact("Segments", `${cache.reused_segments || 0} reused / ${cache.new_segments || 0} new`)}
      ${decisionFact("Expected next request", step.decision.continuation ? "yes" : "no")}
    </section>
    <section class="cache-layout">
      ${cacheCard("Prefix identity", [
        hashRow("Prefix hash", cache.prefix_hash),
        ["Retention", cache.retention || "-"],
        ["Namespace", cache.namespace || "-"],
      ])}
      ${cacheCard("Payload identity", [
        hashRow("Payload hash", cache.payload_hash),
        ["Toolset", cache.toolset_id ? `${cache.toolset_id}@${cache.toolset_epoch || 0}` : "-"],
      ])}
      ${cacheCard("Reuse", [
        ["Reused segments", cache.reused_segments || 0],
        ["New segments", cache.new_segments || 0],
        ["Reuse ratio", reuseRatio(cache)],
      ])}
      ${cacheCard("Provider cache tokens", [
        ["Read tokens", cache.cache_read_tokens || 0],
        ["Write tokens", cache.cache_write_tokens || 0],
      ])}
    </section>
    <section class="detail-section ${focusClass("segments")}">
      <div class="section-label split">
        <span>Raw segment ledger</span>
        <span>${segments.length} segments</span>
      </div>
      ${focusNote("segments", "Opened from State Flow. These are the immutable raw prompt-cache segments captured for this provider request.")}
      <div class="segment-list">
        ${segments.map((segment, index) => renderSegment(step.step, segment, index)).join("") || "<p class=\"empty-note\">No raw prompt segments were captured.</p>"}
      </div>
    </section>
  `;
}

function renderRawTab(step) {
  const payload = {
    request: step.request,
    provider_events: step.providerEvents,
    engine_events: step.engineEvents,
    transitions: step.transitions,
  };
  return `
    <section class="detail-section ${focusClass("events")}">
      <div class="section-label split">
        <span>Engine events</span>
        <span>${step.engineEvents.length} events</span>
      </div>
      ${focusNote("events", "Opened from State Flow. These are the engine-side events behind this state transition.")}
      <div class="engine-event-list">
        ${step.engineEvents.map(renderEngineEvent).join("") || "<p class=\"empty-note\">No engine events were captured for this step.</p>"}
      </div>
    </section>
    <div class="code-actions">
      <button class="secondary tiny" type="button" data-copy="step-json" data-step="${step.step}">Copy step JSON</button>
    </div>
    <pre class="raw raw-block">${escapeHTML(JSON.stringify(payload, null, 2))}</pre>
  `;
}

function renderSessionMessages(messages) {
  el.messageCount.textContent = String(messages.length);
  el.sessionMessages.classList.toggle("empty", messages.length === 0);
  el.sessionMessages.innerHTML = "";
  if (messages.length === 0) {
    el.sessionMessages.textContent = "No session messages yet.";
    return;
  }
  messages.forEach((message, index) => {
    const row = document.createElement("div");
    row.className = `message-row role-${escapeAttr(message.role)}`;
    row.innerHTML = renderMessageInner(message, index, "Session message");
    el.sessionMessages.appendChild(row);
  });
}

function renderRawTrace() {
  if (!state.lastResult) {
    el.rawJson.textContent = "{}";
    return;
  }
  if (!state.traceFilter) {
    el.rawJson.textContent = JSON.stringify(state.lastResult, null, 2);
    return;
  }
  el.rawJson.textContent = JSON.stringify({
    query: state.traceFilter,
    matches: collectMatches(state.lastResult, state.traceFilter),
  }, null, 2);
}

function renderMessage(message, index, options = {}) {
  if (options.collapsible) {
    return `
      <details class="message-card role-${escapeAttr(message.role)} ${focusClass("messages")}">
        <summary>${renderMessageSummary(message, index)}</summary>
        <div class="message-body">${renderMessageDetails(message)}</div>
      </details>
    `;
  }
  return `
    <div class="message-row role-${escapeAttr(message.role)} ${focusClass("messages")}">
      ${renderMessageInner(message, index, "Message")}
    </div>
  `;
}

function renderMessageInner(message, index, label) {
  const content = message.content || message.tool_args || message.tool_name || "";
  return `
      <strong>${escapeHTML(label)} #${index + 1} · ${escapeHTML(message.role || "-")}</strong>
      <p>${escapeHTML(content)}</p>
      ${message.tool_name ? `<small>${escapeHTML(message.tool_name)} · ${escapeHTML(message.tool_call_id || "")}</small>` : ""}
  `;
}

function renderMessageSummary(message, index) {
  const content = message.content || message.tool_args || message.tool_name || "";
  return `
    <span class="state-badge info">${escapeHTML(message.role || "-")}</span>
    <strong>#${index + 1}</strong>
    <span>${escapeHTML(previewText(content, 140))}</span>
    ${message.tool_name ? `<small>${escapeHTML(message.tool_name)} · ${escapeHTML(message.tool_call_id || "")}</small>` : ""}
  `;
}

function renderMessageDetails(message) {
  return `
    ${renderKeyValueGrid([
      ["Role", message.role || "-"],
      ["Tool name", message.tool_name || "-"],
      ["Tool call ID", message.tool_call_id || "-"],
    ])}
    <pre class="text-block">${escapeHTML(message.content || message.tool_args || "")}</pre>
  `;
}

function renderToolDefinition(tool) {
  return `
    <div class="tool-definition">
      <strong>${escapeHTML(tool.name || tool.Name || "-")}</strong>
      <p>${escapeHTML(tool.description || tool.Description || "")}</p>
    </div>
  `;
}

function renderProviderEvent(event, index) {
  const type = eventType(event);
  const usage = event.usage || event.Usage || {};
  const calls = toolCalls(event);
  const summary = [
    event.text || event.Text || "",
    event.reason || event.Reason || "",
    calls.length ? `${calls.length} tool call(s)` : "",
    totalTokens(usage) ? `${totalTokens(usage)} tokens` : "",
  ].filter(Boolean).join(" · ") || "(empty)";
  return `
    <details class="event-card ${escapeAttr(type)}">
      <summary>
        <span class="state-badge ${eventSeverity(type, event)}">${escapeHTML(type)}</span>
        <strong>#${index + 1}</strong>
        <span>${escapeHTML(summary)}</span>
      </summary>
      <div class="event-body">
        ${event.text || event.Text ? `<pre class="text-block">${escapeHTML(event.text || event.Text)}</pre>` : ""}
        ${event.reason || event.Reason ? `<p class="reason">${escapeHTML(event.reason || event.Reason)}</p>` : ""}
        ${calls.length ? `<div class="tool-call-list">${calls.map(renderToolCall).join("")}</div>` : ""}
        ${totalTokens(usage) ? renderUsage(usage) : ""}
        <div class="code-actions">
          <button class="secondary tiny" type="button" data-copy="event-json" data-step="${event.step || event.Step}" data-event-index="${index}">Copy event JSON</button>
        </div>
        <pre class="raw raw-block">${escapeHTML(JSON.stringify(event, null, 2))}</pre>
      </div>
    </details>
  `;
}

function renderEngineEvent(event) {
  const type = eventKind(event);
  return `
    <details class="engine-event-card ${escapeAttr(engineEventSeverity(type, event))}">
      <summary>
        <span class="state-badge ${escapeAttr(engineEventSeverity(type, event))}">${escapeHTML(type)}</span>
        <strong>step ${escapeHTML(event.step || event.Step || "-")}</strong>
        <span>${escapeHTML(engineEventDetail(event) || event.message || event.Message || "(empty)")}</span>
      </summary>
      <pre class="raw raw-block">${escapeHTML(JSON.stringify(event, null, 2))}</pre>
    </details>
  `;
}

function renderToolCall(call) {
  return `
    <div class="tool-call">
      <strong>${escapeHTML(call.name || call.Name || "-")}</strong>
      <small>${escapeHTML(call.id || call.ID || "")}${call.read_only || call.ReadOnly ? " · read only" : ""}</small>
      <pre>${escapeHTML(call.args || call.Args || "")}</pre>
    </div>
  `;
}

function renderUsage(usage) {
  return `
    <div class="usage-grid">
      ${usageItem("Input", usageValue(usage, "input_tokens", "InputTokens"))}
      ${usageItem("Output", usageValue(usage, "output_tokens", "OutputTokens"))}
      ${usageItem("Reasoning", usageValue(usage, "reasoning_tokens", "ReasoningTokens"))}
      ${usageItem("Cache read", usageValue(usage, "cache_read_tokens", "CacheReadTokens"))}
      ${usageItem("Cache write", usageValue(usage, "cache_write_tokens", "CacheWriteTokens"))}
      ${usageItem("Cost", formatCost(usageValue(usage, "cost_usd", "CostUSD")))}
    </div>
  `;
}

function renderSegment(step, segment, index) {
  const stateLabel = segment.reused ? "reused" : "new";
  const open = state.detailFocus === "segments" ? " open" : "";
  return `
    <details class="segment-card ${stateLabel} ${focusClass("segments")}"${open}>
      <summary>
        <span class="state-badge ${stateLabel}">${stateLabel}</span>
        <strong>Segment #${index + 1} · seq ${segment.sequence || 0} · epoch ${segment.epoch || 0}</strong>
        <span>${escapeHTML(segment.role || segment.fragment_type || segment.id || "")}</span>
        <small>${escapeHTML(shortHash(segment.sha256 || ""))} · ${segment.byte_length || 0} bytes</small>
      </summary>
      <div class="segment-body">
        ${renderKeyValueGrid([
          ["ID", segment.id || "-"],
          ["SHA-256", segment.sha256 || "-"],
          ["Fingerprint", segment.fingerprint || "-"],
          ["Fragment type", segment.fragment_type || "-"],
          ["Schema version", segment.schema_version || "-"],
          ["Adapter version", segment.adapter_version || "-"],
          ["Structured ref", segment.structured_ref_id || "-"],
          ["Epoch", segment.epoch || 0],
          ["Sequence", segment.sequence || 0],
        ])}
        <div class="code-actions">
          <button class="secondary tiny" type="button" data-copy="segment-raw" data-step="${step}" data-segment-index="${index}">Copy raw segment</button>
          <button class="secondary tiny" type="button" data-copy="hash" data-value="${escapeAttr(segment.sha256 || "")}">Copy hash</button>
        </div>
        <pre class="raw raw-block">${escapeHTML(segment.raw || segment.raw_preview || "")}</pre>
      </div>
    </details>
  `;
}

function renderDiagnostic(item) {
  return `
    <div class="diagnostic ${escapeAttr(item.severity)}">
      <strong>${escapeHTML(item.title)}</strong>
      <span>${escapeHTML(item.detail || "")}</span>
      ${item.step ? `<small>step ${item.step}</small>` : ""}
    </div>
  `;
}

function renderSequenceItem(item) {
  const action = sequenceAction(item);
  const chips = item.type === "provider_request" ? renderSequenceRequestChips(item.step) : "";
  return `
    <div class="sequence-item ${escapeAttr(item.kind)}">
      <button class="sequence-summary" type="button" data-select-step="${item.step}" data-open-tab="${escapeAttr(action.tab)}" data-detail-focus="${escapeAttr(action.focus)}">
        <span class="state-badge ${escapeAttr(item.severity || "info")}">${escapeHTML(item.kind)}</span>
        <strong>${escapeHTML(item.title)}</strong>
        <span>${escapeHTML(item.detail || "")}</span>
      </button>
      ${chips}
    </div>
  `;
}

function renderSequenceRequestChips(stepNumber) {
  const step = stepModels(state.lastResult).find((model) => model.step === stepNumber);
  if (!step) return "";
  return `
    <div class="flow-actions sequence-actions">
      ${flowChip(`${step.request.messages.length} messages`, step.step, "request", "messages")}
      ${flowChip(`${step.request.raw_segments?.length || 0} raw segments`, step.step, "cache", "segments")}
    </div>
  `;
}

function sequenceAction(item) {
  if (item.type === "provider_request") return { tab: "request", focus: "messages" };
  if (item.kind === "provider") return { tab: "stream", focus: "events" };
  if (item.type === "context_continue") return { tab: "overview", focus: "decision" };
  if (item.type === "context_compact") return { tab: "cache", focus: "segments" };
  if (["tool_call", "tool_result", "run_end", "budget_exceeded", "provider_retry"].includes(item.type)) return { tab: "raw", focus: "events" };
  return { tab: "overview", focus: "sequence" };
}

function focusClass(key) {
  return state.detailFocus === key ? "focus-target" : "";
}

function focusNote(key, text) {
  if (state.detailFocus !== key) return "";
  return `<div class="focus-note">${escapeHTML(text)}</div>`;
}

function renderKeyValueGrid(rows) {
  return `
    <div class="kv-grid">
      ${rows.map((row) => Array.isArray(row) ? `
        <div>
          <span>${escapeHTML(row[0])}</span>
          <strong>${escapeHTML(row[1])}</strong>
        </div>
      ` : row).join("")}
    </div>
  `;
}

function decisionFact(label, value) {
  return `
    <div>
      <span>${escapeHTML(label)}</span>
      <strong>${escapeHTML(value)}</strong>
    </div>
  `;
}

function stepDecisionPath(step) {
  const decision = step.decision;
  const pieces = [];
  const hasProviderFinish = step.engineEvents.some((event) => eventKind(event) === "provider_finish");
  const source = hasProviderFinish && !decision.inferred ? "provider_finish captured" : "finish inferred from provider done";
  if (decision.finish || decision.rawFinish) pieces.push(`${source}: ${decision.finish || decision.rawFinish}`);
  if (step.toolCalls.length === 0) pieces.push("no tool calls");
  if (decision.completion) pieces.push(`completion_reason=${decision.completion}`);
  if (decision.continuation) pieces.push(`continuation_reason=${decision.continuation}`);
  if (!decision.continuation) pieces.push("no continuation");
  if (pieces.length === 0) pieces.push("no explicit terminal decision captured");
  return pieces.map((piece) => `<span>${escapeHTML(piece)}</span>`).join("<b>→</b>");
}

function cacheCard(title, rows) {
  return `
    <div class="cache-card">
      <h4>${escapeHTML(title)}</h4>
      ${renderKeyValueGrid(rows)}
    </div>
  `;
}

function hashRow(label, hash) {
  if (!hash) return [label, "-"];
  return `
    <div class="hash-row">
      <span>${escapeHTML(label)}</span>
      <strong title="${escapeHTML(hash)}">${escapeHTML(shortHash(hash))}</strong>
      <button class="secondary tiny" type="button" data-copy="hash" data-value="${escapeAttr(hash)}">Copy</button>
    </div>
  `;
}

function summaryTile(label, value) {
  return `
    <div class="summary-tile">
      <span>${escapeHTML(label)}</span>
      <strong>${escapeHTML(value)}</strong>
    </div>
  `;
}

function usageItem(label, value) {
  return `
    <div>
      <span>${escapeHTML(label)}</span>
      <strong>${escapeHTML(value)}</strong>
    </div>
  `;
}

function resetRunView(status) {
  state.lastResult = null;
  state.selectedStep = null;
  state.activeTab = "overview";
  state.streamFilter = "all";
  state.traceFilter = "";
  el.traceSearch.value = "";
  setStatus(status);
  el.runTitle.textContent = status === "running" ? "Running agent..." : "Inspect one Floret turn";
  el.summaryText.textContent = status === "running" ? "Waiting for engine events and provider stream." : "Run an agent turn to inspect state transitions and provider interaction.";
  ["metricStatus", "metricSteps", "metricRequests", "metricTools", "metricTokens", "metricDuration"].forEach((key) => {
    el[key].textContent = "-";
  });
  el.eventCount.textContent = "0 events";
  el.transitions.textContent = "No transitions yet.";
  el.transitions.className = "flow-list empty";
  el.stepList.textContent = "No provider steps yet.";
  el.stepList.className = "step-list empty";
  el.sessionMessages.textContent = "No session messages yet.";
  el.sessionMessages.className = "message-list empty";
  el.finalOutput.textContent = "The agent output will appear here.";
  el.outputMeta.textContent = "waiting";
  el.rawJson.textContent = "{}";
  renderFlowHealth(null);
  renderTabs();
  renderStepDetail();
}

function renderError(message) {
  setStatus("error");
  el.summaryText.textContent = message;
  el.finalOutput.textContent = message;
  el.outputMeta.textContent = "error";
}

function stepModels(result) {
  if (!result) return [];
  const requests = result.observation?.provider_requests || [];
  const providerEvents = result.observation?.provider_events || [];
  const engineEvents = result.events || [];
  const transitions = result.observation?.transitions || [];
  return requests.map((request) => {
    const step = request.step || request.Step || 0;
    request = {
      ...request,
      messages: request.messages || request.Messages || [],
      tools: request.tools || request.Tools || [],
      raw_segments: request.raw_segments || request.RawSegments || [],
      cache_summary: request.cache_summary || request.CacheSummary || {},
    };
    const pEvents = providerEvents.filter((event) => (event.step || event.Step) === step);
    const eEvents = engineEvents.filter((event) => (event.step || event.Step) === step);
    const tEvents = transitions.filter((item) => item.step === step);
    const usage = sumUsage(pEvents);
    const toolCallItems = pEvents.flatMap(toolCalls);
    const toolResultItems = eEvents.filter((event) => event.type === "tool_result" || event.Type === "tool_result");
    const verifier = verifyStepFlow(request, pEvents, eEvents, result);
    const decision = stepDecision(step, pEvents, eEvents, result, toolCallItems, toolResultItems);
    const diagnostics = [...verifier.diagnostics, ...stepDiagnostics(request, pEvents, eEvents, tEvents, result)];
    const severity = diagnostics.some((item) => item.severity === "bad") ? "bad" : diagnostics.some((item) => item.severity === "warn") ? "warn" : "ok";
    return {
      step,
      request,
      provider: request.provider || request.Provider || "-",
      model: request.model || request.Model || "-",
      providerEvents: pEvents,
      engineEvents: eEvents,
      transitions: tEvents,
      usage,
      toolCalls: toolCallItems,
      toolResults: toolResultItems,
      cache: request.cache_summary || request.CacheSummary || {},
      sequence: verifier.sequence,
      diagnostics,
      decision,
      severity,
      label: decision.shortLabel || (severity === "ok" ? "ok" : severity === "warn" ? "review" : "attention"),
    };
  });
}

function selectedStepModel() {
  return stepModels(state.lastResult).find((step) => step.step === state.selectedStep) || null;
}

function stepSequence(step) {
  return step.sequence || [];
}

function runDiagnostics(result) {
  const diagnostics = [];
  const requests = result.observation?.provider_requests || [];
  const providerEvents = result.observation?.provider_events || [];
  const engineEvents = result.events || [];
  if (result.status && result.status !== "completed") {
    diagnostics.push({
      severity: result.status === "waiting" ? "warn" : "bad",
      title: `Run ended as ${result.status}`,
      detail: result.error || result.summary || "The terminal state needs review.",
    });
  }
  if (requests.length === 0) {
    diagnostics.push({ severity: "bad", title: "No provider request captured", detail: "The agent loop did not reach a model call." });
  }
  providerEvents.forEach((event) => {
    const type = eventType(event);
    const reason = event.reason || event.Reason || "";
    if (type === "empty") diagnostics.push({ severity: "warn", step: event.step, title: "Provider returned empty output", detail: event.reason || "" });
    if (type === "truncated") diagnostics.push({ severity: "bad", step: event.step, title: "Provider output was truncated", detail: event.reason || "" });
    if (type === "done" && ["content_filter", "error", "cancelled", "canceled"].includes(reason)) {
      diagnostics.push({ severity: "bad", step: event.step, title: `Provider finish ${reason}`, detail: "The provider terminal reason requires engine recovery or failure handling." });
    }
  });
  engineEvents.forEach((event) => {
    const type = event.type || event.Type;
    if (type === "budget_exceeded") diagnostics.push({ severity: "bad", step: event.step, title: "Budget exceeded", detail: event.message || "" });
    if (type === "provider_retry") diagnostics.push({ severity: "warn", step: event.step, title: "Provider retry", detail: event.message || "" });
    if (type === "context_compact") diagnostics.push({ severity: "info", step: event.step, title: "Context compacted", detail: "The memory manager compacted context during this run." });
    if (event.err || event.Err) diagnostics.push({ severity: "bad", step: event.step, title: `${type} error`, detail: event.err || event.Err });
  });
  stepModels(result).forEach((step) => {
    step.diagnostics.forEach((item) => diagnostics.push(item));
  });
  return diagnostics;
}

function verifyStepFlow(request, providerEvents, engineEvents, result) {
  const step = request.step || request.Step || 0;
  const diagnostics = [];
  const sequence = buildStepTimeline(request, providerEvents, engineEvents);
  const types = sequence.map((item) => item.type);
  const hasProviderRequest = types.includes("provider_request");
  const hasStream = providerEvents.length > 0;
  const hasProviderDone = providerEvents.some((event) => eventType(event) === "done");
  const hasProviderTerminal = providerEvents.some((event) => ["done", "empty", "truncated"].includes(eventType(event)));
  const calls = providerEvents.flatMap(toolCalls);
  const signalCalls = calls.filter(isSignalToolCall);
  const normalCalls = calls.filter((call) => !isSignalToolCall(call));
  const toolCallEvents = engineEvents.filter((event) => eventKind(event) === "tool_call");
  const toolResultEvents = engineEvents.filter((event) => eventKind(event) === "tool_result");
  const stepEndEvents = engineEvents.filter((event) => eventKind(event) === "step_end");
  const runEndEvents = engineEvents.filter((event) => eventKind(event) === "run_end");
  const retryEvents = engineEvents.filter((event) => eventKind(event) === "provider_retry");
  const terminalSignalStep = signalCalls.length > 0 && runEndEvents.length > 0;

  if (!hasProviderRequest) {
    diagnostics.push({ severity: "bad", step, title: "Missing provider_request event", detail: "A provider request was observed, but the engine trace did not emit provider_request for this step." });
  }
  if (!hasStream) {
    diagnostics.push({ severity: "warn", step, title: "Missing provider stream", detail: "Expected at least one provider stream event after provider_request." });
  }
  if (hasStream && !hasProviderTerminal) {
    diagnostics.push({ severity: "warn", step, title: "Provider stream has no terminal event", detail: "Expected done, empty, or truncated before the engine leaves the step." });
  }
  if (hasStream && !hasProviderDone && !providerEvents.some((event) => ["empty", "truncated"].includes(eventType(event)))) {
    diagnostics.push({ severity: "warn", step, title: "No provider done event", detail: "The provider stream ended without an explicit done event." });
  }
  if (normalCalls.length > 0 && toolCallEvents.length < normalCalls.length) {
    diagnostics.push({ severity: "bad", step, title: "Missing tool_call event", detail: `Expected ${normalCalls.length} engine tool_call event(s), captured ${toolCallEvents.length}.` });
  }
  if (normalCalls.length > 0 && toolResultEvents.length < normalCalls.length) {
    diagnostics.push({ severity: "bad", step, title: "Missing tool_result event", detail: `Expected ${normalCalls.length} tool_result event(s), captured ${toolResultEvents.length}.` });
  }
  const interruptCalls = signalCalls.filter((call) => isInterruptToolCall(call));
  if (interruptCalls.length > 0 && toolResultEvents.length > 0) {
    diagnostics.push({ severity: "warn", step, title: "Interrupt produced tool_result", detail: "ask_user should interrupt without normal tool execution." });
  }
  if (normalCalls.length > 0 && stepEndEvents.length === 0 && step !== lastStepNumber(result)) {
    diagnostics.push({ severity: "bad", step, title: "Missing step_end", detail: "A non-terminal step with normal tool execution should emit step_end before the next provider request." });
  }
  if (normalCalls.length === 0 && signalCalls.length === 0 && stepEndEvents.length === 0 && runEndEvents.length === 0 && retryEvents.length === 0) {
    diagnostics.push({ severity: "warn", step, title: "Step has no closing event", detail: "Expected step_end, run_end, or provider_retry after the provider stream." });
  }
  if (terminalSignalStep && stepEndEvents.length > 0) {
    diagnostics.push({ severity: "info", step, title: "Signal step also emitted step_end", detail: "Signal tools normally finish through run_end without a step_end event." });
  }
  const orderIssue = firstOrderIssue(sequence);
  if (orderIssue) diagnostics.push(orderIssue);

  return { diagnostics, sequence };
}

function buildStepTimeline(request, providerEvents, engineEvents) {
  const step = request.step || request.Step || 0;
  const items = [];
  engineEvents.forEach((event, index) => {
    const type = eventKind(event);
    items.push({
      kind: "engine",
      type,
      step,
      order: timeOrder(event.timestamp || event.Timestamp, 20 + index),
      severity: engineEventSeverity(type, event),
      title: `engine ${type}`,
      detail: engineEventDetail(event),
    });
  });
  if (request.observed_at || request.ObservedAt) {
    items.push({
      kind: "engine",
      type: "provider_request",
      step,
      order: timeOrder(request.observed_at || request.ObservedAt, 10),
      severity: "info",
      title: "engine provider_request",
      detail: `${request.messages?.length || 0} messages · ${request.raw_segments?.length || 0} raw segments`,
    });
  }
  providerEvents.forEach((event, index) => {
    const type = eventType(event);
    items.push({
      kind: "provider",
      type: `provider_${type}`,
      step,
      order: timeOrder(event.observed_at || event.ObservedAt, 100 + index),
      severity: eventSeverity(type, event),
      title: `provider ${type}`,
      detail: providerEventSummary(event, index),
    });
  });
  return items
    .sort((a, b) => a.order - b.order)
    .filter((item, index, all) => !isDuplicateProviderRequest(item, all[index - 1]));
}

function firstOrderIssue(sequence) {
  const index = (type) => sequence.findIndex((item) => item.type === type || item.type === `provider_${type}`);
  const providerRequest = index("provider_request");
  const firstProviderEvent = sequence.findIndex((item) => item.kind === "provider");
  const firstToolCall = index("tool_call");
  const firstToolResult = index("tool_result");
  const stepEnd = index("step_end");
  const runEnd = index("run_end");
  const step = sequence[0]?.step || 0;
  if (providerRequest >= 0 && firstProviderEvent >= 0 && firstProviderEvent < providerRequest) {
    return { severity: "bad", step, title: "Provider stream before provider_request", detail: "Captured provider stream ordering does not match the expected agent loop." };
  }
  if (firstToolResult >= 0 && firstToolCall >= 0 && firstToolResult < firstToolCall) {
    return { severity: "bad", step, title: "tool_result before tool_call", detail: "Engine event order is inconsistent for this step." };
  }
  if (stepEnd >= 0 && firstProviderEvent >= 0 && stepEnd < firstProviderEvent) {
    return { severity: "bad", step, title: "step_end before provider stream", detail: "Expected provider output before step_end." };
  }
  if (runEnd >= 0 && providerRequest >= 0 && runEnd < providerRequest) {
    return { severity: "bad", step, title: "run_end before provider_request", detail: "Terminal run event appeared before the step request." };
  }
  return null;
}

function isDuplicateProviderRequest(item, previous) {
  return item?.type === "provider_request" && previous?.type === "provider_request" && Math.abs(item.order - previous.order) < 3;
}

function stepDiagnostics(request, providerEvents, engineEvents, transitions, result) {
  const diagnostics = [];
  const step = request.step || request.Step || 0;
  providerEvents.forEach((event) => {
    const type = eventType(event);
    const reason = event.reason || event.Reason || "";
    if (type === "empty") diagnostics.push({ severity: "warn", step, title: "Empty provider event", detail: event.reason || "" });
    if (type === "truncated") diagnostics.push({ severity: "bad", step, title: "Truncated provider event", detail: event.reason || "" });
    if (type === "done" && ["content_filter", "error", "cancelled", "canceled"].includes(reason)) {
      diagnostics.push({ severity: "bad", step, title: `Terminal provider reason ${reason}`, detail: "Engine should surface this finish reason explicitly." });
    }
  });
  if ((request.raw_segments || []).length === 0) {
    diagnostics.push({ severity: "info", step, title: "No raw prompt segments", detail: "Prompt cache ledger data was not present on this request." });
  }
  if (result.status !== "completed" && step === lastStepNumber(result)) {
    diagnostics.push({ severity: "bad", step, title: `Terminal state ${result.status}`, detail: result.error || result.summary || "" });
  }
  if (transitions.length === 0) {
    diagnostics.push({ severity: "info", step, title: "No state transition rows", detail: "This step has no derived transition entries." });
  }
  return diagnostics;
}

function stepDecision(step, providerEvents, engineEvents, result, toolCallItems, toolResultItems) {
  const stepEnd = lastEvent(engineEvents, "step_end");
  const runEnd = lastEvent(engineEvents, "run_end");
  const providerFinish = lastEvent(engineEvents, "provider_finish");
  const contextContinue = lastEvent(engineEvents, "context_continue");
  const providerDone = [...providerEvents].reverse().find((event) => eventType(event) === "done");
  const finish = eventValue(stepEnd, "finish_reason", "FinishReason") ||
    eventValue(runEnd, "finish_reason", "FinishReason") ||
    eventValue(providerFinish, "finish_reason", "FinishReason") ||
    eventValue(providerDone, "reason", "Reason") ||
    "";
  const rawFinish = eventValue(stepEnd, "raw_finish_reason", "RawFinishReason") ||
    eventValue(runEnd, "raw_finish_reason", "RawFinishReason") ||
    eventValue(providerFinish, "raw_finish_reason", "RawFinishReason") ||
    eventValue(providerDone, "reason", "Reason") ||
    "";
  const completion = eventValue(stepEnd, "completion_reason", "CompletionReason") || eventValue(runEnd, "completion_reason", "CompletionReason") || "";
  const continuation = eventValue(stepEnd, "continuation_reason", "ContinuationReason") ||
    eventValue(contextContinue, "continuation_reason", "ContinuationReason") ||
    eventValue(runEnd, "continuation_reason", "ContinuationReason") ||
    "";
  const inferred = Boolean(stepEnd?.finish_inferred || stepEnd?.FinishInferred || runEnd?.finish_inferred || runEnd?.FinishInferred || providerFinish?.finish_inferred || providerFinish?.FinishInferred);
  const isLast = step === lastStepNumber(result);
  const status = isLast ? result.status : "";
  if (completion === "natural_stop") {
    return {
      title: "Natural stop",
      detail: "Assistant text reached a terminal provider finish with no pending tool calls.",
      finish,
      rawFinish,
      completion,
      continuation,
      inferred,
      severity: "ok",
      shortLabel: "natural",
      terminal: finish ? `finish=${finish}` : "",
    };
  }
  if (completion) {
    return { title: `Completed by ${completion}`, detail: "The engine completed this step through an explicit completion policy.", finish, rawFinish, completion, continuation, inferred, severity: "ok", shortLabel: "done" };
  }
  if (continuation) {
    return {
      title: `Continue: ${continuation}`,
      detail: continuationDetail(continuation),
      finish,
      rawFinish,
      completion,
      continuation,
      inferred,
      severity: continuation === "provider_truncated" ? "warn" : "info",
      shortLabel: "continue",
    };
  }
  if (status === "waiting") {
    return { title: "Waiting for user", detail: "The model called ask_user, so the turn interrupted before normal tool execution.", finish, rawFinish, completion, continuation, inferred, severity: "warn", shortLabel: "waiting" };
  }
  if (status === "failed" || status === "error") {
    return { title: "Failed", detail: result.error || "The engine ended with a failure state.", finish, rawFinish, completion, continuation, inferred, severity: "bad", shortLabel: "failed" };
  }
  if (status === "cancelled") {
    return { title: "Cancelled", detail: result.error || "The run was cancelled or timed out.", finish, rawFinish, completion, continuation, inferred, severity: "bad", shortLabel: "cancelled" };
  }
  if (toolCallItems.length) {
    const missingResults = Math.max(0, toolCallItems.length - toolResultItems.length);
    return {
      title: missingResults ? "Tools pending" : "Tool results returned",
      detail: missingResults ? `${missingResults} tool call(s) still need results or a follow-up model step.` : "Normal tool results were fed back to the model in the next request.",
      finish,
      rawFinish,
      completion,
      continuation,
      inferred,
      severity: missingResults ? "warn" : "ok",
      shortLabel: "tools",
    };
  }
  if (finish) {
    return { title: `Provider finish: ${finish}`, detail: "Provider returned a terminal reason; inspect engine events to see the resulting state.", finish, rawFinish, completion, continuation, inferred, severity: finishSeverity(finish), shortLabel: finish };
  }
  return { title: "Observed", detail: "No explicit completion or continuation decision was captured for this step.", finish, rawFinish, completion, continuation, inferred, severity: "info", shortLabel: "observed" };
}

function runDecisionSummary(result) {
  const steps = stepModels(result);
  const last = steps[steps.length - 1];
  if (!last) {
    return { title: result.status || "-", detail: "No provider step was captured.", severity: result.status === "completed" ? "ok" : "bad", terminal: result.error || "" };
  }
  if (result.status === "completed") {
    const detail = [
      last.decision.finish ? `finish=${last.decision.finish}` : "",
      last.decision.completion ? `completion=${last.decision.completion}` : "",
      last.decision.continuation ? `continue=${last.decision.continuation}` : "no continuation",
    ].filter(Boolean).join(" · ");
    return {
      title: last.decision.title,
      detail: detail || result.summary || "Run completed.",
      severity: "ok",
      terminal: detail || result.summary || "",
    };
  }
  return {
    title: result.status || last.decision.title,
    detail: result.error || last.decision.detail || result.summary || "",
    severity: result.status === "waiting" ? "warn" : "bad",
    terminal: last.decision.finish ? `finish=${last.decision.finish}` : "",
  };
}

function lastEvent(events, type) {
  for (let i = events.length - 1; i >= 0; i -= 1) {
    if (eventKind(events[i]) === type) return events[i];
  }
  return null;
}

function eventValue(event, lower, upper) {
  return event?.[lower] || event?.[upper] || "";
}

function continuationDetail(reason) {
  if (reason === "tool_results") return "The step produced normal tool results, so the engine continued with another provider request.";
  if (reason === "compaction") return "The context was compacted before the next provider request.";
  if (reason === "provider_truncated") return "The provider output hit a length limit; Floret compacted and retried within the continuation budget.";
  if (reason === "retry_empty") return "The provider returned no usable output, so Floret retried the request.";
  if (reason === "hook") return "A host stop hook requested an auditable continuation prompt.";
  if (reason === "no_progress") return "The step did not produce enough progress to finish yet.";
  return "The engine continued because the step left pending work.";
}

function finishSeverity(reason) {
  if (["content_filter", "error", "cancelled", "canceled", "length"].includes(reason)) return "bad";
  if (reason === "tool_calls") return "info";
  return "ok";
}

function aggregateCache(result) {
  const out = { reusedSegments: 0, newSegments: 0, cacheReadTokens: 0, cacheWriteTokens: 0 };
  (result?.observation?.provider_requests || []).forEach((request) => {
    const cache = request.cache_summary || {};
    out.reusedSegments += cache.reused_segments || 0;
    out.newSegments += cache.new_segments || 0;
    out.cacheReadTokens += cache.cache_read_tokens || 0;
    out.cacheWriteTokens += cache.cache_write_tokens || 0;
  });
  return out;
}

function lastStepNumber(result) {
  const steps = (result.observation?.provider_requests || []).map((request) => request.step || request.Step || 0);
  return steps.length ? Math.max(...steps) : 0;
}

function sumUsage(events) {
  return events.reduce((sum, event) => addUsage(sum, event.usage || event.Usage || {}), {});
}

function addUsage(a, b) {
  return {
    input_tokens: usageValue(a, "input_tokens", "InputTokens") + usageValue(b, "input_tokens", "InputTokens"),
    output_tokens: usageValue(a, "output_tokens", "OutputTokens") + usageValue(b, "output_tokens", "OutputTokens"),
    reasoning_tokens: usageValue(a, "reasoning_tokens", "ReasoningTokens") + usageValue(b, "reasoning_tokens", "ReasoningTokens"),
    cache_read_tokens: usageValue(a, "cache_read_tokens", "CacheReadTokens") + usageValue(b, "cache_read_tokens", "CacheReadTokens"),
    cache_write_tokens: usageValue(a, "cache_write_tokens", "CacheWriteTokens") + usageValue(b, "cache_write_tokens", "CacheWriteTokens"),
    total_tokens: usageValue(a, "total_tokens", "TotalTokens") + usageValue(b, "total_tokens", "TotalTokens"),
    cost_usd: usageValue(a, "cost_usd", "CostUSD") + usageValue(b, "cost_usd", "CostUSD"),
  };
}

function toolCalls(event) {
  return event.tool_calls || event.ToolCalls || [];
}

function eventType(event) {
  return event.type || event.Type || "event";
}

function eventKind(event) {
  return event.type || event.Type || "event";
}

function isSignalToolCall(call) {
  const name = call.name || call.Name || "";
  return name === "ask_user";
}

function isInterruptToolCall(call) {
  const name = call.name || call.Name || "";
  return name === "ask_user";
}

function timeOrder(value, fallback) {
  if (!value) return fallback;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return fallback;
  return date.getTime() + fallback / 1000;
}

function providerEventSummary(event, index) {
  const usage = event.usage || event.Usage || {};
  const calls = toolCalls(event);
  return [
    `#${index + 1}`,
    event.text || event.Text || "",
    event.reason || event.Reason || "",
    calls.length ? `${calls.length} tool call(s)` : "",
    totalTokens(usage) ? `${totalTokens(usage)} tokens` : "",
  ].filter(Boolean).join(" · ");
}

function eventSeverity(type, event = {}) {
  const reason = event.reason || event.Reason || "";
  if (type === "truncated") return "bad";
  if (type === "empty") return "warn";
  if (type === "done" && ["content_filter", "error", "cancelled", "canceled", "length"].includes(reason)) return "bad";
  if (type === "usage" || type === "done") return "ok";
  return "info";
}

function engineEventSeverity(type, event) {
  if (type === "budget_exceeded" || event.err || event.Err) return "bad";
  if (type === "provider_finish") return finishSeverity(event.finish_reason || event.FinishReason || "");
  if (type === "provider_retry") return "warn";
  if (type === "tool_result" || type === "run_end" || type === "step_end") return "ok";
  return "info";
}

function engineEventDetail(event) {
  const type = eventKind(event);
  if (type === "provider_finish") {
    return [
      event.finish_reason || event.FinishReason ? `finish=${event.finish_reason || event.FinishReason}` : "",
      event.raw_finish_reason || event.RawFinishReason ? `raw=${event.raw_finish_reason || event.RawFinishReason}` : "",
      event.finish_inferred || event.FinishInferred ? "inferred" : "",
    ].filter(Boolean).join(" · ");
  }
  if (type === "step_end" || type === "run_end") {
    return [
      event.completion_reason || event.CompletionReason ? `completion=${event.completion_reason || event.CompletionReason}` : "",
      event.continuation_reason || event.ContinuationReason ? `continue=${event.continuation_reason || event.ContinuationReason}` : "",
      event.finish_reason || event.FinishReason ? `finish=${event.finish_reason || event.FinishReason}` : "",
      event.err || event.Err || "",
    ].filter(Boolean).join(" · ");
  }
  if (type === "context_continue") {
    return [event.continuation_reason || event.ContinuationReason || "", event.result || event.Result || "", event.message || event.Message || ""].filter(Boolean).join(" · ");
  }
  return event.message || event.Message || event.tool_name || event.ToolName || event.err || event.Err || "";
}

function reuseRatio(cache) {
  const reused = cache.reused_segments || 0;
  const fresh = cache.new_segments || 0;
  const total = reused + fresh;
  if (!total) return "0%";
  return `${Math.round((reused / total) * 100)}%`;
}

function severityLabel(level) {
  if (level === "bad") return "Needs attention";
  if (level === "warn") return "Review suggested";
  return "Informational";
}

function eventTotal(result) {
  return (result?.events?.length || 0) + (result?.observation?.provider_events?.length || 0) + (result?.observation?.transitions?.length || 0);
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
  const explicit = usageValue(usage, "total_tokens", "TotalTokens");
  if (explicit) return explicit;
  return ["input_tokens", "output_tokens", "reasoning_tokens", "cache_read_tokens", "cache_write_tokens"].reduce((sum, key) => sum + (usage[key] || 0), 0) ||
    ["InputTokens", "OutputTokens", "ReasoningTokens", "CacheReadTokens", "CacheWriteTokens"].reduce((sum, key) => sum + (usage[key] || 0), 0);
}

function usageValue(usage, lower, upper) {
  return Number(usage?.[lower] || usage?.[upper] || 0);
}

function shortHash(value) {
  value = String(value || "");
  return value.length > 12 ? value.slice(0, 12) : value;
}

function previewText(value, limit) {
  value = String(value || "").replace(/\s+/g, " ").trim();
  if (!value) return "(empty)";
  if (value.length <= limit) return value;
  return `${value.slice(0, Math.max(0, limit - 1))}...`;
}

function transitionClass(status) {
  if (["completed", "step_finished", "tool_result_received"].includes(status)) return "ok";
  if (["failed", "error", "budget_exceeded", "cancelled"].includes(status)) return "bad";
  if (["waiting", "provider_waiting", "tool_calling"].includes(status)) return "active";
  return "info";
}

function formatDuration(ms) {
  if (!ms) return "0 ms";
  if (ms < 1000) return `${ms} ms`;
  return `${(ms / 1000).toFixed(1)} s`;
}

function formatCost(value) {
  const n = Number(value || 0);
  if (!n) return "$0";
  return `$${n.toFixed(n < 0.01 ? 5 : 3)}`;
}

function formatTime(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
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

async function copyRaw() {
  await navigator.clipboard.writeText(el.rawJson.textContent);
  flashCopied(el.copyRaw, "Copy JSON");
}

async function copyDynamic(button) {
  const text = copyPayload(button);
  if (!text) return;
  await navigator.clipboard.writeText(text);
  flashCopied(button, button.dataset.originalLabel || button.textContent || "Copy");
}

function copyPayload(button) {
  const kind = button.dataset.copy;
  if (kind === "hash") return button.dataset.value || "";
  const step = Number(button.dataset.step);
  const model = stepModels(state.lastResult).find((item) => item.step === step);
  if (!model) return "";
  if (kind === "step-json") {
    return JSON.stringify({
      request: model.request,
      provider_events: model.providerEvents,
      engine_events: model.engineEvents,
      transitions: model.transitions,
    }, null, 2);
  }
  if (kind === "segment-raw") {
    const index = Number(button.dataset.segmentIndex);
    const segment = model.request.raw_segments?.[index];
    return segment?.raw || segment?.raw_preview || "";
  }
  if (kind === "event-json") {
    const index = Number(button.dataset.eventIndex);
    return JSON.stringify(model.providerEvents[index] || {}, null, 2);
  }
  return "";
}

function flashCopied(button, fallback) {
  if (!button.dataset.originalLabel) button.dataset.originalLabel = fallback || button.textContent || "Copy";
  button.textContent = "Copied";
  setTimeout(() => {
    button.textContent = button.dataset.originalLabel;
  }, 1200);
}

function collectMatches(value, query) {
  const matches = [];
  const needle = query.toLowerCase();
  walkJSON(value, "result", needle, matches);
  return matches.slice(0, 200);
}

function walkJSON(value, path, needle, matches) {
  if (matches.length >= 200) return;
  if (value == null) return;
  if (typeof value !== "object") {
    const text = String(value);
    if (text.toLowerCase().includes(needle)) matches.push({ path, value: text });
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((item, index) => walkJSON(item, `${path}[${index}]`, needle, matches));
    return;
  }
  Object.entries(value).forEach(([key, child]) => {
    const childPath = `${path}.${key}`;
    if (key.toLowerCase().includes(needle)) {
      matches.push({ path: childPath, value: summarizeJSON(child) });
    }
    walkJSON(child, childPath, needle, matches);
  });
}

function summarizeJSON(value) {
  if (value == null) return null;
  if (typeof value !== "object") return String(value);
  const text = JSON.stringify(value);
  return text.length > 500 ? `${text.slice(0, 500)}...` : text;
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
