import { clone, defaultProfile, escapeHTML, profileLabel, providerCatalog, state } from "../state.js";

export function renderSettings() {
  const draft = state.settingsDraft;
  const profiles = draft?.profiles?.length ? draft.profiles : state.config?.profiles?.length ? state.config.profiles : [defaultProfile()];
  const activeID = draft?.active_profile_id || state.config?.active_profile_id || profiles[0].id;
  const active = profiles.find((profile) => profile.id === activeID) || profiles[0];
  const providers = providerCatalog();
  const searchProvider = draft?.search_provider || state.config?.search_provider || {};
  const isSaving = state.action === "save-settings";
  const profileKeyDraft = active.api_key || "";
  const searchKeyDraft = searchProvider.api_key || "";
  const webSearch = normalizeWebSearch(active.web_search);
  const searchMode = webSearchMode(webSearch);
  const wireShapes = state.config?.search_wire_shapes || [];
  return `
    <section class="settings-page">
      <header class="settings-head">
        <div>
          <h1>Settings</h1>
          <p class="muted">Provider profiles and local quality checks live here, away from the session workflow.</p>
        </div>
      </header>
      <div class="settings-grid">
        <form class="profile-card" data-settings-form data-current-profile-id="${escapeHTML(activeID)}">
          <h2>Model Configuration</h2>
          <label class="field">
            <span>Saved configuration</span>
            <select name="active_profile_id" ${isSaving ? "disabled" : ""}>
              ${profiles.map((profile) => `<option value="${escapeHTML(profile.id)}" ${profile.id === activeID ? "selected" : ""}>${escapeHTML(profileLabel(profile))}</option>`).join("")}
            </select>
          </label>
          <label class="field"><span>Name</span><input name="name" value="${escapeHTML(active.name || "")}" ${isSaving ? "disabled" : ""} /></label>
          <label class="field">
            <span>Provider</span>
            <select name="provider" ${isSaving ? "disabled" : ""}>
              ${providers.map((provider) => `<option value="${escapeHTML(provider.id)}" ${provider.id === active.provider ? "selected" : ""}>${escapeHTML(provider.name || provider.id)} · ${escapeHTML(provider.id)}</option>`).join("")}
              ${providers.some((provider) => provider.id === active.provider) ? "" : `<option value="${escapeHTML(active.provider || "openai-compatible")}" selected>${escapeHTML(active.provider || "custom")}</option>`}
            </select>
          </label>
          <label class="field"><span>Model</span><input name="model" value="${escapeHTML(active.model || "")}" ${isSaving ? "disabled" : ""} /></label>
          <label class="field"><span>Base URL</span><input name="base_url" value="${escapeHTML(active.base_url || "")}" placeholder="https://api.example.com/v1" ${isSaving ? "disabled" : ""} /></label>
          <label class="field"><span>API key</span><input name="api_key" type="password" value="${escapeHTML(profileKeyDraft)}" placeholder="${active.api_key_set ? "saved key retained if empty" : "optional"}" ${isSaving ? "disabled" : ""} /></label>
          <label class="field"><span>Fake response</span><input name="fake_response" value="${escapeHTML(active.fake_response || "")}" ${isSaving ? "disabled" : ""} /></label>
          <div class="settings-divider"></div>
          <h2>Web Search Capability</h2>
          <p class="muted">web_search is configured per provider profile. Provider-hosted search is preferred when enabled; Brave client search is shown only when selected here.</p>
          <label class="field">
            <span>Search mode</span>
            <select name="search_mode" ${isSaving ? "disabled" : ""}>
              <option value="provider_hosted" ${searchMode === "provider_hosted" ? "selected" : ""}>Provider-hosted web search</option>
              <option value="client" ${searchMode === "client" ? "selected" : ""}>Client search via Brave</option>
              <option value="disabled" ${searchMode === "disabled" ? "selected" : ""}>Disabled</option>
            </select>
          </label>
          <label class="field">
            <span>Hosted wire shape</span>
            <select name="search_wire_shape" ${isSaving || searchMode !== "provider_hosted" ? "disabled" : ""}>
              ${wireShapeOptions(wireShapes, webSearch.provider_hosted?.wire_shape)}
            </select>
          </label>
          <div class="tool-boundary-grid">
            <div>
              <strong>Active source</strong>
              <span>${escapeHTML(searchModeLabel(searchMode, webSearch.provider_hosted?.wire_shape))}</span>
            </div>
            <div>
              <strong>Brave API key</strong>
              <span>${state.config?.search_provider?.api_key_set ? "saved" : "not saved"}</span>
            </div>
            <div>
              <strong>Local client</strong>
              <span>${searchMode === "client" ? "enabled" : "not exposed"}</span>
            </div>
          </div>
          <label class="field"><span>Brave API key</span><input name="search_api_key" type="password" value="${escapeHTML(searchKeyDraft)}" placeholder="${state.config?.search_provider?.api_key_set ? "saved key retained if empty" : "required only for client search"}" ${isSaving || searchMode !== "client" ? "disabled" : ""} /></label>
          <label class="field"><span>Brave endpoint override</span><input name="search_endpoint" value="${escapeHTML(searchProvider.endpoint || "")}" placeholder="default Brave Web Search endpoint" ${isSaving || searchMode !== "client" ? "disabled" : ""} /></label>
          <div class="form-actions">
            <button type="button" data-duplicate-profile ${isSaving ? "disabled" : ""}>Duplicate</button>
            <button class="primary ${isSaving ? "is-pending" : ""}" type="submit" ${isSaving ? "disabled" : ""}>${isSaving ? "Saving..." : "Save .env.local"}</button>
          </div>
        </form>
        <section class="check-card">
          <h2>Quality Checks</h2>
          <p class="muted">Run local checks without leaving the console.</p>
          <div class="form-actions">
            ${renderCheckButton("unit", "go test")}
            ${renderCheckButton("race", "race")}
            ${renderCheckButton("eval-demo", "eval demo")}
          </div>
          <div class="settings-divider"></div>
          <h2>Tool Scenario Checks</h2>
          <p class="muted">Saved repeatable scenarios for multi-tool, multi-turn agent behavior. Mock scenarios are deterministic; live scenarios use the saved active provider profile.</p>
          <div class="tool-boundary-grid">
            <div>
              <strong>Mock suite</strong>
              <span>read/grep/shell, mutation recovery, web_search + web_fetch</span>
            </div>
            <div>
              <strong>Live profile</strong>
              <span>${escapeHTML(profileLabel(active))}</span>
            </div>
            <div>
              <strong>Search mode</strong>
              <span>${escapeHTML(searchModeLabel(searchMode, webSearch.provider_hosted?.wire_shape))}</span>
            </div>
          </div>
          <div class="form-actions">
            ${renderCheckButton("tool-scenarios", "tool scenarios")}
            ${renderCheckButton("live-tool-scenarios", "live tool scenarios")}
          </div>
          <pre id="checkOutput" class="code-block">${escapeHTML(state.checkResult || "No check has run in this browser session.")}</pre>
        </section>
      </div>
    </section>
  `;
}

function renderCheckButton(target, label) {
  const isRunning = state.action === "run-check" && state.actionTarget === target;
  const anyRunning = state.action === "run-check";
  return `<button type="button" class="${isRunning ? "is-pending" : ""}" data-run-check="${escapeHTML(target)}" ${anyRunning ? "disabled" : ""}>${isRunning ? "Running..." : escapeHTML(label)}</button>`;
}

export function bindSettings(root, handlers) {
  const form = root.querySelector("[data-settings-form]");
  const persistDraft = () => {
    state.settingsDraft = readSettingsDraft(form);
  };
  form?.addEventListener("input", (event) => {
    if (event.isComposing) return;
    persistDraft();
  });
  form?.addEventListener("change", (event) => {
    if (event.target === form.elements.active_profile_id || event.target === form.elements.provider) return;
    persistDraft();
    if (event.target === form.elements.search_mode) {
      handlers.onSwitchSearchMode?.();
    }
  });
  form?.elements.active_profile_id?.addEventListener("change", () => {
    state.settingsDraft = readSettingsDraft(form, form.dataset.currentProfileId || "");
    state.settingsDraft.active_profile_id = form.elements.active_profile_id.value;
    handlers.onSwitchProfile(form.elements.active_profile_id.value);
  });
  form?.elements.provider?.addEventListener("change", () => {
    state.settingsDraft = readSettingsDraft(form, form.dataset.currentProfileId || "");
    handlers.onSwitchProvider(form.elements.provider.value);
  });
  form?.addEventListener("submit", (event) => {
    event.preventDefault();
    handlers.onSave(readSettingsDraft(form));
  });
  root.querySelector("[data-duplicate-profile]")?.addEventListener("click", () => handlers.onDuplicate());
  root.querySelectorAll("[data-run-check]").forEach((button) => {
    button.addEventListener("click", () => handlers.onRunCheck(button.dataset.runCheck || "unit"));
  });
}

export function readSettingsDraft(form, activeIDOverride = "") {
  const profiles = clone(state.settingsDraft?.profiles || state.config?.profiles || [defaultProfile()]);
  const activeID = activeIDOverride || form.elements.active_profile_id.value;
  const index = profiles.findIndex((profile) => profile.id === activeID);
  const current = index >= 0 ? profiles[index] : defaultProfile();
  const next = {
    ...current,
    id: current.id || activeID || "profile",
    name: form.elements.name.value,
    provider: form.elements.provider.value,
    model: form.elements.model.value,
    base_url: form.elements.base_url.value,
    api_key: form.elements.api_key.value,
    api_key_set: current.api_key_set,
    fake_response: form.elements.fake_response.value,
    web_search: readWebSearchCapability(form, current),
  };
  if (index >= 0) profiles[index] = next;
  return {
    active_profile_id: next.id,
    profiles,
    search_provider: {
      provider: "brave",
      api_key: form.elements.search_api_key?.value || "",
      endpoint: form.elements.search_endpoint?.value || "",
    },
  };
}

function normalizeWebSearch(value) {
  const next = clone(value || {});
  next.provider_hosted = next.provider_hosted || {};
  next.client = next.client || {};
  if (!next.client.provider) next.client.provider = "brave";
  return next;
}

function webSearchMode(capability) {
  if (capability.disabled) return "disabled";
  if (capability.client?.enabled) return "client";
  if (capability.provider_hosted?.enabled) return "provider_hosted";
  return "disabled";
}

function readWebSearchCapability(form, current) {
  const previous = normalizeWebSearch(current.web_search);
  const mode = form.elements.search_mode?.value || "disabled";
  const selectedWireShape = form.elements.search_wire_shape?.value || previous.provider_hosted?.wire_shape || "";
  const supportedWireShapes = previous.provider_hosted?.supported_wire_shapes?.length
    ? previous.provider_hosted.supported_wire_shapes
    : (state.config?.search_wire_shapes || []).map((shape) => shape.id);
  if (mode === "provider_hosted") {
    return {
      provider_hosted: {
        enabled: true,
        wire_shape: selectedWireShape,
        supported_wire_shapes: supportedWireShapes,
      },
      client: { enabled: false, provider: "brave" },
    };
  }
  if (mode === "client") {
    return {
      provider_hosted: {
        enabled: false,
        wire_shape: selectedWireShape,
        supported_wire_shapes: supportedWireShapes,
      },
      client: { enabled: true, provider: "brave" },
    };
  }
  return {
    provider_hosted: {
      enabled: false,
      wire_shape: selectedWireShape,
      supported_wire_shapes: supportedWireShapes,
    },
    client: { enabled: false, provider: "brave" },
    disabled: true,
  };
}

function wireShapeOptions(wireShapes, selected) {
  const shapes = wireShapes?.length ? wireShapes : [{ id: selected || "openai_chat_web_search_options", title: selected || "OpenAI-compatible chat web_search_options" }];
  const active = selected || shapes[0]?.id || "";
  return shapes.map((shape) => `<option value="${escapeHTML(shape.id)}" ${shape.id === active ? "selected" : ""}>${escapeHTML(shape.title || shape.id)}</option>`).join("");
}

function searchModeLabel(mode, wireShape) {
  if (mode === "provider_hosted") return wireShape ? `provider-hosted · ${wireShape}` : "provider-hosted";
  if (mode === "client") return "client · brave";
  return "disabled";
}
