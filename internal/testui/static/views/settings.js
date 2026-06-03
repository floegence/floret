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
          <h2>Search Provider</h2>
          <p class="muted">Client web_search uses Brave Search for providers that do not offer hosted web search. Hosted provider tools remain separate in the Inspector.</p>
          <div class="tool-boundary-grid">
            <div>
              <strong>Provider</strong>
              <span>${escapeHTML(searchProvider.provider || "brave")}</span>
            </div>
            <div>
              <strong>API key</strong>
              <span>${state.config?.search_provider?.api_key_set ? "saved" : "not saved"}</span>
            </div>
            <div>
              <strong>Tool</strong>
              <span>web_search</span>
            </div>
          </div>
          <label class="field"><span>Brave API key</span><input name="search_api_key" type="password" value="${escapeHTML(searchKeyDraft)}" placeholder="${state.config?.search_provider?.api_key_set ? "saved key retained if empty" : "required for web_search"}" ${isSaving ? "disabled" : ""} /></label>
          <label class="field"><span>Endpoint override</span><input name="search_endpoint" value="${escapeHTML(searchProvider.endpoint || "")}" placeholder="default Brave Web Search endpoint" ${isSaving ? "disabled" : ""} /></label>
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
  form?.addEventListener("input", persistDraft);
  form?.addEventListener("change", (event) => {
    if (event.target === form.elements.active_profile_id || event.target === form.elements.provider) return;
    persistDraft();
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
