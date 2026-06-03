import { clone, defaultProfile, escapeHTML, profileLabel, providerCatalog, state } from "../state.js";

export function renderSettings() {
  const profiles = state.config?.profiles?.length ? state.config.profiles : [defaultProfile()];
  const activeID = state.config?.active_profile_id || profiles[0].id;
  const active = profiles.find((profile) => profile.id === activeID) || profiles[0];
  const providers = providerCatalog();
  const isSaving = state.action === "save-settings";
  return `
    <section class="settings-page">
      <header class="settings-head">
        <div>
          <h1>Settings</h1>
          <p class="muted">Provider profiles and local quality checks live here, away from the session workflow.</p>
        </div>
      </header>
      <div class="settings-grid">
        <form class="profile-card" data-settings-form>
          <h2>Model Configuration</h2>
          <label class="field">
            <span>Saved configuration</span>
            <select name="active_profile_id">
              ${profiles.map((profile) => `<option value="${escapeHTML(profile.id)}" ${profile.id === activeID ? "selected" : ""}>${escapeHTML(profileLabel(profile))}</option>`).join("")}
            </select>
          </label>
          <label class="field"><span>Name</span><input name="name" value="${escapeHTML(active.name || "")}" /></label>
          <label class="field">
            <span>Provider</span>
            <select name="provider">
              ${providers.map((provider) => `<option value="${escapeHTML(provider.id)}" ${provider.id === active.provider ? "selected" : ""}>${escapeHTML(provider.name || provider.id)} · ${escapeHTML(provider.id)}</option>`).join("")}
              ${providers.some((provider) => provider.id === active.provider) ? "" : `<option value="${escapeHTML(active.provider || "openai-compatible")}" selected>${escapeHTML(active.provider || "custom")}</option>`}
            </select>
          </label>
          <label class="field"><span>Model</span><input name="model" value="${escapeHTML(active.model || "")}" /></label>
          <label class="field"><span>Base URL</span><input name="base_url" value="${escapeHTML(active.base_url || "")}" placeholder="https://api.example.com/v1" /></label>
          <label class="field"><span>API key</span><input name="api_key" type="password" placeholder="${active.api_key_set ? "saved key retained if empty" : "optional"}" /></label>
          <label class="field"><span>Fake response</span><input name="fake_response" value="${escapeHTML(active.fake_response || "")}" /></label>
          <div class="settings-divider"></div>
          <h2>Search Provider</h2>
          <p class="muted">Client web_search uses Brave Search for providers that do not offer hosted web search. Hosted provider tools remain separate in the Inspector.</p>
          <div class="tool-boundary-grid">
            <div>
              <strong>Provider</strong>
              <span>${escapeHTML(state.config?.search_provider?.provider || "brave")}</span>
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
          <label class="field"><span>Brave API key</span><input name="search_api_key" type="password" placeholder="${state.config?.search_provider?.api_key_set ? "saved key retained if empty" : "required for web_search"}" /></label>
          <label class="field"><span>Endpoint override</span><input name="search_endpoint" value="${escapeHTML(state.config?.search_provider?.endpoint || "")}" placeholder="default Brave Web Search endpoint" /></label>
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
  form?.elements.active_profile_id?.addEventListener("change", () => handlers.onSwitchProfile(form.elements.active_profile_id.value));
  form?.elements.provider?.addEventListener("change", () => handlers.onSwitchProvider(form.elements.provider.value));
  form?.addEventListener("submit", (event) => {
    event.preventDefault();
    const profiles = clone(state.config?.profiles || [defaultProfile()]);
    const activeID = form.elements.active_profile_id.value;
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
    handlers.onSave({
      active_profile_id: next.id,
      profiles,
      search_provider: {
        provider: "brave",
        api_key: form.elements.search_api_key?.value || "",
        endpoint: form.elements.search_endpoint?.value || "",
      },
    });
  });
  root.querySelector("[data-duplicate-profile]")?.addEventListener("click", () => handlers.onDuplicate());
  root.querySelectorAll("[data-run-check]").forEach((button) => {
    button.addEventListener("click", () => handlers.onRunCheck(button.dataset.runCheck || "unit"));
  });
}
