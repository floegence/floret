import { contextPolicyForProfile, currentProfile, escapeHTML, state } from "../state.js";

export const FRONTEND_DESIGN_SKILL_URL = "https://github.com/anthropics/skills/tree/main/skills/frontend-design";
export const LANDING_ARTIFACT_PATH = ".floret-test-ui/artifacts/frontend-design-landing/index.html";
export const LANDING_ARTIFACT_URL = "/artifacts/frontend-design-landing/index.html";
export const LANDING_DEMO_TOOLS = ["read", "list", "glob", "grep", "apply_patch", "write"];

export function renderSkills() {
  const draft = skillInstallDraft();
  const preview = state.skillsPreview;
  const capabilities = state.config?.capabilities || {};
  const skills = capabilities.skills || [];
  const sources = capabilities.skill_sources || [];
  const diagnostics = (capabilities.diagnostics || []).filter((item) => item.capability === "skills" || item.source_kind || String(item.kind || "").startsWith("skill"));
  const isPreviewing = state.action === "skill-preview";
  const isInstalling = state.action === "skill-install";
  return `
    <section class="skills-page">
      <header class="skills-head">
        <div>
          <h1>Skills</h1>
          <p class="muted">Install host-managed Agent Skills for this test console. Floret core still only loads, validates, discloses, and audits them at runtime.</p>
        </div>
        <a class="button ghost" href="/sessions/new" data-link>New Session</a>
      </header>
      <div class="skills-layout">
        <section class="skill-install-panel">
          <div>
            <h2>Install Agent Skill</h2>
            <p class="muted">Preview a GitHub tree, blob, or raw SKILL.md URL before writing it into the managed test UI skill source.</p>
          </div>
          <form class="skill-install-form" data-skill-install-form>
            <label class="field" for="skill-url">
              <span>GitHub skill URL</span>
              <input id="skill-url" name="url" aria-label="GitHub skill URL" value="${escapeHTML(draft.url || FRONTEND_DESIGN_SKILL_URL)}" placeholder="${escapeHTML(FRONTEND_DESIGN_SKILL_URL)}" ${isPreviewing || isInstalling ? "disabled" : ""} />
            </label>
            ${preview ? renderPreview(preview, draft, isInstalling) : renderPreviewEmpty()}
            <div class="form-actions">
              <span class="muted">Preview downloads metadata and file sizes. Install writes only after confirmation.</span>
              <span class="action-cluster">
                <button type="button" class="${isPreviewing ? "is-pending" : ""}" data-preview-skill ${isPreviewing || isInstalling ? "disabled" : ""}>${isPreviewing ? "Previewing..." : "Preview"}</button>
                <button class="primary ${isInstalling ? "is-pending" : ""}" type="submit" ${!preview || isPreviewing || isInstalling ? "disabled" : ""}>${installLabel(preview, isInstalling)}</button>
              </span>
            </div>
          </form>
        </section>

        <section class="skill-status-panel">
          <div>
            <h2>Installed Skills</h2>
            <p class="muted">The read-only <strong>skill</strong> tool is registered by capability setup when skills are enabled, so it does not need a local tool checkbox.</p>
          </div>
          ${renderSkillSources(sources)}
          ${renderInstalledSkills(skills)}
          ${renderSkillDiagnostics(diagnostics)}
        </section>

        <section class="skill-demo-panel">
          <div>
            <h2>Landing Demo</h2>
            <p class="muted">Use frontend-design through progressive disclosure, then create a self-contained artifact in the managed artifact directory.</p>
          </div>
          <div class="tool-boundary-grid">
            <div>
              <strong>Skill call</strong>
              <span>${escapeHTML('skill {"name":"frontend-design"}')}</span>
            </div>
            <div>
              <strong>Selected tools</strong>
              <span>${escapeHTML(LANDING_DEMO_TOOLS.join(", "))}</span>
            </div>
            <div>
              <strong>Artifact</strong>
              <span>${escapeHTML(LANDING_ARTIFACT_PATH)}</span>
            </div>
          </div>
          <pre class="code-block">${escapeHTML(landingDemoPrompt())}</pre>
          <div class="form-actions">
            <a class="button ghost" href="${LANDING_ARTIFACT_URL}" target="_blank" rel="noreferrer">Open artifact</a>
            <button type="button" class="primary" data-use-landing-demo ${hasSkill(skills, "frontend-design") ? "" : "disabled"}>Use in Landing Demo</button>
          </div>
        </section>
      </div>
    </section>
  `;
}

export function bindSkills(root, handlers) {
  const form = root.querySelector("[data-skill-install-form]");
  const persistDraft = () => {
    state.skillsInstallDraft = readSkillInstallDraft(form);
  };
  form?.addEventListener("input", (event) => {
    if (event.isComposing) return;
    persistDraft();
  });
  form?.addEventListener("change", persistDraft);
  root.querySelector("[data-preview-skill]")?.addEventListener("click", () => {
    persistDraft();
    handlers.onPreview?.(readSkillInstallDraft(form));
  });
  form?.addEventListener("submit", (event) => {
    event.preventDefault();
    persistDraft();
    handlers.onInstall?.(readSkillInstallDraft(form));
  });
  root.querySelectorAll("[data-use-landing-demo]").forEach((button) => {
    button.addEventListener("click", () => handlers.onLandingDemo?.());
  });
}

export function readSkillInstallDraft(form) {
  if (!form) return skillInstallDraft();
  return {
    url: form.elements.url?.value || "",
    replace: Boolean(form.elements.replace?.checked),
  };
}

export function landingDemoDraft(tools = state.config?.tools || []) {
  const profile = currentProfile();
  const available = new Set((tools || []).filter((tool) => tool.available !== false).map((tool) => tool.name));
  return {
    profile_id: profile.id,
    message: landingDemoPrompt(),
    system_prompt: "You are Floret. Follow the user's requested tool sequence exactly, use progressive disclosure for Agent Skills, and answer with the artifact URL after the file is written.",
    selected_tools: LANDING_DEMO_TOOLS.filter((name) => available.has(name)),
    context_policy: contextPolicyForProfile(profile),
  };
}

export function landingDemoPrompt() {
  return [
    "First call the read-only skill tool with {\"name\":\"frontend-design\"}.",
    "After the tool result, design a distinctive, production-grade landing page for a fictional product named Floret Canvas: an agent workspace for composing tool-rich AI sessions.",
    `Prefer apply_patch to create ${LANDING_ARTIFACT_PATH}; use write only if you need to create the complete file in one call.`,
    "The file must be a complete self-contained HTML document with polished CSS and lightweight JavaScript. Make it visually striking, accessible, responsive, and avoid generic purple-gradient AI aesthetics.",
    "Do not use shell. End by telling me to open /artifacts/frontend-design-landing/index.html.",
  ].join("\n");
}

function skillInstallDraft() {
  return state.skillsInstallDraft || { url: FRONTEND_DESIGN_SKILL_URL, replace: false };
}

function renderPreview(preview, draft, isInstalling) {
  const files = preview.files || [];
  const replace = Boolean(draft.replace);
  return `
    <section class="skill-preview" data-skill-preview>
      <div class="skill-preview-head">
        <div>
          <strong>${escapeHTML(preview.name || "skill")}</strong>
          <span class="muted">${escapeHTML(preview.description || "")}</span>
        </div>
        <span class="status-pill ${preview.requires_replace ? "waiting" : "completed"}">${preview.requires_replace ? "replace" : "ready"}</span>
      </div>
      <div class="skill-facts">
        ${renderFact("Repo", preview.repo)}
        ${renderFact("Ref", preview.ref)}
        ${renderFact("Source path", preview.source_path)}
        ${renderFact("License", preview.license || "not declared")}
        ${renderFact("Files", `${files.length} files · ${formatBytes(preview.total_bytes)}`)}
        ${renderFact("Target", preview.target_path)}
      </div>
      ${preview.requires_replace ? `
        <label class="replace-confirm">
          <input type="checkbox" name="replace" ${replace ? "checked" : ""} ${isInstalling ? "disabled" : ""} />
          <span>Replace the installed copy. Existing hash ${escapeHTML(shortHash(preview.existing_hash))}; new hash ${escapeHTML(shortHash(preview.content_hash))}.</span>
        </label>
      ` : ""}
      <details class="skill-file-list">
        <summary>${escapeHTML(files.length)} staged file(s)</summary>
        <div>
          ${files.map((file) => `<span>${escapeHTML(file.path)} <small>${escapeHTML(formatBytes(file.bytes))}</small></span>`).join("")}
        </div>
      </details>
    </section>
  `;
}

function renderPreviewEmpty() {
  return `
    <section class="skill-preview empty-preview">
      <strong>No preview yet</strong>
      <span class="muted">Use Preview to inspect SKILL.md metadata, file count, size, target path, and replace status.</span>
    </section>
  `;
}

function installLabel(preview, isInstalling) {
  if (isInstalling) return preview?.requires_replace ? "Replacing..." : "Installing...";
  return preview?.requires_replace ? "Replace Skill" : "Install Skill";
}

function renderSkillSources(sources) {
  if (!sources.length) {
    return `<div class="event-item"><strong>Skill sources</strong><span class="muted">none configured</span></div>`;
  }
  return `
    <div class="source-list">
      ${sources.map((source) => `
        <div class="source-row">
          <strong>${escapeHTML(source.label || source.kind || "source")}</strong>
          <span class="muted">${escapeHTML(source.root || "")}</span>
          <span class="status-pill ${source.enabled ? "completed" : "waiting"}">${source.enabled ? "enabled" : "disabled"}</span>
          <span class="tiny-pill">${source.managed ? "managed" : "external"}</span>
          <span class="tiny-pill">${Number(source.skill_count || 0)} skill(s)</span>
        </div>
      `).join("")}
    </div>
  `;
}

function renderInstalledSkills(skills) {
  if (!skills.length) {
    return `<div class="empty-skill-list"><strong>No skills detected</strong><span class="muted">Install frontend-design to enable the landing demo.</span></div>`;
  }
  return `
    <div class="skill-list">
      ${skills.map((skill) => `
        <article class="skill-row">
          <div>
            <strong>${escapeHTML(skill.name || "skill")}</strong>
            <span class="muted">${escapeHTML(skill.description || "")}</span>
          </div>
          <span class="tiny-pill">${escapeHTML(skill.source_label || skill.source_kind || "source")}</span>
          <span class="tiny-pill">${escapeHTML(skill.license || "license n/a")}</span>
          <span class="status-pill completed">${escapeHTML(skill.status || "detected")}</span>
          ${skill.name === "frontend-design" ? `<button type="button" data-use-landing-demo>Use in Landing Demo</button>` : ""}
        </article>
      `).join("")}
    </div>
  `;
}

function renderSkillDiagnostics(diagnostics) {
  if (!diagnostics.length) return "";
  return `
    <div class="event-list">
      <strong>Diagnostics</strong>
      ${diagnostics.map((item) => `
        <div class="event-item event-error">
          <strong>${escapeHTML(item.kind || "diagnostic")}</strong>
          <span class="muted">${escapeHTML(item.message || "")}</span>
          ${item.next_action ? `<span>${escapeHTML(item.next_action)}</span>` : ""}
        </div>
      `).join("")}
    </div>
  `;
}

function renderFact(label, value) {
  return `
    <div>
      <strong>${escapeHTML(label)}</strong>
      <span>${escapeHTML(value || "-")}</span>
    </div>
  `;
}

function formatBytes(value) {
  const bytes = Number(value || 0);
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

function shortHash(value) {
  if (!value) return "none";
  return `${value.slice(0, 8)}...${value.slice(-6)}`;
}

function hasSkill(skills, name) {
  return (skills || []).some((skill) => skill.name === name);
}
