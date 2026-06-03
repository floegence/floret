import { escapeHTML, groupTools, toolNamesForPreset } from "../state.js";

export function renderToolMatrix({ tools, selected, editable = true, name = "tools" }) {
  const selectedSet = new Set(selected || []);
  const groups = groupTools(tools);
  return `
    <div class="preset-bar" data-tool-presets="${escapeHTML(name)}">
      ${["chat", "read", "coding", "shell", "all"].map((preset) => `<button type="button" class="small" data-tool-preset="${preset}" ${editable ? "" : "disabled"}>${presetLabel(preset)}</button>`).join("")}
    </div>
    <p class="tool-boundary-note">Network tools are explicit capabilities: web_fetch fetches a known URL; web_search searches by query through the configured client search provider. Neither is a weather API.</p>
    ${groups.map((group) => renderToolGroup(group, selectedSet, editable, name)).join("")}
  `;
}

function renderToolGroup(group, selectedSet, editable, name) {
  return `
    <section class="tool-table" aria-label="${escapeHTML(group.title)} tools">
      <div class="tool-row head">
        <span>Enabled</span>
        <span>Tool</span>
        <span>Scope</span>
        <span>Risk</span>
        <span>Description</span>
      </div>
      ${group.tools.map((tool) => renderToolRow(tool, selectedSet.has(tool.name), editable, name)).join("")}
    </section>
  `;
}

function renderToolRow(tool, checked, editable, name) {
  return `
    <label class="tool-row">
      <span><input type="checkbox" name="${escapeHTML(name)}" value="${escapeHTML(tool.name)}" ${checked ? "checked" : ""} ${editable ? "" : "disabled"} /></span>
      <span class="tool-name"><strong>${escapeHTML(tool.title || tool.name)}</strong><small>${escapeHTML(tool.name)}</small></span>
      <span>${escapeHTML(tool.group_title || tool.group || "tool")}</span>
      <span class="risk">${escapeHTML(tool.risk || "read")}</span>
      <span>${escapeHTML(tool.description || "")}</span>
    </label>
  `;
}

function presetLabel(preset) {
  switch (preset) {
    case "chat":
      return "Chat";
    case "read":
      return "Read";
    case "coding":
      return "Coding";
    case "shell":
      return "Shell";
    case "all":
      return "All";
    default:
      return preset;
  }
}

export function readSelectedTools(container, name = "tools") {
  return Array.from(container.querySelectorAll(`input[name="${CSS.escape(name)}"]:checked`)).map((input) => input.value);
}

export function bindToolPresets(container, tools, name = "tools", afterChange = () => {}) {
  container.querySelectorAll(`[data-tool-presets="${CSS.escape(name)}"] [data-tool-preset]`).forEach((button) => {
    button.addEventListener("click", () => {
      const next = new Set(toolNamesForPreset(button.dataset.toolPreset || "chat", tools));
      container.querySelectorAll(`input[name="${CSS.escape(name)}"]`).forEach((input) => {
        input.checked = next.has(input.value);
      });
      afterChange();
    });
  });
  container.querySelectorAll(`input[name="${CSS.escape(name)}"]`).forEach((input) => {
    input.addEventListener("change", afterChange);
  });
}
