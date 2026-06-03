async function requestJSON(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: {
      "content-type": "application/json",
      ...(options.headers || {}),
    },
  });
  const text = await response.text();
  const data = text ? JSON.parse(text) : null;
  if (!response.ok) {
    const message = data?.error || `${response.status} ${response.statusText}`;
    const error = new Error(message);
    error.status = response.status;
    error.payload = data;
    throw error;
  }
  return data;
}

export const api = {
  config() {
    return requestJSON("/api/config", { headers: {} });
  },
  saveConfig(payload) {
    return requestJSON("/api/config", { method: "PUT", body: JSON.stringify(payload) });
  },
  sessions() {
    return requestJSON("/api/agent/sessions", { headers: {} });
  },
  session(id) {
    return requestJSON(`/api/agent/sessions/${encodeURIComponent(id)}`, { headers: {} });
  },
  createSession(payload) {
    return requestJSON("/api/agent/sessions", { method: "POST", body: JSON.stringify(payload) });
  },
  createAndRunSession(payload) {
    return requestJSON("/api/agent/sessions/run", { method: "POST", body: JSON.stringify(payload) });
  },
  appendTurn(id, payload) {
    return requestJSON(`/api/agent/sessions/${encodeURIComponent(id)}/turns`, { method: "POST", body: JSON.stringify(payload) });
  },
  updateTools(id, payload) {
    return requestJSON(`/api/agent/sessions/${encodeURIComponent(id)}/tools`, { method: "PATCH", body: JSON.stringify(payload) });
  },
  deleteSession(id) {
    return requestJSON(`/api/agent/sessions/${encodeURIComponent(id)}`, { method: "DELETE" });
  },
  interfaceProbe(payload) {
    return requestJSON("/api/agent/interface-probe", { method: "POST", body: JSON.stringify(payload) });
  },
  runCheck(target) {
    return requestJSON("/api/run", { method: "POST", body: JSON.stringify({ target }) });
  },
};
