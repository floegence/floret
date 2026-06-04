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
  async streamTurn(id, payload, onEvent) {
    const response = await fetch(`/api/agent/sessions/${encodeURIComponent(id)}/turns/stream`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const text = await response.text();
      let data = null;
      try {
        data = text ? JSON.parse(text) : null;
      } catch {
        data = null;
      }
      const error = new Error(data?.error || `${response.status} ${response.statusText}`);
      error.status = response.status;
      error.payload = data;
      throw error;
    }
    if (!response.body) {
      throw new Error("streaming response is not readable");
    }
    await readSSE(response.body, onEvent);
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

async function readSSE(body, onEvent) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let split = eventBoundary(buffer);
    while (split >= 0) {
      const frame = buffer.slice(0, split);
      buffer = buffer.slice(split + (buffer[split] === "\r" ? 4 : 2));
      consumeSSEFrame(frame, onEvent);
      split = eventBoundary(buffer);
    }
  }
  buffer += decoder.decode();
  if (buffer.trim()) consumeSSEFrame(buffer, onEvent);
}

function eventBoundary(buffer) {
  const lf = buffer.indexOf("\n\n");
  const crlf = buffer.indexOf("\r\n\r\n");
  if (lf < 0) return crlf;
  if (crlf < 0) return lf;
  return Math.min(lf, crlf);
}

function consumeSSEFrame(frame, onEvent) {
  const lines = frame.split(/\r?\n/);
  const data = [];
  for (const line of lines) {
    if (!line || line.startsWith(":")) continue;
    if (line.startsWith("data:")) data.push(line.slice(5).trimStart());
  }
  if (!data.length) return;
  onEvent(JSON.parse(data.join("\n")));
}
