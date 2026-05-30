const vscode = require("vscode");
const cp = require("child_process");
const path = require("path");

let serverProcess;
let panel;

function activate(context) {
  context.subscriptions.push(
    vscode.commands.registerCommand("localDevAgent.openChat", async () => {
      await ensureServer(context);
      openChatPanel(context);
    }),
    vscode.commands.registerCommand("localDevAgent.askActiveFile", async () => {
      await ensureServer(context);
      openChatPanel(context);
      const editor = vscode.window.activeTextEditor;
      if (!editor) {
        return;
      }
      const rel = vscode.workspace.asRelativePath(editor.document.uri);
      panel.webview.postMessage({
        type: "seed",
        text: `Explain and review the active file: ${rel}`
      });
    })
  );

  context.subscriptions.push({
    dispose: () => {
      if (serverProcess) {
        serverProcess.kill();
      }
    }
  });
}

async function ensureServer(context) {
  const config = vscode.workspace.getConfiguration("localDevAgent");
  if (!config.get("autoStartServer")) {
    return;
  }
  const serverUrl = config.get("serverUrl");
  if (await isHealthy(serverUrl)) {
    return;
  }
  if (serverProcess) {
    return;
  }

  const extensionRoot = context.extensionPath;
  const serverPath = path.resolve(extensionRoot, "..", "server", "agent_server.py");
  const workspaceRoot = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath || process.cwd();
  const pythonPath = config.get("pythonPath") || "python";
  const port = new URL(serverUrl).port || "8765";

  serverProcess = cp.spawn(pythonPath, [serverPath, "--workspace", workspaceRoot, "--port", port], {
    cwd: path.dirname(serverPath),
    windowsHide: true
  });

  serverProcess.stderr.on("data", (data) => console.warn(String(data)));
  serverProcess.on("exit", () => {
    serverProcess = undefined;
  });

  await sleep(900);
}

async function isHealthy(serverUrl) {
  try {
    const res = await fetch(`${serverUrl}/health`);
    return res.ok;
  } catch {
    return false;
  }
}

function openChatPanel(context) {
  if (panel) {
    panel.reveal(vscode.ViewColumn.Beside);
    return;
  }

  panel = vscode.window.createWebviewPanel(
    "localDevAgentChat",
    "Local Dev Agent",
    vscode.ViewColumn.Beside,
    { enableScripts: true, retainContextWhenHidden: true }
  );
  panel.webview.html = renderWebview();
  panel.onDidDispose(() => {
    panel = undefined;
  });

  panel.webview.onDidReceiveMessage(async (message) => {
    if (message.type !== "ask") {
      return;
    }
    const config = vscode.workspace.getConfiguration("localDevAgent");
    const serverUrl = config.get("serverUrl");
    try {
      const response = await fetch(`${serverUrl}/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ prompt: message.prompt, mode: message.mode || "chat" })
      });
      const data = await response.json();
      panel.webview.postMessage({ type: "answer", data });
    } catch (error) {
      panel.webview.postMessage({ type: "answer", data: { error: String(error) } });
    }
  });
}

function renderWebview() {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <style>
    :root {
      --bg: #111315;
      --panel: #1b1f23;
      --text: #e8edf2;
      --muted: #9aa7b2;
      --accent: #53b39d;
      --border: #313940;
      --danger: #ff7a7a;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: var(--vscode-font-family, system-ui, sans-serif);
      background: var(--bg);
      color: var(--text);
    }
    main {
      display: grid;
      grid-template-rows: 1fr auto;
      height: 100vh;
    }
    #log {
      overflow: auto;
      padding: 16px;
    }
    .msg {
      border: 1px solid var(--border);
      background: var(--panel);
      border-radius: 8px;
      padding: 12px;
      margin-bottom: 12px;
      white-space: pre-wrap;
      line-height: 1.45;
    }
    .meta {
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 6px;
    }
    form {
      display: grid;
      grid-template-columns: auto 1fr auto;
      gap: 8px;
      padding: 12px;
      border-top: 1px solid var(--border);
      background: #15181b;
    }
    select, textarea, button {
      font: inherit;
      color: var(--text);
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 6px;
    }
    textarea {
      min-height: 44px;
      max-height: 180px;
      resize: vertical;
      padding: 10px;
    }
    button {
      padding: 0 14px;
      background: var(--accent);
      color: #06110e;
      border: 0;
      font-weight: 700;
      cursor: pointer;
    }
    button:disabled {
      opacity: 0.6;
      cursor: wait;
    }
    .error { color: var(--danger); }
  </style>
</head>
<body>
  <main>
    <section id="log">
      <div class="msg">
        <div class="meta">Local Dev Agent</div>
        Ask about this workspace, request a plan, or ask for a patch. The model call stays on your local endpoint.
      </div>
    </section>
    <form id="form">
      <select id="mode" title="Mode">
        <option value="chat">Chat</option>
        <option value="plan">Plan</option>
        <option value="patch">Patch</option>
        <option value="review">Review</option>
      </select>
      <textarea id="prompt" placeholder="Ask the local agent..."></textarea>
      <button id="send" type="submit">Send</button>
    </form>
  </main>
  <script>
    const vscode = acquireVsCodeApi();
    const form = document.getElementById("form");
    const promptEl = document.getElementById("prompt");
    const modeEl = document.getElementById("mode");
    const sendEl = document.getElementById("send");
    const logEl = document.getElementById("log");

    form.addEventListener("submit", (event) => {
      event.preventDefault();
      const prompt = promptEl.value.trim();
      if (!prompt) return;
      addMessage("You", prompt);
      sendEl.disabled = true;
      vscode.postMessage({ type: "ask", prompt, mode: modeEl.value });
      promptEl.value = "";
    });

    window.addEventListener("message", (event) => {
      const message = event.data;
      if (message.type === "seed") {
        promptEl.value = message.text;
        promptEl.focus();
      }
      if (message.type === "answer") {
        sendEl.disabled = false;
        const data = message.data || {};
        if (data.error) {
          addMessage("Error", data.error, true);
          return;
        }
        addMessage("Local Dev Agent", data.answer + "\\n\\nmodel: " + data.model + " | context: " + data.contextChars + " chars | " + data.elapsedSeconds + "s");
      }
    });

    function addMessage(author, text, error) {
      const node = document.createElement("div");
      node.className = "msg" + (error ? " error" : "");
      const meta = document.createElement("div");
      meta.className = "meta";
      meta.textContent = author;
      const body = document.createElement("div");
      body.textContent = text;
      node.append(meta, body);
      logEl.append(node);
      logEl.scrollTop = logEl.scrollHeight;
    }
  </script>
</body>
</html>`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function deactivate() {
  if (serverProcess) {
    serverProcess.kill();
  }
}

module.exports = { activate, deactivate };

