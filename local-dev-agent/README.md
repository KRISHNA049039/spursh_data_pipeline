# Local Dev Agent

Local Dev Agent is an air-gapped developer assistant scaffold for VS Code. It is designed to work with local OpenAI-compatible model servers such as Ollama, vLLM, llama.cpp server, or LM Studio.

The project has two parts:

- `server/`: a Python backend that indexes workspace files, builds compact coding context, and calls a local LLM endpoint.
- `vscode-extension/`: a VS Code chat panel that starts the backend and sends prompts with the active workspace context.

## Capabilities

- Chat with a local model from VS Code
- Ask questions about the current workspace
- Generate implementation plans
- Draft file edits as unified diffs for human review
- Keep provider logic isolated for later fine-tuned models
- Run without external network access after dependencies/model binaries are staged

This is intentionally conservative: it does not auto-apply patches yet. For air-gapped development, reviewable diffs are safer as the first version.

## Quick Start

### 1. Start a local model

For Ollama:

```powershell
ollama serve
ollama pull qwen2.5-coder:7b
```

For LM Studio, enable the local server and use its OpenAI-compatible URL.

For vLLM:

```powershell
python -m vllm.entrypoints.openai.api_server --model Qwen/Qwen2.5-Coder-7B-Instruct --host 127.0.0.1 --port 8000
```

### 2. Configure the agent

Copy `.env.example` to `.env` and edit values if needed.

```powershell
Copy-Item .env.example .env
```

Defaults target Ollama:

```text
LOCAL_AGENT_MODEL=qwen2.5-coder:7b
LOCAL_AGENT_BASE_URL=http://127.0.0.1:11434/v1
LOCAL_AGENT_API_KEY=ollama
```

### 3. Run the backend

```powershell
python .\server\agent_server.py --workspace ".."
```

Open [http://127.0.0.1:8765/health](http://127.0.0.1:8765/health) to verify it is alive.

You can also use the helper script:

```powershell
.\scripts\Start-LocalDevAgent.ps1 -Workspace ".."
```

### 4. Load the VS Code extension

1. Open `local-dev-agent/vscode-extension` in VS Code.
2. Press `F5` to start an Extension Development Host.
3. Run `Local Dev Agent: Open Chat`.

The extension starts the backend automatically when possible.

## Recommended Local Models

Good first choices for coding:

- `qwen2.5-coder:7b` for balanced speed and quality
- `qwen2.5-coder:14b` if your workstation has enough VRAM/RAM
- `deepseek-coder-v2-lite-instruct` for code-heavy tasks
- `codellama:13b-instruct` for broad compatibility

Use larger models when the machine can hold them comfortably. In an air-gapped network, reliability usually matters more than squeezing out the largest model.

## Fine-Tuning Path

The agent already separates prompts, model adapters, and context building. Later you can fine-tune on:

- Accepted patches
- Internal coding standards
- Architecture decision records
- Secure coding examples
- Review comments and final fixes

Suggested data format is JSONL:

```jsonl
{"messages":[{"role":"system","content":"You are an internal coding agent."},{"role":"user","content":"Task plus relevant files..."},{"role":"assistant","content":"Plan and patch..."}]}
```

Keep secrets out of training data. For regulated environments, run a redaction step before generating fine-tuning records.

## Offline Packaging Notes

For a fully air-gapped setup, stage these outside the network:

- Python 3.11+ installer
- VS Code extension folder
- Model weights or Ollama model cache
- Any optional Python wheels if you later add dependencies

This initial backend uses only the Python standard library.
