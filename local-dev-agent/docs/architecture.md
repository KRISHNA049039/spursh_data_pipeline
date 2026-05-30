# Local Dev Agent Architecture

## Goals

- Run in air-gapped development networks
- Use VS Code as the primary interface
- Support multiple local model servers through one adapter
- Keep every code change reviewable
- Prepare a clean path for fine-tuned internal models

## Components

```text
VS Code Extension
  |
  | HTTP on localhost
  v
Agent Backend
  |-- WorkspaceIndex
  |-- Prompt builder
  |-- LocalLLMClient
  `-- Patch-oriented response modes
  |
  | OpenAI-compatible API
  v
Local Model Runtime
  |-- Ollama
  |-- vLLM
  |-- LM Studio
  `-- llama.cpp server
```

## Request Flow

1. The user sends a request from the VS Code panel.
2. The extension POSTs to `http://127.0.0.1:8765/chat`.
3. The backend selects relevant workspace files.
4. The backend builds a coding-agent prompt.
5. The backend calls `/v1/chat/completions` on the local model server.
6. The response is shown in VS Code.

## Fine-Tuning Extension Points

- Replace `LocalLLMClient.model` with a fine-tuned model name.
- Add a `training/` exporter that stores accepted prompt/context/patch records as JSONL.
- Add a redaction stage before records leave developer machines.
- Add retrieval over internal docs when an approved offline vector store is available.

## Safety Defaults

- The current app does not apply patches automatically.
- Workspace indexing ignores common generated and dependency folders.
- File paths are resolved under the workspace root before reading.
- The backend binds to `127.0.0.1` by default.

