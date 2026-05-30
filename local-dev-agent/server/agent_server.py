#!/usr/bin/env python3
"""Local-first coding agent backend.

The server intentionally uses only the Python standard library so it can run in
air-gapped networks without pip installs.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import pathlib
import socketserver
import sys
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler
from typing import Any


DEFAULT_IGNORE_DIRS = {
    ".git",
    ".hg",
    ".svn",
    ".venv",
    "venv",
    "__pycache__",
    "node_modules",
    "dist",
    "build",
    "cdk.out",
    ".pytest_cache",
    ".mypy_cache",
}

DEFAULT_INCLUDE = [
    "*.py",
    "*.js",
    "*.ts",
    "*.tsx",
    "*.jsx",
    "*.json",
    "*.md",
    "*.yaml",
    "*.yml",
    "*.toml",
    "*.ini",
    "*.txt",
    "*.sh",
    "*.ps1",
]


def load_dotenv(path: pathlib.Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


class WorkspaceIndex:
    def __init__(self, root: pathlib.Path, max_file_chars: int = 12000) -> None:
        self.root = root.resolve()
        self.max_file_chars = max_file_chars

    def list_files(self, limit: int = 240) -> list[str]:
        files: list[str] = []
        for path in self.root.rglob("*"):
            if len(files) >= limit:
                break
            if not path.is_file() or self._ignored(path):
                continue
            rel = path.relative_to(self.root).as_posix()
            if self._included(rel):
                files.append(rel)
        return sorted(files)

    def read_file(self, rel_path: str) -> str:
        safe = self._safe_path(rel_path)
        data = safe.read_text(encoding="utf-8", errors="replace")
        if len(data) > self.max_file_chars:
            return data[: self.max_file_chars] + "\n\n[truncated]\n"
        return data

    def relevant_context(self, query: str, max_chars: int) -> str:
        query_terms = {term.lower() for term in query.replace("\\", "/").split() if len(term) > 2}
        scored: list[tuple[int, str]] = []
        for rel in self.list_files():
            lower = rel.lower()
            score = sum(3 for term in query_terms if term in lower)
            if pathlib.PurePosixPath(rel).name.lower() in {"readme.md", "package.json", "pyproject.toml"}:
                score += 2
            if score > 0 or len(scored) < 24:
                scored.append((score, rel))
        scored.sort(key=lambda item: (-item[0], item[1]))

        chunks: list[str] = []
        total = 0
        for _, rel in scored[:24]:
            try:
                content = self.read_file(rel)
            except OSError:
                continue
            block = f"\n--- FILE: {rel} ---\n{content}\n"
            if total + len(block) > max_chars:
                remaining = max_chars - total
                if remaining > 500:
                    chunks.append(block[:remaining])
                break
            chunks.append(block)
            total += len(block)
        return "".join(chunks).strip()

    def _safe_path(self, rel_path: str) -> pathlib.Path:
        candidate = (self.root / rel_path).resolve()
        if self.root != candidate and self.root not in candidate.parents:
            raise ValueError("Path escapes workspace")
        if not candidate.is_file():
            raise FileNotFoundError(rel_path)
        return candidate

    def _ignored(self, path: pathlib.Path) -> bool:
        parts = set(path.relative_to(self.root).parts)
        return bool(parts & DEFAULT_IGNORE_DIRS)

    def _included(self, rel_path: str) -> bool:
        return any(fnmatch.fnmatch(rel_path, pattern) for pattern in DEFAULT_INCLUDE)


class LocalLLMClient:
    def __init__(self, base_url: str, api_key: str, model: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model

    def chat(self, messages: list[dict[str, str]], temperature: float = 0.2) -> str:
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "stream": False,
        }
        body = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            f"{self.base_url}/chat/completions",
            data=body,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=180) as response:
                data = json.loads(response.read().decode("utf-8"))
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Local model request failed: {exc}") from exc
        return data["choices"][0]["message"]["content"]


SYSTEM_PROMPT = """You are Local Dev Agent, an offline coding assistant for VS Code.
Work like a careful senior engineer. Use the provided workspace context only when relevant.
Prefer small, reviewable changes. When asked to edit code, return a concise plan followed by a unified diff.
Never claim you changed files unless a tool or human actually applied the patch."""


class AgentState:
    def __init__(self, workspace: pathlib.Path) -> None:
        dotenv = pathlib.Path(__file__).resolve().parents[1] / ".env"
        load_dotenv(dotenv)
        self.workspace = workspace.resolve()
        self.index = WorkspaceIndex(self.workspace)
        self.max_context_chars = int(os.getenv("LOCAL_AGENT_MAX_CONTEXT_CHARS", "48000"))
        self.llm = LocalLLMClient(
            base_url=os.getenv("LOCAL_AGENT_BASE_URL", "http://127.0.0.1:11434/v1"),
            api_key=os.getenv("LOCAL_AGENT_API_KEY", "ollama"),
            model=os.getenv("LOCAL_AGENT_MODEL", "qwen2.5-coder:7b"),
        )

    def answer(self, prompt: str, mode: str = "chat") -> dict[str, Any]:
        context = self.index.relevant_context(prompt, self.max_context_chars)
        mode_instruction = {
            "chat": "Answer the user directly and cite workspace file paths when useful.",
            "plan": "Return a practical implementation plan with risks and tests.",
            "patch": "Return a unified diff only after a short explanation. Do not invent unseen files unless clearly needed.",
            "review": "Review for bugs, risks, and missing tests. Lead with findings.",
        }.get(mode, "Answer the user directly.")
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": f"Mode: {mode}\nInstruction: {mode_instruction}\n\nWorkspace context:\n{context}\n\nUser request:\n{prompt}",
            },
        ]
        started = time.time()
        answer = self.llm.chat(messages)
        return {
            "answer": answer,
            "model": self.llm.model,
            "elapsedSeconds": round(time.time() - started, 2),
            "contextChars": len(context),
        }


def make_handler(state: AgentState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/health":
                self._json({"ok": True, "workspace": str(state.workspace), "model": state.llm.model})
                return
            if self.path == "/files":
                self._json({"files": state.index.list_files()})
                return
            self.send_error(404)

        def do_POST(self) -> None:
            try:
                length = int(self.headers.get("Content-Length", "0"))
                payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
                if self.path == "/chat":
                    prompt = str(payload.get("prompt", "")).strip()
                    mode = str(payload.get("mode", "chat")).strip()
                    if not prompt:
                        self._json({"error": "prompt is required"}, status=400)
                        return
                    self._json(state.answer(prompt, mode))
                    return
                self.send_error(404)
            except Exception as exc:  # noqa: BLE001 - server boundary
                self._json({"error": str(exc)}, status=500)

        def log_message(self, fmt: str, *args: Any) -> None:
            sys.stderr.write("[local-agent] " + fmt % args + "\n")

        def _json(self, payload: dict[str, Any], status: int = 200) -> None:
            body = json.dumps(payload, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

    return Handler


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Local Dev Agent backend.")
    parser.add_argument("--workspace", default=os.getcwd(), help="Workspace root to index")
    parser.add_argument("--host", default=os.getenv("LOCAL_AGENT_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("LOCAL_AGENT_PORT", "8765")))
    args = parser.parse_args()

    state = AgentState(pathlib.Path(args.workspace))
    handler = make_handler(state)
    with socketserver.ThreadingTCPServer((args.host, args.port), handler) as httpd:
        print(f"Local Dev Agent listening on http://{args.host}:{args.port}")
        print(f"Workspace: {state.workspace}")
        print(f"Model: {state.llm.model} via {state.llm.base_url}")
        httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

