from __future__ import annotations

import configparser
import json
import os
import shlex
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


class NamespaceXattrError(RuntimeError):
    def __init__(self, method: str, url: str, status: int, body: str):
        detail = body.strip() or "<empty response body>"
        super().__init__(f"{method} {url} failed with HTTP {status}: {detail}")
        self.method = method
        self.url = url
        self.status = status
        self.body = body


def normalize_remote_path(remote_path: str) -> str:
    cleaned = str(remote_path).strip("/")
    return f"/{cleaned}" if cleaned else "/"


def encode_namespace_path(remote_path: str) -> str:
    return urllib.parse.quote(normalize_remote_path(remote_path), safe="")


def _extract_bearer_token_from_file(tokenfile: Path) -> str | None:
    try:
        text = tokenfile.read_text(encoding="utf-8")
    except OSError:
        return None

    tokens = [
        line.split("=", 1)[1].strip()
        for line in text.splitlines()
        if line.lstrip().startswith("bearer_token") and "=" in line
    ]
    if len(tokens) > 1:
        raise RuntimeError(f"token file {tokenfile} contains multiple bearer_token entries")
    if tokens:
        value = tokens[0]
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        value = value.strip()
        if value:
            return value

    lines = text.splitlines()
    if not lines:
        return None
    first_line = lines[0].strip()
    return first_line or None


def extract_bearer_token(
    remote_cfg: configparser.SectionProxy | None,
    tokenfile: Path,
) -> str | None:
    if remote_cfg is not None:
        direct = (remote_cfg.get("bearer_token", fallback="") or "").strip()
        if direct:
            return direct

        command_text = (remote_cfg.get("bearer_token_command", fallback="") or "").strip()
        if command_text:
            try:
                result = subprocess.run(
                    shlex.split(command_text),
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
            except Exception as exc:
                raise RuntimeError(f"could not resolve bearer_token_command: {exc}") from exc
            for line in reversed(result.stdout.splitlines()):
                token = line.strip()
                if token:
                    return token
            raise RuntimeError("bearer_token_command produced no token on stdout")

    env_token = os.environ.get("BEARER_TOKEN", "").strip()
    if env_token:
        return env_token

    return _extract_bearer_token_from_file(tokenfile)


class NamespaceXattrClient:
    def __init__(self, api: str, bearer_token: str):
        self.api = api.rstrip("/")
        self.bearer_token = bearer_token

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, str] | None = None,
        payload: dict[str, object] | None = None,
    ) -> Any:
        url = f"{self.api}/{endpoint.lstrip('/')}"
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.bearer_token}",
        }
        data = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")

        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                body = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise NamespaceXattrError(method, url, exc.code, body) from None
        except urllib.error.URLError as exc:
            raise RuntimeError(f"{method} {url} failed: {exc.reason}") from exc

        if not body.strip():
            return {}
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"{method} {url} returned non-JSON data: {body[:400]}") from exc

    def list_xattrs(self, remote_path: str) -> dict[str, str]:
        encoded = encode_namespace_path(remote_path)
        data = self._request("GET", f"namespace/{encoded}", params={"xattr": "true"})
        if not isinstance(data, dict):
            raise RuntimeError(f"unexpected xattr listing response for {remote_path!r}: {data!r}")
        xattrs = data.get("extendedAttributes", {})
        return dict(xattrs) if isinstance(xattrs, dict) else {}

    def set_xattrs(self, remote_path: str, mapping: dict[str, str]) -> None:
        encoded = encode_namespace_path(remote_path)
        self._request(
            "POST",
            f"namespace/{encoded}",
            payload={"action": "set-xattr", "attributes": mapping},
        )

    def remove_xattrs(self, remote_path: str, names: list[str] | tuple[str, ...] | set[str] | str) -> None:
        encoded = encode_namespace_path(remote_path)
        payload_names: object
        if isinstance(names, str):
            payload_names = names
        else:
            payload_names = list(names)
        self._request(
            "POST",
            f"namespace/{encoded}",
            payload={"action": "rm-xattr", "names": payload_names},
        )
