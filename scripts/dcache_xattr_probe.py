#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import posixpath
import re
import shlex
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zlib
from pathlib import Path

from dcache_cp.cli import (
    load_rclone_config,
    parse_remote_prefix,
    resolve_api_url,
    resolve_config_for_prefix,
    resolve_remote_name,
    run_command,
)
from dcache_cp.tempfiles import get_temp_root, named_tempfile


LOG = logging.getLogger("dcache_xattr_probe")


class _ApiError(RuntimeError):
    def __init__(self, method: str, url: str, status: int, body: str, headers: dict[str, str] | None = None):
        detail = body.strip() or "<empty response body>"
        super().__init__(f"{method} {url} failed with HTTP {status}: {detail}")
        self.method = method
        self.url = url
        self.status = status
        self.body = body
        self.headers = headers or {}


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def _trim_debug_text(text: str, *, limit: int = 4000) -> str:
    cleaned = text.strip()
    if not cleaned:
        return "<empty>"
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[:limit]}\n... <truncated {len(cleaned) - limit} chars>"


def _payload_debug_summary(payload: dict[str, object] | None) -> str:
    if payload is None:
        return "<none>"

    summary: dict[str, object] = {}
    for key, value in payload.items():
        if isinstance(value, str):
            summary[key] = {
                "type": "str",
                "len": len(value),
                "preview": value[:160],
            }
        elif isinstance(value, dict):
            nested: dict[str, object] = {}
            for nested_key, nested_value in value.items():
                if isinstance(nested_value, str):
                    nested[nested_key] = {
                        "type": "str",
                        "len": len(nested_value),
                        "preview": nested_value[:160],
                    }
                else:
                    nested[nested_key] = nested_value
            summary[key] = nested
        else:
            summary[key] = value
    return json.dumps(summary, ensure_ascii=True, sort_keys=True)


def _headers_debug_summary(headers: dict[str, str]) -> str:
    if not headers:
        return "<none>"
    return json.dumps(dict(sorted(headers.items())), ensure_ascii=True)


def _normalize_remote_path(remote_path: str) -> str:
    cleaned = remote_path.strip("/")
    return f"/{cleaned}" if cleaned else "/"


def _encode_namespace_path(remote_path: str) -> str:
    return urllib.parse.quote(_normalize_remote_path(remote_path), safe="")


def _default_probe_ada() -> str:
    env = os.environ.get("ADA")
    if env:
        return env
    candidate = Path(__file__).resolve().parents[1] / "src" / "dcache_cp" / "vendor" / "ada"
    try:
        if candidate.is_file():
            return str(candidate)
    except OSError:
        pass
    return "ada"


def _read_shell_config_value(path: Path, key: str) -> str | None:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None

    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line or "=" not in line:
            continue
        name, value = line.split("=", 1)
        if name.strip() != key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        return value.strip() or None
    return None


def _read_ada_default_api() -> str | None:
    vendor_default = Path(__file__).resolve().parents[1] / "src" / "dcache_cp" / "vendor" / "etc" / "ada.conf"
    candidates = [vendor_default, Path("/etc/ada.conf"), Path.home() / ".ada" / "ada.conf"]
    api: str | None = None
    for candidate in candidates:
        value = _read_shell_config_value(candidate, "api")
        if value:
            api = value
    return api


def _resolve_probe_api(explicit: str | None, remote_cfg) -> str | None:
    return (
        explicit
        or os.environ.get("DCACHE_API")
        or os.environ.get("ADA_API")
        or os.environ.get("ada_api")
        or remote_cfg.get("api", fallback=None)
        or _read_ada_default_api()
    )


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

    first_line = text.splitlines()[0].strip() if text.splitlines() else ""
    return first_line or None


def _extract_bearer_token(remote_cfg, tokenfile: Path) -> str | None:
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

    file_token = _extract_bearer_token_from_file(tokenfile)
    if file_token:
        return file_token

    return None


class _ApiXattrBackend:
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
    ) -> object:
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
            response_headers = {key: value for key, value in exc.headers.items()}
            raise _ApiError(method, url, exc.code, body, response_headers) from None
        except urllib.error.URLError as exc:
            raise RuntimeError(f"{method} {url} failed: {exc.reason}") from exc

        if not body.strip():
            return {}
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"{method} {url} returned non-JSON data: {body[:400]}") from exc

    def _set_xattrs(self, encoded: str, mapping: dict[str, str]) -> None:
        self._request(
            "POST",
            f"namespace/{encoded}",
            payload={"action": "set-xattr", "attributes": mapping},
        )

    def _remove_xattr(self, encoded: str, key: str) -> None:
        self._request(
            "POST",
            f"namespace/{encoded}",
            payload={"action": "rm-xattr", "names": [key]},
        )

    def set_xattrs(self, remote_path: str, mapping: dict[str, str]) -> None:
        encoded = _encode_namespace_path(remote_path)
        self._set_xattrs(encoded, mapping)

    def list_xattrs(self, remote_path: str) -> dict[str, str]:
        encoded = _encode_namespace_path(remote_path)
        data = self._request("GET", f"namespace/{encoded}", params={"xattr": "true"})
        if not isinstance(data, dict):
            raise RuntimeError(f"unexpected xattr listing response for {_normalize_remote_path(remote_path)!r}: {data!r}")
        xattrs = data.get("extendedAttributes", {})
        return dict(xattrs) if isinstance(xattrs, dict) else {}

    def get_xattr(self, remote_path: str, key: str) -> str | None:
        value = self.list_xattrs(remote_path).get(key)
        return str(value) if value is not None else None

    def remove_xattr(self, remote_path: str, key: str) -> None:
        encoded = _encode_namespace_path(remote_path)
        self._remove_xattr(encoded, key)

    def find_xattr_paths(self, remote_dir: str, key: str, value_regex: str) -> list[str]:
        pattern = re.compile(value_regex)
        matches: list[str] = []
        self._find_xattr_in_dir(_normalize_remote_path(remote_dir), key, pattern, matches)
        return matches

    def _find_xattr_in_dir(
        self,
        remote_dir: str,
        key: str,
        pattern: re.Pattern[str],
        matches: list[str],
    ) -> None:
        encoded = _encode_namespace_path(remote_dir)
        data = self._request(
            "GET",
            f"namespace/{encoded}",
            params={"children": "true", "xattr": "true"},
        )
        if not isinstance(data, dict):
            raise RuntimeError(f"unexpected recursive xattr response for {remote_dir!r}: {data!r}")

        self._append_match(matches, remote_dir, data.get("extendedAttributes", {}), key, pattern)

        base = remote_dir.rstrip("/") or "/"
        for child in data.get("children", []) or []:
            if not isinstance(child, dict):
                continue
            file_name = child.get("fileName")
            if not isinstance(file_name, str) or not file_name:
                continue
            child_path = posixpath.join(base, file_name)
            self._append_match(matches, child_path, child.get("extendedAttributes", {}), key, pattern)
            if child.get("fileType") == "DIR":
                self._find_xattr_in_dir(child_path, key, pattern, matches)

    @staticmethod
    def _append_match(
        matches: list[str],
        path: str,
        xattrs: object,
        key: str,
        pattern: re.Pattern[str],
    ) -> None:
        if not isinstance(xattrs, dict):
            return
        if key == "--all":
            if any(isinstance(value, str) and pattern.search(value) for value in xattrs.values()):
                matches.append(path)
            return
        value = xattrs.get(key)
        if isinstance(value, str) and pattern.search(value):
            matches.append(path)


class _AdaXattrBackend:
    def __init__(self, ada_cmd: str, tokenfile: Path, api: str | None):
        self.ada_base = _ada_base_cmd(ada_cmd, tokenfile, api)

    def set_xattrs(self, remote_path: str, mapping: dict[str, str]) -> None:
        payload_file = _write_attr_file(mapping)
        try:
            run_command(self.ada_base + ["--setxattr", _normalize_remote_path(remote_path), payload_file])
        finally:
            Path(payload_file).unlink(missing_ok=True)

    def list_xattrs(self, remote_path: str) -> dict[str, str]:
        result = run_command(self.ada_base + ["--lsxattr", _normalize_remote_path(remote_path)])
        output = (result.stdout or "").strip()
        if not output:
            return {}
        data = json.loads(output)
        return data if isinstance(data, dict) else {}

    def get_xattr(self, remote_path: str, key: str) -> str | None:
        result = run_command(
            self.ada_base + ["--lsxattr", _normalize_remote_path(remote_path), key],
            check=False,
        )
        if result.returncode != 0:
            return None
        output = (result.stdout or "").strip()
        if not output:
            return None
        value = json.loads(output)
        return value if isinstance(value, str) else json.dumps(value, ensure_ascii=True, separators=(",", ":"))

    def remove_xattr(self, remote_path: str, key: str) -> None:
        run_command(self.ada_base + ["--rmxattr", _normalize_remote_path(remote_path), key])

    def find_xattr_paths(self, remote_dir: str, key: str, value_regex: str) -> list[str]:
        result = run_command(
            self.ada_base
            + ["--findxattr", _normalize_remote_path(remote_dir), key, value_regex, "--recursive"],
            check=False,
        )
        if result.returncode != 0:
            return []
        paths: list[str] = []
        for line in (result.stdout or "").splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            path, _, _ = stripped.partition("\t")
            if path:
                paths.append(path)
        return paths


def _ada_base_cmd(ada_cmd: str, tokenfile: Path, api: str | None) -> list[str]:
    cmd = [ada_cmd, "--tokenfile", str(tokenfile)]
    if api:
        cmd += ["--api", api]
    return cmd


def _assert_ada_supports_xattrs(ada_cmd: str) -> None:
    result = run_command([ada_cmd, "--help"], check=False)
    help_text = (result.stdout or "") + (result.stderr or "")
    required = ("--setxattr", "--lsxattr", "--rmxattr", "--findxattr")
    missing = [flag for flag in required if flag not in help_text]
    if missing:
        raise RuntimeError(
            f"ada executable {ada_cmd!r} does not support xattr commands; missing: {', '.join(missing)}"
        )


def _remote_target(remote: str, remote_path: str) -> str:
    cleaned = remote_path.strip("/")
    return f"{remote}:{cleaned}" if cleaned else f"{remote}:"


def _payload_of_size(target_bytes: int) -> str:
    lines: list[str] = []
    size = 0
    index = 0
    while size < target_bytes:
        line = (
            f"cohort_{index % 8}/batch_{index % 17}/sample_{index:06d}.fastq.gz"
            f"\tbundle_{index % 23:03d}"
        )
        lines.append(line)
        size += len(line) + 1
        index += 1
    return "\n".join(lines)


def _encode_payload(text: str) -> str:
    compressed = zlib.compress(text.encode("utf-8"), level=9)
    return base64.urlsafe_b64encode(compressed).decode("ascii")


def _write_attr_file(payload: dict[str, str]) -> str:
    handle = named_tempfile("w", suffix=".json", delete=False, encoding="utf-8")
    try:
        json.dump(payload, handle, ensure_ascii=True)
        handle.write("\n")
        handle.close()
        return handle.name
    except Exception:
        handle.close()
        Path(handle.name).unlink(missing_ok=True)
        raise


def _assert_contains(haystack: str, needle: str, *, context: str) -> None:
    if needle not in haystack:
        raise AssertionError(f"did not find {needle!r} in {context}\n--- output ---\n{haystack}")


def _write_local_probe_tree(root: Path) -> None:
    (root / "alpha.txt").write_text("alpha\n", encoding="utf-8")
    (root / "nested").mkdir(parents=True, exist_ok=True)
    (root / "nested" / "beta.txt").write_text("beta\n", encoding="utf-8")
    (root / "nested" / "deep").mkdir(parents=True, exist_ok=True)
    (root / "nested" / "deep" / "gamma.bin").write_bytes(b"gamma\x00\x01\x02\n")


def _probe(args: argparse.Namespace) -> None:
    prefix, remote_path = parse_remote_prefix(args.target)
    if not prefix:
        raise ValueError("target must have a remote prefix such as dcache:/path/to/test")

    config_path = resolve_config_for_prefix(prefix, args.config)
    config = load_rclone_config(config_path)
    remote = resolve_remote_name(config, args.remote)
    remote_cfg = config[remote]
    api = _resolve_probe_api(args.api, remote_cfg)
    bearer_token = _extract_bearer_token(remote_cfg, config_path)

    LOG.info("config   : %s", config_path)
    LOG.info("remote   : %s", remote)
    LOG.info("api      : %s", api or "<none>")
    LOG.info("token    : %s", "present" if bearer_token else "missing")

    if api and bearer_token:
        xattr_backend = _ApiXattrBackend(api, bearer_token)
        backend_name = "api"
    else:
        LOG.info("xattrs   : falling back to ada backend because direct REST config is incomplete")
        _assert_ada_supports_xattrs(args.ada)
        xattr_backend = _AdaXattrBackend(args.ada, config_path, api)
        backend_name = "ada"

    run_id = f"probe-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
    remote_root = posixpath.join(remote_path.strip("/"), run_id) if remote_path.strip("/") else run_id
    remote_root_target = _remote_target(remote, remote_root)

    LOG.info("xattrs   : %s backend", backend_name)
    if backend_name == "api":
        LOG.info("xattr api: POST /namespace/<path> action=set-xattr|rm-xattr and GET ?xattr=true")
    LOG.info("test root: %s", remote_root_target)

    success = False
    try:
        with tempfile.TemporaryDirectory(prefix="dcache-xattr-probe-", dir=str(get_temp_root())) as tmpdir:
            local_root = Path(tmpdir) / "probe"
            local_root.mkdir(parents=True, exist_ok=True)
            _write_local_probe_tree(local_root)

            run_command(["rclone", "--config", str(config_path), "mkdir", remote_root_target])
            run_command(["rclone", "--config", str(config_path), "copy", str(local_root), remote_root_target])

            remote_alpha = posixpath.join(remote_root, "alpha.txt")
            remote_nested = posixpath.join(remote_root, "nested")
            remote_beta = posixpath.join(remote_root, "nested", "beta.txt")

            LOG.info("testing directory xattrs on /%s", remote_root)
            dir_summary_v1 = json.dumps({"kind": "root", "run_id": run_id, "version": 1}, separators=(",", ":"))
            dir_summary_v2 = json.dumps({"kind": "root", "run_id": run_id, "version": 2}, separators=(",", ":"))
            xattr_backend.set_xattrs(
                remote_root,
                {
                    "dcache_cp.probe.marker": run_id,
                    "dcache_cp.probe.summary": dir_summary_v1,
                },
            )
            listing = xattr_backend.list_xattrs(remote_root)
            if listing.get("dcache_cp.probe.marker") != run_id:
                raise AssertionError(f"unexpected root marker xattr: {listing!r}")
            if listing.get("dcache_cp.probe.summary") != dir_summary_v1:
                raise AssertionError(f"unexpected root summary xattr: {listing!r}")

            xattr_backend.set_xattrs(remote_root, {"dcache_cp.probe.summary": dir_summary_v2})
            updated_summary = xattr_backend.get_xattr(remote_root, "dcache_cp.probe.summary")
            if updated_summary != dir_summary_v2:
                raise AssertionError(f"unexpected updated directory summary: {updated_summary!r}")

            LOG.info("testing nested directory xattrs on /%s", remote_nested)
            xattr_backend.set_xattrs(
                remote_nested,
                {
                    "dcache_cp.probe.dir_kind": "nested",
                    "dcache_cp.probe.marker": run_id,
                },
            )
            nested_listing = xattr_backend.list_xattrs(remote_nested)
            if nested_listing.get("dcache_cp.probe.dir_kind") != "nested":
                raise AssertionError(f"unexpected nested directory xattrs: {nested_listing!r}")

            LOG.info("testing file xattrs on /%s", remote_alpha)
            xattr_backend.set_xattrs(remote_alpha, {"dcache_cp.probe.short": "hello-world"})
            short_listing = xattr_backend.get_xattr(remote_alpha, "dcache_cp.probe.short")
            if short_listing != "hello-world":
                raise AssertionError(f"unexpected short file xattr: {short_listing!r}")

            for size in args.plain_size:
                payload = _payload_of_size(size)
                LOG.info("testing plain payload update on /%s (%d bytes)", remote_alpha, len(payload.encode("utf-8")))
                xattr_backend.set_xattrs(remote_alpha, {"dcache_cp.probe.long_plain": payload})
                long_listing = xattr_backend.get_xattr(remote_alpha, "dcache_cp.probe.long_plain")
                if long_listing != payload:
                    raise AssertionError(f"plain payload mismatch for {size}: {long_listing!r}")

            for size in args.encoded_size:
                source_payload = _payload_of_size(size)
                encoded_payload = _encode_payload(source_payload)
                LOG.info(
                    "testing encoded payload update on /%s (source %d bytes, encoded %d chars)",
                    remote_beta,
                    len(source_payload.encode("utf-8")),
                    len(encoded_payload),
                )
                xattr_backend.set_xattrs(
                    remote_beta,
                    {
                        "dcache_cp.probe.codec": "tsv+zlib+base64url",
                        "dcache_cp.probe.encoded": encoded_payload,
                    },
                )
                encoded_listing = xattr_backend.get_xattr(remote_beta, "dcache_cp.probe.encoded")
                if encoded_listing != encoded_payload:
                    raise AssertionError(f"encoded payload mismatch for {size}: {encoded_listing!r}")

            LOG.info("testing recursive findxattr from /%s", remote_root)
            find_paths = xattr_backend.find_xattr_paths(remote_root, "dcache_cp.probe.marker", f"^{run_id}$")
            if _normalize_remote_path(remote_root) not in find_paths:
                raise AssertionError(f"recursive xattr search did not find root marker: {find_paths!r}")
            if _normalize_remote_path(remote_nested) not in find_paths:
                raise AssertionError(f"recursive xattr search did not find nested marker: {find_paths!r}")

            LOG.info("testing xattr removal on /%s", remote_alpha)
            xattr_backend.remove_xattr(remote_alpha, "dcache_cp.probe.short")
            removed_listing = xattr_backend.get_xattr(remote_alpha, "dcache_cp.probe.short")
            if removed_listing is not None:
                raise AssertionError(f"removed xattr value still visible: {removed_listing!r}")

            success = True
            LOG.info("probe completed successfully for %s", remote_root_target)
    finally:
        if args.keep_remote:
            LOG.info("kept remote test tree: %s", remote_root_target)
        elif not success:
            LOG.warning("probe failed; keeping remote test tree for inspection: %s", remote_root_target)
        else:
            LOG.info("cleaning up remote test tree: %s", remote_root_target)
            run_command(["rclone", "--config", str(config_path), "purge", remote_root_target], check=False)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create a small remote test tree on dCache and exercise file and directory xattrs, "
            "using the dCache namespace REST API directly and falling back to the ada CLI only when needed."
        )
    )
    parser.add_argument("target", help="Remote target directory, e.g. dcache:/tmp/xattr-probe")
    parser.add_argument("--config", type=Path, help="Explicit rclone config path")
    parser.add_argument("--remote", help="Explicit rclone remote name")
    parser.add_argument(
        "--ada",
        default=_default_probe_ada(),
        help="ada executable used only if the script cannot use the direct REST xattr backend",
    )
    parser.add_argument("--api", help="Explicit dCache API URL")
    parser.add_argument(
        "--plain-size",
        type=int,
        action="append",
        default=[],
        help="Target plain-text payload size in bytes; may be specified multiple times",
    )
    parser.add_argument(
        "--encoded-size",
        type=int,
        action="append",
        default=[],
        help="Target source payload size before zlib+base64url encoding; may be specified multiple times",
    )
    parser.add_argument("--keep-remote", action="store_true", help="Do not remove the remote probe tree after the run")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if not args.plain_size:
        args.plain_size = [4096, 65536, 262144]
    if not args.encoded_size:
        args.encoded_size = [65536, 262144]
    _setup_logging(args.verbose)
    try:
        _probe(args)
    except Exception as exc:
        LOG.error("probe failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
