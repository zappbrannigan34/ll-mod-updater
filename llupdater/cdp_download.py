import asyncio
import json
import time
from pathlib import Path

import requests
import websockets


class CDPDownloadError(RuntimeError):
    pass


def _endpoint_json_url(cdp_endpoint: str) -> str:
    base = (cdp_endpoint or "http://127.0.0.1:9222").rstrip("/")
    return base + "/json/version"


def _tmp_suffix(path: Path) -> bool:
    lowered = path.name.lower()
    return lowered.endswith(".crdownload") or lowered.endswith(".tmp")


def download_via_cdp(download_url: str, downloads_dir: Path, settings: dict) -> Path:
    cdp_endpoint = str(settings.get("cdp_endpoint") or "http://127.0.0.1:9222")
    timeout_sec = max(30, int(settings.get("cdp_download_timeout_seconds", 300)))
    return asyncio.run(_download_via_cdp_async(download_url, downloads_dir, cdp_endpoint, timeout_sec))


async def _download_via_cdp_async(
    download_url: str,
    downloads_dir: Path,
    cdp_endpoint: str,
    timeout_sec: int,
) -> Path:
    downloads_dir.mkdir(parents=True, exist_ok=True)

    try:
        version_info = requests.get(_endpoint_json_url(cdp_endpoint), timeout=5).json()
    except Exception as exc:
        raise CDPDownloadError(f"Cannot reach CDP endpoint: {cdp_endpoint}") from exc

    ws_url = version_info.get("webSocketDebuggerUrl")
    if not ws_url:
        raise CDPDownloadError("CDP endpoint missing webSocketDebuggerUrl")

    before = {p.name: p.stat().st_mtime for p in downloads_dir.iterdir() if p.is_file()}

    async with websockets.connect(ws_url, max_size=2**24, ping_interval=None) as ws:
        request_id = 0

        async def call(method: str, params: dict | None = None, session_id: str | None = None, timeout: int = 20) -> dict:
            nonlocal request_id
            request_id += 1
            payload = {"id": request_id, "method": method, "params": params or {}}
            if session_id:
                payload["sessionId"] = session_id

            await ws.send(json.dumps(payload))
            deadline = time.time() + timeout

            while True:
                remaining = max(0.1, deadline - time.time())
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except TimeoutError as exc:
                    raise CDPDownloadError(f"CDP timeout waiting for response: {method}") from exc

                message = json.loads(raw)
                if message.get("id") != request_id:
                    continue

                if "error" in message:
                    err = message.get("error", {})
                    raise CDPDownloadError(f"CDP error {method}: {err.get('message', err)}")

                return message

        target_response = await call("Target.createTarget", {"url": "about:blank"})
        target_id = target_response.get("result", {}).get("targetId")
        if not target_id:
            raise CDPDownloadError("CDP did not return targetId")

        attach_response = await call("Target.attachToTarget", {"targetId": target_id, "flatten": True})
        session_id = attach_response.get("result", {}).get("sessionId")
        if not session_id:
            raise CDPDownloadError("CDP did not return sessionId")

        await call(
            "Browser.setDownloadBehavior",
            {
                "behavior": "allow",
                "downloadPath": str(downloads_dir),
                "eventsEnabled": True,
            },
            timeout=10,
        )

        await call("Page.enable", {}, session_id=session_id, timeout=10)
        await call("Page.navigate", {"url": download_url}, session_id=session_id, timeout=20)
        await asyncio.sleep(2)
        await call(
            "Runtime.evaluate",
            {
                "expression": "(() => { const a = document.querySelector(\"a[href*='do=download'][href*='confirm=1']\"); if (a) { a.click(); return a.href; } return ''; })()",
                "returnByValue": True,
            },
            session_id=session_id,
            timeout=15,
        )

        deadline = time.time() + timeout_sec
        stable_hits = 0
        stable_key: tuple[str, int] | None = None
        active_guid: str | None = None
        suggested_filename = ""

        while time.time() < deadline:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1)
                message = json.loads(raw)
                method = str(message.get("method") or "")
                params = message.get("params") or {}

                if method == "Browser.downloadWillBegin":
                    url = str(params.get("url") or "")
                    if "loverslab.com" in url or not active_guid:
                        active_guid = str(params.get("guid") or "") or active_guid
                        suggested_filename = str(params.get("suggestedFilename") or "")

                elif method == "Browser.downloadProgress":
                    guid = str(params.get("guid") or "")
                    state = str(params.get("state") or "")

                    if active_guid and guid and guid != active_guid:
                        pass
                    else:
                        if not active_guid and guid:
                            active_guid = guid

                        if state == "canceled":
                            raise CDPDownloadError("CDP download canceled")

                        if state == "completed":
                            file_path = str(params.get("filePath") or "").strip()
                            if file_path:
                                resolved = Path(file_path)
                                if resolved.exists() and resolved.is_file():
                                    await call("Target.closeTarget", {"targetId": target_id}, timeout=10)
                                    return resolved

                            if active_guid:
                                guid_path = downloads_dir / active_guid
                                if guid_path.exists() and guid_path.is_file():
                                    await call("Target.closeTarget", {"targetId": target_id}, timeout=10)
                                    return guid_path

                            if suggested_filename:
                                suggested_path = downloads_dir / suggested_filename
                                if suggested_path.exists() and suggested_path.is_file():
                                    await call("Target.closeTarget", {"targetId": target_id}, timeout=10)
                                    return suggested_path
            except TimeoutError:
                pass

            candidates: list[Path] = []
            active_tmp = False

            for path in downloads_dir.iterdir():
                if not path.is_file():
                    continue

                if path.name not in before:
                    if _tmp_suffix(path):
                        active_tmp = True
                    else:
                        candidates.append(path)
                    continue

                previous_mtime = before.get(path.name, 0.0)
                if path.stat().st_mtime > previous_mtime + 0.2:
                    if _tmp_suffix(path):
                        active_tmp = True
                    else:
                        candidates.append(path)

            if candidates and not active_tmp:
                latest = max(candidates, key=lambda p: p.stat().st_mtime)
                size = latest.stat().st_size
                key = (str(latest), size)

                if stable_key == key:
                    stable_hits += 1
                else:
                    stable_key = key
                    stable_hits = 0

                if stable_hits >= 2:
                    await call("Target.closeTarget", {"targetId": target_id}, timeout=10)
                    return latest

        await call("Target.closeTarget", {"targetId": target_id}, timeout=10)
        raise CDPDownloadError("CDP download timeout")
