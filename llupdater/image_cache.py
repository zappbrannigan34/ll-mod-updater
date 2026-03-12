import hashlib
import re
from pathlib import Path
from urllib.parse import urlparse

import requests

from .config import MEDIA_CACHE_DIR
from .net import apply_network_to_session

_ALLOWED_EXT = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".avif"}
_CONTENT_TYPE_EXT = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "image/bmp": ".bmp",
    "image/avif": ".avif",
}
_SAFE_NAME = re.compile(r"^[a-f0-9]{64}\.[a-z0-9]{2,5}$")


def is_safe_cached_filename(name: str) -> bool:
    return bool(_SAFE_NAME.match(name or ""))


def media_api_url(name: str) -> str:
    return f"/api/media/{name}"


def _hash_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def _existing_file(url_hash: str) -> Path | None:
    for path in MEDIA_CACHE_DIR.glob(f"{url_hash}.*"):
        if path.is_file():
            return path
    return None


def _is_stale(path: Path, max_age_hours: int) -> bool:
    age_seconds = max(0.0, (Path(path).stat().st_mtime))
    import time

    return (time.time() - age_seconds) > max_age_hours * 3600


def _pick_ext(url: str, content_type: str) -> str:
    ctype = (content_type or "").split(";", 1)[0].strip().lower()
    if ctype in _CONTENT_TYPE_EXT:
        return _CONTENT_TYPE_EXT[ctype]

    suffix = Path(urlparse(url).path).suffix.lower()
    if suffix in _ALLOWED_EXT:
        return suffix
    return ".img"


def cache_remote_image(url: str, settings: dict, force_refresh: bool = False) -> str:
    if not url:
        return ""

    MEDIA_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    url_hash = _hash_url(url)
    existing = _existing_file(url_hash)
    max_age_hours = max(1, int(settings.get("image_cache_hours", 720)))

    if existing is not None and not force_refresh and not _is_stale(existing, max_age_hours):
        return existing.name

    timeout = max(10, int(settings.get("request_timeout", 45)))
    headers = {
        "User-Agent": str(settings.get("user_agent") or "Mozilla/5.0"),
        "Referer": "https://www.loverslab.com/",
    }

    session = requests.Session()
    apply_network_to_session(session, settings)

    try:
        resp = session.get(url, headers=headers, timeout=timeout, stream=True)
        if resp.status_code != 200:
            return existing.name if existing is not None else ""

        content_type = resp.headers.get("content-type", "")
        if not content_type.lower().startswith("image/"):
            return existing.name if existing is not None else ""

        ext = _pick_ext(url, content_type)
        out_path = MEDIA_CACHE_DIR / f"{url_hash}{ext}"
        tmp_path = MEDIA_CACHE_DIR / f"{url_hash}.tmp"

        with tmp_path.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)

        if existing is not None and existing != out_path and existing.exists():
            existing.unlink(missing_ok=True)

        tmp_path.replace(out_path)
        return out_path.name
    except Exception:
        return existing.name if existing is not None else ""
