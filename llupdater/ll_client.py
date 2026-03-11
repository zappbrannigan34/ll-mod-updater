import datetime as dt
import json
import re
from pathlib import Path
from urllib.parse import unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .config import BASE_URL, CATEGORY_URL_TEMPLATE

try:
    import cloudscraper  # type: ignore
except Exception:  # pragma: no cover
    cloudscraper = None


class LLRequestError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        signal: str = "",
        retry_after: int | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.signal = signal
        self.retry_after = retry_after


def normalize_mod_url(url: str) -> str:
    url = url.strip()
    if not url:
        raise ValueError("Empty URL")
    if not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url
    if "loverslab.com" not in url:
        raise ValueError("URL must point to loverslab.com")
    if "/files/file/" not in url:
        raise ValueError("URL must be a LoversLab file URL")
    parsed = urlparse(url)
    clean = f"https://{parsed.netloc}{parsed.path}"
    if not clean.endswith("/"):
        clean += "/"
    return clean


def extract_mod_id(url: str) -> str:
    match = re.search(r"/files/file/(\d+)-", url)
    if match:
        return match.group(1)
    # fallback for unknown format
    return re.sub(r"\W+", "_", url).strip("_")


def _session(settings: dict) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": settings.get("user_agent", ""),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )
    cookie = (settings.get("ll_cookie") or "").strip()
    if cookie:
        session.headers["Cookie"] = cookie
    return session


def _cloudflare_blocked(resp: requests.Response) -> bool:
    if resp.status_code not in (403, 503):
        return False
    body = resp.text[:8000].lower()
    return "just a moment" in body or "cloudflare" in body


def _parse_retry_after(value: str | None) -> int | None:
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return int(value)
    return None


def _raise_for_http_error(resp: requests.Response, context: str) -> None:
    status = resp.status_code
    retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
    snippet = (resp.text or "")[:400].lower()

    if status == 429:
        raise LLRequestError(
            f"{context}: HTTP 429 (rate limited)",
            status_code=status,
            signal="http_429",
            retry_after=retry_after,
        )
    if status == 503:
        raise LLRequestError(
            f"{context}: HTTP 503",
            status_code=status,
            signal="http_503",
            retry_after=retry_after,
        )
    if status == 403 and ("cloudflare" in snippet or "just a moment" in snippet):
        raise LLRequestError(
            f"{context}: Cloudflare challenge/block",
            status_code=status,
            signal="cloudflare_challenge",
            retry_after=retry_after,
        )
    if status == 403 and "error 1020" in snippet:
        raise LLRequestError(
            f"{context}: Cloudflare 1020 access denied",
            status_code=status,
            signal="cloudflare_1020",
            retry_after=retry_after,
        )

    raise LLRequestError(
        f"{context}: HTTP {status}",
        status_code=status,
        signal="http_error",
        retry_after=retry_after,
    )


def _get_html(url: str, settings: dict) -> str:
    timeout = int(settings.get("request_timeout", 45))
    session = _session(settings)
    resp = session.get(url, timeout=timeout, allow_redirects=True)

    if _cloudflare_blocked(resp):
        if cloudscraper is None:
            raise LLRequestError(
                "Cloudflare blocked request. Add ll_cookie from logged-in browser or install cloudscraper support.",
                status_code=resp.status_code,
                signal="cloudflare_challenge",
                retry_after=_parse_retry_after(resp.headers.get("Retry-After")),
            )
        scraper = cloudscraper.create_scraper()
        scraper.headers.update(session.headers)
        resp = scraper.get(url, timeout=timeout, allow_redirects=True)

    if resp.status_code >= 400:
        _raise_for_http_error(resp, "Request failed")
    return resp.text


def discover_mods(scan_pages: int, settings: dict) -> list[dict]:
    found: dict[str, dict] = {}
    pages = max(1, int(scan_pages))

    for page in range(1, pages + 1):
        page_url = CATEGORY_URL_TEMPLATE.format(page=page)
        html = _get_html(page_url, settings)
        soup = BeautifulSoup(html, "html.parser")

        for link in soup.select("a[href*='/files/file/']"):
            href = link.get("href") or ""
            if "/files/file/" not in href:
                continue
            full_url = urljoin(BASE_URL, href)
            mod_id = extract_mod_id(full_url)
            title = link.get_text(" ", strip=True)
            if not title:
                title = f"Mod {mod_id}"

            if mod_id not in found:
                found[mod_id] = {
                    "id": mod_id,
                    "title": title,
                    "url": normalize_mod_url(full_url),
                }

    return sorted(found.values(), key=lambda x: int(x["id"]) if x["id"].isdigit() else x["id"])


def _json_ld_webapp_data(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = (script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            if item.get("@type") in {"WebApplication", "SoftwareApplication"}:
                return item
    return {}


def fetch_mod_metadata(mod_url: str, settings: dict) -> dict:
    html = _get_html(mod_url, settings)
    soup = BeautifulSoup(html, "html.parser")
    webapp = _json_ld_webapp_data(html)

    title = (
        webapp.get("name")
        or (soup.select_one("meta[property='og:title']") or {}).get("content")
        or (soup.title.string.strip() if soup.title and soup.title.string else "")
    )

    software_version = webapp.get("softwareVersion", "")
    date_modified = webapp.get("dateModified", "")
    download_url = webapp.get("downloadUrl", "")

    if not date_modified:
        og_updated = soup.select_one("meta[property='og:updated_time']")
        if og_updated:
            date_modified = og_updated.get("content", "")

    if not download_url:
        download_url = mod_url.rstrip("/") + "/?do=download"

    if not software_version and title:
        match = re.search(r"\bv\d+[\w\-.\[\] ]*", title, flags=re.IGNORECASE)
        if match:
            software_version = match.group(0).strip()

    return {
        "title": title or mod_url,
        "software_version": str(software_version or "").strip(),
        "date_modified": str(date_modified or "").strip(),
        "download_url": str(download_url or "").strip(),
    }


def _filename_from_response(resp: requests.Response, fallback_url: str) -> str:
    disposition = resp.headers.get("content-disposition", "")
    match = re.search(r"filename\*=UTF-8''([^;]+)", disposition)
    if match:
        return unquote(match.group(1)).strip('"')
    match = re.search(r'filename="?([^";]+)"?', disposition)
    if match:
        return unquote(match.group(1)).strip()

    parsed = urlparse(fallback_url)
    name = Path(parsed.path).name
    return unquote(name) or "download.bin"


def _safe_filename(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._\- ]+", "_", name).strip()
    return cleaned or "download.bin"


def download_mod_file(download_url: str, settings: dict, downloads_dir: Path) -> Path:
    timeout = int(settings.get("request_timeout", 45))
    session = _session(settings)
    resp = session.get(download_url, timeout=timeout, allow_redirects=True, stream=True)

    if _cloudflare_blocked(resp):
        if cloudscraper is None:
            raise LLRequestError(
                "Cloudflare blocked file download. Refresh ll_cookie from logged-in browser.",
                status_code=resp.status_code,
                signal="cloudflare_challenge",
                retry_after=_parse_retry_after(resp.headers.get("Retry-After")),
            )
        scraper = cloudscraper.create_scraper()
        scraper.headers.update(session.headers)
        resp = scraper.get(download_url, timeout=timeout, allow_redirects=True, stream=True)

    if resp.status_code >= 400:
        _raise_for_http_error(resp, "Download failed")

    content_type = (resp.headers.get("content-type") or "").lower()
    disposition = resp.headers.get("content-disposition") or ""

    if "text/html" in content_type and "filename" not in disposition:
        body = resp.text[:1000].lower()
        if "sign in" in body or "login" in body:
            raise LLRequestError(
                "Received login page instead of file",
                status_code=200,
                signal="login_required",
            )
        if "just a moment" in body or "cloudflare" in body:
            raise LLRequestError(
                "Received Cloudflare challenge page instead of file",
                status_code=200,
                signal="cloudflare_challenge",
            )
        raise LLRequestError(
            "Received HTML instead of file",
            status_code=200,
            signal="unexpected_html",
        )

    downloads_dir.mkdir(parents=True, exist_ok=True)

    filename = _safe_filename(_filename_from_response(resp, str(resp.url)))
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = downloads_dir / f"{timestamp}_{filename}"

    with out_path.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 512):
            if chunk:
                f.write(chunk)

    return out_path
