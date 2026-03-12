import datetime as dt
import json
import re
from pathlib import Path
from typing import Callable
from urllib.parse import unquote, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from .config import BASE_URL, CATEGORY_URL_TEMPLATE, SIMS4_CATEGORY_URL
from .net import apply_network_to_session

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


_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".avif"}
_NOISE_IMAGE_PATH_PARTS = (
    "/resources/emoticons/",
    "/themes/",
    "/applications/core/interface/",
)


def _normalize_image_url(raw: str) -> str:
    value = str(raw or "").strip()
    if not value or value.startswith("data:"):
        return ""

    if "," in value and " " in value and not value.startswith("http") and not value.startswith("/"):
        value = value.split(",")[-1].strip()

    if " " in value:
        value = value.split(" ", 1)[0].strip()

    return urljoin(BASE_URL, value)


def _is_image_like_url(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    path = parsed.path.lower()
    if any(part in path for part in _NOISE_IMAGE_PATH_PARTS):
        return False
    ext = Path(path).suffix.lower()
    return ext in _IMAGE_EXTENSIONS


def _thumb_to_full_candidates(url: str) -> list[str]:
    out: list[str] = []
    parsed = urlparse(url)
    path = parsed.path

    variants: list[str] = []
    if "/thumb-" in path:
        variants.append(path.replace("/thumb-", "/"))
    if ".thumb." in path:
        variants.append(path.replace(".thumb.", ".", 1))

    for variant in variants:
        rebuilt = urlunparse((parsed.scheme, parsed.netloc, variant, parsed.params, parsed.query, parsed.fragment))
        if rebuilt != url and rebuilt not in out:
            out.append(rebuilt)

    return out


def _image_attr_candidates(img) -> list[str]:
    candidates: list[str] = []
    for attr in ("data-src", "data-original", "data-fileurl", "src"):
        raw = (img.get(attr) or "").strip()
        if raw:
            candidates.append(raw)

    srcset = (img.get("srcset") or "").strip()
    if srcset:
        for part in srcset.split(","):
            part = part.strip()
            if not part:
                continue
            url_only = part.split(" ", 1)[0].strip()
            if url_only:
                candidates.append(url_only)

    parent = getattr(img, "parent", None)
    if parent is not None and getattr(parent, "name", "") == "a":
        href = (parent.get("href") or "").strip()
        if href:
            candidates.append(href)

    return candidates


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


def extract_category_info(url: str) -> tuple[str, str]:
    match = re.search(r"/files/category/(\d+)-([^/]+)/", url)
    if match:
        return match.group(1), match.group(2)
    match = re.search(r"/files/category/(\d+)-([^/?#]+)", url)
    if match:
        return match.group(1), match.group(2)
    match = re.search(r"/files/category/(\d+)/", url)
    if match:
        return match.group(1), ""
    return "", ""


def _session(settings: dict) -> requests.Session:
    session = requests.Session()
    apply_network_to_session(session, settings)
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
        apply_network_to_session(scraper, settings)
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


def discover_sims4_categories(settings: dict) -> list[dict]:
    html = _get_html(SIMS4_CATEGORY_URL, settings)
    soup = BeautifulSoup(html, "html.parser")
    block = soup.select_one("#elDownloadsCategoriesBlock")
    if block is None:
        return []

    root_ul = block.select_one("ul.ipsSideMenu_list")
    if root_ul is None:
        return []

    categories: list[dict] = []

    def parse_ul(ul, parent_id: str | None = None) -> None:
        for li in ul.find_all("li", recursive=False):
            anchor = li.find("a", class_="ipsSideMenu_item", recursive=False)
            if anchor is None:
                continue

            href = anchor.get("href") or ""
            full_url = urljoin(BASE_URL, href)
            category_id, slug = extract_category_info(full_url)
            if not category_id:
                continue

            badge = anchor.select_one(".cDownloadsCategoryCount, .ipsBadge")
            raw_badge_text = badge.get_text(" ", strip=True) if badge else "0"
            count_text = raw_badge_text
            count_text = re.sub(r"[^0-9]", "", count_text)
            count = int(count_text) if count_text else 0

            strong = anchor.find("strong")
            strong_is_count = bool(strong and "cDownloadsCategoryCount" in (strong.get("class") or []))
            if strong and not strong_is_count:
                name = strong.get_text(" ", strip=True).strip()
            else:
                raw_name = anchor.get_text(" ", strip=True).strip()
                if count_text and raw_name.startswith(count_text):
                    raw_name = raw_name[len(count_text) :].strip()
                name = raw_name

            item = {
                "id": category_id,
                "name": name,
                "slug": slug,
                "url": full_url.rstrip("/") + "/",
                "count": count,
                "parent_id": parent_id,
            }
            categories.append(item)

            child_ul = li.find("ul", class_="ipsSideMenu_list", recursive=False)
            if child_ul is not None:
                parse_ul(child_ul, parent_id=category_id)

    parse_ul(root_ul)

    by_id: dict[str, dict] = {}
    for item in categories:
        by_id[item["id"]] = item

    ordered = list(by_id.values())
    ordered.sort(key=lambda x: int(x["id"]))
    return ordered


def _extract_last_page(soup: BeautifulSoup) -> int:
    max_page = 1
    for link in soup.select("a[href*='/page/']"):
        href = link.get("href") or ""
        match = re.search(r"/page/(\d+)/", href)
        if match:
            max_page = max(max_page, int(match.group(1)))
    return max_page


def _category_page_url(category_url: str, page: int) -> str:
    base = category_url.rstrip("/") + "/"
    if page <= 1:
        return base
    return urljoin(base, f"page/{page}/")


def _extract_downloads_count(text: str) -> int:
    raw = (text or "").lower()

    def parse_compact_number(token: str) -> int:
        value = (token or "").strip().lower().replace("_", "")
        if not value:
            return 0

        unit = ""
        if value[-1:] in {"k", "m", "b"}:
            unit = value[-1]
            value = value[:-1].strip()

        if not value:
            return 0

        if "," in value and "." not in value:
            parts = value.split(",")
            if len(parts[-1]) <= 2:
                value = value.replace(",", ".")
            else:
                value = value.replace(",", "")
        else:
            value = value.replace(",", "")

        try:
            base = float(value)
        except Exception:
            return 0

        scale = 1
        if unit == "k":
            scale = 1_000
        elif unit == "m":
            scale = 1_000_000
        elif unit == "b":
            scale = 1_000_000_000

        return max(0, int(base * scale))

    match_after = re.search(r"downloads?\s*[:\-]?\s*([0-9][0-9,._]*(?:\s*[kmb])?)\b", raw)
    if match_after:
        return parse_compact_number(match_after.group(1))

    match = re.search(r"([0-9][0-9,._]*(?:\s*[kmb])?)\s*downloads\b", raw)
    if match:
        return parse_compact_number(match.group(1))

    # fallback: if wording differs, try nearest numeric token before "downloads"
    idx = raw.find("downloads")
    if idx >= 0:
        prefix = raw[:idx].strip()
        tail = re.search(r"([0-9][0-9,._]*(?:\s*[kmb])?)\s*$", prefix)
        if tail:
            return parse_compact_number(tail.group(1))

    return 0


def discover_mods_in_category(
    category: dict,
    settings: dict,
    max_pages: int | None = None,
    page_delay_seconds: float = 0,
    on_page: Callable[[int, int, int], None] | None = None,
) -> list[dict]:
    category_url = str(category.get("url") or "").strip()
    if not category_url:
        return []

    first_html = _get_html(_category_page_url(category_url, 1), settings)
    first_soup = BeautifulSoup(first_html, "html.parser")
    last_page = _extract_last_page(first_soup)
    if max_pages is not None:
        last_page = max(1, min(last_page, int(max_pages)))

    found: dict[str, dict] = {}

    def parse_page_soup(soup: BeautifulSoup) -> None:
        table = soup.select_one(".cDownloadsCategoryTable")
        if table is None:
            return

        rows = table.select("ol.ipsDataList li.ipsDataItem")

        for row in rows:
            title_link = row.select_one(
                "span.ipsType_break a[href*='/files/file/'], h4 a[href*='/files/file/'], .ipsDataItem_main a[href*='/files/file/']"
            )
            file_link = title_link or row.select_one("a[href*='/files/file/']")
            if file_link is None:
                continue

            href = file_link.get("href") or ""
            if "/files/file/" not in href:
                continue

            full_url = urljoin(BASE_URL, href)
            mod_id = extract_mod_id(full_url)
            title = (title_link.get_text(" ", strip=True) if title_link else "").strip() or f"Mod {mod_id}"
            downloads_count = _extract_downloads_count(row.get_text(" ", strip=True))
            thumb_img = row.select_one("img[src]")
            thumbnail_url = urljoin(BASE_URL, thumb_img.get("src")) if thumb_img and thumb_img.get("src") else ""

            existing = found.get(mod_id)
            if existing is None:
                found[mod_id] = {
                    "id": mod_id,
                    "title": title,
                    "url": normalize_mod_url(full_url),
                    "category_id": str(category.get("id") or ""),
                    "category_name": str(category.get("name") or ""),
                    "downloads_count": downloads_count,
                    "thumbnail_url": thumbnail_url,
                }
            else:
                if existing.get("title", "").startswith("Mod ") and title:
                    existing["title"] = title
                existing["downloads_count"] = max(int(existing.get("downloads_count") or 0), downloads_count)
                if not existing.get("thumbnail_url") and thumbnail_url:
                    existing["thumbnail_url"] = thumbnail_url

    parse_page_soup(first_soup)
    if on_page is not None:
        on_page(1, last_page, len(found))

    for page in range(2, last_page + 1):
        page_html = _get_html(_category_page_url(category_url, page), settings)
        page_soup = BeautifulSoup(page_html, "html.parser")
        parse_page_soup(page_soup)
        if on_page is not None:
            on_page(page, last_page, len(found))
        if page_delay_seconds > 0:
            import time

            time.sleep(page_delay_seconds)

    items = list(found.values())
    items.sort(key=lambda x: int(x["id"]) if x["id"].isdigit() else x["id"])
    return items


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


def fetch_mod_details(mod_url: str, settings: dict) -> dict:
    html = _get_html(mod_url, settings)
    soup = BeautifulSoup(html, "html.parser")

    title = ""
    og_title = soup.select_one("meta[property='og:title']")
    if og_title:
        title = (og_title.get("content") or "").strip()
    if not title and soup.title and soup.title.string:
        title = soup.title.string.strip()

    summary = ""
    og_desc = soup.select_one("meta[property='og:description']")
    if og_desc:
        summary = (og_desc.get("content") or "").strip()

    details_block = None
    for selector in [
        ".ipsType_richText.cDownloads_submit_info",
        ".ipsType_richText[data-controller]",
        ".ipsType_richText",
    ]:
        details_block = soup.select_one(selector)
        if details_block is not None:
            break

    description_html = ""
    description_text = ""
    images: list[str] = []

    def add_image(raw: str) -> None:
        normalized = _normalize_image_url(raw)
        if not normalized or not _is_image_like_url(normalized):
            return

        for candidate in [*_thumb_to_full_candidates(normalized), normalized]:
            if not _is_image_like_url(candidate):
                continue
            if candidate not in images:
                images.append(candidate)

    if details_block is not None:
        description_html = str(details_block)
        description_text = details_block.get_text(" ", strip=True)

        for img in details_block.select("img"):
            for raw in _image_attr_candidates(img):
                add_image(raw)

        for link in details_block.select("a[href]"):
            add_image(link.get("href") or "")

    for selector in [".cDownloadsSubmitShot img", ".ipsAttachLink img", ".ipsImage img"]:
        for img in soup.select(selector):
            for raw in _image_attr_candidates(img):
                add_image(raw)

    for selector in [".ipsAttachLink[href]", ".ipsImage_thumb[href]", ".cDownloadsSubmitShot a[href]"]:
        for link in soup.select(selector):
            add_image(link.get("href") or "")

    if not summary and description_text:
        summary = description_text[:500]

    thumb = ""
    og_image = soup.select_one("meta[property='og:image']")
    if og_image:
        thumb = (og_image.get("content") or "").strip()

    if thumb:
        add_image(thumb)

    return {
        "title": title,
        "summary": summary,
        "description_html": description_html,
        "description_text": description_text,
        "images": images[:30],
        "thumbnail_url": thumb,
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
        apply_network_to_session(scraper, settings)
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
