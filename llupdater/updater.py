import datetime as dt
import random
import re
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .config import MEDIA_CACHE_DIR
from .cdp_download import CDPDownloadError, download_via_cdp
from .deploy import deploy_download
from .image_cache import cache_remote_image, is_safe_cached_filename, media_api_url
from .ll_client import (
    LLRequestError,
    discover_mods_in_category,
    discover_sims4_categories,
    discover_mods,
    download_mod_file,
    extract_mod_id,
    fetch_mod_details,
    fetch_mod_metadata,
    normalize_mod_url,
)
from .net import proxy_enabled


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _to_iso(ts: dt.datetime) -> str:
    return ts.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _now_iso() -> str:
    return _to_iso(_utcnow())


def _parse_iso(raw: str | None) -> dt.datetime | None:
    if not raw:
        return None
    try:
        value = raw.strip()
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return dt.datetime.fromisoformat(value).astimezone(dt.timezone.utc)
    except Exception:
        return None


def _seconds_until(target: dt.datetime | None, now: dt.datetime) -> int:
    if target is None:
        return 0
    return max(0, int((target - now).total_seconds()))


def _clean_subdir_segment(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._\-]+", "_", value).strip().strip("/\\")
    return cleaned or "mod"


def _default_mod_entry(
    mod_id: str,
    title: str,
    url: str,
    manager_root_subdir: str = "_LL_MOD_MANAGER",
    category_id: str = "",
    category_name: str = "",
    downloads_count: int = 0,
    thumbnail_url: str = "",
) -> dict[str, Any]:
    safe_title = re.sub(r"[^a-zA-Z0-9]+", "_", title).strip("_").lower()
    if not safe_title:
        safe_title = f"mod_{mod_id}"

    manager_root = _clean_subdir_segment(manager_root_subdir or "_LL_MOD_MANAGER")

    return {
        "id": str(mod_id),
        "title": title,
        "url": url,
        "category_id": category_id,
        "category_name": category_name,
        "category_ids": [category_id] if category_id else [],
        "downloads_count": int(downloads_count or 0),
        "popularity_cached_at": _now_iso(),
        "thumbnail_url": thumbnail_url,
        "manual_added": False,
        "enabled": False,
        "install_subdir": f"{manager_root}/{mod_id}_{safe_title}",
        "version": "",
        "date_modified": "",
        "remote_version": "",
        "remote_date_modified": "",
        "status": "new",
        "last_checked": "",
        "last_installed": "",
        "deployed_files": [],
    }


def _default_queue_item(mod: dict[str, Any], meta: dict[str, Any], now_iso: str) -> dict[str, Any]:
    return {
        "id": f"mod-{mod['id']}",
        "mod_id": str(mod["id"]),
        "title": mod.get("title", ""),
        "download_url": meta.get("download_url", ""),
        "target_version": meta.get("software_version", ""),
        "target_date_modified": meta.get("date_modified", ""),
        "state": "queued",
        "attempts": 0,
        "not_before": "",
        "created_at": now_iso,
        "updated_at": now_iso,
        "completed_at": "",
        "last_error": "",
        "download_path": "",
    }


class ModUpdater:
    def __init__(self, store) -> None:
        self.store = store
        self._catalog_scan_lock = threading.Lock()
        self._discover_progress_lock = threading.Lock()
        self._discover_started_mono: float | None = None
        self._discover_progress: dict[str, Any] = self._empty_discover_progress()

    @staticmethod
    def _empty_discover_progress() -> dict[str, Any]:
        return {
            "running": False,
            "stage": "idle",
            "full_catalog": True,
            "scan_pages": 0,
            "started_at": "",
            "finished_at": "",
            "elapsed_seconds": 0,
            "current_category_id": "",
            "current_category_name": "",
            "current_category_page": 0,
            "current_category_pages": 0,
            "categories_total": 0,
            "categories_done": 0,
            "mods_discovered": 0,
            "mods_unique": 0,
            "merged_total": 0,
            "cache_scan_total": 0,
            "cache_scan_done": 0,
            "thumbnails_cached": 0,
            "details_total_target": 0,
            "details_done": 0,
            "details_errors": 0,
            "resumed_from_checkpoint": False,
            "checkpoint_status": "idle",
            "checkpoint_category_index": 0,
            "checkpoint_page": 1,
            "last_checkpoint_at": "",
            "retries_total": 0,
            "retry_count_current": 0,
            "retry_backoff_seconds": 0,
            "last_result": None,
            "last_error": "",
        }

    @staticmethod
    def _default_full_scan_checkpoint() -> dict[str, Any]:
        return {
            "active": False,
            "interrupted": False,
            "full_catalog": True,
            "token": "",
            "signature": {},
            "started_at": "",
            "updated_at": "",
            "completed_at": "",
            "resume_count": 0,
            "categories_total": 0,
            "categories_done": 0,
            "current_category_index": 0,
            "current_page": 1,
            "mods_discovered": 0,
            "mods_unique": 0,
            "retries_total": 0,
            "last_error": "",
            "last_error_at": "",
        }

    def _load_full_scan_checkpoint(self, runtime: dict[str, Any]) -> dict[str, Any]:
        raw = runtime.get("full_scan_checkpoint")
        merged = self._default_full_scan_checkpoint()
        if isinstance(raw, dict):
            merged.update(raw)
        return merged

    def _save_full_scan_checkpoint(self, checkpoint: dict[str, Any]) -> None:
        self.store.save_runtime({"full_scan_checkpoint": checkpoint})

    def _clear_full_scan_checkpoint(self) -> None:
        self._save_full_scan_checkpoint(self._default_full_scan_checkpoint())

    def _set_discover_progress(self, **kwargs: Any) -> None:
        with self._discover_progress_lock:
            self._discover_progress.update(kwargs)
            if self._discover_progress.get("running") and self._discover_started_mono is not None:
                self._discover_progress["elapsed_seconds"] = int(max(0.0, time.monotonic() - self._discover_started_mono))

    def _begin_discover_progress(self, scan_pages: int | None, full_catalog: bool) -> None:
        now_iso = _now_iso()
        with self._discover_progress_lock:
            self._discover_started_mono = time.monotonic()
            self._discover_progress = self._empty_discover_progress()
            self._discover_progress.update(
                {
                    "running": True,
                    "stage": "starting",
                    "full_catalog": bool(full_catalog),
                    "scan_pages": int(scan_pages or 0),
                    "started_at": now_iso,
                }
            )

    def _finish_discover_progress(self, *, result: dict[str, Any] | None = None, error: str = "") -> None:
        with self._discover_progress_lock:
            elapsed = 0
            if self._discover_started_mono is not None:
                elapsed = int(max(0.0, time.monotonic() - self._discover_started_mono))
            self._discover_progress.update(
                {
                    "running": False,
                    "stage": "error" if error else "done",
                    "finished_at": _now_iso(),
                    "elapsed_seconds": elapsed,
                    "last_result": result,
                    "last_error": error,
                }
            )
            self._discover_started_mono = None

    def get_discover_progress(self) -> dict[str, Any]:
        with self._discover_progress_lock:
            snapshot = dict(self._discover_progress)
            if snapshot.get("running") and self._discover_started_mono is not None:
                snapshot["elapsed_seconds"] = int(max(0.0, time.monotonic() - self._discover_started_mono))
            return snapshot

    def is_discover_running(self) -> bool:
        return bool(self.get_discover_progress().get("running"))

    def is_catalog_scan_running(self) -> bool:
        return self._catalog_scan_lock.locked()

    @staticmethod
    def _image_source_mode(settings: dict[str, Any]) -> str:
        mode = str(settings.get("image_source_mode") or "cache").strip().lower()
        return "remote" if mode == "remote" else "cache"

    @classmethod
    def _use_remote_images(cls, settings: dict[str, Any]) -> bool:
        return cls._image_source_mode(settings) == "remote"

    @staticmethod
    def _details_cache_stale(mod: dict[str, Any], settings: dict[str, Any]) -> bool:
        cache_hours = max(1, int(settings.get("details_cache_hours", 720)))
        cached_at = _parse_iso(mod.get("details_cached_at"))
        if cached_at is None:
            return True
        age_sec = (_utcnow() - cached_at).total_seconds()
        return age_sec > cache_hours * 3600

    @staticmethod
    def _media_name_from_api_url(url: str) -> str:
        raw = str(url or "").strip()
        prefix = "/api/media/"
        if not raw.startswith(prefix):
            return ""
        name = raw[len(prefix) :].strip()
        if not is_safe_cached_filename(name):
            return ""
        return name

    def _media_url_available(self, url: str) -> bool:
        name = self._media_name_from_api_url(url)
        if not name:
            return False
        path = MEDIA_CACHE_DIR / name
        return path.exists() and path.is_file()

    def _sanitize_cached_details(self, mod: dict[str, Any], details: dict[str, Any]) -> dict[str, Any]:
        out = dict(details)

        raw_images = out.get("cached_images") if isinstance(out.get("cached_images"), list) else []
        cached_images: list[str] = []
        seen: set[str] = set()
        for item in raw_images:
            src = str(item or "").strip()
            if not src or src in seen:
                continue
            if self._media_url_available(src):
                cached_images.append(src)
                seen.add(src)

        details_thumb = str(out.get("thumbnail_cached_url") or "").strip()
        if details_thumb and not self._media_url_available(details_thumb):
            details_thumb = ""

        mod_thumb = str(mod.get("thumbnail_cached_url") or "").strip()
        if mod_thumb and not self._media_url_available(mod_thumb):
            mod_thumb = ""

        primary_thumb = mod_thumb or details_thumb
        if primary_thumb and primary_thumb not in seen:
            cached_images.insert(0, primary_thumb)
            seen.add(primary_thumb)

        out["cached_images"] = cached_images
        out["thumbnail_cached_url"] = primary_thumb or (cached_images[0] if cached_images else "")
        return out

    def _details_data_corrupted(self, mod: dict[str, Any]) -> bool:
        details = mod.get("details")
        if not isinstance(details, dict):
            return False

        details_title = str(details.get("title") or "")
        mod_title = str(mod.get("title") or "")
        if details_title == "Test Mod" and mod_title != "Test Mod":
            return True

        raw_images = details.get("images") if isinstance(details.get("images"), list) else []
        raw_cached = details.get("cached_images") if isinstance(details.get("cached_images"), list) else []
        suspect_pool = [str(details.get("thumbnail_url") or "")] + [str(x or "") for x in raw_images] + [str(x or "") for x in raw_cached]
        suspect_text = "\n".join(suspect_pool)
        if "fake-" in suspect_text or "fake-thumb" in suspect_text:
            return True
        if "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" in suspect_text:
            return True

        thumb_cached = str(details.get("thumbnail_cached_url") or "").strip()
        if thumb_cached and not self._media_url_available(thumb_cached):
            return True

        cached_list = [str(x or "").strip() for x in raw_cached if str(x or "").strip()]
        if cached_list and not any(self._media_url_available(x) for x in cached_list):
            return True

        return False

    @staticmethod
    def _build_fallback_details(mod: dict[str, Any]) -> dict[str, Any]:
        cached_images: list[str] = []
        thumb_cached = str(mod.get("thumbnail_cached_url") or "")
        if thumb_cached:
            cached_images.append(thumb_cached)

        return {
            "title": mod.get("title", ""),
            "summary": "",
            "description_html": "",
            "description_text": "",
            "images": [],
            "cached_images": cached_images,
            "thumbnail_url": mod.get("thumbnail_url", ""),
            "thumbnail_cached_url": thumb_cached,
        }

    def _cache_details_media(
        self,
        mod: dict[str, Any],
        details: dict[str, Any],
        settings: dict[str, Any],
        *,
        force_refresh: bool,
    ) -> dict[str, Any]:
        def canonical_source_key(src: str) -> str:
            parsed = urlparse(src)
            path = parsed.path.lower().replace("/thumb-", "/")
            path = path.replace(".thumb.", ".")
            return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}"

        sources: list[str] = []
        for src in details.get("images", []) or []:
            s = str(src or "").strip()
            if s:
                sources.append(s)

        thumb_source = str(details.get("thumbnail_url") or mod.get("thumbnail_url") or "").strip()
        if thumb_source:
            sources.append(thumb_source)

        unique_sources: list[str] = []
        seen: set[str] = set()
        for src in sources:
            if src in seen:
                continue
            seen.add(src)
            unique_sources.append(src)

        cached_images: list[str] = []
        seen_source_keys: set[str] = set()
        for src in unique_sources:
            name = cache_remote_image(src, settings, force_refresh=force_refresh)
            if name:
                local = media_api_url(name)
                source_key = canonical_source_key(src)
                if source_key in seen_source_keys:
                    continue
                seen_source_keys.add(source_key)
                if local not in cached_images:
                    cached_images.append(local)

        details["cached_images"] = cached_images
        details["thumbnail_cached_url"] = cached_images[0] if cached_images else ""
        if thumb_source and not details.get("thumbnail_url"):
            details["thumbnail_url"] = thumb_source

        mod["thumbnail_cached_url"] = details.get("thumbnail_cached_url", "")
        return details

    def _merge_discovered_item(
        self,
        by_id: dict[str, dict[str, Any]],
        item: dict[str, Any],
        *,
        manager_root_subdir: str,
        full_catalog: bool,
        run_downloads: dict[str, int],
    ) -> None:
        mod_id = str(item["id"])
        category_id = str(item.get("category_id") or "")
        category_name = str(item.get("category_name") or "")
        discovered_downloads = int(item.get("downloads_count") or 0)
        run_downloads[mod_id] = max(run_downloads.get(mod_id, 0), discovered_downloads)
        downloads_count = run_downloads[mod_id]
        thumbnail_url = str(item.get("thumbnail_url") or "")

        if mod_id in by_id:
            by_id[mod_id]["title"] = item["title"]
            by_id[mod_id]["url"] = item["url"]
            if full_catalog:
                by_id[mod_id]["downloads_count"] = downloads_count
            else:
                by_id[mod_id]["downloads_count"] = max(int(by_id[mod_id].get("downloads_count") or 0), downloads_count)
            by_id[mod_id]["popularity_cached_at"] = _now_iso()
            if not by_id[mod_id].get("thumbnail_url") and thumbnail_url:
                by_id[mod_id]["thumbnail_url"] = thumbnail_url
            if category_id:
                if not by_id[mod_id].get("category_id"):
                    by_id[mod_id]["category_id"] = category_id
                if not by_id[mod_id].get("category_name"):
                    by_id[mod_id]["category_name"] = category_name

                category_ids = {str(x) for x in (by_id[mod_id].get("category_ids", []) or []) if str(x).strip()}
                category_ids.add(category_id)
                by_id[mod_id]["category_ids"] = sorted(
                    category_ids,
                    key=lambda x: int(x) if x.isdigit() else x,
                )

            by_id[mod_id]["install_subdir"] = _default_mod_entry(
                mod_id,
                item["title"],
                item["url"],
                manager_root_subdir,
                category_id,
                category_name,
                downloads_count,
                thumbnail_url,
            )["install_subdir"]
        else:
            by_id[mod_id] = _default_mod_entry(
                mod_id,
                item["title"],
                item["url"],
                manager_root_subdir,
                category_id,
                category_name,
                downloads_count,
                thumbnail_url,
            )

    def discover(
        self,
        scan_pages: int | None = None,
        full_catalog: bool = True,
        refresh_cache: bool = True,
    ) -> dict[str, int]:
        if not self._catalog_scan_lock.acquire(blocking=False):
            raise RuntimeError("discover already running")

        self._begin_discover_progress(scan_pages=scan_pages, full_catalog=full_catalog)
        try:
            settings = self.store.get_settings()
            runtime_state = self.store.get_runtime()
            use_remote_images = self._use_remote_images(settings)
            manager_root_subdir = str(settings.get("manager_root_subdir") or "_LL_MOD_MANAGER")
            found: list[dict[str, Any]] = []
            found_count = 0
            categories: list[dict[str, Any]] = []
            partial_catalog = False
            unique_found_ids: set[str] = set()
            retries_total = 0
            checkpoint_status = "idle"
            checkpoint_saved_at = ""

            current = self.store.get_mods()
            by_id = {m["id"]: m for m in current}
            run_downloads: dict[str, int] = {}

            checkpoint = self._default_full_scan_checkpoint()
            dirty_mods = False
            pages_since_flush = 0

            def sorted_mods(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
                out = list(values)
                out.sort(key=lambda m: int(m["id"]) if str(m["id"]).isdigit() else str(m["id"]))
                return out

            def flush_partial_state(*, force_checkpoint: bool = False) -> None:
                nonlocal dirty_mods, pages_since_flush, checkpoint_saved_at

                if dirty_mods:
                    self.store.save_mods(sorted_mods(list(by_id.values())))
                    dirty_mods = False

                if not full_catalog:
                    return

                flush_every = max(1, int(settings.get("full_scan_checkpoint_flush_pages", 3)))
                if force_checkpoint or pages_since_flush >= flush_every:
                    checkpoint["updated_at"] = _now_iso()
                    self._save_full_scan_checkpoint(checkpoint)
                    checkpoint_saved_at = str(checkpoint.get("updated_at") or "")
                    self._set_discover_progress(
                        last_checkpoint_at=checkpoint_saved_at,
                        checkpoint_status=checkpoint_status,
                        checkpoint_category_index=int(checkpoint.get("current_category_index", 0) or 0),
                        checkpoint_page=int(checkpoint.get("current_page", 1) or 1),
                        retries_total=int(checkpoint.get("retries_total", 0) or 0),
                    )
                    pages_since_flush = 0

            scan_token = ""

            if full_catalog:
                self._set_discover_progress(stage="loading_categories")
                categories = discover_sims4_categories(settings)
                self.store.save_categories(categories, _now_iso())

                category_max_pages = int(settings.get("catalog_max_pages_per_category", 0))
                max_pages = int(scan_pages) if scan_pages is not None else (category_max_pages if category_max_pages > 0 else None)
                page_delay = max(0.0, float(settings.get("catalog_page_delay_seconds", 1)))
                category_delay = max(0.0, float(settings.get("catalog_category_delay_seconds", 2)))
                max_categories = int(settings.get("catalog_max_categories_per_run", 0))
                partial_catalog = bool(max_pages is not None or max_categories > 0)
                resume_enabled = bool(settings.get("full_scan_resume_enabled", True))

                retry_max_attempts = max(0, int(settings.get("full_scan_max_retries_per_step", 5)))
                retry_base_seconds = max(1.0, float(settings.get("full_scan_retry_base_seconds", 5)))
                retry_max_seconds = max(retry_base_seconds, float(settings.get("full_scan_retry_max_seconds", 300)))

                eligible_categories = [c for c in categories if int(c.get("count") or 0) > 0]
                if max_categories > 0:
                    eligible_categories = eligible_categories[:max_categories]

                category_ids = [str(c.get("id") or "") for c in eligible_categories]
                signature = {
                    "category_ids": category_ids,
                    "max_pages": int(max_pages or 0),
                    "max_categories": max_categories,
                    "refresh_cache": bool(refresh_cache),
                    "image_source_mode": self._image_source_mode(settings),
                }

                previous_checkpoint = self._load_full_scan_checkpoint(runtime_state)
                can_resume = (
                    resume_enabled
                    and bool(previous_checkpoint.get("active"))
                    and bool(previous_checkpoint.get("interrupted"))
                    and previous_checkpoint.get("signature") == signature
                    and bool(previous_checkpoint.get("token"))
                )

                start_category_index = 0
                start_page = 1

                if can_resume:
                    checkpoint = previous_checkpoint
                    scan_token = str(checkpoint.get("token") or _now_iso())
                    start_category_index = max(0, int(checkpoint.get("current_category_index", 0) or 0))
                    start_page = max(1, int(checkpoint.get("current_page", 1) or 1))
                    retries_total = max(0, int(checkpoint.get("retries_total", 0) or 0))
                    checkpoint["interrupted"] = False
                    checkpoint["resume_count"] = int(checkpoint.get("resume_count", 0) or 0) + 1
                    checkpoint["updated_at"] = _now_iso()
                    checkpoint_saved_at = str(checkpoint.get("updated_at") or "")
                    checkpoint_status = "resumed"
                    self._save_full_scan_checkpoint(checkpoint)

                    for mod in by_id.values():
                        if str(mod.get("last_full_scan_token") or "") == scan_token:
                            unique_found_ids.add(str(mod.get("id") or ""))

                    found_count = max(int(checkpoint.get("mods_discovered", 0) or 0), len(unique_found_ids))

                    self._set_discover_progress(
                        resumed_from_checkpoint=True,
                        checkpoint_status="resumed",
                        checkpoint_category_index=start_category_index,
                        checkpoint_page=start_page,
                        last_checkpoint_at=checkpoint_saved_at,
                        retries_total=retries_total,
                        mods_discovered=found_count,
                        mods_unique=len(unique_found_ids),
                    )
                else:
                    scan_token = _now_iso()
                    checkpoint = self._default_full_scan_checkpoint()
                    checkpoint.update(
                        {
                            "active": True,
                            "interrupted": False,
                            "full_catalog": True,
                            "token": scan_token,
                            "signature": signature,
                            "started_at": _now_iso(),
                            "updated_at": _now_iso(),
                            "categories_total": len(eligible_categories),
                            "categories_done": 0,
                            "current_category_index": 0,
                            "current_page": 1,
                            "mods_discovered": 0,
                            "mods_unique": 0,
                            "retries_total": 0,
                            "last_error": "",
                            "last_error_at": "",
                        }
                    )
                    checkpoint_saved_at = str(checkpoint.get("updated_at") or "")
                    checkpoint_status = "running"
                    self._save_full_scan_checkpoint(checkpoint)
                    self._set_discover_progress(
                        resumed_from_checkpoint=False,
                        checkpoint_status="running",
                        checkpoint_category_index=0,
                        checkpoint_page=1,
                        last_checkpoint_at=checkpoint_saved_at,
                    )

                self._set_discover_progress(
                    stage="discovering_categories",
                    categories_total=len(eligible_categories),
                    categories_done=min(start_category_index, len(eligible_categories)),
                )

                for index in range(start_category_index, len(eligible_categories)):
                    category = eligible_categories[index]
                    category_pages_total = 0
                    resume_page = start_page if index == start_category_index else 1

                    checkpoint["current_category_index"] = index
                    checkpoint["current_page"] = resume_page
                    checkpoint["categories_done"] = index
                    checkpoint["active"] = True
                    checkpoint["interrupted"] = False
                    checkpoint_status = "running"
                    flush_partial_state(force_checkpoint=True)

                    self._set_discover_progress(
                        current_category_id=str(category.get("id") or ""),
                        current_category_name=str(category.get("name") or ""),
                        current_category_page=max(0, resume_page - 1),
                        current_category_pages=0,
                        categories_done=index,
                        checkpoint_category_index=index,
                        checkpoint_page=resume_page,
                        checkpoint_status=checkpoint_status,
                    )

                    attempt = 0
                    while True:
                        attempt += 1

                        def on_category_page(page: int, total_pages: int, _category_unique_count: int, page_items: list[dict[str, Any]]) -> None:
                            nonlocal category_pages_total, found_count, dirty_mods, pages_since_flush
                            category_pages_total = int(total_pages)

                            for page_item in page_items:
                                mod_id = str(page_item.get("id") or "")
                                if not mod_id:
                                    continue
                                found.append(page_item)
                                found_count += 1
                                unique_found_ids.add(mod_id)
                                self._merge_discovered_item(
                                    by_id,
                                    page_item,
                                    manager_root_subdir=manager_root_subdir,
                                    full_catalog=True,
                                    run_downloads=run_downloads,
                                )
                                target = by_id.get(mod_id)
                                if target is not None:
                                    target["last_full_scan_token"] = scan_token
                                dirty_mods = True

                            if page < total_pages:
                                checkpoint["current_category_index"] = index
                                checkpoint["current_page"] = page + 1
                                checkpoint["categories_done"] = index
                            else:
                                checkpoint["current_category_index"] = index + 1
                                checkpoint["current_page"] = 1
                                checkpoint["categories_done"] = index + 1

                            checkpoint["mods_discovered"] = found_count
                            checkpoint["mods_unique"] = len(unique_found_ids)
                            checkpoint["retries_total"] = retries_total
                            checkpoint["last_error"] = ""
                            checkpoint["interrupted"] = False

                            pages_since_flush += 1

                            self._set_discover_progress(
                                current_category_page=int(page),
                                current_category_pages=int(total_pages),
                                mods_discovered=found_count,
                                mods_unique=len(unique_found_ids),
                                categories_done=int(checkpoint.get("categories_done", 0) or 0),
                                checkpoint_category_index=int(checkpoint.get("current_category_index", 0) or 0),
                                checkpoint_page=int(checkpoint.get("current_page", 1) or 1),
                                retries_total=retries_total,
                                retry_count_current=0,
                                retry_backoff_seconds=0,
                            )
                            flush_partial_state()

                        try:
                            discover_mods_in_category(
                                category,
                                settings,
                                max_pages=max_pages,
                                page_delay_seconds=page_delay,
                                start_page=resume_page,
                                on_page=on_category_page,
                            )

                            checkpoint["categories_done"] = index + 1
                            checkpoint["current_category_index"] = index + 1
                            checkpoint["current_page"] = 1
                            checkpoint["mods_discovered"] = found_count
                            checkpoint["mods_unique"] = len(unique_found_ids)
                            checkpoint["last_error"] = ""
                            checkpoint["interrupted"] = False
                            checkpoint_status = "running"
                            flush_partial_state(force_checkpoint=True)

                            self._set_discover_progress(
                                categories_done=index + 1,
                                mods_discovered=found_count,
                                mods_unique=len(unique_found_ids),
                                current_category_page=category_pages_total,
                                retries_total=retries_total,
                                retry_count_current=0,
                                retry_backoff_seconds=0,
                                checkpoint_status=checkpoint_status,
                            )
                            break
                        except Exception as exc:
                            retries_total += 1
                            checkpoint["retries_total"] = retries_total
                            checkpoint["interrupted"] = True
                            checkpoint["last_error"] = str(exc)
                            checkpoint["last_error_at"] = _now_iso()
                            checkpoint_status = "retrying"
                            flush_partial_state(force_checkpoint=True)

                            if attempt > retry_max_attempts:
                                checkpoint_status = "interrupted"
                                self._set_discover_progress(
                                    last_error=str(exc),
                                    checkpoint_status=checkpoint_status,
                                    retries_total=retries_total,
                                    retry_count_current=attempt,
                                    retry_backoff_seconds=0,
                                )
                                raise

                            backoff_seconds = min(retry_max_seconds, retry_base_seconds * (2 ** max(0, attempt - 1)))
                            self._set_discover_progress(
                                last_error=str(exc),
                                checkpoint_status=checkpoint_status,
                                retries_total=retries_total,
                                retry_count_current=attempt,
                                retry_backoff_seconds=int(backoff_seconds),
                            )
                            time.sleep(backoff_seconds)
                            resume_page = max(1, int(checkpoint.get("current_page", resume_page) or 1))

                    start_page = 1
                    if category_delay > 0 and index < len(eligible_categories) - 1:
                        time.sleep(category_delay)

                checkpoint["active"] = False
                checkpoint["interrupted"] = False
                checkpoint["completed_at"] = _now_iso()
                checkpoint_status = "completed"
                flush_partial_state(force_checkpoint=True)
                self._clear_full_scan_checkpoint()
            else:
                self._set_discover_progress(stage="discovering_pages")
                pages = int(scan_pages or settings.get("scan_pages", 5))
                found = discover_mods(pages, settings)
                unique_found_ids = {str(x.get("id")) for x in found}
                found_count = len(found)
                self._set_discover_progress(mods_discovered=len(found), mods_unique=len(unique_found_ids))

            self._set_discover_progress(stage="merging_mods")
            if not full_catalog:
                for item in found:
                    self._merge_discovered_item(
                        by_id,
                        item,
                        manager_root_subdir=manager_root_subdir,
                        full_catalog=False,
                        run_downloads=run_downloads,
                    )

            if full_catalog and not partial_catalog:
                discovered_ids = {
                    str(mod.get("id"))
                    for mod in by_id.values()
                    if str(mod.get("last_full_scan_token") or "") == scan_token
                }
                merged = [
                    m
                    for m in by_id.values()
                    if str(m.get("id")) in discovered_ids or bool(m.get("manual_added", False))
                ]
            else:
                merged = list(by_id.values())

            thumbnails_cached = 0
            details_refreshed = 0
            details_errors = 0

            self._set_discover_progress(
                merged_total=len(merged),
                cache_scan_total=len(merged),
                cache_scan_done=0,
            )

            if full_catalog and refresh_cache and bool(settings.get("refresh_details_on_full_scan", True)):
                max_details = int(settings.get("details_max_per_full_scan", 0))
                details_delay = max(0.0, float(settings.get("details_refresh_delay_seconds", 0.5)))

                stale_candidates = 0
                for mod in merged:
                    if self._details_cache_stale(mod, settings) or self._details_data_corrupted(mod):
                        stale_candidates += 1
                        if max_details > 0 and stale_candidates >= max_details:
                            break

                details_target = stale_candidates if max_details <= 0 else min(stale_candidates, max_details)
                self._set_discover_progress(stage="refreshing_cache", details_total_target=details_target)

                cache_scan_done = 0
                for mod in merged:
                    cache_scan_done += 1

                    thumb_url = str(mod.get("thumbnail_url") or "").strip()
                    if thumb_url and not use_remote_images:
                        cached_name = cache_remote_image(thumb_url, settings, force_refresh=False)
                        if cached_name:
                            local_thumb = media_api_url(cached_name)
                            if mod.get("thumbnail_cached_url") != local_thumb:
                                mod["thumbnail_cached_url"] = local_thumb
                                thumbnails_cached += 1

                    attempted_details = False
                    needs_details_refresh = self._details_cache_stale(mod, settings) or self._details_data_corrupted(mod)
                    if not (max_details > 0 and details_refreshed >= max_details) and needs_details_refresh:
                        attempted_details = True
                        try:
                            details = fetch_mod_details(mod["url"], settings)
                            if use_remote_images:
                                details["cached_images"] = []
                                details["thumbnail_cached_url"] = ""
                            else:
                                details = self._cache_details_media(mod, details, settings, force_refresh=True)
                            mod["details"] = details
                            mod["details_cached_at"] = _now_iso()
                            mod["details_error"] = ""
                            details_refreshed += 1
                        except Exception as exc:
                            mod["details_error"] = str(exc)
                            details_errors += 1

                        if details_delay > 0:
                            time.sleep(details_delay)

                    if attempted_details or cache_scan_done % 25 == 0 or cache_scan_done == len(merged):
                        self._set_discover_progress(
                            cache_scan_done=cache_scan_done,
                            thumbnails_cached=thumbnails_cached,
                            details_done=details_refreshed,
                            details_errors=details_errors,
                        )
            else:
                self._set_discover_progress(stage="finalizing")

            merged.sort(key=lambda m: int(m["id"]) if str(m["id"]).isdigit() else str(m["id"]))
            self.store.save_mods(merged)
            result = {
                "discovered": found_count if full_catalog else len(found),
                "unique_discovered": len(unique_found_ids) if full_catalog else len({str(x.get('id')) for x in found}),
                "categories": len(categories),
                "total": len(merged),
                "details_refreshed": details_refreshed,
                "details_errors": details_errors,
                "thumbnails_cached": thumbnails_cached,
            }
            self._set_discover_progress(
                checkpoint_status="completed" if full_catalog else self.get_discover_progress().get("checkpoint_status", "idle"),
                retries_total=retries_total,
                retry_count_current=0,
                retry_backoff_seconds=0,
                last_checkpoint_at=checkpoint_saved_at,
            )
            self._finish_discover_progress(result=result)
            return result
        except Exception as exc:
            if full_catalog:
                runtime_after_error = self.store.get_runtime()
                checkpoint_error = self._load_full_scan_checkpoint(runtime_after_error)
                checkpoint_error["active"] = True
                checkpoint_error["interrupted"] = True
                checkpoint_error["last_error"] = str(exc)
                checkpoint_error["last_error_at"] = _now_iso()
                checkpoint_error["updated_at"] = checkpoint_error["last_error_at"]
                self._save_full_scan_checkpoint(checkpoint_error)
            self._finish_discover_progress(error=str(exc))
            raise
        finally:
            self._catalog_scan_lock.release()

    def add_mod_url(self, url: str) -> dict[str, Any]:
        clean = normalize_mod_url(url)
        mod_id = extract_mod_id(clean)

        mods = self.store.get_mods()
        if any(str(m["id"]) == str(mod_id) for m in mods):
            return {"added": False, "reason": "already_exists"}

        settings = self.store.get_settings()
        manager_root_subdir = str(settings.get("manager_root_subdir") or "_LL_MOD_MANAGER")
        meta = fetch_mod_metadata(clean, settings)
        entry = _default_mod_entry(mod_id, meta.get("title") or f"Mod {mod_id}", clean, manager_root_subdir)
        entry["manual_added"] = True
        mods.append(entry)
        mods.sort(key=lambda m: int(m["id"]) if str(m["id"]).isdigit() else str(m["id"]))
        self.store.save_mods(mods)
        return {"added": True, "id": mod_id}

    def discover_new_mods_lazy(self) -> dict[str, Any]:
        if not self._catalog_scan_lock.acquire(blocking=False):
            return {
                "started": False,
                "reason": "discover_running",
                "categories": 0,
                "scanned_categories": 0,
                "seen_ids": 0,
                "new_mods": 0,
                "thumbnails_cached": 0,
                "details_cached": 0,
                "details_errors": 0,
                "retry_checked": 0,
                "retry_fixed": 0,
                "retry_errors": 0,
                "total": len(self.store.get_mods()),
            }

        runtime_state = self.store.get_runtime()
        try:
            settings = self.store.get_settings()
            use_remote_images = self._use_remote_images(settings)
            manager_root_subdir = str(settings.get("manager_root_subdir") or "_LL_MOD_MANAGER")
            runtime_updates: dict[str, Any] = {}

            pages_per_category = max(1, int(settings.get("new_mods_scan_pages_per_category", 1)))
            max_categories = max(0, int(settings.get("new_mods_max_categories_per_run", 0)))
            page_delay = max(0.0, float(settings.get("new_mods_page_delay_seconds", 0.25)))
            category_delay = max(0.0, float(settings.get("new_mods_category_delay_seconds", 0.5)))

            categories = discover_sims4_categories(settings)
            self.store.save_categories(categories, _now_iso())

            eligible_categories = [c for c in categories if int(c.get("count") or 0) > 0]
            failed_priority = [str(x) for x in (runtime_state.get("lazy_new_mods_failed_categories") or []) if str(x).strip()]
            if failed_priority:
                failed_set = set(failed_priority)
                eligible_categories.sort(key=lambda c: (0 if str(c.get("id") or "") in failed_set else 1))

            scan_categories = eligible_categories
            if max_categories > 0:
                if eligible_categories:
                    cursor = int(runtime_state.get("lazy_new_mods_cursor", 0)) % len(eligible_categories)
                    scan_categories = [eligible_categories[(cursor + i) % len(eligible_categories)] for i in range(min(max_categories, len(eligible_categories)))]
                    runtime_updates["lazy_new_mods_cursor"] = (cursor + len(scan_categories)) % len(eligible_categories)
                else:
                    scan_categories = []
                    runtime_updates["lazy_new_mods_cursor"] = 0
            else:
                runtime_updates["lazy_new_mods_cursor"] = 0

            existing_mods = self.store.get_mods()
            existing_ids = {str(m.get("id")) for m in existing_mods}

            new_entries: dict[str, dict[str, Any]] = {}
            seen_ids: set[str] = set()
            thumbnails_cached = 0
            details_cached = 0
            details_errors = 0
            retry_checked = 0
            retry_fixed = 0
            retry_errors = 0
            failed_categories: list[str] = []

            for index, category in enumerate(scan_categories):
                category_items: list[dict[str, Any]] = []
                try:
                    category_items = discover_mods_in_category(
                        category,
                        settings,
                        max_pages=pages_per_category,
                        page_delay_seconds=page_delay,
                    )
                except Exception:
                    category_id = str(category.get("id") or "").strip()
                    if category_id:
                        failed_categories.append(category_id)
                    continue

                for item in category_items:
                    mod_id = str(item.get("id") or "")
                    if not mod_id:
                        continue

                    seen_ids.add(mod_id)
                    if mod_id in existing_ids or mod_id in new_entries:
                        continue

                    category_id = str(item.get("category_id") or "")
                    category_name = str(item.get("category_name") or "")
                    downloads_count = int(item.get("downloads_count") or 0)
                    thumbnail_url = str(item.get("thumbnail_url") or "")

                    new_entries[mod_id] = _default_mod_entry(
                        mod_id,
                        str(item.get("title") or f"Mod {mod_id}"),
                        str(item.get("url") or ""),
                        manager_root_subdir,
                        category_id,
                        category_name,
                        downloads_count,
                        thumbnail_url,
                    )

                if category_delay > 0 and index < len(scan_categories) - 1:
                    time.sleep(category_delay)

            runtime_updates["lazy_new_mods_failed_categories"] = failed_categories

            total_after = len(existing_mods)
            new_added = 0

            refresh_details = bool(settings.get("new_mods_refresh_details_on_scan", True))
            details_max = max(0, int(settings.get("new_mods_details_max_per_run", 0)))
            details_delay = max(0.0, float(settings.get("new_mods_details_delay_seconds", 0.75)))

            if new_entries:
                details_attempts = 0
                for entry in new_entries.values():
                    thumb_url = str(entry.get("thumbnail_url") or "").strip()
                    if thumb_url and not use_remote_images:
                        cached_name = cache_remote_image(thumb_url, settings, force_refresh=False)
                        if cached_name:
                            entry["thumbnail_cached_url"] = media_api_url(cached_name)
                            thumbnails_cached += 1

                    if not refresh_details:
                        continue
                    if details_max > 0 and details_attempts >= details_max:
                        break

                    details_attempts += 1
                    try:
                        details = fetch_mod_details(entry["url"], settings)
                        if use_remote_images:
                            details["cached_images"] = []
                            details["thumbnail_cached_url"] = ""
                        else:
                            details = self._cache_details_media(entry, details, settings, force_refresh=False)
                        entry["details"] = details
                        entry["details_cached_at"] = _now_iso()
                        entry["details_error"] = ""
                        details_cached += 1
                    except Exception as exc:
                        entry["details"] = {}
                        entry["details_cached_at"] = ""
                        entry["details_error"] = str(exc)
                        details_errors += 1

                    if details_delay > 0:
                        time.sleep(details_delay)

            merged = existing_mods
            if new_entries:
                latest_mods = self.store.get_mods()
                by_id = {str(m.get("id")): m for m in latest_mods}

                for mod_id, entry in new_entries.items():
                    if mod_id in by_id:
                        continue
                    by_id[mod_id] = entry
                    new_added += 1

                if new_added > 0:
                    merged = list(by_id.values())
                    merged.sort(key=lambda m: int(m["id"]) if str(m["id"]).isdigit() else str(m["id"]))
                    self.store.save_mods(merged)
                    total_after = len(merged)
                else:
                    merged = latest_mods
                    total_after = len(latest_mods)

            retry_enabled = bool(settings.get("new_mods_retry_failed_cache_enabled", True))
            retry_limit = max(0, int(settings.get("new_mods_retry_failed_cache_per_run", 25)))
            retry_delay = max(0.0, float(settings.get("new_mods_retry_failed_cache_delay_seconds", 0.75)))

            if retry_enabled and merged and not use_remote_images:
                candidates: list[dict[str, Any]] = []
                for mod in merged:
                    thumb_source = str(mod.get("thumbnail_url") or "").strip()
                    thumb_cached = str(mod.get("thumbnail_cached_url") or "").strip()
                    thumb_missing = bool(thumb_source) and not self._media_url_available(thumb_cached)

                    details_obj = mod.get("details") if isinstance(mod.get("details"), dict) else {}
                    cached_images_raw = details_obj.get("cached_images") if isinstance(details_obj.get("cached_images"), list) else []
                    has_cached_images = any(self._media_url_available(str(x or "")) for x in cached_images_raw)

                    details_missing = not str(mod.get("details_cached_at") or "").strip()
                    details_failed = bool(str(mod.get("details_error") or "").strip())
                    image_list_missing = bool(details_obj) and not has_cached_images

                    if thumb_missing or details_missing or details_failed or image_list_missing:
                        candidates.append(mod)

                selected: list[dict[str, Any]] = []
                if candidates:
                    if retry_limit <= 0 or retry_limit >= len(candidates):
                        selected = candidates
                        runtime_updates["lazy_cache_retry_cursor"] = 0
                    else:
                        cursor = int(runtime_state.get("lazy_cache_retry_cursor", 0)) % len(candidates)
                        selected = [candidates[(cursor + i) % len(candidates)] for i in range(retry_limit)]
                        runtime_updates["lazy_cache_retry_cursor"] = (cursor + len(selected)) % len(candidates)
                else:
                    runtime_updates["lazy_cache_retry_cursor"] = 0

                changed_retry = False
                for mod in selected:
                    retry_checked += 1
                    mod_changed = False

                    thumb_url = str(mod.get("thumbnail_url") or "").strip()
                    if thumb_url and not self._media_url_available(str(mod.get("thumbnail_cached_url") or "")):
                        cached_name = cache_remote_image(thumb_url, settings, force_refresh=False)
                        if cached_name:
                            mod["thumbnail_cached_url"] = media_api_url(cached_name)
                            thumbnails_cached += 1
                            mod_changed = True

                    details_obj = mod.get("details") if isinstance(mod.get("details"), dict) else {}
                    cached_images_raw = details_obj.get("cached_images") if isinstance(details_obj.get("cached_images"), list) else []
                    has_cached_images = any(self._media_url_available(str(x or "")) for x in cached_images_raw)

                    needs_details = bool(str(mod.get("details_error") or "").strip()) or not str(mod.get("details_cached_at") or "").strip() or not has_cached_images

                    if needs_details:
                        try:
                            details = fetch_mod_details(mod["url"], settings)
                            details = self._cache_details_media(mod, details, settings, force_refresh=False)
                            mod["details"] = details
                            mod["details_cached_at"] = _now_iso()
                            mod["details_error"] = ""
                            details_cached += 1
                            mod_changed = True
                        except Exception as exc:
                            mod["details_error"] = str(exc)
                            details_errors += 1
                            retry_errors += 1

                        if retry_delay > 0:
                            time.sleep(retry_delay)

                    if mod_changed:
                        retry_fixed += 1
                        changed_retry = True

                if changed_retry:
                    merged.sort(key=lambda m: int(m["id"]) if str(m["id"]).isdigit() else str(m["id"]))
                    self.store.save_mods(merged)
                    total_after = len(merged)

            if runtime_updates:
                self.store.save_runtime(runtime_updates)

            return {
                "started": True,
                "reason": "ok",
                "categories": len(eligible_categories),
                "scanned_categories": len(scan_categories),
                "seen_ids": len(seen_ids),
                "new_mods": new_added,
                "thumbnails_cached": thumbnails_cached,
                "details_cached": details_cached,
                "details_errors": details_errors,
                "retry_checked": retry_checked,
                "retry_fixed": retry_fixed,
                "retry_errors": retry_errors,
                "total": total_after,
            }
        finally:
            self._catalog_scan_lock.release()

    @staticmethod
    def _safe_within(base: Path, candidate: Path) -> bool:
        try:
            candidate.resolve().relative_to(base.resolve())
            return True
        except Exception:
            return False

    def _remove_mod_from_game_catalog(self, mod: dict[str, Any], settings: dict[str, Any]) -> bool:
        mods_dir = Path(settings.get("mods_dir", "")).expanduser()
        changed = False

        deployed_files = list(mod.get("deployed_files") or [])
        for path_str in deployed_files:
            p = Path(path_str)
            try:
                if p.exists() and self._safe_within(mods_dir, p):
                    if p.is_file() or p.is_symlink():
                        p.unlink(missing_ok=True)
                        changed = True
            except Exception:
                continue

        install_subdir = str(mod.get("install_subdir") or "").strip().strip("/\\")
        if install_subdir:
            install_root = (mods_dir / install_subdir)
            try:
                if install_root.exists() and install_root.is_dir() and self._safe_within(mods_dir, install_root):
                    import shutil

                    shutil.rmtree(install_root)
                    changed = True
            except Exception:
                pass

        mod["deployed_files"] = []
        mod["version"] = ""
        mod["date_modified"] = ""
        mod["status"] = "disabled"
        return changed

    @staticmethod
    def _drop_pending_mod_queue_items(queue: list[dict[str, Any]], mod_id: str) -> tuple[list[dict[str, Any]], int]:
        before = len(queue)
        kept = [x for x in queue if not (str(x.get("mod_id")) == str(mod_id) and x.get("state") != "done")]
        return kept, before - len(kept)

    def set_mod_enabled(self, mod_id: str, enabled: bool) -> bool:
        mods = self.store.get_mods()
        queue = self.store.get_queue()
        settings = self.store.get_settings()
        changed = False
        queue_changed = False
        deploy_changed = False

        for mod in mods:
            if str(mod["id"]) == str(mod_id):
                next_enabled = bool(enabled)
                if mod.get("enabled") != next_enabled:
                    changed = True
                mod["enabled"] = next_enabled

                if not next_enabled:
                    queue, removed = self._drop_pending_mod_queue_items(queue, str(mod_id))
                    if removed > 0:
                        queue_changed = True
                    deploy_changed = self._remove_mod_from_game_catalog(mod, settings) or deploy_changed
                break

        if changed or deploy_changed:
            self.store.save_mods(mods)
        if queue_changed:
            self.store.save_queue(queue)
        return changed or queue_changed or deploy_changed

    def update_mod_config(self, mod_id: str, install_subdir: str) -> bool:
        mods = self.store.get_mods()
        settings = self.store.get_settings()
        manager_root = _clean_subdir_segment(str(settings.get("manager_root_subdir") or "_LL_MOD_MANAGER"))
        changed = False
        for mod in mods:
            if str(mod["id"]) == str(mod_id):
                leaf = _clean_subdir_segment((install_subdir or "").strip())
                if not leaf:
                    leaf = _clean_subdir_segment(f"{mod['id']}_{mod.get('title', 'mod')}")
                mod["install_subdir"] = f"{manager_root}/{leaf}"
                changed = True
                break
        if changed:
            self.store.save_mods(mods)
        return changed

    @staticmethod
    def _is_update_needed(mod: dict[str, Any], meta: dict[str, Any]) -> bool:
        remote_version = (meta.get("software_version") or "").strip()
        remote_date = (meta.get("date_modified") or "").strip()
        local_version = (mod.get("version") or "").strip()
        local_date = (mod.get("date_modified") or "").strip()

        if not remote_version and not remote_date:
            return False
        if not local_version and not local_date:
            return True
        if remote_version and remote_version != local_version:
            return True
        if remote_date and remote_date != local_date:
            return True
        return False

    @staticmethod
    def _metadata_delay_seconds(settings: dict[str, Any]) -> int:
        min_delay = max(0, int(settings.get("metadata_min_delay_seconds", 2)))
        max_delay = max(min_delay, int(settings.get("metadata_max_delay_seconds", 6)))
        return random.randint(min_delay, max_delay)

    @staticmethod
    def _download_spacing_seconds(settings: dict[str, Any]) -> int:
        min_delay = max(0, int(settings.get("download_min_delay_seconds", 90)))
        max_delay = max(min_delay, int(settings.get("download_max_delay_seconds", 180)))
        return random.randint(min_delay, max_delay)

    @staticmethod
    def _backoff_seconds(settings: dict[str, Any], attempts: int) -> int:
        base = max(1, int(settings.get("backoff_base_minutes", 5))) * 60
        cap = max(base, int(settings.get("backoff_max_minutes", 720)) * 60)
        return min(cap, base * (2 ** max(0, attempts - 1)))

    @staticmethod
    def _prune_download_events(runtime: dict[str, Any], now: dt.datetime) -> list[dt.datetime]:
        out: list[dt.datetime] = []
        for raw in runtime.get("download_events", []) or []:
            ts = _parse_iso(raw)
            if ts is None:
                continue
            if (now - ts).total_seconds() <= 86400:
                out.append(ts)
        out.sort()
        runtime["download_events"] = [_to_iso(x) for x in out]
        return out

    def _rate_gate(self, settings: dict[str, Any], runtime: dict[str, Any], now: dt.datetime) -> tuple[bool, int, str]:
        cooldown_until = _parse_iso(runtime.get("cooldown_until"))
        if cooldown_until and cooldown_until > now:
            return False, _seconds_until(cooldown_until, now), "cooldown"

        next_download_after = _parse_iso(runtime.get("next_download_after"))
        if next_download_after and next_download_after > now:
            return False, _seconds_until(next_download_after, now), "spacing"

        events = self._prune_download_events(runtime, now)
        per_hour = max(1, int(settings.get("max_downloads_per_hour", 10)))
        per_day = max(1, int(settings.get("max_downloads_per_day", 100)))

        hourly = [e for e in events if (now - e).total_seconds() <= 3600]
        if len(hourly) >= per_hour:
            oldest = hourly[0]
            wait = int((oldest + dt.timedelta(hours=1) - now).total_seconds()) + 1
            return False, max(1, wait), "hourly_limit"

        if len(events) >= per_day:
            oldest = events[0]
            wait = int((oldest + dt.timedelta(days=1) - now).total_seconds()) + 1
            return False, max(1, wait), "daily_limit"

        return True, 0, "ok"

    def _record_download_start(self, settings: dict[str, Any], runtime: dict[str, Any], now: dt.datetime) -> None:
        events = self._prune_download_events(runtime, now)
        events.append(now)
        runtime["download_events"] = [_to_iso(x) for x in events]
        runtime["next_download_after"] = _to_iso(now + dt.timedelta(seconds=self._download_spacing_seconds(settings)))
        runtime["last_queue_action"] = _to_iso(now)

    def _download_file(self, item: dict[str, Any], settings: dict[str, Any]) -> Path:
        downloads_dir = Path(settings["downloads_dir"]).expanduser()
        backend = str(settings.get("download_backend", "cdp_preferred") or "cdp_preferred")

        if proxy_enabled(settings):
            return download_mod_file(item["download_url"], settings, downloads_dir)

        if backend == "cdp":
            try:
                return download_via_cdp(item["download_url"], downloads_dir, settings)
            except CDPDownloadError as exc:
                raise LLRequestError(str(exc), signal="cdp_error") from exc

        if backend == "http":
            return download_mod_file(item["download_url"], settings, downloads_dir)

        if backend == "http_preferred":
            try:
                return download_mod_file(item["download_url"], settings, downloads_dir)
            except LLRequestError:
                try:
                    return download_via_cdp(item["download_url"], downloads_dir, settings)
                except CDPDownloadError as cdp_exc:
                    raise LLRequestError(str(cdp_exc), signal="cdp_error") from cdp_exc

        # default: cdp_preferred
        try:
            return download_via_cdp(item["download_url"], downloads_dir, settings)
        except CDPDownloadError:
            return download_mod_file(item["download_url"], settings, downloads_dir)

    def _apply_signal_cooldown(
        self,
        settings: dict[str, Any],
        runtime: dict[str, Any],
        *,
        signal: str,
        retry_after: int | None,
        attempts: int,
        now: dt.datetime,
    ) -> int:
        signal = signal or "unknown"
        runtime["last_signal"] = signal

        if signal == "http_429":
            wait = retry_after or max(60, int(settings.get("cooldown_429_minutes", 60)) * 60)
            runtime["cooldown_until"] = _to_iso(now + dt.timedelta(seconds=wait))
            runtime["consecutive_503"] = 0
            return wait

        if signal in {"cloudflare_1020", "cloudflare_challenge", "login_required"}:
            wait = max(3600, int(settings.get("cooldown_hard_block_hours", 24)) * 3600)
            runtime["cooldown_until"] = _to_iso(now + dt.timedelta(seconds=wait))
            runtime["consecutive_503"] = 0
            return wait

        if signal == "http_503":
            runtime["consecutive_503"] = int(runtime.get("consecutive_503", 0)) + 1
            wait = max(
                int(settings.get("cooldown_503_minutes", 60)) * 60,
                self._backoff_seconds(settings, attempts),
            )
            if int(runtime.get("consecutive_503", 0)) >= 3:
                wait = max(wait, 12 * 3600)
            runtime["cooldown_until"] = _to_iso(now + dt.timedelta(seconds=wait))
            return wait

        runtime["consecutive_503"] = 0
        return self._backoff_seconds(settings, attempts)

    @staticmethod
    def _queue_item_for_mod(queue: list[dict[str, Any]], mod_id: str) -> dict[str, Any] | None:
        for item in queue:
            if str(item.get("mod_id")) == str(mod_id):
                return item
        return None

    def _enqueue_update(
        self,
        queue: list[dict[str, Any]],
        mod: dict[str, Any],
        meta: dict[str, Any],
    ) -> dict[str, Any]:
        now_iso = _now_iso()
        current = self._queue_item_for_mod(queue, str(mod["id"]))
        if current is None:
            item = _default_queue_item(mod, meta, now_iso)
            queue.append(item)
            return item

        current["title"] = mod.get("title", current.get("title", ""))
        current["download_url"] = meta.get("download_url", current.get("download_url", ""))
        current["target_version"] = meta.get("software_version", current.get("target_version", ""))
        current["target_date_modified"] = meta.get("date_modified", current.get("target_date_modified", ""))
        current["state"] = "queued"
        current["updated_at"] = now_iso
        current["completed_at"] = ""
        current["last_error"] = ""
        return current

    def _select_mods_for_check(
        self,
        mods: list[dict[str, Any]],
        runtime: dict[str, Any],
        enabled_only: bool,
        max_checks: int,
    ) -> list[dict[str, Any]]:
        pool = [m for m in mods if (m.get("enabled", False) or not enabled_only)]
        if not pool:
            return []

        max_checks = max(1, min(max_checks, len(pool)))
        cursor = int(runtime.get("metadata_cursor", 0)) % len(pool)

        selected: list[dict[str, Any]] = []
        for idx in range(max_checks):
            selected.append(pool[(cursor + idx) % len(pool)])

        runtime["metadata_cursor"] = (cursor + max_checks) % len(pool)
        return selected

    def check_updates(self, install: bool, enabled_only: bool = True) -> dict[str, Any]:
        settings = self.store.get_settings()
        mods = self.store.get_mods()
        queue = self.store.get_queue()
        runtime = self.store.get_runtime()

        max_checks = int(settings.get("max_metadata_checks_per_run", 50))
        targets = self._select_mods_for_check(mods, runtime, enabled_only, max_checks)

        checked = 0
        updates_available = 0
        queued = 0
        errors = 0
        details: list[dict[str, Any]] = []

        for idx, mod in enumerate(targets):
            checked += 1
            item = {"id": mod["id"], "title": mod.get("title", ""), "action": "checked"}

            try:
                meta = fetch_mod_metadata(mod["url"], settings)
                now_iso = _now_iso()
                mod["last_checked"] = now_iso
                mod["remote_version"] = meta.get("software_version", "")
                mod["remote_date_modified"] = meta.get("date_modified", "")

                needs_update = self._is_update_needed(mod, meta)
                if needs_update:
                    updates_available += 1
                    if install and mod.get("enabled", False):
                        self._enqueue_update(queue, mod, meta)
                        mod["status"] = "queued"
                        item["action"] = "queued"
                        queued += 1
                    else:
                        mod["status"] = "update_available"
                        item["action"] = "update_available"
                else:
                    mod["status"] = "up_to_date"
                    item["action"] = "up_to_date"

            except LLRequestError as exc:
                errors += 1
                mod["last_checked"] = _now_iso()
                mod["status"] = f"error: {exc}"
                item["action"] = "error"
                item["error"] = str(exc)
                runtime["last_error"] = str(exc)
                wait = self._apply_signal_cooldown(
                    settings,
                    runtime,
                    signal=exc.signal,
                    retry_after=exc.retry_after,
                    attempts=1,
                    now=_utcnow(),
                )
                item["cooldown_seconds"] = wait

                if exc.signal in {"http_429", "http_503", "cloudflare_challenge", "cloudflare_1020", "login_required"}:
                    details.append(item)
                    break

            except Exception as exc:
                errors += 1
                mod["last_checked"] = _now_iso()
                mod["status"] = f"error: {exc}"
                item["action"] = "error"
                item["error"] = str(exc)

            details.append(item)

            if idx < len(targets) - 1:
                time.sleep(self._metadata_delay_seconds(settings))

        self.store.save_mods(mods)
        self.store.save_queue(queue)
        self.store.save_runtime(runtime)

        return {
            "checked": checked,
            "updates_available": updates_available,
            "queued": queued,
            "errors": errors,
            "details": details,
            "queue_size": len(queue),
        }

    def process_queue_once(self, force: bool = False) -> dict[str, Any]:
        settings = self.store.get_settings()
        if not force and not bool(settings.get("queue_worker_enabled", True)):
            return {"processed": False, "reason": "queue_worker_disabled"}

        queue = self.store.get_queue()
        mods = self.store.get_mods()
        runtime = self.store.get_runtime()
        now = _utcnow()

        eligible_states = {"queued", "retry", "in_progress"}
        candidates = [x for x in queue if x.get("state") in eligible_states]
        if not candidates:
            return {"processed": False, "reason": "queue_empty"}

        ready: list[dict[str, Any]] = []
        wait_targets: list[dt.datetime] = []
        for item in candidates:
            not_before = _parse_iso(item.get("not_before"))
            if not_before and not_before > now:
                wait_targets.append(not_before)
                continue
            ready.append(item)

        if not ready:
            next_ready = min(wait_targets) if wait_targets else None
            return {
                "processed": False,
                "reason": "queue_waiting",
                "wait_seconds": _seconds_until(next_ready, now),
            }

        ready.sort(key=lambda x: (x.get("created_at") or "", x.get("mod_id") or ""))
        item = ready[0]

        allowed, wait_seconds, gate_reason = self._rate_gate(settings, runtime, now)
        if not allowed:
            self.store.save_runtime(runtime)
            return {
                "processed": False,
                "reason": gate_reason,
                "wait_seconds": wait_seconds,
            }

        mod = next((m for m in mods if str(m.get("id")) == str(item.get("mod_id"))), None)
        if mod is None:
            item["state"] = "failed"
            item["last_error"] = "mod_not_found"
            item["updated_at"] = _now_iso()
            self.store.save_queue(queue)
            return {"processed": False, "reason": "mod_not_found", "mod_id": item.get("mod_id")}

        item["state"] = "in_progress"
        item["attempts"] = int(item.get("attempts", 0)) + 1
        item["updated_at"] = _now_iso()

        manager_root_subdir = str(settings.get("manager_root_subdir") or "_LL_MOD_MANAGER")
        mod["install_subdir"] = _default_mod_entry(
            str(mod["id"]),
            str(mod.get("title", "")),
            str(mod.get("url", "")),
            manager_root_subdir,
        )["install_subdir"]

        self.store.save_queue(queue)

        self._record_download_start(settings, runtime, now)

        try:
            download_path = self._download_file(item, settings)
            deployed = deploy_download(download_path, mod, settings)

            mod["version"] = item.get("target_version") or mod.get("remote_version") or mod.get("version", "")
            mod["date_modified"] = item.get("target_date_modified") or mod.get("remote_date_modified") or mod.get("date_modified", "")
            mod["last_installed"] = _now_iso()
            mod["deployed_files"] = deployed
            mod["status"] = "updated"

            item["state"] = "done"
            item["download_path"] = str(download_path)
            item["last_error"] = ""
            item["completed_at"] = _now_iso()
            item["updated_at"] = _now_iso()

            runtime["consecutive_503"] = 0
            runtime["last_signal"] = ""
            runtime["last_error"] = ""
            runtime["last_queue_action"] = _now_iso()

            self.store.save_mods(mods)
            self.store.save_queue(queue)
            self.store.save_runtime(runtime)

            return {
                "processed": True,
                "action": "updated",
                "mod_id": mod["id"],
                "title": mod.get("title", ""),
                "deployed_count": len(deployed),
            }

        except LLRequestError as exc:
            wait = self._apply_signal_cooldown(
                settings,
                runtime,
                signal=exc.signal,
                retry_after=exc.retry_after,
                attempts=int(item.get("attempts", 1)),
                now=_utcnow(),
            )
            retry_limit = max(1, int(settings.get("queue_retry_limit", 20)))
            item["state"] = "queued" if int(item.get("attempts", 0)) < retry_limit else "failed"
            item["not_before"] = _to_iso(_utcnow() + dt.timedelta(seconds=wait))
            item["last_error"] = str(exc)
            item["updated_at"] = _now_iso()
            mod["status"] = f"error: {exc}"

            runtime["last_error"] = str(exc)
            runtime["last_queue_action"] = _now_iso()

            self.store.save_mods(mods)
            self.store.save_queue(queue)
            self.store.save_runtime(runtime)

            return {
                "processed": False,
                "reason": "download_error",
                "mod_id": mod["id"],
                "error": str(exc),
                "signal": exc.signal,
                "wait_seconds": wait,
            }

        except Exception as exc:
            wait = self._backoff_seconds(settings, int(item.get("attempts", 1)))
            retry_limit = max(1, int(settings.get("queue_retry_limit", 20)))

            item["state"] = "queued" if int(item.get("attempts", 0)) < retry_limit else "failed"
            item["not_before"] = _to_iso(_utcnow() + dt.timedelta(seconds=wait))
            item["last_error"] = str(exc)
            item["updated_at"] = _now_iso()
            mod["status"] = f"error: {exc}"

            runtime["last_error"] = str(exc)
            runtime["last_signal"] = "unknown"
            runtime["last_queue_action"] = _now_iso()

            self.store.save_mods(mods)
            self.store.save_queue(queue)
            self.store.save_runtime(runtime)

            return {
                "processed": False,
                "reason": "runtime_error",
                "mod_id": mod["id"],
                "error": str(exc),
                "wait_seconds": wait,
            }

    def reset_runtime_limits(self) -> dict[str, Any]:
        runtime = self.store.save_runtime(
            {
                "next_download_after": "",
                "cooldown_until": "",
                "consecutive_503": 0,
                "last_signal": "",
                "last_error": "",
            }
        )
        return runtime

    def clear_completed_queue(self) -> dict[str, int]:
        queue = self.store.get_queue()
        before = len(queue)
        queue = [x for x in queue if x.get("state") != "done"]
        after = len(queue)
        self.store.save_queue(queue)
        return {"removed": before - after, "left": after}

    def queue_snapshot(self) -> dict[str, Any]:
        queue = self.store.get_queue()
        runtime = self.store.get_runtime()

        counts: dict[str, int] = {}
        for item in queue:
            state = item.get("state", "unknown")
            counts[state] = counts.get(state, 0) + 1

        now = _utcnow()
        pending = [x for x in queue if x.get("state") in {"queued", "retry", "in_progress"}]
        pending.sort(key=lambda x: (x.get("not_before") or "", x.get("created_at") or ""))
        next_item = pending[0] if pending else None
        next_not_before = _parse_iso(next_item.get("not_before")) if next_item else None

        return {
            "total": len(queue),
            "counts": counts,
            "next_mod_id": next_item.get("mod_id") if next_item else None,
            "next_wait_seconds": _seconds_until(next_not_before, now),
            "cooldown_seconds": _seconds_until(_parse_iso(runtime.get("cooldown_until")), now),
            "next_download_spacing_seconds": _seconds_until(_parse_iso(runtime.get("next_download_after")), now),
            "runtime": runtime,
            "items": queue,
        }

    def get_mod_details(
        self,
        mod_id: str,
        force_refresh: bool = False,
        allow_remote_fetch: bool = False,
    ) -> dict[str, Any]:
        mods = self.store.get_mods()
        settings = self.store.get_settings()
        use_remote_images = self._use_remote_images(settings)
        target = next((m for m in mods if str(m.get("id")) == str(mod_id)), None)
        if target is None:
            raise ValueError("mod not found")

        cached_details = target.get("details") if isinstance(target.get("details"), dict) else None
        stale = self._details_cache_stale(target, settings)
        corrupted = self._details_data_corrupted(target)

        if isinstance(cached_details, dict):
            cached_details = self._sanitize_cached_details(target, cached_details)

        has_remote_visuals = False
        if isinstance(cached_details, dict):
            has_remote_visuals = bool(cached_details.get("images")) or bool(str(cached_details.get("thumbnail_url") or target.get("thumbnail_url") or "").strip())

        if isinstance(cached_details, dict) and not force_refresh and not stale and not corrupted and (not use_remote_images or has_remote_visuals):
            if not cached_details.get("thumbnail_cached_url") and target.get("thumbnail_cached_url") and self._media_url_available(str(target.get("thumbnail_cached_url") or "")):
                cached_details["thumbnail_cached_url"] = target.get("thumbnail_cached_url")
            if not cached_details.get("cached_images") and target.get("thumbnail_cached_url") and self._media_url_available(str(target.get("thumbnail_cached_url") or "")):
                cached_details["cached_images"] = [target.get("thumbnail_cached_url")]
            return cached_details

        if allow_remote_fetch:
            details = fetch_mod_details(target["url"], settings)
            if use_remote_images:
                details["cached_images"] = []
                details["thumbnail_cached_url"] = ""
            else:
                details = self._cache_details_media(target, details, settings, force_refresh=force_refresh or stale)
            target["details"] = details
            target["details_cached_at"] = _now_iso()
            target["details_error"] = ""
            self.store.save_mods(mods)
            return details

        fallback = self._build_fallback_details(target)
        if corrupted:
            return fallback
        if isinstance(cached_details, dict):
            merged = dict(cached_details)
            if not merged.get("cached_images"):
                merged["cached_images"] = fallback.get("cached_images", [])
            if not merged.get("thumbnail_cached_url"):
                merged["thumbnail_cached_url"] = fallback.get("thumbnail_cached_url", "")
            if not merged.get("thumbnail_url"):
                merged["thumbnail_url"] = fallback.get("thumbnail_url", "")
            return merged
        return fallback
