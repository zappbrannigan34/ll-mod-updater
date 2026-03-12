import os
import sys
from pathlib import Path

if getattr(sys, "frozen", False):
    APP_DIR = Path(sys.executable).resolve().parent
else:
    APP_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = APP_DIR / "data"
MEDIA_CACHE_DIR = DATA_DIR / "media_cache"

SETTINGS_FILE = DATA_DIR / "settings.json"
MODS_FILE = DATA_DIR / "mods.json"
CATEGORIES_FILE = DATA_DIR / "categories.json"
QUEUE_FILE = DATA_DIR / "queue.json"
RUNTIME_FILE = DATA_DIR / "runtime.json"


def _detect_sims4_mods_dir() -> Path:
    def windows_documents_dir() -> Path | None:
        if os.name != "nt":
            return None
        try:
            import winreg  # type: ignore

            key_path = r"Software\Microsoft\Windows\CurrentVersion\Explorer\User Shell Folders"
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path) as key:
                raw, _ = winreg.QueryValueEx(key, "Personal")

            expanded = os.path.expandvars(str(raw))
            return Path(expanded)
        except Exception:
            return None

    home = Path.home()
    one_drive = Path(os.environ["OneDrive"]) if os.environ.get("OneDrive") else None
    documents_dir = windows_documents_dir()

    candidates: list[Path] = []
    if documents_dir is not None:
        candidates.append(documents_dir / "Electronic Arts" / "The Sims 4" / "Mods")

    if one_drive is not None:
        candidates.append(one_drive / "Documents" / "Electronic Arts" / "The Sims 4" / "Mods")
        candidates.append(one_drive / "Docs" / "Electronic Arts" / "The Sims 4" / "Mods")

    candidates.append(home / "Documents" / "Electronic Arts" / "The Sims 4" / "Mods")
    candidates.append(home / "OneDrive" / "Documents" / "Electronic Arts" / "The Sims 4" / "Mods")
    candidates.append(home / "OneDrive" / "Docs" / "Electronic Arts" / "The Sims 4" / "Mods")

    for candidate in candidates:
        if candidate.exists():
            return candidate

    return candidates[0]


SIMS4_MODS_DEFAULT = _detect_sims4_mods_dir()

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

DEFAULT_SETTINGS = {
    "ui_language": "auto",
    "image_source_mode": "cache",
    "mods_dir": str(SIMS4_MODS_DEFAULT),
    "staging_dir": str(DATA_DIR / "staging"),
    "downloads_dir": str(DATA_DIR / "downloads"),
    "backups_dir": str(DATA_DIR / "backups"),
    "manager_root_subdir": "_LL_MOD_MANAGER",
    "deploy_method": "hardlink",
    "scan_pages": 5,
    "catalog_max_pages_per_category": 0,
    "catalog_page_delay_seconds": 1,
    "catalog_category_delay_seconds": 2,
    "catalog_max_categories_per_run": 0,
    "poll_minutes": 60,
    "auto_new_mods_enabled": True,
    "new_mods_poll_minutes": 180,
    "new_mods_scan_pages_per_category": 1,
    "new_mods_max_categories_per_run": 0,
    "new_mods_page_delay_seconds": 0.25,
    "new_mods_category_delay_seconds": 0.5,
    "new_mods_refresh_details_on_scan": True,
    "new_mods_details_max_per_run": 0,
    "new_mods_details_delay_seconds": 0.75,
    "new_mods_retry_failed_cache_enabled": True,
    "new_mods_retry_failed_cache_per_run": 25,
    "new_mods_retry_failed_cache_delay_seconds": 0.75,
    "queue_poll_seconds": 20,
    "auto_tracking_enabled": True,
    "queue_worker_enabled": True,
    "metadata_min_delay_seconds": 2,
    "metadata_max_delay_seconds": 6,
    "max_metadata_checks_per_run": 50,
    "download_min_delay_seconds": 90,
    "download_max_delay_seconds": 180,
    "max_downloads_per_hour": 10,
    "max_downloads_per_day": 100,
    "queue_retry_limit": 20,
    "backoff_base_minutes": 5,
    "backoff_max_minutes": 720,
    "cooldown_429_minutes": 60,
    "cooldown_503_minutes": 60,
    "cooldown_hard_block_hours": 24,
    "download_backend": "cdp_preferred",
    "cdp_endpoint": "http://127.0.0.1:9222",
    "cdp_download_timeout_seconds": 300,
    "details_cache_hours": 720,
    "image_cache_hours": 720,
    "refresh_details_on_full_scan": True,
    "details_max_per_full_scan": 0,
    "details_refresh_delay_seconds": 0.5,
    "proxy_enabled": False,
    "proxy_url": "",
    "ll_cookie": "",
    "user_agent": DEFAULT_USER_AGENT,
    "request_timeout": 45,
}

BASE_URL = "https://www.loverslab.com"
CATEGORY_URL_TEMPLATE = BASE_URL + "/files/category/161-the-sims-4/page/{page}/"
SIMS4_CATEGORY_URL = BASE_URL + "/files/category/161-the-sims-4/"
