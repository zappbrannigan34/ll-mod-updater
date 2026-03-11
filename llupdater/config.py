from pathlib import Path

APP_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = APP_DIR / "data"

SETTINGS_FILE = DATA_DIR / "settings.json"
MODS_FILE = DATA_DIR / "mods.json"
QUEUE_FILE = DATA_DIR / "queue.json"
RUNTIME_FILE = DATA_DIR / "runtime.json"

SIMS4_MODS_DEFAULT = Path.home() / "Documents" / "Electronic Arts" / "The Sims 4" / "Mods"

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

DEFAULT_SETTINGS = {
    "mods_dir": str(SIMS4_MODS_DEFAULT),
    "staging_dir": str(DATA_DIR / "staging"),
    "downloads_dir": str(DATA_DIR / "downloads"),
    "backups_dir": str(DATA_DIR / "backups"),
    "deploy_method": "copy",
    "scan_pages": 5,
    "poll_minutes": 60,
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
    "ll_cookie": "",
    "user_agent": DEFAULT_USER_AGENT,
    "request_timeout": 45,
}

BASE_URL = "https://www.loverslab.com"
CATEGORY_URL_TEMPLATE = BASE_URL + "/files/category/161-the-sims-4/page/{page}/"
