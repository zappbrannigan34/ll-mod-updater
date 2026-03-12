const viewState = {
  mods: [],
  categories: [],
  modsSearch: "",
  modsEnabledFilter: "all",
  modsCategoryFilter: "",
  categorySearch: "",
  modsSort: "downloads_desc",
  locale: "en",
  settings: {},
  activeTab: "mods",
  selectedModId: "",
  detailsCache: {},
  detailsLoading: false,
  discoverLastRunning: false,
};

let discoverPollHandle = null;

const I18N = {
  en: {
    appTitle: "LL Sims 4 Mod Manager",
    tabMods: "Mods",
    tabQueue: "Queue",
    tabActions: "Actions",
    tabSettings: "Settings",
    tabLog: "Log",
    settingsTitle: "Settings",
    lblModsDir: "Mods folder",
    lblManagerRootSubdir: "Manager root subfolder",
    lblUiLanguage: "Interface language",
    lblImageSourceMode: "Image source mode",
    optLanguageAuto: "Auto (browser)",
    optLanguageEn: "English",
    optLanguageRu: "Russian",
    optImageSourceCache: "Local cache (recommended)",
    optImageSourceRemote: "Direct from site",
    lblDeployMethod: "Deploy method",
    lblScanPages: "Quick mode page count (N)",
    lblCatalogMaxPagesPerCategory: "Max pages/category (0=all)",
    lblPollMinutes: "Auto-check interval (minutes)",
    lblQueuePollSeconds: "Background installer tick (seconds)",
    lblMaxMetadataChecksPerRun: "Max metadata checks per run",
    lblMetadataMinDelaySeconds: "Metadata delay min (seconds)",
    lblMetadataMaxDelaySeconds: "Metadata delay max (seconds)",
    lblDownloadMinDelaySeconds: "Download delay min (seconds)",
    lblDownloadMaxDelaySeconds: "Download delay max (seconds)",
    lblMaxDownloadsPerHour: "Max downloads/hour",
    lblMaxDownloadsPerDay: "Max downloads/day",
    lblBackoffBaseMinutes: "Backoff base (minutes)",
    lblBackoffMaxMinutes: "Backoff max (minutes)",
    lblQueueRetryLimit: "Retry limit per item",
    lblCooldown429Minutes: "Cooldown on 429 (minutes)",
    lblCooldown503Minutes: "Cooldown on 503 (minutes)",
    lblCooldownHardBlockHours: "Hard cooldown (hours)",
    lblDownloadBackend: "Download backend",
    lblCdpEndpoint: "CDP endpoint",
    lblCdpDownloadTimeoutSeconds: "CDP download timeout (seconds)",
    lblProxyEnabled: "Use proxy for all LoversLab connections",
    lblProxyUrl: "Proxy URL",
    lblLlCookie: "Cookie (from logged-in browser)",
    lblUserAgent: "User-Agent",
    lblAutoTrackingEnabled: "Enable scheduled auto tracking",
    lblQueueWorkerEnabled: "Enable automatic installer",
    actionsTitle: "Actions",
    categoriesTitle: "Sims 4 Categories",
    modsTitle: "Mods",
    btnUpdateEnabledInMods: "Check updates now",
    queueTitle: "Queue",
    logTitle: "Log",
    detailsTitle: "Mod Details",
    btnRefreshDetails: "Refresh Details",
    btnSave: "Save settings",
    btnPickFolder: "Pick folder",
    btnSyncFull: "Full sync (all categories + cache refresh)",
    btnSyncQuick: "Check updates now",
    btnCheck: "Check updates now",
    btnEnqueue: "Check updates now",
    btnProcessOne: "",
    btnClearDone: "",
    btnResetLimits: "",
    btnAddUrl: "Add mod by URL",
    btnClearFilters: "Clear filters",
    searchMods: "Search by title/ID...",
    filterAllMods: "All mods",
    filterEnabledOnly: "Enabled only",
    filterDisabledOnly: "Disabled only",
    searchCats: "Search categories...",
    allCategories: "All categories",
    sortByPopularity: "Popularity (downloads)",
    sortTitleAsc: "Title A-Z",
    sortTitleDesc: "Title Z-A",
    sortIdDesc: "ID desc",
    sortIdAsc: "ID asc",
    thAuto: "Auto",
    thId: "ID",
    thTitle: "Title",
    thCategory: "Category",
    thLocal: "Local",
    thRemote: "Remote",
    thDownloads: "Downloads",
    thStatus: "Status",
    thInstall: "Install Subdir",
    queueTotal: "Total",
    queueNextMod: "Next mod_id",
    queueWait: "Wait until next",
    queueCooldown: "Global cooldown",
    queueSpacing: "Spacing until start",
    queueTop: "Top queue items",
    subdirPlaceholder: "mod subfolder name",
    selectFolderCancelled: "Folder selection cancelled",
    selectFolderDone: "Mods folder selected",
    settingsSaved: "Settings saved",
    catalogSync: "Sync",
    checkResult: "Checked",
    updatedAvailable: "updates_available",
    queued: "auto_install_started",
    errors: "errors",
    queueStep: "Queue step",
    queueCleared: "Queue cleanup",
    limitsReset: "Runtime cooldown/limits reset",
    modAdded: "Mod added",
    startupError: "Startup error",
    detailsNone: "Select a mod from the list.",
    detailsLoading: "Loading details...",
    detailsSummary: "Summary",
    detailsDescription: "Description",
    detailsImages: "Images",
    detailsOpenPage: "Open LoversLab page",
    detailsDownloads: "Downloads",
    detailsCategory: "Category",
    detailsStatus: "Status",
    detailsVersion: "Version",
    detailsRemoteVersion: "Remote version",
    discoverProgressTitle: "Full scan progress",
    discoverIdle: "No scan running.",
    discoverStage: "Stage",
    discoverElapsed: "Elapsed",
    discoverCategory: "Current category",
    discoverCategoryPages: "Category pages",
    discoverResume: "Resumed from",
    discoverCheckpoint: "Checkpoint",
    discoverRetries: "Retries",
    discoverBackoff: "Retry backoff",
    discoverCategories: "Categories",
    discoverMods: "Mods",
    discoverCache: "Cache pass",
    discoverDetails: "Details",
    discoverError: "Error",
    discoverStarted: "Full scan started in background",
    discoverAlreadyRunning: "Scan is already running, showing live progress",
  },
  ru: {
    appTitle: "LL Sims 4 Менеджер Модов",
    tabMods: "Моды",
    tabQueue: "Очередь",
    tabActions: "Действия",
    tabSettings: "Настройки",
    tabLog: "Лог",
    settingsTitle: "Настройки",
    lblModsDir: "Папка Mods",
    lblManagerRootSubdir: "Подкаталог менеджера",
    lblUiLanguage: "Язык интерфейса",
    lblImageSourceMode: "Источник картинок",
    optLanguageAuto: "Авто (язык браузера)",
    optLanguageEn: "English",
    optLanguageRu: "Русский",
    optImageSourceCache: "Локальный кэш (рекомендуется)",
    optImageSourceRemote: "Напрямую с сайта",
    lblDeployMethod: "Метод деплоя",
    lblScanPages: "Количество страниц для быстрого режима (N)",
    lblCatalogMaxPagesPerCategory: "Макс. страниц/категорию (0=все)",
    lblPollMinutes: "Интервал автопроверки (мин)",
    lblQueuePollSeconds: "Тик фоновой установки (сек)",
    lblMaxMetadataChecksPerRun: "Макс. metadata-проверок за запуск",
    lblMetadataMinDelaySeconds: "Metadata delay min (сек)",
    lblMetadataMaxDelaySeconds: "Metadata delay max (сек)",
    lblDownloadMinDelaySeconds: "Download delay min (сек)",
    lblDownloadMaxDelaySeconds: "Download delay max (сек)",
    lblMaxDownloadsPerHour: "Макс. скачиваний/час",
    lblMaxDownloadsPerDay: "Макс. скачиваний/день",
    lblBackoffBaseMinutes: "Backoff base (мин)",
    lblBackoffMaxMinutes: "Backoff max (мин)",
    lblQueueRetryLimit: "Лимит retry на элемент",
    lblCooldown429Minutes: "Cooldown на 429 (мин)",
    lblCooldown503Minutes: "Cooldown на 503 (мин)",
    lblCooldownHardBlockHours: "Жесткий cooldown (часы)",
    lblDownloadBackend: "Download backend",
    lblCdpEndpoint: "CDP endpoint",
    lblCdpDownloadTimeoutSeconds: "CDP timeout (сек)",
    lblProxyEnabled: "Использовать прокси для всех соединений LoversLab",
    lblProxyUrl: "URL прокси",
    lblLlCookie: "Cookie (из залогиненного браузера)",
    lblUserAgent: "User-Agent",
    lblAutoTrackingEnabled: "Включить автообновление по таймеру",
    lblQueueWorkerEnabled: "Включить автоматическую установку",
    actionsTitle: "Действия",
    categoriesTitle: "Категории Sims 4",
    modsTitle: "Моды",
    btnUpdateEnabledInMods: "Проверить обновления сейчас",
    queueTitle: "Очередь",
    logTitle: "Лог",
    detailsTitle: "Карточка мода",
    btnRefreshDetails: "Обновить карточку",
    btnSave: "Сохранить настройки",
    btnPickFolder: "Выбрать папку",
    btnSyncFull: "Полный sync (все категории + refresh кэша)",
    btnSyncQuick: "Проверить обновления сейчас",
    btnCheck: "Проверить обновления сейчас",
    btnEnqueue: "Проверить обновления сейчас",
    btnProcessOne: "",
    btnClearDone: "",
    btnResetLimits: "",
    btnAddUrl: "Добавить мод по URL",
    btnClearFilters: "Сбросить фильтры",
    searchMods: "Поиск по названию/ID...",
    filterAllMods: "Все моды",
    filterEnabledOnly: "Только включенные",
    filterDisabledOnly: "Только выключенные",
    searchCats: "Поиск категории...",
    allCategories: "Все категории",
    sortByPopularity: "Популярность (скачивания)",
    sortTitleAsc: "Название A-Z",
    sortTitleDesc: "Название Z-A",
    sortIdDesc: "ID убыв.",
    sortIdAsc: "ID возр.",
    thAuto: "Auto",
    thId: "ID",
    thTitle: "Название",
    thCategory: "Категория",
    thLocal: "Текущая",
    thRemote: "Удаленная",
    thDownloads: "Скачивания",
    thStatus: "Статус",
    thInstall: "Подпапка установки",
    queueTotal: "Всего",
    queueNextMod: "Следующий mod_id",
    queueWait: "Ждать до следующего",
    queueCooldown: "Глобальный cooldown",
    queueSpacing: "Spacing до следующего старта",
    queueTop: "Top queue items",
    subdirPlaceholder: "имя подпапки мода",
    selectFolderCancelled: "Выбор папки отменен",
    selectFolderDone: "Папка Mods выбрана",
    settingsSaved: "Настройки сохранены",
    catalogSync: "Sync",
    checkResult: "Проверено",
    updatedAvailable: "update_available",
    queued: "запущено автообновлений",
    errors: "ошибок",
    queueStep: "Queue step",
    queueCleared: "Очистка очереди",
    limitsReset: "Сброшены cooldown/лимиты рантайма",
    modAdded: "Добавлен мод",
    startupError: "Ошибка запуска",
    detailsNone: "Выбери мод в списке.",
    detailsLoading: "Загружаю карточку...",
    detailsSummary: "Кратко",
    detailsDescription: "Описание",
    detailsImages: "Картинки",
    detailsOpenPage: "Открыть страницу LoversLab",
    detailsDownloads: "Скачивания",
    detailsCategory: "Категория",
    detailsStatus: "Статус",
    detailsVersion: "Версия",
    detailsRemoteVersion: "Удаленная версия",
    discoverProgressTitle: "Прогресс полного скана",
    discoverIdle: "Скан сейчас не запущен.",
    discoverStage: "Этап",
    discoverElapsed: "Прошло",
    discoverCategory: "Текущая категория",
    discoverCategoryPages: "Страницы категории",
    discoverResume: "Возобновлено с",
    discoverCheckpoint: "Чекпоинт",
    discoverRetries: "Повторы",
    discoverBackoff: "Пауза перед повтором",
    discoverCategories: "Категории",
    discoverMods: "Моды",
    discoverCache: "Проход кэша",
    discoverDetails: "Детали",
    discoverError: "Ошибка",
    discoverStarted: "Полный скан запущен в фоне",
    discoverAlreadyRunning: "Скан уже идет, показываю live-прогресс",
  },
};

const TOOLTIPS = {
  en: {
    tab_btn_mods: "Open the mods catalog, filters, and mod card.",
    tab_btn_queue: "Open queue status and pending work items.",
    tab_btn_actions: "Open manual actions for sync, checks, and queue operations.",
    tab_btn_settings: "Open application settings.",
    tab_btn_log: "Open runtime log output.",
    mods_dir: "Target The Sims 4 Mods directory for deployment.",
    pick_mods_dir: "Choose Mods directory from system dialog.",
    manager_root_subdir: "Root subfolder created inside Mods for this manager.",
    ui_language: "Set UI language manually, or follow browser language with Auto.",
    image_source_mode: "Choose local cached images or direct remote images from LoversLab.",
    deploy_method: "How files are deployed: copy, hardlink, or symlink.",
    scan_pages: "Pages for quick sync mode.",
    catalog_max_pages_per_category: "Limit pages per category for full sync. 0 means all pages.",
    poll_minutes: "How often automatic update checks run.",
    queue_poll_seconds: "How often background installer checks pending installs.",
    max_metadata_checks_per_run: "Max mods checked per run to reduce load.",
    metadata_min_delay_seconds: "Minimum delay between metadata requests.",
    metadata_max_delay_seconds: "Maximum delay between metadata requests.",
    download_min_delay_seconds: "Minimum delay between downloads.",
    download_max_delay_seconds: "Maximum delay between downloads.",
    max_downloads_per_hour: "Hourly download cap.",
    max_downloads_per_day: "Daily download cap.",
    backoff_base_minutes: "Base retry backoff for failed queue items.",
    backoff_max_minutes: "Maximum retry backoff cap.",
    queue_retry_limit: "Max retry attempts for one queue item.",
    cooldown_429_minutes: "Cooldown after HTTP 429 rate limit.",
    cooldown_503_minutes: "Cooldown after HTTP 503 server errors.",
    cooldown_hard_block_hours: "Long cooldown after hard block/challenge signals.",
    download_backend: "Downloader backend selection.",
    cdp_endpoint: "Chrome DevTools endpoint used by CDP downloader.",
    cdp_download_timeout_seconds: "Timeout for a single CDP download.",
    proxy_enabled: "Force all LoversLab app requests through configured proxy.",
    proxy_url: "Proxy URL. Required when proxy mode is enabled.",
    ll_cookie: "LoversLab cookie from logged-in browser session.",
    user_agent: "User-Agent header for app requests.",
    auto_tracking_enabled: "Enable periodic automatic update checks.",
    queue_worker_enabled: "Enable background automatic installer.",
    save_settings: "Save current settings to disk.",
    discover: "Run full Sims 4 sync and refresh local details/image cache.",
    discover_quick: "Checks enabled mods and installs updates automatically.",
    check_only: "Check selected mods for updates only.",
    update_enabled: "Check enabled mods and enqueue updates.",
    process_queue_once: "Process one queue item now.",
    clear_done_queue: "Remove completed items from queue.",
    reset_runtime_limits: "Reset cooldown and runtime rate-limit counters.",
    custom_url: "Paste LoversLab file URL to add a mod manually.",
    add_mod: "Add mod from the URL field.",
    mods_search: "Search mods by title, id, or category.",
    mods_enabled_filter: "Show all mods, enabled only, or disabled only.",
    mods_category_filter: "Filter list by category.",
    mods_sort: "Sort visible mods.",
    mods_update_enabled: "Check updates for enabled mods and enqueue installs.",
    mods_clear_filters: "Reset search, category, and sort.",
    refresh_details: "Refresh selected mod details and local media cache.",
    tooltipCategoryChip: "Filter mod list by this category",
    tooltipModRow: "Click to select this mod and open its card",
    tooltipModToggle: "Enable automatic update tracking for this mod",
    tooltipModLink: "Open this mod page on LoversLab",
    tooltipInstallSubdir: "Subfolder name under manager root for this mod",
    tooltipOpenPage: "Open selected mod page on LoversLab",
  },
  ru: {
    tab_btn_mods: "Открыть каталог модов, фильтры и карточку мода.",
    tab_btn_queue: "Открыть состояние и элементы очереди.",
    tab_btn_actions: "Открыть ручные действия: sync, проверки и очередь.",
    tab_btn_settings: "Открыть настройки приложения.",
    tab_btn_log: "Открыть лог работы приложения.",
    mods_dir: "Папка The Sims 4 Mods, куда идет деплой.",
    pick_mods_dir: "Выбрать папку Mods через системный диалог.",
    manager_root_subdir: "Корневой подкаталог менеджера внутри Mods.",
    ui_language: "Ручной выбор языка интерфейса или Auto по языку браузера.",
    image_source_mode: "Выбери локальные кэш-картинки или прямые картинки с LoversLab.",
    deploy_method: "Способ деплоя: copy, hardlink или symlink.",
    scan_pages: "Количество страниц для быстрого sync.",
    catalog_max_pages_per_category: "Лимит страниц на категорию для full sync. 0 = все страницы.",
    poll_minutes: "Интервал автоматической проверки обновлений.",
    queue_poll_seconds: "Как часто фоновая установка проверяет ожидающие установки.",
    max_metadata_checks_per_run: "Максимум модов на одну проверку, чтобы снизить нагрузку.",
    metadata_min_delay_seconds: "Минимальная задержка между metadata-запросами.",
    metadata_max_delay_seconds: "Максимальная задержка между metadata-запросами.",
    download_min_delay_seconds: "Минимальная задержка между скачиваниями.",
    download_max_delay_seconds: "Максимальная задержка между скачиваниями.",
    max_downloads_per_hour: "Лимит скачиваний в час.",
    max_downloads_per_day: "Лимит скачиваний в день.",
    backoff_base_minutes: "Базовый backoff для повторов в очереди.",
    backoff_max_minutes: "Максимальный потолок backoff.",
    queue_retry_limit: "Максимум попыток для одного элемента очереди.",
    cooldown_429_minutes: "Пауза после HTTP 429 (rate limit).",
    cooldown_503_minutes: "Пауза после HTTP 503.",
    cooldown_hard_block_hours: "Длинная пауза после hard block/challenge.",
    download_backend: "Выбор backend-а для скачивания.",
    cdp_endpoint: "Адрес Chrome DevTools для CDP-скачиваний.",
    cdp_download_timeout_seconds: "Таймаут одного CDP-скачивания.",
    proxy_enabled: "Принудительно вести все запросы к LoversLab через прокси.",
    proxy_url: "URL прокси. Обязателен при включенном proxy mode.",
    ll_cookie: "Cookie LoversLab из залогиненного браузера.",
    user_agent: "Заголовок User-Agent для запросов.",
    auto_tracking_enabled: "Включить периодическую авто-проверку обновлений.",
    queue_worker_enabled: "Включить фоновую автоматическую установку.",
    save_settings: "Сохранить текущие настройки на диск.",
    discover: "Запустить полный sync Sims 4 и refresh локального кэша деталей/картинок.",
    discover_quick: "Проверяет включенные моды и автоматически устанавливает обновления.",
    check_only: "Только проверить моды на обновления.",
    update_enabled: "Проверить включенные моды и поставить обновления в очередь.",
    process_queue_once: "Сейчас обработать 1 элемент очереди.",
    clear_done_queue: "Удалить завершенные элементы из очереди.",
    reset_runtime_limits: "Сбросить cooldown и runtime-счетчики лимитов.",
    custom_url: "Вставь URL мода LoversLab для ручного добавления.",
    add_mod: "Добавить мод из поля URL.",
    mods_search: "Поиск модов по названию, id или категории.",
    mods_enabled_filter: "Показывать все моды, только включенные или только выключенные.",
    mods_category_filter: "Фильтр списка по категории.",
    mods_sort: "Сортировка видимого списка модов.",
    mods_update_enabled: "Проверить обновления включенных модов и поставить установку в очередь.",
    mods_clear_filters: "Сбросить поиск, категорию и сортировку.",
    refresh_details: "Обновить детали выбранного мода и локальный медиакэш.",
    tooltipCategoryChip: "Отфильтровать список модов по этой категории",
    tooltipModRow: "Кликни, чтобы выбрать мод и открыть карточку",
    tooltipModToggle: "Включить авто-отслеживание обновлений для этого мода",
    tooltipModLink: "Открыть страницу мода на LoversLab",
    tooltipInstallSubdir: "Имя подпапки мода внутри корня менеджера",
    tooltipOpenPage: "Открыть страницу выбранного мода на LoversLab",
  },
};

const TOOLTIP_IDS = [
  "tab_btn_mods",
  "tab_btn_queue",
  "tab_btn_actions",
  "tab_btn_settings",
  "tab_btn_log",
  "mods_dir",
  "pick_mods_dir",
  "manager_root_subdir",
  "ui_language",
  "image_source_mode",
  "deploy_method",
  "scan_pages",
  "catalog_max_pages_per_category",
  "poll_minutes",
  "queue_poll_seconds",
  "max_metadata_checks_per_run",
  "metadata_min_delay_seconds",
  "metadata_max_delay_seconds",
  "download_min_delay_seconds",
  "download_max_delay_seconds",
  "max_downloads_per_hour",
  "max_downloads_per_day",
  "backoff_base_minutes",
  "backoff_max_minutes",
  "queue_retry_limit",
  "cooldown_429_minutes",
  "cooldown_503_minutes",
  "cooldown_hard_block_hours",
  "download_backend",
  "cdp_endpoint",
  "cdp_download_timeout_seconds",
  "proxy_enabled",
  "proxy_url",
  "ll_cookie",
  "user_agent",
  "auto_tracking_enabled",
  "queue_worker_enabled",
  "save_settings",
  "discover",
  "discover_quick",
  "check_only",
  "update_enabled",
  "process_queue_once",
  "clear_done_queue",
  "reset_runtime_limits",
  "custom_url",
  "add_mod",
  "mods_search",
  "mods_enabled_filter",
  "mods_category_filter",
  "mods_sort",
  "mods_update_enabled",
  "mods_clear_filters",
  "refresh_details",
];

function detectBrowserLocale() {
  const raw = (navigator.language || navigator.userLanguage || "en").toLowerCase();
  return raw.startsWith("ru") ? "ru" : "en";
}

function normalizeUiLanguage(value) {
  const v = String(value || "auto").toLowerCase();
  if (v === "ru" || v === "en") {
    return v;
  }
  return "auto";
}

function normalizeImageSourceMode(value) {
  const v = String(value || "cache").toLowerCase();
  return v === "remote" ? "remote" : "cache";
}

function resolveLocale(settings) {
  const uiLanguage = normalizeUiLanguage(settings?.ui_language);
  if (uiLanguage === "ru" || uiLanguage === "en") {
    return uiLanguage;
  }
  return detectBrowserLocale();
}

function t(key) {
  const dict = I18N[viewState.locale] || I18N.en;
  return dict[key] || I18N.en[key] || key;
}

function tt(key) {
  const dict = TOOLTIPS[viewState.locale] || TOOLTIPS.en;
  return dict[key] || TOOLTIPS.en[key] || "";
}

function setText(id, key) {
  const el = document.getElementById(id);
  if (el) {
    el.textContent = t(key);
  }
}

function setTitle(id, key = id) {
  const el = document.getElementById(id);
  if (!el) {
    return;
  }
  const value = tt(key);
  if (value) {
    el.title = value;
  }
}

function applyTooltips() {
  for (const id of TOOLTIP_IDS) {
    setTitle(id, id);
  }
}

function formatNum(value) {
  const num = Number(value || 0);
  if (!Number.isFinite(num)) {
    return "0";
  }
  return num.toLocaleString();
}

function formatCompact(value) {
  const num = Number(value || 0);
  if (!Number.isFinite(num) || num <= 0) {
    return "0";
  }

  if (num >= 1_000_000_000) {
    return `${(num / 1_000_000_000).toFixed(num >= 10_000_000_000 ? 0 : 1)}B`;
  }
  if (num >= 1_000_000) {
    return `${(num / 1_000_000).toFixed(num >= 10_000_000 ? 0 : 1)}M`;
  }
  if (num >= 1_000) {
    return `${(num / 1_000).toFixed(num >= 10_000 ? 0 : 1)}K`;
  }
  return String(Math.round(num));
}

function formatDuration(totalSeconds) {
  const sec = Math.max(0, Number(totalSeconds || 0));
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = Math.floor(sec % 60);
  if (h > 0) {
    return `${h}h ${m}m ${s}s`;
  }
  if (m > 0) {
    return `${m}m ${s}s`;
  }
  return `${s}s`;
}

function discoverProgressPercent(progress) {
  if (!progress) {
    return 0;
  }

  const stage = String(progress.stage || "");
  if (stage === "done" || stage === "error") {
    return 100;
  }

  const categoriesTotal = Number(progress.categories_total || 0);
  const categoriesDone = Number(progress.categories_done || 0);
  const categoryPage = Number(progress.current_category_page || 0);
  const categoryPages = Number(progress.current_category_pages || 0);
  const cacheTotal = Number(progress.cache_scan_total || 0);
  const cacheDone = Number(progress.cache_scan_done || 0);
  const detailsTotal = Number(progress.details_total_target || 0);
  const detailsDone = Number(progress.details_done || 0) + Number(progress.details_errors || 0);

  let value = 0;
  if (stage === "discovering_categories" && categoriesTotal > 0) {
    const pageFraction = categoryPages > 0 && categoriesDone < categoriesTotal ? Math.min(categoryPage, categoryPages) / categoryPages : 0;
    value = ((categoriesDone + pageFraction) / categoriesTotal) * 55;
  } else if (stage === "merging_mods") {
    value = 60;
  } else if (stage === "refreshing_cache") {
    const cachePart = cacheTotal > 0 ? (cacheDone / cacheTotal) * 35 : 0;
    const detailsPart = detailsTotal > 0 ? (Math.min(detailsDone, detailsTotal) / detailsTotal) * 5 : 0;
    value = 60 + cachePart + detailsPart;
  } else if (stage === "finalizing") {
    value = 98;
  } else if (progress.running) {
    value = 2;
  }

  return Math.max(0, Math.min(100, Math.round(value)));
}

function renderDiscoverProgress(progress) {
  const percentEl = document.getElementById("discover_progress_percent");
  const barEl = document.getElementById("discover_progress_bar");
  const textEl = document.getElementById("discover_progress_text");
  if (!percentEl || !barEl || !textEl) {
    return;
  }

  if (!progress) {
    percentEl.textContent = "0%";
    barEl.style.width = "0%";
    textEl.textContent = t("discoverIdle");
    return;
  }

  const running = !!progress.running;
  const percent = discoverProgressPercent(progress);
  percentEl.textContent = `${percent}%`;
  barEl.style.width = `${percent}%`;

  const lines = [];
  lines.push(`${t("discoverStage")}: ${progress.stage || "-"}`);
  lines.push(`${t("discoverElapsed")}: ${formatDuration(progress.elapsed_seconds || 0)}`);

  if (progress.resumed_from_checkpoint) {
    lines.push(`${t("discoverResume")}: category_index=${Number(progress.checkpoint_category_index || 0)}, page=${Number(progress.checkpoint_page || 1)}`);
  }

  if (progress.last_checkpoint_at) {
    lines.push(`${t("discoverCheckpoint")}: ${progress.last_checkpoint_at} (${progress.checkpoint_status || "-"})`);
  }

  const retriesTotal = Number(progress.retries_total || 0);
  const retriesCurrent = Number(progress.retry_count_current || 0);
  if (retriesTotal > 0 || retriesCurrent > 0) {
    lines.push(`${t("discoverRetries")}: total=${retriesTotal}, current=${retriesCurrent}`);
  }

  const retryBackoff = Number(progress.retry_backoff_seconds || 0);
  if (retryBackoff > 0) {
    lines.push(`${t("discoverBackoff")}: ${formatDuration(retryBackoff)}`);
  }

  if (progress.current_category_name || progress.current_category_id) {
    lines.push(`${t("discoverCategory")}: ${progress.current_category_name || "-"} (${progress.current_category_id || "-"})`);
  }

  if (Number(progress.current_category_pages || 0) > 0) {
    lines.push(`${t("discoverCategoryPages")}: ${Number(progress.current_category_page || 0)}/${Number(progress.current_category_pages || 0)}`);
  }

  const categoriesTotal = Number(progress.categories_total || 0);
  if (categoriesTotal > 0) {
    lines.push(`${t("discoverCategories")}: ${Number(progress.categories_done || 0)}/${categoriesTotal}`);
  }

  const modsDiscovered = Number(progress.mods_discovered || 0);
  const modsUnique = Number(progress.mods_unique || 0);
  if (modsDiscovered > 0 || modsUnique > 0) {
    lines.push(`${t("discoverMods")}: discovered=${formatNum(modsDiscovered)} unique=${formatNum(modsUnique)} merged=${formatNum(progress.merged_total || 0)}`);
  }

  const cacheTotal = Number(progress.cache_scan_total || 0);
  if (cacheTotal > 0) {
    lines.push(`${t("discoverCache")}: ${Number(progress.cache_scan_done || 0)}/${cacheTotal}, thumbs=${formatNum(progress.thumbnails_cached || 0)}`);
  }

  const detailsTotal = Number(progress.details_total_target || 0);
  const detailsDone = Number(progress.details_done || 0);
  const detailsErrors = Number(progress.details_errors || 0);
  if (detailsTotal > 0 || detailsDone > 0 || detailsErrors > 0) {
    lines.push(`${t("discoverDetails")}: ok=${detailsDone}/${detailsTotal}, errors=${detailsErrors}`);
  }

  if (!running && progress.last_error) {
    lines.push(`${t("discoverError")}: ${progress.last_error}`);
  }

  if (!running && !progress.last_error && progress.last_result) {
    lines.push(`result: ${JSON.stringify(progress.last_result)}`);
  }

  if (!running && !progress.last_error && !progress.last_result && lines.length <= 2) {
    textEl.textContent = t("discoverIdle");
    return;
  }

  textEl.textContent = lines.join("\n");
}

async function fetchDiscoverProgress(refreshOnDone) {
  try {
    const data = await api("/api/discover/progress");
    const progress = data.progress || null;
    renderDiscoverProgress(progress);

    const running = !!(progress && progress.running);
    if (running) {
      viewState.discoverLastRunning = true;
      if (!discoverPollHandle) {
        discoverPollHandle = setInterval(() => {
          void fetchDiscoverProgress(true);
        }, 2500);
      }
      return;
    }

    if (discoverPollHandle) {
      clearInterval(discoverPollHandle);
      discoverPollHandle = null;
    }

    if (viewState.discoverLastRunning && refreshOnDone) {
      viewState.discoverLastRunning = false;
      await refreshState();
      return;
    }

    viewState.discoverLastRunning = false;
  } catch (err) {
    logLine(`${t("startupError")}: ${err.message}`);
  }
}

function asNumber(value, fallback) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const data = await response.json();
  if (!response.ok || data.ok === false) {
    throw new Error(data.error || `HTTP ${response.status}`);
  }
  return data;
}

function logLine(text) {
  const log = document.getElementById("log");
  const ts = new Date().toISOString();
  log.textContent = `[${ts}] ${text}\n` + log.textContent;
}

function switchTab(tab) {
  viewState.activeTab = tab;
  for (const btn of document.querySelectorAll(".tab-button")) {
    btn.classList.toggle("active", btn.dataset.tab === tab);
  }
  for (const pane of document.querySelectorAll(".tab-pane")) {
    pane.classList.toggle("active", pane.dataset.tabPane === tab);
  }
}

function applyI18n() {
  document.title = t("appTitle");
  setText("app_title", "appTitle");
  setText("tab_btn_mods", "tabMods");
  setText("tab_btn_queue", "tabQueue");
  setText("tab_btn_actions", "tabActions");
  setText("tab_btn_settings", "tabSettings");
  setText("tab_btn_log", "tabLog");
  setText("settings_title", "settingsTitle");
  setText("lbl_mods_dir", "lblModsDir");
  setText("lbl_manager_root_subdir", "lblManagerRootSubdir");
  setText("lbl_ui_language", "lblUiLanguage");
  setText("lbl_image_source_mode", "lblImageSourceMode");
  setText("lbl_deploy_method", "lblDeployMethod");
  setText("lbl_scan_pages", "lblScanPages");
  setText("lbl_catalog_max_pages_per_category", "lblCatalogMaxPagesPerCategory");
  setText("lbl_poll_minutes", "lblPollMinutes");
  setText("lbl_queue_poll_seconds", "lblQueuePollSeconds");
  setText("lbl_max_metadata_checks_per_run", "lblMaxMetadataChecksPerRun");
  setText("lbl_metadata_min_delay_seconds", "lblMetadataMinDelaySeconds");
  setText("lbl_metadata_max_delay_seconds", "lblMetadataMaxDelaySeconds");
  setText("lbl_download_min_delay_seconds", "lblDownloadMinDelaySeconds");
  setText("lbl_download_max_delay_seconds", "lblDownloadMaxDelaySeconds");
  setText("lbl_max_downloads_per_hour", "lblMaxDownloadsPerHour");
  setText("lbl_max_downloads_per_day", "lblMaxDownloadsPerDay");
  setText("lbl_backoff_base_minutes", "lblBackoffBaseMinutes");
  setText("lbl_backoff_max_minutes", "lblBackoffMaxMinutes");
  setText("lbl_queue_retry_limit", "lblQueueRetryLimit");
  setText("lbl_cooldown_429_minutes", "lblCooldown429Minutes");
  setText("lbl_cooldown_503_minutes", "lblCooldown503Minutes");
  setText("lbl_cooldown_hard_block_hours", "lblCooldownHardBlockHours");
  setText("lbl_download_backend", "lblDownloadBackend");
  setText("lbl_cdp_endpoint", "lblCdpEndpoint");
  setText("lbl_cdp_download_timeout_seconds", "lblCdpDownloadTimeoutSeconds");
  setText("lbl_proxy_enabled", "lblProxyEnabled");
  setText("lbl_proxy_url", "lblProxyUrl");
  setText("lbl_ll_cookie", "lblLlCookie");
  setText("lbl_user_agent", "lblUserAgent");
  setText("lbl_auto_tracking_enabled", "lblAutoTrackingEnabled");
  setText("lbl_queue_worker_enabled", "lblQueueWorkerEnabled");
  setText("actions_title", "actionsTitle");
  setText("categories_title", "categoriesTitle");
  setText("mods_title", "modsTitle");
  setText("queue_title", "queueTitle");
  setText("log_title", "logTitle");
  setText("details_title", "detailsTitle");
  setText("discover_progress_title", "discoverProgressTitle");

  setText("save_settings", "btnSave");
  setText("pick_mods_dir", "btnPickFolder");
  setText("discover", "btnSyncFull");
  setText("check_only", "btnCheck");
  setText("update_enabled", "btnEnqueue");
  setText("process_queue_once", "btnProcessOne");
  setText("clear_done_queue", "btnClearDone");
  setText("reset_runtime_limits", "btnResetLimits");
  setText("add_mod", "btnAddUrl");
  setText("mods_update_enabled", "btnUpdateEnabledInMods");
  setText("mods_clear_filters", "btnClearFilters");
  setText("refresh_details", "btnRefreshDetails");

  setText("th_auto", "thAuto");
  setText("th_id", "thId");
  setText("th_title", "thTitle");
  setText("th_category", "thCategory");
  setText("th_local", "thLocal");
  setText("th_remote", "thRemote");
  setText("th_downloads", "thDownloads");
  setText("th_status", "thStatus");
  setText("th_install", "thInstall");

  const modsSearch = document.getElementById("mods_search");
  if (modsSearch) {
    modsSearch.placeholder = t("searchMods");
  }

  const uiLangSelect = document.getElementById("ui_language");
  if (uiLangSelect) {
    const map = {
      auto: "optLanguageAuto",
      en: "optLanguageEn",
      ru: "optLanguageRu",
    };
    for (const option of uiLangSelect.options) {
      const key = map[option.value];
      if (key) {
        option.textContent = t(key);
      }
    }
  }

  const imageModeSelect = document.getElementById("image_source_mode");
  if (imageModeSelect) {
    const map = {
      cache: "optImageSourceCache",
      remote: "optImageSourceRemote",
    };
    for (const option of imageModeSelect.options) {
      const key = map[option.value];
      if (key) {
        option.textContent = t(key);
      }
    }
  }

  const categorySearch = document.getElementById("category_search");
  if (categorySearch) {
    categorySearch.placeholder = t("searchCats");
  }

  const catFilter = document.getElementById("mods_category_filter");
  if (catFilter && catFilter.options.length > 0) {
    catFilter.options[0].textContent = t("allCategories");
  }

  const enabledFilter = document.getElementById("mods_enabled_filter");
  if (enabledFilter) {
    const map = {
      all: "filterAllMods",
      enabled: "filterEnabledOnly",
      disabled: "filterDisabledOnly",
    };
    for (const option of enabledFilter.options) {
      const key = map[option.value];
      if (key) {
        option.textContent = t(key);
      }
    }
  }

  const sortSelect = document.getElementById("mods_sort");
  if (sortSelect) {
    const map = {
      downloads_desc: "sortByPopularity",
      title_asc: "sortTitleAsc",
      title_desc: "sortTitleDesc",
      id_desc: "sortIdDesc",
      id_asc: "sortIdAsc",
    };
    for (const option of sortSelect.options) {
      const key = map[option.value];
      if (key) {
        option.textContent = t(key);
      }
    }
  }

  applyTooltips();
}

function fillSettings(settings) {
  document.getElementById("mods_dir").value = settings.mods_dir || "";
  document.getElementById("manager_root_subdir").value = settings.manager_root_subdir || "_LL_MOD_MANAGER";
  document.getElementById("ui_language").value = normalizeUiLanguage(settings.ui_language);
  document.getElementById("image_source_mode").value = normalizeImageSourceMode(settings.image_source_mode);
  document.getElementById("deploy_method").value = settings.deploy_method || "hardlink";
  const scanPagesInput = document.getElementById("scan_pages");
  if (scanPagesInput) {
    scanPagesInput.value = settings.scan_pages || 5;
  }
  document.getElementById("catalog_max_pages_per_category").value = settings.catalog_max_pages_per_category || 0;
  document.getElementById("poll_minutes").value = settings.poll_minutes || 60;
  document.getElementById("queue_poll_seconds").value = settings.queue_poll_seconds || 20;
  document.getElementById("max_metadata_checks_per_run").value = settings.max_metadata_checks_per_run || 50;
  document.getElementById("metadata_min_delay_seconds").value = settings.metadata_min_delay_seconds || 2;
  document.getElementById("metadata_max_delay_seconds").value = settings.metadata_max_delay_seconds || 6;
  document.getElementById("download_min_delay_seconds").value = settings.download_min_delay_seconds || 90;
  document.getElementById("download_max_delay_seconds").value = settings.download_max_delay_seconds || 180;
  document.getElementById("max_downloads_per_hour").value = settings.max_downloads_per_hour || 10;
  document.getElementById("max_downloads_per_day").value = settings.max_downloads_per_day || 100;
  document.getElementById("backoff_base_minutes").value = settings.backoff_base_minutes || 5;
  document.getElementById("backoff_max_minutes").value = settings.backoff_max_minutes || 720;
  document.getElementById("queue_retry_limit").value = settings.queue_retry_limit || 20;
  document.getElementById("cooldown_429_minutes").value = settings.cooldown_429_minutes || 60;
  document.getElementById("cooldown_503_minutes").value = settings.cooldown_503_minutes || 60;
  document.getElementById("cooldown_hard_block_hours").value = settings.cooldown_hard_block_hours || 24;
  document.getElementById("download_backend").value = settings.download_backend || "cdp_preferred";
  document.getElementById("cdp_endpoint").value = settings.cdp_endpoint || "http://127.0.0.1:9222";
  document.getElementById("cdp_download_timeout_seconds").value = settings.cdp_download_timeout_seconds || 300;
  document.getElementById("proxy_enabled").checked = !!settings.proxy_enabled;
  document.getElementById("proxy_url").value = settings.proxy_url || "";
  document.getElementById("ll_cookie").value = settings.ll_cookie || "";
  document.getElementById("user_agent").value = settings.user_agent || "";
  document.getElementById("auto_tracking_enabled").checked = !!settings.auto_tracking_enabled;
  document.getElementById("queue_worker_enabled").checked = !!settings.queue_worker_enabled;
}

function collectSettings() {
  const scanPagesInput = document.getElementById("scan_pages");
  const scanPagesValue = scanPagesInput ? scanPagesInput.value : 5;

  return {
    mods_dir: document.getElementById("mods_dir").value.trim(),
    manager_root_subdir: document.getElementById("manager_root_subdir").value.trim() || "_LL_MOD_MANAGER",
    ui_language: normalizeUiLanguage(document.getElementById("ui_language").value),
    image_source_mode: normalizeImageSourceMode(document.getElementById("image_source_mode").value),
    deploy_method: document.getElementById("deploy_method").value,
    scan_pages: asNumber(scanPagesValue, 5),
    catalog_max_pages_per_category: asNumber(document.getElementById("catalog_max_pages_per_category").value, 0),
    poll_minutes: asNumber(document.getElementById("poll_minutes").value, 60),
    queue_poll_seconds: asNumber(document.getElementById("queue_poll_seconds").value, 20),
    max_metadata_checks_per_run: asNumber(document.getElementById("max_metadata_checks_per_run").value, 50),
    metadata_min_delay_seconds: asNumber(document.getElementById("metadata_min_delay_seconds").value, 2),
    metadata_max_delay_seconds: asNumber(document.getElementById("metadata_max_delay_seconds").value, 6),
    download_min_delay_seconds: asNumber(document.getElementById("download_min_delay_seconds").value, 90),
    download_max_delay_seconds: asNumber(document.getElementById("download_max_delay_seconds").value, 180),
    max_downloads_per_hour: asNumber(document.getElementById("max_downloads_per_hour").value, 10),
    max_downloads_per_day: asNumber(document.getElementById("max_downloads_per_day").value, 100),
    backoff_base_minutes: asNumber(document.getElementById("backoff_base_minutes").value, 5),
    backoff_max_minutes: asNumber(document.getElementById("backoff_max_minutes").value, 720),
    queue_retry_limit: asNumber(document.getElementById("queue_retry_limit").value, 20),
    cooldown_429_minutes: asNumber(document.getElementById("cooldown_429_minutes").value, 60),
    cooldown_503_minutes: asNumber(document.getElementById("cooldown_503_minutes").value, 60),
    cooldown_hard_block_hours: asNumber(document.getElementById("cooldown_hard_block_hours").value, 24),
    download_backend: document.getElementById("download_backend").value,
    cdp_endpoint: document.getElementById("cdp_endpoint").value.trim(),
    cdp_download_timeout_seconds: asNumber(document.getElementById("cdp_download_timeout_seconds").value, 300),
    proxy_enabled: document.getElementById("proxy_enabled").checked,
    proxy_url: document.getElementById("proxy_url").value.trim(),
    ll_cookie: document.getElementById("ll_cookie").value.trim(),
    user_agent: document.getElementById("user_agent").value.trim(),
    auto_tracking_enabled: document.getElementById("auto_tracking_enabled").checked,
    queue_worker_enabled: document.getElementById("queue_worker_enabled").checked,
  };
}

function modMatchesFilters(mod) {
  const query = viewState.modsSearch.trim().toLowerCase();
  if (query) {
    const hay = `${mod.id} ${mod.title || ""} ${mod.category_name || ""}`.toLowerCase();
    if (!hay.includes(query)) {
      return false;
    }
  }

  const enabledFilter = String(viewState.modsEnabledFilter || "all");
  if (enabledFilter === "enabled" && !mod.enabled) {
    return false;
  }
  if (enabledFilter === "disabled" && mod.enabled) {
    return false;
  }

  const categoryFilter = (viewState.modsCategoryFilter || "").trim();
  if (categoryFilter) {
    const ids = new Set((mod.category_ids || []).map((x) => String(x)));
    if (mod.category_id) {
      ids.add(String(mod.category_id));
    }
    if (!ids.has(categoryFilter)) {
      return false;
    }
  }

  return true;
}

function compareMods(a, b) {
  switch (viewState.modsSort) {
    case "title_asc":
      return (a.title || "").localeCompare(b.title || "", undefined, { sensitivity: "base" });
    case "title_desc":
      return (b.title || "").localeCompare(a.title || "", undefined, { sensitivity: "base" });
    case "id_asc":
      return Number(a.id || 0) - Number(b.id || 0);
    case "id_desc":
      return Number(b.id || 0) - Number(a.id || 0);
    case "downloads_desc":
    default: {
      const d = Number(b.downloads_count || 0) - Number(a.downloads_count || 0);
      if (d !== 0) {
        return d;
      }
      return Number(b.id || 0) - Number(a.id || 0);
    }
  }
}

function getVisibleMods() {
  return viewState.mods.filter(modMatchesFilters).sort(compareMods);
}

function renderModsCategoryFilter() {
  const select = document.getElementById("mods_category_filter");
  if (!select) {
    return;
  }

  const current = viewState.modsCategoryFilter || "";
  select.innerHTML = "";

  const allOption = document.createElement("option");
  allOption.value = "";
  allOption.textContent = t("allCategories");
  select.appendChild(allOption);

  for (const cat of viewState.categories) {
    const option = document.createElement("option");
    option.value = String(cat.id || "");
    option.textContent = `${cat.parent_id ? "↳ " : ""}${cat.name || cat.id}`;
    select.appendChild(option);
  }

  select.value = current;
}

function renderCategories() {
  const list = document.getElementById("categories_list");
  if (!list) {
    return;
  }
  list.innerHTML = "";

  const cats = viewState.categories;

  for (const cat of cats) {
    const chip = document.createElement("button");
    chip.type = "button";
    chip.className = "category-chip";
    chip.title = `${tt("tooltipCategoryChip")}: ${cat.name || cat.id || ""}`;
    if (String(cat.id || "") === String(viewState.modsCategoryFilter || "")) {
      chip.classList.add("active");
    }
    chip.addEventListener("click", () => {
      viewState.modsCategoryFilter = String(cat.id || "");
      const select = document.getElementById("mods_category_filter");
      if (select) {
        select.value = viewState.modsCategoryFilter;
      }
      renderCategories();
      renderMods();
    });

    const left = document.createElement("span");
    left.className = "category-chip-name";
    left.textContent = `${cat.parent_id ? "↳ " : ""}${cat.name || cat.id}`;

    const right = document.createElement("span");
    right.className = "category-chip-count";
    right.textContent = formatNum(cat.count || 0);

    chip.append(left, right);
    list.appendChild(chip);
  }
}

function ensureSelectedMod(visibleMods) {
  if (!visibleMods.length) {
    viewState.selectedModId = "";
    return;
  }

  const selected = String(viewState.selectedModId || "");
  if (!selected || !visibleMods.some((m) => String(m.id) === selected)) {
    viewState.selectedModId = String(visibleMods[0].id);
  }
}

function renderMods() {
  const body = document.getElementById("mods_table");
  if (!body) {
    return;
  }

  body.innerHTML = "";
  const visible = getVisibleMods();
  ensureSelectedMod(visible);
  const selectedId = String(viewState.selectedModId || "");

  for (const mod of visible) {
    const tr = document.createElement("tr");
    tr.title = tt("tooltipModRow");
    if (String(mod.id) === selectedId) {
      tr.classList.add("selected-row");
    }
    tr.addEventListener("click", () => {
      void setSelectedMod(mod.id, false);
    });

    const enabledTd = document.createElement("td");
    const enabledCb = document.createElement("input");
    enabledCb.type = "checkbox";
    enabledCb.title = tt("tooltipModToggle");
    enabledCb.checked = !!mod.enabled;
    enabledCb.addEventListener("click", (e) => e.stopPropagation());
    enabledCb.addEventListener("change", () => {
      void toggleMod(mod.id, enabledCb.checked);
    });
    enabledTd.appendChild(enabledCb);

    const idTd = document.createElement("td");
    idTd.textContent = String(mod.id || "-");

    const titleTd = document.createElement("td");
    const link = document.createElement("a");
    link.className = "mod-title-link";
    link.href = mod.url;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    link.title = tt("tooltipModLink");
    link.textContent = mod.title || mod.url;
    link.addEventListener("click", (e) => e.stopPropagation());
    titleTd.appendChild(link);

    const categoryTd = document.createElement("td");
    categoryTd.textContent = mod.category_name || "-";

    const localVerTd = document.createElement("td");
    localVerTd.textContent = mod.version || "-";

    const remoteVerTd = document.createElement("td");
    remoteVerTd.textContent = mod.remote_version || "-";

    const downloadsTd = document.createElement("td");
    downloadsTd.textContent = formatCompact(mod.downloads_count || 0);

    const statusTd = document.createElement("td");
    statusTd.textContent = mod.status || "-";

    const subdirTd = document.createElement("td");
    const subdirInput = document.createElement("input");
    subdirInput.type = "text";
    const leaf = (mod.install_subdir || "").split("/").pop() || "";
    subdirInput.value = leaf;
    subdirInput.title = tt("tooltipInstallSubdir");
    subdirInput.placeholder = t("subdirPlaceholder");
    subdirInput.addEventListener("click", (e) => e.stopPropagation());
    subdirInput.addEventListener("change", () => {
      void saveSubdir(mod.id, subdirInput.value);
    });
    subdirTd.appendChild(subdirInput);

    tr.append(enabledTd, idTd, titleTd, categoryTd, localVerTd, remoteVerTd, downloadsTd, statusTd, subdirTd);
    body.appendChild(tr);
  }

  renderModDetailsCard();
}

function selectedMod() {
  const id = String(viewState.selectedModId || "");
  return viewState.mods.find((m) => String(m.id) === id) || null;
}

function renderModDetailsCard() {
  const card = document.getElementById("mod_details_card");
  if (!card) {
    return;
  }
  card.innerHTML = "";

  const mod = selectedMod();
  if (!mod) {
    card.textContent = t("detailsNone");
    return;
  }

  if (viewState.detailsLoading) {
    card.textContent = t("detailsLoading");
    return;
  }

  const details = viewState.detailsCache[String(mod.id)] || {};

  const title = document.createElement("h3");
  title.className = "details-title";
  title.textContent = mod.title || `Mod ${mod.id}`;

  const meta = document.createElement("div");
  meta.className = "details-meta";
  meta.textContent = `ID: ${mod.id} | ${t("detailsCategory")}: ${mod.category_name || "-"} | ${t("detailsDownloads")}: ${formatCompact(mod.downloads_count || 0)} | ${t("detailsStatus")}: ${mod.status || "-"}`;

  const versions = document.createElement("div");
  versions.className = "details-meta";
  versions.textContent = `${t("detailsVersion")}: ${mod.version || "-"} | ${t("detailsRemoteVersion")}: ${mod.remote_version || "-"}`;

  const openLink = document.createElement("a");
  openLink.href = mod.url;
  openLink.target = "_blank";
  openLink.rel = "noopener noreferrer";
  openLink.title = tt("tooltipOpenPage");
  openLink.textContent = t("detailsOpenPage");

  const summaryTitle = document.createElement("div");
  summaryTitle.className = "details-meta";
  summaryTitle.textContent = `${t("detailsSummary")}:`;

  const summary = document.createElement("p");
  summary.className = "details-summary";
  summary.textContent = details.summary || details.description_text || "-";

  const descTitle = document.createElement("div");
  descTitle.className = "details-meta";
  descTitle.textContent = `${t("detailsDescription")}:`;

  const desc = document.createElement("p");
  desc.className = "details-summary";
  const descText = details.description_text || "";
  desc.textContent = descText ? descText.slice(0, 2500) : "-";

  card.append(title, meta, versions, openLink, summaryTitle, summary, descTitle, desc);

  const imageMode = normalizeImageSourceMode(viewState.settings?.image_source_mode);
  const images = [];

  if (imageMode === "remote") {
    const modThumbRemote = mod.thumbnail_url || "";
    if (modThumbRemote) {
      images.push(modThumbRemote);
    }
    if (Array.isArray(details.images)) {
      images.push(...details.images);
    }
    const detailsThumbRemote = details.thumbnail_url || "";
    if (detailsThumbRemote) {
      images.push(detailsThumbRemote);
    }
  } else {
    const modThumb = mod.thumbnail_cached_url || "";
    if (modThumb) {
      images.push(modThumb);
    }

    if (Array.isArray(details.cached_images)) {
      images.push(...details.cached_images);
    }
    const thumbCached = details.thumbnail_cached_url || "";
    if (thumbCached) {
      images.push(thumbCached);
    }
  }

  const uniqueImages = [...new Set(images.filter(Boolean))].slice(0, 12);

  if (uniqueImages.length) {
    const imgTitle = document.createElement("div");
    imgTitle.className = "details-meta";
    imgTitle.textContent = `${t("detailsImages")}:`;
    card.appendChild(imgTitle);

    const grid = document.createElement("div");
    grid.className = "details-images";
    for (const src of uniqueImages) {
      const img = document.createElement("img");
      img.src = src;
      img.loading = "lazy";
      img.alt = mod.title || "mod image";
      img.addEventListener("error", () => {
        img.remove();
      });
      grid.appendChild(img);
    }
    card.appendChild(grid);
  }
}

async function loadSelectedModDetails(forceRefresh) {
  const mod = selectedMod();
  if (!mod) {
    return;
  }
  const key = String(mod.id);

  if (!forceRefresh && viewState.detailsCache[key]) {
    renderModDetailsCard();
    return;
  }

  viewState.detailsLoading = true;
  renderModDetailsCard();
  try {
    const data = await api(`/api/mod_details/${key}`);
    viewState.detailsCache[key] = data.details || {};
  } catch (err) {
    logLine(`${t("startupError")}: ${err.message}`);
  } finally {
    viewState.detailsLoading = false;
    renderModDetailsCard();
  }
}

async function setSelectedMod(modId, forceRefresh) {
  const nextId = String(modId || "");
  if (!nextId) {
    return;
  }
  viewState.selectedModId = nextId;
  renderMods();
  await loadSelectedModDetails(!!forceRefresh);
}

function renderQueue(queue) {
  const block = document.getElementById("queue_info");
  if (!block) {
    return;
  }

  const counts = queue?.counts || {};
  const lines = [
    `${t("queueTotal")}: ${queue?.total ?? 0}`,
    `State counts: ${JSON.stringify(counts)}`,
    `${t("queueNextMod")}: ${queue?.next_mod_id ?? "-"}`,
    `${t("queueWait")}: ${queue?.next_wait_seconds ?? 0} sec`,
    `${t("queueCooldown")}: ${queue?.cooldown_seconds ?? 0} sec`,
    `${t("queueSpacing")}: ${queue?.next_download_spacing_seconds ?? 0} sec`,
  ];

  const items = (queue?.items || []).slice(0, 15);
  if (items.length > 0) {
    lines.push("", `${t("queueTop")}:`);
    for (const item of items) {
      lines.push(
        `${item.mod_id} | ${item.state} | attempts=${item.attempts || 0} | not_before=${item.not_before || "now"} | err=${item.last_error || "-"}`,
      );
    }
  }

  block.textContent = lines.join("\n");
}

async function refreshState() {
  const state = await api("/api/state");
  const settings = state.settings || {};
  const prevImageMode = normalizeImageSourceMode(viewState.settings?.image_source_mode);
  const nextImageMode = normalizeImageSourceMode(settings.image_source_mode);
  if (prevImageMode !== nextImageMode) {
    viewState.detailsCache = {};
  }
  viewState.settings = settings;
  const resolvedLocale = resolveLocale(settings);
  if (viewState.locale !== resolvedLocale) {
    viewState.locale = resolvedLocale;
    applyI18n();
  }
  fillSettings(settings);

  viewState.mods = state.mods || [];
  viewState.categories = state.categories?.categories || [];

  if (!viewState.modsCategoryFilter && viewState.categories.length > 0) {
    viewState.modsCategoryFilter = String(viewState.categories[0].id || "");
  }

  renderModsCategoryFilter();
  renderCategories();
  renderMods();
  renderQueue(state.queue);
  renderDiscoverProgress(state.discover || null);

  if (state.discover && state.discover.running) {
    viewState.discoverLastRunning = true;
    if (!discoverPollHandle) {
      discoverPollHandle = setInterval(() => {
        void fetchDiscoverProgress(true);
      }, 2500);
    }
  }

  if (viewState.selectedModId) {
    await loadSelectedModDetails(false);
  }
}

async function toggleMod(id, enabled) {
  await api("/api/toggle_mod", {
    method: "POST",
    body: JSON.stringify({ id, enabled }),
  });
  const mod = viewState.mods.find((m) => String(m.id) === String(id));
  if (mod) {
    mod.enabled = !!enabled;
  }
  renderMods();
}

async function saveSubdir(id, installSubdir) {
  await api("/api/mod_config", {
    method: "POST",
    body: JSON.stringify({ id, install_subdir: installSubdir }),
  });
}

async function runCheck(install) {
  const data = await api("/api/check_updates", {
    method: "POST",
    body: JSON.stringify({ install, enabled_only: true }),
  });
  const result = data.result || {};
  const autoProcess = data.auto_process || {};
  if (install) {
    logLine(
      `${t("checkResult")}: ${result.checked || 0}, ${t("updatedAvailable")}: ${result.updates_available || 0}, ${t("queued")}: ${result.queued || 0}, ${t("errors")}: ${result.errors || 0}`,
    );
    if (autoProcess.processed) {
      logLine(`${t("detailsStatus")}: ${autoProcess.action || "updated"}, mod_id=${autoProcess.mod_id || "-"}`);
    }
  } else {
    logLine(`${t("checkResult")}: ${result.checked || 0}, ${t("updatedAvailable")}: ${result.updates_available || 0}, ${t("errors")}: ${result.errors || 0}`);
  }
  await refreshState();
}

async function processQueueOnce() {
  const data = await api("/api/queue/process_once", {
    method: "POST",
    body: JSON.stringify({ force: true }),
  });
  logLine(`${t("queueStep")}: ${JSON.stringify(data.result || {})}`);
  await refreshState();
}

async function clearDoneQueue() {
  const data = await api("/api/queue/clear_done", {
    method: "POST",
    body: JSON.stringify({}),
  });
  logLine(`${t("queueCleared")}: removed=${data.result.removed}, left=${data.result.left}`);
  await refreshState();
}

async function pickModsDir() {
  const data = await api("/api/pick_mods_dir", {
    method: "POST",
    body: JSON.stringify({}),
  });
  if (data.cancelled) {
    logLine(t("selectFolderCancelled"));
    return;
  }
  if (data.settings?.mods_dir) {
    document.getElementById("mods_dir").value = data.settings.mods_dir;
    logLine(`${t("selectFolderDone")}: ${data.settings.mods_dir}`);
  }
}

async function resetRuntimeLimits() {
  await api("/api/runtime/reset_limits", {
    method: "POST",
    body: JSON.stringify({}),
  });
  logLine(t("limitsReset"));
  await refreshState();
}

async function runDiscover(fullCatalog) {
  const scanPagesInput = document.getElementById("scan_pages");
  const scanPages = asNumber(scanPagesInput ? scanPagesInput.value : 5, 5);
  const payload = fullCatalog ? { full_catalog: true } : { scan_pages: scanPages, full_catalog: false };
  try {
    const data = await api("/api/discover/start", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    renderDiscoverProgress(data.progress || null);
    viewState.discoverLastRunning = true;
    logLine(`${t("catalogSync")}: ${t("discoverStarted")}`);
    if (!discoverPollHandle) {
      discoverPollHandle = setInterval(() => {
        void fetchDiscoverProgress(true);
      }, 2500);
    }
    await fetchDiscoverProgress(true);
  } catch (err) {
    logLine(`${t("catalogSync")}: ${err.message}`);
    if (String(err.message || "").toLowerCase().includes("already running")) {
      logLine(t("discoverAlreadyRunning"));
      await fetchDiscoverProgress(true);
    }
  }
}

function bindUI() {
  for (const btn of document.querySelectorAll(".tab-button")) {
    btn.addEventListener("click", () => switchTab(btn.dataset.tab || "mods"));
  }

  document.getElementById("pick_mods_dir").addEventListener("click", () => {
    void pickModsDir();
  });

  document.getElementById("ui_language").addEventListener("change", (event) => {
    const selected = normalizeUiLanguage(event.target.value);
    const nextLocale = selected === "auto" ? detectBrowserLocale() : selected;
    if (viewState.locale !== nextLocale) {
      viewState.locale = nextLocale;
      applyI18n();
      renderModsCategoryFilter();
      renderCategories();
      renderMods();
      renderModDetailsCard();
    }
  });

  document.getElementById("save_settings").addEventListener("click", () => {
    const payload = collectSettings();
    void api("/api/settings", { method: "POST", body: JSON.stringify(payload) }).then(async () => {
      logLine(t("settingsSaved"));
      await refreshState();
    });
  });

  document.getElementById("discover").addEventListener("click", () => {
    void runDiscover(true);
  });

  document.getElementById("discover_quick").addEventListener("click", () => {
    void runCheck(true);
  });

  document.getElementById("add_mod").addEventListener("click", async () => {
    const url = document.getElementById("custom_url").value.trim();
    if (!url) {
      return;
    }
    await api("/api/add_mod", {
      method: "POST",
      body: JSON.stringify({ url }),
    });
    logLine(`${t("modAdded")}: ${url}`);
    document.getElementById("custom_url").value = "";
    await refreshState();
  });

  document.getElementById("mods_search").addEventListener("input", (event) => {
    viewState.modsSearch = event.target.value || "";
    renderMods();
  });

  document.getElementById("mods_enabled_filter").addEventListener("change", (event) => {
    viewState.modsEnabledFilter = event.target.value || "all";
    renderMods();
  });

  document.getElementById("mods_category_filter").addEventListener("change", (event) => {
    viewState.modsCategoryFilter = event.target.value || "";
    renderCategories();
    renderMods();
  });

  document.getElementById("mods_sort").addEventListener("change", (event) => {
    viewState.modsSort = event.target.value || "downloads_desc";
    renderMods();
  });

  document.getElementById("mods_update_enabled").addEventListener("click", () => {
    void runCheck(true);
  });

  document.getElementById("mods_clear_filters").addEventListener("click", () => {
    viewState.modsSearch = "";
    viewState.modsEnabledFilter = "all";
    viewState.modsCategoryFilter = viewState.categories.length ? String(viewState.categories[0].id || "") : "";
    viewState.modsSort = "downloads_desc";
    document.getElementById("mods_search").value = "";
    document.getElementById("mods_enabled_filter").value = "all";
    document.getElementById("mods_sort").value = "downloads_desc";
    renderModsCategoryFilter();
    renderCategories();
    renderMods();
  });

  const categorySearch = document.getElementById("category_search");
  if (categorySearch) {
    categorySearch.addEventListener("input", (event) => {
      viewState.categorySearch = event.target.value || "";
      renderCategories();
    });
  }

  document.getElementById("refresh_details").addEventListener("click", () => {
    void loadSelectedModDetails(true);
  });
}

async function boot() {
  viewState.locale = detectBrowserLocale();
  applyI18n();
  bindUI();
  switchTab("mods");
  await refreshState();
}

boot().catch((err) => {
  logLine(`${t("startupError")}: ${err.message}`);
});
