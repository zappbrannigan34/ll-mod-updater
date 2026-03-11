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

function asNumber(value, fallback) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function fillSettings(settings) {
  document.getElementById("mods_dir").value = settings.mods_dir || "";
  document.getElementById("deploy_method").value = settings.deploy_method || "copy";
  document.getElementById("scan_pages").value = settings.scan_pages || 5;
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
  document.getElementById("ll_cookie").value = settings.ll_cookie || "";
  document.getElementById("user_agent").value = settings.user_agent || "";
  document.getElementById("auto_tracking_enabled").checked = !!settings.auto_tracking_enabled;
  document.getElementById("queue_worker_enabled").checked = !!settings.queue_worker_enabled;
}

function collectSettings() {
  return {
    mods_dir: document.getElementById("mods_dir").value.trim(),
    deploy_method: document.getElementById("deploy_method").value,
    scan_pages: asNumber(document.getElementById("scan_pages").value, 5),
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
    ll_cookie: document.getElementById("ll_cookie").value.trim(),
    user_agent: document.getElementById("user_agent").value.trim(),
    auto_tracking_enabled: document.getElementById("auto_tracking_enabled").checked,
    queue_worker_enabled: document.getElementById("queue_worker_enabled").checked,
  };
}

async function toggleMod(id, enabled) {
  await api("/api/toggle_mod", {
    method: "POST",
    body: JSON.stringify({ id, enabled }),
  });
  logLine(`Мод ${id}: auto=${enabled}`);
}

async function saveSubdir(id, install_subdir) {
  await api("/api/mod_config", {
    method: "POST",
    body: JSON.stringify({ id, install_subdir }),
  });
}

function renderMods(mods) {
  const body = document.getElementById("mods_table");
  body.innerHTML = "";

  for (const mod of mods) {
    const tr = document.createElement("tr");

    const enabledTd = document.createElement("td");
    const enabledCb = document.createElement("input");
    enabledCb.type = "checkbox";
    enabledCb.checked = !!mod.enabled;
    enabledCb.addEventListener("change", () => toggleMod(mod.id, enabledCb.checked));
    enabledTd.appendChild(enabledCb);

    const idTd = document.createElement("td");
    idTd.textContent = mod.id;

    const titleTd = document.createElement("td");
    const link = document.createElement("a");
    link.href = mod.url;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    link.textContent = mod.title || mod.url;
    titleTd.appendChild(link);

    const localVerTd = document.createElement("td");
    localVerTd.textContent = mod.version || "-";

    const remoteVerTd = document.createElement("td");
    remoteVerTd.textContent = mod.remote_version || "-";

    const statusTd = document.createElement("td");
    statusTd.textContent = mod.status || "-";

    const subdirTd = document.createElement("td");
    const subdirInput = document.createElement("input");
    subdirInput.type = "text";
    subdirInput.value = mod.install_subdir || "";
    subdirInput.placeholder = "например: _LL";
    subdirInput.addEventListener("change", () => saveSubdir(mod.id, subdirInput.value));
    subdirTd.appendChild(subdirInput);

    tr.append(enabledTd, idTd, titleTd, localVerTd, remoteVerTd, statusTd, subdirTd);
    body.appendChild(tr);
  }
}

function renderQueue(queue) {
  const block = document.getElementById("queue_info");
  const counts = queue?.counts || {};
  const lines = [
    `Всего: ${queue?.total ?? 0}`,
    `State counts: ${JSON.stringify(counts)}`,
    `Следующий mod_id: ${queue?.next_mod_id ?? "-"}`,
    `Ждать до следующего: ${queue?.next_wait_seconds ?? 0} сек`,
    `Глобальный cooldown: ${queue?.cooldown_seconds ?? 0} сек`,
    `Spacing до следующего старта: ${queue?.next_download_spacing_seconds ?? 0} сек`,
  ];

  const items = (queue?.items || []).slice(0, 15);
  if (items.length > 0) {
    lines.push("", "Top queue items:");
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
  fillSettings(state.settings);
  renderMods(state.mods);
  renderQueue(state.queue);
  if (state.scheduler?.last_scan_run) {
    logLine(`Последний scan: ${state.scheduler.last_scan_run}`);
  }
}

async function runCheck(install) {
  const data = await api("/api/check_updates", {
    method: "POST",
    body: JSON.stringify({ install, enabled_only: true }),
  });
  const result = data.result;
  if (install) {
    logLine(
      `Проверено: ${result.checked}, update_available: ${result.updates_available}, в очередь: ${result.queued}, ошибок: ${result.errors}`,
    );
  } else {
    logLine(`Проверено: ${result.checked}, update_available: ${result.updates_available}, ошибок: ${result.errors}`);
  }
  await refreshState();
}

async function processQueueOnce() {
  const data = await api("/api/queue/process_once", {
    method: "POST",
    body: JSON.stringify({ force: true }),
  });
  const result = data.result;
  logLine(`Queue step: ${JSON.stringify(result)}`);
  await refreshState();
}

async function clearDoneQueue() {
  const data = await api("/api/queue/clear_done", { method: "POST", body: JSON.stringify({}) });
  logLine(`Очистка очереди: removed=${data.result.removed}, left=${data.result.left}`);
  await refreshState();
}

function bindUI() {
  document.getElementById("save_settings").addEventListener("click", async () => {
    const payload = collectSettings();
    await api("/api/settings", { method: "POST", body: JSON.stringify(payload) });
    logLine("Настройки сохранены");
    await refreshState();
  });

  document.getElementById("discover").addEventListener("click", async () => {
    const scan_pages = asNumber(document.getElementById("scan_pages").value, 5);
    const data = await api("/api/discover", {
      method: "POST",
      body: JSON.stringify({ scan_pages }),
    });
    logLine(`Каталог обновлен: найдено ${data.result.discovered}, всего ${data.result.total}`);
    await refreshState();
  });

  document.getElementById("check_only").addEventListener("click", async () => {
    await runCheck(false);
  });

  document.getElementById("update_enabled").addEventListener("click", async () => {
    await runCheck(true);
  });

  document.getElementById("process_queue_once").addEventListener("click", async () => {
    await processQueueOnce();
  });

  document.getElementById("clear_done_queue").addEventListener("click", async () => {
    await clearDoneQueue();
  });

  document.getElementById("add_mod").addEventListener("click", async () => {
    const url = document.getElementById("custom_url").value.trim();
    if (!url) {
      return;
    }
    await api("/api/add_mod", { method: "POST", body: JSON.stringify({ url }) });
    logLine(`Добавлен мод: ${url}`);
    document.getElementById("custom_url").value = "";
    await refreshState();
  });
}

async function boot() {
  bindUI();
  await refreshState();
}

boot().catch((err) => {
  logLine(`Ошибка запуска: ${err.message}`);
});
