# LL Sims 4 Mod Manager (LoversLab)

Local mod manager for **The Sims 4** with automatic LoversLab update tracking and installation.

User flow is simple:
1. Run one initial full sync.
2. Enable `Auto` for mods you want managed.
3. Use `Check updates now` (or leave background auto-check enabled).

Queueing, throttling, retries, and cooldowns are internal and automatic.

## Key Features

- Full Sims 4 catalog discovery (dynamic subcategories, including new ones)
- Mod list with search/filter/sort and details card
- Auto-update only for enabled (`Auto`) mods
- Safe download throttling and retry/backoff logic
- Deploy into `Mods/<manager_root_subdir>/<mod_id>_<slug>/...`
- Deploy methods: `hardlink` (default), `copy`, `symlink`
- Image mode:
  - `Local cache (recommended)`
  - `Direct from site`
- Optional strict proxy mode for all app-side LoversLab traffic

## Requirements

- Windows
- 7-Zip installed for `.rar` / `.7z` archives (`7z.exe`)

## End-User Install (No Terminal)

1. Download `LL-Sims4-Mod-Manager.exe` from Releases.
2. Put the `.exe` in its own folder (for example: `C:\LL\Manager`).
3. Double-click the `.exe`.
4. Browser opens automatically at `http://127.0.0.1:8765`.

Notes:
- On first run, Windows SmartScreen may ask for confirmation.
- A `data` folder is created next to the `.exe`.

## First Run Setup

In `Settings`:

1. `Mods folder` -> select your `The Sims 4\Mods` folder
2. `Manager root subfolder` -> keep `_LL_MOD_MANAGER` unless you need custom
3. `Deploy method` -> keep `hardlink` (recommended)
4. Paste `Cookie` from your logged-in LoversLab browser session
5. Enable proxy if needed
6. Click `Save settings`

Then in `Actions`:

1. Click `Full sync (all categories + cache refresh)`
2. Go to `Mods`, enable `Auto` for desired mods
3. Click `Check updates now`

## Daily Use

- `Check updates now` checks enabled mods and automatically starts installing updates.
- You can also leave background auto-tracking enabled.

## Auto Checkbox Behavior

If you disable (`Auto` off) a mod:

- It is excluded from automatic updates
- Its deployed files are automatically removed from the game `Mods` folder
- Pending internal install tasks for that mod are automatically removed

## Buttons (Simple Explanation)

- `Full sync (all categories + cache refresh)`
  - Full Sims 4 catalog pass and metadata/cache refresh

- `Check updates now`
  - Checks enabled mods and auto-installs available updates

## Image Source Modes

`Settings -> Image source mode`:

- `Local cache (recommended)`
  - Faster card rendering after cache warm-up, fewer remote requests at view time

- `Direct from site`
  - Loads card images directly from LoversLab, no local image cache usage for display

## Important Limits

- `hardlink` works only within the same drive/volume
- `symlink` on Windows may require Developer Mode or elevated privileges
- Zero-ban-risk does not exist; conservative throttling and backoff are used

## Troubleshooting

- If `.exe` does not start: check SmartScreen/antivirus and try again
- If downloads fail: verify LoversLab cookie is still valid
- If proxy mode is enabled: verify proxy is alive and correctly configured
- For `.rar`/`.7z` extraction errors: install 7-Zip

## Maintainer: Release via GitHub Actions

Releases are built and published by GitHub Actions workflow:
`/.github/workflows/release.yml`

To create a release:

```bash
git tag v0.1.2
git push origin v0.1.2
```

Workflow does this automatically:

- builds `LL-Sims4-Mod-Manager.exe` on `windows-latest`
- creates/updates GitHub Release for that tag
- uploads the `.exe` as a Release asset
- uploads the `.exe` as a workflow artifact

## Advanced: Run from Source (Optional)

If you want to run from source instead of the `.exe`:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python launcher.py
```

## Data Files

- `data/mods.json` - mod records and state
- `data/settings.json` - app settings
- `data/queue.json` - internal install queue
- `data/runtime.json` - runtime limits/cursors
- `data/media_cache/` - local image cache (when cache mode is enabled)
