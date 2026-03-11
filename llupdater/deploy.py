import datetime as dt
import os
import shutil
import subprocess
import zipfile
from pathlib import Path

ARCHIVE_EXTENSIONS = {".zip", ".7z", ".rar"}


def _clear_directory(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _extract_with_7zip(archive_path: Path, extract_dir: Path) -> None:
    candidates = ["7z", r"C:\Program Files\7-Zip\7z.exe", r"C:\Program Files (x86)\7-Zip\7z.exe"]
    for exe in candidates:
        try:
            result = subprocess.run(
                [exe, "x", "-y", f"-o{extract_dir}", str(archive_path)],
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError:
            continue

        if result.returncode == 0:
            return
        raise RuntimeError(f"7z extraction failed: {result.stderr.strip() or result.stdout.strip()}")

    raise RuntimeError("7z executable not found. Install 7-Zip for .rar/.7z support.")


def extract_download(download_path: Path, extract_dir: Path) -> None:
    _clear_directory(extract_dir)

    suffix = download_path.suffix.lower()
    if suffix == ".zip":
        with zipfile.ZipFile(download_path, "r") as zf:
            zf.extractall(extract_dir)
        return

    if suffix in {".7z", ".rar"}:
        _extract_with_7zip(download_path, extract_dir)
        return

    if suffix in {".package", ".ts4script"}:
        shutil.copy2(download_path, extract_dir / download_path.name)
        return

    # fallback: try to unpack known formats, otherwise copy as-is
    try:
        shutil.unpack_archive(str(download_path), str(extract_dir))
    except Exception:
        shutil.copy2(download_path, extract_dir / download_path.name)


def _iter_payload_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for p in root.rglob("*"):
        if p.is_dir():
            continue
        if p.suffix.lower() in ARCHIVE_EXTENSIONS:
            continue
        files.append(p)
    return files


def _backup_if_exists(dst: Path, backup_root: Path, mod_id: str, rel_path: Path) -> None:
    if not dst.exists() and not dst.is_symlink():
        return

    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_root / mod_id / stamp / rel_path
    backup_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        if dst.is_symlink() or dst.is_file():
            shutil.copy2(dst, backup_path)
    except Exception:
        # best effort backup; continue deployment if copy fails
        pass


def _remove_previous_files(mod: dict) -> None:
    for path_str in mod.get("deployed_files", []) or []:
        p = Path(path_str)
        try:
            if p.is_symlink() or p.is_file():
                p.unlink(missing_ok=True)
        except Exception:
            continue


def _deploy_file(src: Path, dst: Path, method: str) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() or dst.is_symlink():
        dst.unlink(missing_ok=True)

    if method == "copy":
        shutil.copy2(src, dst)
        return

    if method == "hardlink":
        os.link(src, dst)
        return

    if method == "symlink":
        os.symlink(src, dst)
        return

    raise ValueError(f"Unsupported deploy method: {method}")


def deploy_download(download_path: Path, mod: dict, settings: dict) -> list[str]:
    staging_root = Path(settings["staging_dir"]) / str(mod["id"])
    extract_dir = staging_root / "current"
    mods_dir = Path(settings["mods_dir"]).expanduser()
    backup_root = Path(settings["backups_dir"]) / "deploy"
    method = settings.get("deploy_method", "copy")

    if not mods_dir.exists():
        raise RuntimeError(f"Mods directory not found: {mods_dir}")

    extract_download(download_path, extract_dir)
    payload_files = _iter_payload_files(extract_dir)
    if not payload_files:
        raise RuntimeError("No deployable files found inside download")

    install_subdir = (mod.get("install_subdir") or "").strip().strip("/\\")
    install_root = mods_dir / install_subdir if install_subdir else mods_dir

    _remove_previous_files(mod)

    deployed: list[str] = []
    for src in payload_files:
        rel = src.relative_to(extract_dir)
        dst = install_root / rel
        _backup_if_exists(dst, backup_root, str(mod["id"]), rel)
        _deploy_file(src, dst, method)
        deployed.append(str(dst))

    return deployed
