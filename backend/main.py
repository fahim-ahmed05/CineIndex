from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
import os
import re
from urllib.parse import urlparse

from backend.app.db import init_db, get_conn, ROOT
from backend.app.media.crawler import (
    load_root_configs,
    load_crawl_config,
    crawl_root,
)
from backend.app.media.search import (
    load_media_entries,
    build_choice_list,
    search_media,
    MediaEntry,
)
from backend.app.media.history import get_recent_history

CONFIG_JSON = ROOT / "config.json"
ROOTS_JSON = ROOT / "roots.json"

EPISODE_REGEX = re.compile(r"[sS](\d{1,2})[ ._-]*[eE](\d{1,3})")


# ---------- Banner & setup ----------


def print_banner() -> None:
    banner = r"""
_________ .__              .___            .___             
\_   ___ \|__| ____   ____ |   | ____    __| _/____ ___  ___
/    \  \/|  |/    \_/ __ \|   |/    \  / __ |/ __ \\  \/  /
\     \___|  |   |  \  ___/|   |   |  \/ /_/ \  ___/ >    < 
 \______  /__|___|  /\___  >___|___|  /\____ |\___  >__/\_ \
        \/        \/     \/         \/      \/    \/      \/
"""
    print(banner)
    print(
        "A fast terminal-based media indexer and player for directory-style servers\n"
    )


def ensure_config_files() -> None:
    """
    If roots.json or config.json don't exist, create demo versions.
    """
    created_any = False

    if not ROOTS_JSON.exists():
        demo_roots = [
            {
                "url": "http://example-server/ftps10/Movies/",
                "cookie": "",
                "tag": "FTPS10",
            }
        ]
        ROOTS_JSON.write_text(
            json.dumps(demo_roots, indent=2),
            encoding="utf-8",
        )
        print(f"[SETUP] Created demo roots.json at {ROOTS_JSON}")
        print("        Edit this file and put your actual root URLs.\n")
        created_any = True

    if not CONFIG_JSON.exists():
        demo_cfg = {"video_extensions": [], "blocked_dirs": [], "download_dir": ""}
        CONFIG_JSON.write_text(
            json.dumps(demo_cfg, indent=2),
            encoding="utf-8",
        )
        print(f"[SETUP] Created demo config.json at {CONFIG_JSON}")
        print(
            "        Edit this file and set video_extensions, blocked_dirs, and optional download_dir.\n"
        )
        created_any = True

    if created_any:
        print("Edit roots.json and config.json, then run Build index.\n")


def load_roots_config() -> list[dict]:
    if not ROOTS_JSON.exists():
        return []
    try:
        with ROOTS_JSON.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
    except json.JSONDecodeError as e:
        print(f"Error parsing roots.json: {e}")
    except OSError as e:
        print(f"Error reading roots.json: {e}")
    return []


def load_config() -> dict:
    """
    Load config.json. Returns {} on error/missing.
    """
    if not CONFIG_JSON.exists():
        return {}
    try:
        with CONFIG_JSON.open("r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error parsing config.json: {e}")
        return {}
    except OSError as e:
        print(f"Error reading config.json: {e}")
        return {}


def build_root_tag_map() -> dict[str, str]:
    """
    Build a mapping from root URL (normalized) -> short tag.

    - Uses 'tag' from roots.json if present and non-empty.
    - Otherwise, derives a tag from the URL:
      * last path segment if available (e.g. '/ftps10/' -> 'ftps10')
      * or hostname (e.g. 'cds2.discoveryftp.net').
    """
    roots_raw = load_roots_config()
    mapping: dict[str, str] = {}

    if not roots_raw:
        return mapping

    from backend.app.media.crawler import normalize_root_url

    for r in roots_raw:
        url = (r.get("url") or "").strip()
        if not url:
            continue

        norm = normalize_root_url(url)

        tag = (r.get("tag") or "").strip()
        if not tag:
            parsed = urlparse(norm)
            path_str = parsed.path.strip("/")
            if path_str:
                tag = path_str.split("/")[-1]
            else:
                tag = parsed.netloc

        mapping[norm] = tag

    return mapping


# ---------- Playlist helpers ----------


def _episode_sort_key(filename: str) -> tuple[int, int, str]:
    m = EPISODE_REGEX.search(filename)
    if m:
        season = int(m.group(1))
        episode = int(m.group(2))
        return (season, episode, filename.lower())
    return (9999, 9999, filename.lower())


def build_dir_playlist(entry: MediaEntry, conn) -> tuple[list[MediaEntry], int]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT url, root, path, filename, size, modified
        FROM media
        WHERE path = ?
        """,
        (entry.path,),
    )
    rows = cur.fetchall()

    playlist: list[MediaEntry] = []
    for r in rows:
        playlist.append(
            MediaEntry(
                url=r["url"],
                root=r["root"],
                path=r["path"],
                filename=r["filename"],
                size=r["size"],
                modified=r["modified"],
            )
        )

    if not playlist:
        return [entry], 0

    ep_like = [e for e in playlist if EPISODE_REGEX.search(e.filename)]
    if len(ep_like) < 2:
        return [entry], 0

    playlist.sort(key=lambda e: _episode_sort_key(e.filename))

    start_index = 0
    for i, e in enumerate(playlist):
        if e.url == entry.url:
            start_index = i
            break

    return playlist, start_index


# ---------- mpv player ----------


def play_entry(entry: MediaEntry, conn) -> None:
    script_path = ROOT / "cineindex-history.lua"
    script_arg = None
    if script_path.exists():
        script_arg = f"--script={script_path.as_posix()}"
    else:
        print(
            f"[PLAY] Warning: {script_path} not found; history Lua script will not run."
        )

    cfg = load_config()
    mpv_args = cfg.get("mpv_args", [])
    if not isinstance(mpv_args, list):
        mpv_args = []

    playlist, start_index = build_dir_playlist(entry, conn)

    if len(playlist) == 1:
        cmd = ["mpv", *mpv_args]
        if script_arg:
            cmd.append(script_arg)
        cmd.append(playlist[0].url)

        print(f"\n[PLAY] Running: {' '.join(cmd)}")
        try:
            subprocess.run(cmd)
        except FileNotFoundError:
            print("  !! mpv not found. Make sure it's in PATH or adjust the command.")
        except Exception as e:
            print(f"  !! Error launching mpv: {e}")
        else:
            print("[PLAY] mpv exited.\n")
        return

    try:
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=".m3u", mode="w", encoding="utf-8"
        ) as f:
            playlist_path = f.name
            for e in playlist:
                f.write(e.url + "\n")
    except Exception as e:
        print(f"  !! Failed to create playlist file: {e}")
        cmd = ["mpv", *mpv_args]
        if script_arg:
            cmd.append(script_arg)
        cmd.append(entry.url)
        print(f"\n[PLAY] Fallback: {' '.join(cmd)}")
        try:
            subprocess.run(cmd)
        except Exception as e2:
            print(f"  !! Error launching mpv fallback: {e2}")
        else:
            print("[PLAY] mpv exited.\n")
        return

    cmd = ["mpv", *mpv_args]
    if script_arg:
        cmd.append(script_arg)
    cmd.append(f"--playlist={playlist_path}")
    cmd.append(f"--playlist-start={start_index}")

    print(f"\n[PLAY] Running: {' '.join(cmd)}")
    try:
        subprocess.run(cmd)
    except FileNotFoundError:
        print("  !! mpv not found. Make sure it's in PATH or adjust the command.")
    except Exception as e:
        print(f"  !! Error launching mpv: {e}")
    finally:
        try:
            os.remove(playlist_path)
        except OSError:
            pass

    print("[PLAY] mpv exited.\n")


# ---------- aria2c downloader ----------


def download_entry(entry: MediaEntry) -> None:
    """
    Download a media entry using aria2c.

    - Uses entry.url
    - Saves as entry.filename under the configured download_dir.
    - If download_dir is missing/empty in config.json, use ./downloads in the
      current working directory.
    """
    cfg = load_config()
    dl_dir_val = (cfg.get("download_dir") or "").strip()

    if dl_dir_val:
        dl_dir = Path(dl_dir_val).expanduser()
        if not dl_dir.is_absolute():
            dl_dir = Path.cwd() / dl_dir
    else:
        dl_dir = Path.cwd() / "downloads"

    try:
        dl_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(f"[DL] Failed to create download directory {dl_dir}: {e}")
        print("     Falling back to current working directory.")
        dl_dir = Path.cwd()

    cmd = [
        "aria2c",
        "--continue=true",
        "--max-connection-per-server=4",
        "--split=4",
        "--min-split-size=10M",
        "--dir",
        str(dl_dir),
        "--out",
        entry.filename,
        entry.url,
    ]

    print(f"[DL] Running: {' '.join(str(c) for c in cmd)}")
    try:
        subprocess.run(cmd)
    except FileNotFoundError:
        print("  !! aria2c not found. Make sure it's in PATH.")
    except Exception as e:
        print(f"  !! Error launching aria2c: {e}")
    else:
        print(f"[DL] Finished: {dl_dir / entry.filename}\n")


# ---------- Index operations ----------


def build_index() -> None:
    init_db()
    roots_raw = load_roots_config()
    cfg_raw = load_config()

    if not roots_raw:
        print("[BUILD] No roots configured in roots.json.\n")
        return

    from backend.app.media.crawler import load_root_configs, load_crawl_config

    root_cfgs = load_root_configs(roots_raw, ROOT)
    crawl_cfg = load_crawl_config(cfg_raw)

    conn = get_conn()
    try:
        for rc in root_cfgs:
            crawl_root(rc, crawl_cfg, conn=conn, incremental=False)
    finally:
        conn.close()


def update_index() -> None:
    init_db()
    roots_raw = load_roots_config()
    cfg_raw = load_config()

    if not roots_raw:
        print("[UPDATE] No roots configured in roots.json.\n")
        return

    from backend.app.media.crawler import load_root_configs, load_crawl_config

    root_cfgs = load_root_configs(roots_raw, ROOT)
    crawl_cfg = load_crawl_config(cfg_raw)

    conn = get_conn()
    try:
        for rc in root_cfgs:
            crawl_root(rc, crawl_cfg, conn=conn, incremental=True)
    finally:
        conn.close()


def show_stats() -> None:
    init_db()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM dirs")
        dirs_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM media")
        media_count = cur.fetchone()[0]

        print("\n=== CineIndex Stats ===")
        print(f"Dirs:   {dirs_count}")
        print(f"Media:  {media_count}\n")
    finally:
        conn.close()


# ---------- Search ----------


def search_index() -> None:
    init_db()
    conn = get_conn()
    try:
        print("\n[SEARCH] Loading media entries into memory...")
        entries = load_media_entries(conn)
        print(f"[SEARCH] Loaded {len(entries)} entries.")

        if not entries:
            print("[SEARCH] No media indexed yet. Build the index first.\n")
            return

        print("[SEARCH] Building RapidFuzz choices list...")
        choices = build_choice_list(entries)
        print("[SEARCH] Ready.\n")

        root_tags = build_root_tag_map()

        print("Type a search pattern (e.g. '12 monk s03', 'dragon s02e05').")
        print("Empty pattern at the search prompt returns to main menu.\n")

        while True:
            pattern = input("CineIndex search (ENTER to return): ").strip()
            if not pattern:
                print()
                return

            results = search_media(
                pattern,
                entries=entries,
                choices=choices,
                limit=50,
                score_cutoff=40,
            )

            if not results:
                print("  No matches.\n")
                continue

            print()
            n = len(results)
            for i in reversed(range(n)):
                entry, score = results[i]
                num = i + 1
                size_str = f" | {entry.size}" if entry.size else ""
                mod_str = f" | {entry.modified}" if entry.modified else ""
                print(
                    f"{num:2d}. {entry.filename}{size_str}{mod_str}  (score {score:.1f})"
                )
                display_root = root_tags.get(entry.root, entry.root)
                print(f"    [{display_root}] {entry.path}")
            print()

            while True:
                sel = input("Select number to play (ENTER to new search): ").strip()
                if not sel:
                    print()
                    break
                if not sel.isdigit():
                    print("  Invalid selection.\n")
                    continue

                num = int(sel)
                if not (1 <= num <= n):
                    print("  Out of range.\n")
                    continue

                entry, _score = results[num - 1]
                play_entry(entry, conn)
                break
    finally:
        conn.close()


# ---------- History ----------


def show_history() -> None:
    init_db()
    conn = get_conn()
    try:
        history = get_recent_history(conn)
        if not history:
            print("\n[HISTORY] No watch history yet.\n")
            return

        root_tags = build_root_tag_map()

        print("\n=== CineIndex Watch History (last 50) ===\n")
        n = len(history)
        for i in reversed(range(n)):
            entry, played_at = history[i]
            num = i + 1
            size_str = f" | {entry.size}" if entry.size else ""
            mod_str = f" | {entry.modified}" if entry.modified else ""
            print(f"{num:2d}. {entry.filename}{size_str}{mod_str}")
            display_root = root_tags.get(entry.root, entry.root)
            print(f"    [{display_root}] {entry.path}")
            print(f"    Played at: {played_at}")
        print()

        while True:
            sel = input(
                "Select number to play from history (ENTER to return): "
            ).strip()
            if not sel:
                print()
                break
            if not sel.isdigit():
                print("  Invalid selection.\n")
                continue

            num = int(sel)
            if not (1 <= num <= n):
                print("  Out of range.\n")
                continue

            entry, _played_at = history[num - 1]
            play_entry(entry, conn)
            break
    finally:
        conn.close()


# ---------- Download (aria2) ----------


def download_index() -> None:
    init_db()
    conn = get_conn()
    try:
        print("\n[DL] Loading media entries into memory...")
        entries = load_media_entries(conn)
        print(f"[DL] Loaded {len(entries)} entries.")

        if not entries:
            print("[DL] No media indexed yet. Build the index first.\n")
            return

        print("[DL] Building RapidFuzz choices list...")
        choices = build_choice_list(entries)
        print("[DL] Ready.\n")

        root_tags = build_root_tag_map()

        print(
            "Type a search pattern to choose downloads (e.g. '12 monk s03', 'dragon s02e05')."
        )
        print("Empty pattern returns to main menu.\n")

        while True:
            pattern = input("CineIndex download (ENTER to return): ").strip()
            if not pattern:
                print()
                return

            results = search_media(
                pattern,
                entries=entries,
                choices=choices,
                limit=50,
                score_cutoff=40,
            )

            if not results:
                print("  No matches.\n")
                continue

            print()
            n = len(results)
            for i in reversed(range(n)):
                entry, score = results[i]
                num = i + 1
                size_str = f" | {entry.size}" if entry.size else ""
                mod_str = f" | {entry.modified}" if entry.modified else ""
                print(
                    f"{num:2d}. {entry.filename}{size_str}{mod_str}  (score {score:.1f})"
                )
                display_root = root_tags.get(entry.root, entry.root)
                print(f"    [{display_root}] {entry.path}")
            print()

            while True:
                sel = input(
                    "Select numbers to download (comma-separated, ENTER for new search): "
                ).strip()
                if not sel:
                    print()
                    break

                tokens = [t.strip() for t in sel.replace(",", " ").split()]
                nums: set[int] = set()
                valid = True
                for t in tokens:
                    if not t.isdigit():
                        print(f"  Invalid number: {t}\n")
                        valid = False
                        break
                    num = int(t)
                    if not (1 <= num <= n):
                        print(f"  Out of range: {num}\n")
                        valid = False
                        break
                    nums.add(num)
                if not valid:
                    continue

                for num in sorted(nums):
                    entry, _score = results[num - 1]
                    download_entry(entry)

                break
    finally:
        conn.close()


# ---------- Main loop ----------


def main() -> None:
    print_banner()
    ensure_config_files()

    while True:
        print("=== CineIndex TUI ===")
        print("1. Build index (full crawl)")
        print("2. Update index (incremental)")
        print("3. Show stats")
        print("4. Stream (mpv)")
        print("5. Watch history")
        print("6. Download (aria2)")
        print("0. Quit")
        choice = input("Select an option: ").strip()

        if choice == "1":
            build_index()
        elif choice == "2":
            update_index()
        elif choice == "3":
            show_stats()
        elif choice == "4":
            search_index()
        elif choice == "5":
            show_history()
        elif choice == "6":
            download_index()
        elif choice == "0" or choice == "":
            print("Bye.")
            break
        else:
            print("Invalid choice.\n")


if __name__ == "__main__":
    main()
