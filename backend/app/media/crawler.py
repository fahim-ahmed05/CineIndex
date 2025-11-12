from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, List, Tuple
from pathlib import Path
from urllib.parse import urlparse
import time

import requests

from ..db import get_conn
from .parser import parse_directory_page


@dataclass
class RootConfig:
    url: str
    cookie_file: Optional[Path]


@dataclass
class CrawlConfig:
    video_exts: List[str]
    blocked_dirs: List[str]


def normalize_root_url(url: str) -> str:
    """
    Ensure root URL ends with a slash.
    """
    return url.rstrip("/") + "/"


def load_root_configs(raw_roots: Iterable[dict], backend_root: Path) -> List[RootConfig]:
    roots: List[RootConfig] = []
    for r in raw_roots:
        url = r.get("url", "").strip()
        if not url:
            continue
        url = normalize_root_url(url)
        cookie_val = (r.get("cookie") or "").strip()
        cookie_file: Optional[Path] = None
        if cookie_val:
            cookie_file = Path(cookie_val).expanduser()
            if not cookie_file.is_absolute():
                cookie_file = backend_root / cookie_file
        roots.append(RootConfig(url=url, cookie_file=cookie_file))
    return roots


def load_crawl_config(raw_cfg: dict) -> CrawlConfig:
    exts = [e.lower().lstrip(".") for e in raw_cfg.get("video_extensions", [])]
    blocked = [b.strip().lower() for b in raw_cfg.get("blocked_dirs", [])]
    return CrawlConfig(video_exts=exts, blocked_dirs=blocked)


def _path_from_root(root_url: str, dir_url: str) -> str:
    r = normalize_root_url(root_url)
    if not dir_url.startswith(r):
        return "/"
    rel = dir_url[len(r):].rstrip("/")
    return "/" + rel if rel else "/"


def _is_blocked_dir(path: str, cfg: CrawlConfig) -> bool:
    if not cfg.blocked_dirs:
        return False
    last = path.strip("/").split("/")[-1].lower()
    return last in cfg.blocked_dirs


def _should_keep_file(filename: str, cfg: CrawlConfig) -> bool:
    if not cfg.video_exts:
        return True
    lower = filename.lower()
    dot = lower.rfind(".")
    if dot == -1:
        return False
    ext = lower[dot + 1 :]
    return ext in cfg.video_exts


def _make_session(root_cfg: RootConfig) -> requests.Session:
    s = requests.Session()
    if root_cfg.cookie_file and root_cfg.cookie_file.exists():
        from http.cookiejar import MozillaCookieJar

        cj = MozillaCookieJar()
        try:
            cj.load(str(root_cfg.cookie_file), ignore_discard=True, ignore_expires=True)
            s.cookies = cj
        except Exception as e:
            print(f"[CRAWL] Warning: failed to load cookie file {root_cfg.cookie_file}: {e}")
    return s


def _fetch_page(session: requests.Session, url: str) -> Optional[str]:
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f"[CRAWL] Error fetching {url}: {e}")
        return None


def crawl_root(
    root_cfg: RootConfig,
    cfg: CrawlConfig,
    conn=None,
    incremental: bool = False,
) -> None:
    own_conn = False
    if conn is None:
        conn = get_conn()
        own_conn = True

    session = _make_session(root_cfg)

    try:
        cur = conn.cursor()

        if not incremental:
            print(f"[BUILD] Clearing existing index for root {root_cfg.url}")
            cur.execute("DELETE FROM media WHERE root = ?", (root_cfg.url,))
            cur.execute("DELETE FROM dirs WHERE root = ?", (root_cfg.url,))
            conn.commit()

        queue: List[Tuple[str, Optional[str]]] = []
        queue.append((root_cfg.url, None))

        processed_dirs = 0
        inserted_files = 0
        skipped_dirs = 0

        print(f"[CRAWL] Starting crawl for {root_cfg.url}")
        t0 = time.time()

        while queue:
            dir_url, parent_url = queue.pop(0)
            rel_path = _path_from_root(root_cfg.url, dir_url)

            if _is_blocked_dir(rel_path, cfg):
                print(f"[CRAWL] Skipping blocked dir: {rel_path} ({dir_url})")
                continue

            html = _fetch_page(session, dir_url)
            if html is None:
                continue

            parsed = parse_directory_page(html, dir_url)

            dir_modified = parsed.dir_modified
            if incremental:
                cur.execute(
                    "SELECT modified FROM dirs WHERE url = ?",
                    (dir_url,),
                )
                row = cur.fetchone()
                if row is not None and row["modified"] == dir_modified:
                    skipped_dirs += 1
                    continue

            cur.execute(
                """
                INSERT INTO dirs (url, root, parent, name, modified)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    root=excluded.root,
                    parent=excluded.parent,
                    name=excluded.name,
                    modified=excluded.modified
                """,
                (
                    dir_url,
                    root_cfg.url,
                    parent_url,
                    rel_path.rsplit("/", 1)[-1] if rel_path != "/" else "",
                    dir_modified,
                ),
            )

            cur.execute(
                "DELETE FROM media WHERE root = ? AND path = ?",
                (root_cfg.url, rel_path),
            )

            batch_files = 0
            for f in parsed.files:
                if not _should_keep_file(f.name, cfg):
                    continue

                cur.execute(
                    """
                    INSERT INTO media (url, root, path, filename, modified, size)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(url) DO UPDATE SET
                        root=excluded.root,
                        path=excluded.path,
                        filename=excluded.filename,
                        modified=excluded.modified,
                        size=excluded.size
                    """,
                    (f.url, root_cfg.url, rel_path, f.name, f.modified, f.size),
                )
                batch_files += 1

            inserted_files += batch_files
            processed_dirs += 1

            print(f"[DIR] {dir_url}")
            print(f"  - found {batch_files} files, {len(parsed.subdirs)} subdirs.")

            for d in parsed.subdirs:
                queue.append((d.url, dir_url))

            if processed_dirs % 20 == 0:
                conn.commit()

        conn.commit()
        elapsed = time.time() - t0
        print(f"[CRAWL] Done {root_cfg.url}: dirs={processed_dirs}, skipped={skipped_dirs}, files={inserted_files}, time={elapsed:.1f}s\n")
    finally:
        if own_conn:
            conn.close()
