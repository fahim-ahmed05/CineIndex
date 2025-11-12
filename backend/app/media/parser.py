from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import urljoin, urlparse, urlunparse, unquote

from bs4 import BeautifulSoup


@dataclass
class ParsedDirEntry:
    name: str
    url: str
    modified: Optional[str]


@dataclass
class ParsedFileEntry:
    name: str
    url: str
    modified: Optional[str]
    size: Optional[str]


@dataclass
class ParsedPage:
    dir_modified: Optional[str]
    subdirs: List[ParsedDirEntry]
    files: List[ParsedFileEntry]


def _normalize_url(base: str, href: str) -> str:
    url = urljoin(base, href)
    parsed = urlparse(url)
    parsed = parsed._replace(fragment="")
    return urlunparse(parsed)


def _decode_name(text: str) -> str:
    return unquote(text.strip())


def _parse_generic_table(soup: BeautifulSoup, base_url: str) -> ParsedPage:
    table = soup.find("table")
    if not table:
        return ParsedPage(None, [], [])

    subdirs: List[ParsedDirEntry] = []
    files: List[ParsedFileEntry] = []
    times: List[str] = []

    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        img = tds[0].find("img")
        alt = img.get("alt", "") if img else ""
        name_td = tds[1]
        a = name_td.find("a")
        if not a:
            continue

        href = a.get("href", "")
        label = a.text.strip()

        if alt.upper().startswith("[PARENTDIR]") or href in ("../", ".."):
            continue

        modified = None
        size = None

        if len(tds) >= 3:
            modified = tds[2].get_text(strip=True) or None
        if len(tds) >= 4:
            size = tds[3].get_text(strip=True) or None

        if modified:
            times.append(modified)

        url = _normalize_url(base_url, href)
        decoded_name = _decode_name(label)

        is_dir = href.endswith("/") or "[DIR]" in alt.upper() or "folder" in alt.lower()

        if is_dir:
            subdirs.append(ParsedDirEntry(decoded_name, url, modified))
        else:
            files.append(ParsedFileEntry(decoded_name, url, modified, size))

    dir_modified = max(times) if times else None
    return ParsedPage(dir_modified, subdirs, files)


def _parse_h5ai_fallback(soup: BeautifulSoup, base_url: str) -> ParsedPage:
    fb = soup.find("div", id="fallback")
    if not fb:
        return ParsedPage(None, [], [])

    table = fb.find("table")
    if not table:
        return ParsedPage(None, [], [])

    subdirs: List[ParsedDirEntry] = []
    files: List[ParsedFileEntry] = []
    times: List[str] = []

    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue

        name_td = tds[1]
        a = name_td.find("a")
        if not a:
            continue

        href = a.get("href", "")
        label = a.text.strip()

        if href == ".." or "Parent Directory" in label:
            continue

        modified = tds[2].get_text(strip=True) or None
        size = tds[3].get_text(strip=True) or None

        if modified:
            times.append(modified)

        img = tds[0].find("img")
        alt = img.get("alt", "") if img else ""

        url = _normalize_url(base_url, href)
        decoded_name = _decode_name(label)

        is_dir = href.endswith("/") or "folder" in alt.lower()

        if is_dir:
            subdirs.append(ParsedDirEntry(decoded_name, url, modified))
        else:
            files.append(ParsedFileEntry(decoded_name, url, modified, size))

    dir_modified = max(times) if times else None
    return ParsedPage(dir_modified, subdirs, files)


def _parse_discovery_datatable(soup: BeautifulSoup, base_url: str) -> ParsedPage:
    table = soup.find("table", id="example")
    if not table:
        return ParsedPage(None, [], [])

    tbody = table.find("tbody") or table
    subdirs: List[ParsedDirEntry] = []
    files: List[ParsedFileEntry] = []
    times: List[str] = []

    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        name_td = tds[1]
        a = name_td.find("a")
        if not a:
            continue

        href = a.get("href", "")
        label = a.text.strip()

        if href in ("..", "../") or "Parent Directory" in label:
            continue

        size = None
        modified = None
        if len(tds) >= 4:
            size = tds[3].get_text(strip=True) or None
        if len(tds) >= 5:
            modified = tds[4].get_text(strip=True) or None

        if modified:
            times.append(modified)

        url = _normalize_url(base_url, href)
        decoded_name = _decode_name(label)

        is_dir = href.endswith("/") and (size is None or size == "")

        if is_dir:
            subdirs.append(ParsedDirEntry(decoded_name, url, modified))
        else:
            files.append(ParsedFileEntry(decoded_name, url, modified, size))

    dir_modified = max(times) if times else None
    return ParsedPage(dir_modified, subdirs, files)


def parse_directory_page(html: str, base_url: str) -> ParsedPage:
    """
    Detect which style of listing this is and parse accordingly.
    """
    soup = BeautifulSoup(html, "html.parser")

    if soup.find("div", id="fallback"):
        return _parse_h5ai_fallback(soup, base_url)

    if soup.find("table", id="example"):
        return _parse_discovery_datatable(soup, base_url)

    return _parse_generic_table(soup, base_url)
