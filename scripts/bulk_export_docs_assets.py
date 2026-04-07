#!/usr/bin/env python3
"""
Bulk export Sphinx-style docs content:
- Crawl all HTML pages under a docs root
- Download source markdown from /_sources/*
- Download PDF files linked from pages
- Download images referenced in main content

Example:
  python3 scripts/bulk_export_docs_assets.py \
    --base-url https://infrasys-ai.github.io/aiinfra-docs/ \
    --output-dir out/aiinfra-full-export
"""

from __future__ import annotations

import argparse
import json
import os
import posixpath
import time
from collections import deque
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup


@dataclass
class PageMeta:
    url: str
    title: str
    html_local: str
    source_md_url: Optional[str]
    source_md_local: Optional[str]
    pdf_urls: list[str]
    image_urls: list[str]


def normalize_url(url: str, base_url: Optional[str] = None) -> Optional[str]:
    raw = url.strip()
    if not raw:
        return None
    if base_url:
        raw = urljoin(base_url, raw)
    p = urlparse(raw)
    if p.scheme not in {"http", "https"}:
        return None
    # Keep query-less canonical URL for crawl dedupe.
    p = p._replace(fragment="", query="")
    path = p.path or "/"
    if path != "/" and not path.endswith("/"):
        tail = path.rsplit("/", 1)[-1]
        if "." not in tail:
            path = path + "/"
    p = p._replace(path=path)
    return urlunparse(p)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def url_to_relpath(url: str, fallback: str = "index.bin") -> str:
    p = urlparse(url)
    path = unquote(p.path or "/")
    clean = path.lstrip("/")
    if not clean:
        clean = fallback
    if clean.endswith("/"):
        clean += "index.html"
    return clean


def safe_write_bytes(path: Path, data: bytes) -> None:
    ensure_parent(path)
    path.write_bytes(data)


def is_same_docs_scope(url: str, base_url: str) -> bool:
    t = urlparse(url)
    b = urlparse(base_url)
    if t.netloc != b.netloc:
        return False
    bpath = (b.path or "/").rstrip("/")
    tpath = t.path or "/"
    return tpath.startswith(bpath) if bpath else True


def html_links(soup: BeautifulSoup, page_url: str, base_url: str) -> list[str]:
    out: list[str] = []
    for a in soup.find_all("a", href=True):
        link = normalize_url(a["href"], page_url)
        if not link:
            continue
        if not is_same_docs_scope(link, base_url):
            continue
        path = urlparse(link).path.lower()
        if path.endswith("/") or path.endswith(".html") or path.endswith(".htm"):
            out.append(link)
        elif "." not in path.rsplit("/", 1)[-1]:
            out.append(link)
    return out


def get_main_node(soup: BeautifulSoup):
    for selector in ("article", "main", '[role="main"]', ".md-content__inner", ".content", "#content"):
        node = soup.select_one(selector)
        if node is not None:
            return node
    return soup.body if soup.body is not None else soup


def unique_preserve_order(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for x in items:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def collect_page_assets(soup: BeautifulSoup, page_url: str) -> tuple[list[str], list[str]]:
    node = get_main_node(soup)
    pdf_urls: list[str] = []
    image_urls: list[str] = []

    for a in node.find_all("a", href=True):
        u = normalize_url(a["href"], page_url)
        if not u:
            continue
        if urlparse(u).path.lower().endswith(".pdf"):
            pdf_urls.append(u)

    for img in node.find_all("img"):
        src = img.get("src")
        if not src:
            continue
        u = normalize_url(src, page_url)
        if not u:
            continue
        image_urls.append(u)

    # Also catch image links referenced as <a href="...png/jpg/svg/webp">
    for a in node.find_all("a", href=True):
        u = normalize_url(a["href"], page_url)
        if not u:
            continue
        lp = urlparse(u).path.lower()
        if lp.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg")):
            image_urls.append(u)

    return unique_preserve_order(pdf_urls), unique_preserve_order(image_urls)


def source_md_url_for_page(page_url: str, base_url: str) -> Optional[str]:
    p = urlparse(page_url)
    b = urlparse(base_url)
    base_prefix = (b.path or "/").rstrip("/")
    path = p.path or "/"
    if not path.endswith(".html"):
        if path.endswith("/"):
            path = path + "index.html"
        else:
            path = path + "/index.html"
    # Convert docs page to sphinx source path:
    # /aiinfra-docs/00Summary/README.html -> /aiinfra-docs/_sources/00Summary/README.md
    # /aiinfra-docs/index.html -> /aiinfra-docs/_sources/index.md
    if base_prefix and path.startswith(base_prefix):
        rel = path[len(base_prefix) :].lstrip("/")
    else:
        rel = path.lstrip("/")
    rel = rel[:-5] + ".md" if rel.endswith(".html") else rel + ".md"
    source_path = posixpath.join(base_prefix, "_sources", rel)
    return urlunparse((p.scheme, p.netloc, source_path, "", "", ""))


def try_download(session: requests.Session, url: str, timeout: int) -> Optional[requests.Response]:
    try:
        r = session.get(url, timeout=timeout)
        return r
    except Exception:
        return None


def write_summary(output_dir: Path, base_url: str, pages: list[PageMeta], md_count: int, pdf_count: int, image_count: int) -> None:
    summary = output_dir / "README.md"
    lines = [
        "# Docs Bulk Export",
        "",
        f"- Base URL: `{base_url}`",
        f"- HTML pages crawled: `{len(pages)}`",
        f"- Source Markdown downloaded: `{md_count}`",
        f"- PDF downloaded: `{pdf_count}`",
        f"- Images downloaded: `{image_count}`",
        "",
        "## Layout",
        "",
        "- `html/`: crawled HTML pages",
        "- `md_sources/`: source Markdown from site `_sources` interface",
        "- `pdf/`: downloaded PDF files",
        "- `images/`: downloaded images referenced by page body",
        "- `manifest.json`: page-level index",
        "",
    ]
    summary.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(base_url: str, output_dir: Path, max_pages: int, delay: float, timeout: int) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    html_dir = output_dir / "html"
    md_dir = output_dir / "md_sources"
    pdf_dir = output_dir / "pdf"
    img_dir = output_dir / "images"
    for d in (html_dir, md_dir, pdf_dir, img_dir):
        d.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers["User-Agent"] = "docs-bulk-export/1.0"

    visited = set()
    q = deque([base_url])
    pages: list[PageMeta] = []
    downloaded_bins: set[str] = set()
    md_count = 0
    pdf_count = 0
    image_count = 0

    while q and len(visited) < max_pages:
        url = q.popleft()
        if url in visited:
            continue
        visited.add(url)

        r = try_download(session, url, timeout)
        if r is None:
            print(f"[WARN] fetch fail: {url}")
            continue
        if r.status_code != 200:
            print(f"[WARN] {r.status_code}: {url}")
            continue
        ctype = (r.headers.get("Content-Type") or "").lower()
        if "text/html" not in ctype and "application/xhtml+xml" not in ctype:
            continue

        html_bytes = r.content
        page_url = normalize_url(r.url) or url
        local_html = html_dir / url_to_relpath(page_url, "index.html")
        safe_write_bytes(local_html, html_bytes)

        soup = BeautifulSoup(r.text, "html.parser")
        title_node = soup.find("title")
        title = title_node.get_text(" ", strip=True) if title_node else page_url

        for link in html_links(soup, page_url, base_url):
            if link not in visited:
                q.append(link)

        pdf_urls, image_urls = collect_page_assets(soup, page_url)

        # Download source markdown from built-in _sources interface
        source_url = source_md_url_for_page(page_url, base_url)
        source_local_rel: Optional[str] = None
        if source_url:
            rr = try_download(session, source_url, timeout)
            if rr is not None and rr.status_code == 200:
                ct2 = (rr.headers.get("Content-Type") or "").lower()
                # Most markdown responds as text/plain.
                if "text" in ct2 or source_url.lower().endswith(".md"):
                    local_md = md_dir / url_to_relpath(source_url, "index.md")
                    safe_write_bytes(local_md, rr.content)
                    source_local_rel = str(local_md.relative_to(output_dir))
                    md_count += 1
                else:
                    source_url = None
            else:
                source_url = None

        # Download page-linked pdf/images once
        for u in pdf_urls:
            if u in downloaded_bins:
                continue
            rr = try_download(session, u, timeout)
            if rr is None or rr.status_code != 200:
                continue
            local_pdf = pdf_dir / url_to_relpath(u, "file.pdf")
            safe_write_bytes(local_pdf, rr.content)
            downloaded_bins.add(u)
            pdf_count += 1

        for u in image_urls:
            if u in downloaded_bins:
                continue
            rr = try_download(session, u, timeout)
            if rr is None or rr.status_code != 200:
                continue
            local_img = img_dir / url_to_relpath(u, "image.bin")
            safe_write_bytes(local_img, rr.content)
            downloaded_bins.add(u)
            image_count += 1

        pages.append(
            PageMeta(
                url=page_url,
                title=title,
                html_local=str(local_html.relative_to(output_dir)),
                source_md_url=source_url,
                source_md_local=source_local_rel,
                pdf_urls=pdf_urls,
                image_urls=image_urls,
            )
        )
        print(f"[INFO] {len(pages):04d} {page_url}")
        time.sleep(delay)

    manifest = output_dir / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "base_url": base_url,
                "counts": {
                    "pages": len(pages),
                    "source_markdown": md_count,
                    "pdf": pdf_count,
                    "images": image_count,
                },
                "pages": [asdict(p) for p in pages],
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    write_summary(output_dir, base_url, pages, md_count, pdf_count, image_count)
    print(
        f"[DONE] pages={len(pages)} md={md_count} pdf={pdf_count} images={image_count} out={output_dir}"
    )


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Bulk export docs正文 (md/pdf/images/html).")
    ap.add_argument("--base-url", required=True)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--max-pages", type=int, default=5000)
    ap.add_argument("--delay", type=float, default=0.03)
    ap.add_argument("--timeout", type=int, default=20)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    base = normalize_url(args.base_url)
    if not base:
        raise SystemExit("Invalid --base-url")
    out = Path(args.output_dir).resolve()
    run(base, out, args.max_pages, args.delay, args.timeout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

