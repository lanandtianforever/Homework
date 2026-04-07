#!/usr/bin/env python3
"""
Crawl a documentation website and export all pages to Markdown.

Example:
  python3 scripts/crawl_site_to_md.py \
    --base-url https://infrasys-ai.github.io/aiinfra-docs/ \
    --output-dir out/aiinfra-docs-md
"""

from __future__ import annotations

import argparse
import json
import re
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup, NavigableString, Tag


SKIP_TAGS = {
    "script",
    "style",
    "noscript",
    "svg",
    "canvas",
    "iframe",
    "header",
    "footer",
    "nav",
    "form",
}


@dataclass
class PageResult:
    url: str
    title: str
    markdown_path: str
    html_path: str
    status_code: int


def normalize_url(url: str, base_url: Optional[str] = None) -> Optional[str]:
    candidate = url.strip()
    if not candidate:
        return None

    if base_url is not None:
        candidate = urljoin(base_url, candidate)

    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"}:
        return None

    # Drop fragments/query to avoid duplicate-content URLs.
    parsed = parsed._replace(fragment="", query="")

    # Normalize trailing slash consistency:
    # - Keep "/" for root.
    # - Keep trailing "/" for directory-like paths so relative links resolve correctly.
    # - Keep file-like paths (".html", ".md", etc.) without forcing slash.
    path = parsed.path or "/"
    if path != "/" and not path.endswith("/"):
        tail = path.rsplit("/", 1)[-1]
        if "." not in tail:
            path = path + "/"
    parsed = parsed._replace(path=path)
    return urlunparse(parsed)


def is_same_site_doc(url: str, base: str) -> bool:
    target = urlparse(url)
    root = urlparse(base)
    if target.netloc != root.netloc:
        return False
    root_path = (root.path or "/").rstrip("/")
    target_path = target.path or "/"
    return target_path.startswith(root_path) if root_path else True


def extract_links(soup: BeautifulSoup, page_url: str, base_url: str) -> Iterable[str]:
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        if not href or href.startswith("#"):
            continue
        normalized = normalize_url(href, page_url)
        if normalized is None:
            continue
        if is_same_site_doc(normalized, base_url):
            parsed = urlparse(normalized)
            path = parsed.path.lower()
            # Keep crawl focused on navigable docs pages.
            if path.endswith("/") or path.endswith(".html") or path.endswith(".htm"):
                yield normalized
            elif "." not in path.rsplit("/", 1)[-1]:
                yield normalized


def slugify_filename(url: str, base_url: str) -> str:
    parsed = urlparse(url)
    root = urlparse(base_url)

    root_path = (root.path or "/").rstrip("/")
    rel_path = parsed.path
    if root_path and rel_path.startswith(root_path):
        rel_path = rel_path[len(root_path) :]

    rel_path = rel_path.strip("/")
    if not rel_path:
        rel_path = "index"

    rel_path = re.sub(r"\.html?$", "", rel_path, flags=re.IGNORECASE)
    rel_path = rel_path.replace("/", "__")
    rel_path = re.sub(r"[^a-zA-Z0-9._-]+", "-", rel_path).strip("-")

    if not rel_path:
        rel_path = "page"
    return rel_path


def pick_main_content(soup: BeautifulSoup) -> Tag:
    selectors = [
        "article",
        "main",
        '[role="main"]',
        ".md-content__inner",
        ".content",
        "#content",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if node is not None:
            return node
    if soup.body is not None:
        return soup.body
    return soup


def clean_whitespace(text: str) -> str:
    return re.sub(r"[ \t]+", " ", text).strip()


def inline_text(node: Tag) -> str:
    parts: list[str] = []
    for child in node.children:
        if isinstance(child, NavigableString):
            text = str(child)
            if text:
                parts.append(text)
            continue
        if not isinstance(child, Tag):
            continue
        name = child.name.lower()
        if name in {"script", "style", "noscript"}:
            continue
        if name == "code":
            code = child.get_text(" ", strip=True)
            parts.append(f"`{code}`" if code else "")
            continue
        if name == "a":
            href = child.get("href", "").strip()
            label = clean_whitespace(child.get_text(" ", strip=True))
            if label and href:
                parts.append(f"[{label}]({href})")
            elif label:
                parts.append(label)
            continue
        if name == "img":
            alt = clean_whitespace(child.get("alt", ""))
            src = child.get("src", "").strip()
            if src:
                parts.append(f"![{alt}]({src})")
            continue
        if name == "br":
            parts.append("\n")
            continue
        parts.append(inline_text(child))

    return clean_whitespace(" ".join(part for part in parts if part))


def get_code_lang(pre: Tag) -> str:
    code = pre.find("code")
    if code is None:
        return ""
    classes = code.get("class", [])
    for cls in classes:
        if cls.startswith("language-"):
            return cls.split("language-", 1)[1]
    return ""


def table_to_markdown(table: Tag) -> str:
    rows: list[list[str]] = []
    for tr in table.find_all("tr"):
        row = []
        for cell in tr.find_all(["th", "td"]):
            row.append(clean_whitespace(cell.get_text(" ", strip=True)))
        if row:
            rows.append(row)

    if not rows:
        return ""

    width = max(len(r) for r in rows)
    rows = [r + [""] * (width - len(r)) for r in rows]

    header = rows[0]
    divider = ["---"] * width
    body = rows[1:] if len(rows) > 1 else []

    lines = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join(divider) + " |",
    ]
    for row in body:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def block_to_markdown(node: Tag, lines: list[str], depth: int = 0) -> None:
    for child in node.children:
        if isinstance(child, NavigableString):
            continue
        if not isinstance(child, Tag):
            continue

        name = child.name.lower()
        if name in SKIP_TAGS:
            continue

        if name in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            level = int(name[1])
            title = inline_text(child)
            if title:
                lines.append(f"{'#' * level} {title}")
                lines.append("")
            continue

        if name == "p":
            text = inline_text(child)
            if text:
                lines.append(text)
                lines.append("")
            continue

        if name in {"ul", "ol"}:
            ordered = name == "ol"
            idx = 1
            for li in child.find_all("li", recursive=False):
                bullet = f"{idx}. " if ordered else "- "
                text = inline_text(li)
                if text:
                    prefix = "  " * depth + bullet
                    lines.append(prefix + text)
                block_to_markdown(li, lines, depth + 1)
                if ordered:
                    idx += 1
            lines.append("")
            continue

        if name == "blockquote":
            quote = inline_text(child)
            if quote:
                for qline in quote.splitlines():
                    lines.append(f"> {qline}")
                lines.append("")
            continue

        if name == "pre":
            raw = child.get_text("\n", strip=False).strip("\n")
            if raw:
                lang = get_code_lang(child)
                lines.append(f"```{lang}")
                lines.append(raw)
                lines.append("```")
                lines.append("")
            continue

        if name == "table":
            markdown_table = table_to_markdown(child)
            if markdown_table:
                lines.append(markdown_table)
                lines.append("")
            continue

        if name in {"hr"}:
            lines.append("---")
            lines.append("")
            continue

        # Fallback: recurse into structural containers.
        block_to_markdown(child, lines, depth)


def html_to_markdown(html: str, page_url: str) -> tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    main = pick_main_content(soup)
    title_node = soup.find("title")
    title = clean_whitespace(title_node.get_text(" ", strip=True)) if title_node else page_url

    lines: list[str] = []
    block_to_markdown(main, lines)

    # Compress extra blank lines.
    compressed: list[str] = []
    blank = False
    for line in lines:
        is_blank = line.strip() == ""
        if is_blank and blank:
            continue
        compressed.append(line.rstrip())
        blank = is_blank

    body = "\n".join(compressed).strip()
    header = f"# {title}\n\n> Source: {page_url}\n\n"
    markdown = header + body + "\n"
    return title, markdown


def crawl(
    base_url: str,
    output_dir: Path,
    max_pages: int,
    delay_s: float,
    timeout_s: int,
) -> list[PageResult]:
    base_url = normalize_url(base_url)
    if base_url is None:
        raise ValueError("Invalid base URL.")

    output_dir.mkdir(parents=True, exist_ok=True)
    md_dir = output_dir / "md"
    html_dir = output_dir / "html"
    md_dir.mkdir(parents=True, exist_ok=True)
    html_dir.mkdir(parents=True, exist_ok=True)

    visited: set[str] = set()
    queue: deque[str] = deque([base_url])
    results: list[PageResult] = []
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "site-to-md-crawler/1.0 (+https://github.com/)",
        }
    )

    while queue and len(visited) < max_pages:
        url = queue.popleft()
        if url in visited:
            continue
        visited.add(url)

        try:
            resp = session.get(url, timeout=timeout_s)
        except Exception as exc:
            print(f"[WARN] Fetch failed: {url} ({exc})")
            continue

        status = resp.status_code
        ctype = resp.headers.get("Content-Type", "").lower()
        print(f"[INFO] [{len(visited):04d}] {status} {url}")
        if status != 200:
            continue
        if "text/html" not in ctype and "application/xhtml+xml" not in ctype:
            continue

        final_url = normalize_url(resp.url)
        if final_url and final_url != url and final_url not in visited:
            visited.add(final_url)

        html = resp.text
        soup = BeautifulSoup(html, "html.parser")
        for link in extract_links(soup, url, base_url):
            if link not in visited:
                queue.append(link)

        page_url = final_url or url
        file_stem = slugify_filename(page_url, base_url)
        md_path = md_dir / f"{file_stem}.md"
        html_path = html_dir / f"{file_stem}.html"

        title, markdown = html_to_markdown(html, page_url)
        md_path.write_text(markdown, encoding="utf-8")
        html_path.write_text(html, encoding="utf-8")

        results.append(
            PageResult(
                url=page_url,
                title=title or page_url,
                markdown_path=str(md_path.relative_to(output_dir)),
                html_path=str(html_path.relative_to(output_dir)),
                status_code=status,
            )
        )
        time.sleep(delay_s)

    return results


def write_index(output_dir: Path, base_url: str, pages: list[PageResult]) -> None:
    index_md = output_dir / "index.md"
    meta_json = output_dir / "pages.json"

    lines = [
        f"# Crawl Index",
        "",
        f"- Base URL: `{base_url}`",
        f"- Exported Pages: `{len(pages)}`",
        "",
        "| Title | URL | Markdown | HTML |",
        "| --- | --- | --- | --- |",
    ]
    for page in pages:
        title = page.title.replace("|", "\\|")
        url = page.url
        md_rel = page.markdown_path
        html_rel = page.html_path
        lines.append(f"| {title} | {url} | {md_rel} | {html_rel} |")

    index_md.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    meta_json.write_text(
        json.dumps([page.__dict__ for page in pages], ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl website and export pages as Markdown.")
    parser.add_argument(
        "--base-url",
        required=True,
        help="Docs root URL, e.g. https://infrasys-ai.github.io/aiinfra-docs/",
    )
    parser.add_argument(
        "--output-dir",
        default="out/site-md-export",
        help="Output directory.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=2000,
        help="Maximum pages to crawl.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="Delay seconds between requests.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="HTTP timeout seconds.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()
    base_url = normalize_url(args.base_url)
    if base_url is None:
        raise SystemExit("Invalid --base-url.")

    pages = crawl(
        base_url=base_url,
        output_dir=output_dir,
        max_pages=args.max_pages,
        delay_s=args.delay,
        timeout_s=args.timeout,
    )
    write_index(output_dir, base_url, pages)
    print(f"[DONE] Exported {len(pages)} pages into: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
