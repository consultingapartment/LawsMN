#!/usr/bin/env python3
"""Монгол Улсын хүчин төгөлдөр хуулиудыг legalinfo.mn-ээс татаж SQLite-д бүртгэнэ.

Сар бүр (эсвэл гараар) дахин ажиллуулахад:
  - өмнө байгаагүй шинэ хуулийг татна
  - «Нэмэлт өөрчлөлт» табын жагсаалт өөрчлөгдсөн хуулийг дахин татна
  - жагсаалтаас алга болсон хуулийг inactive гэж тэмдэглэнэ (файлыг устгахгүй)

Жишээ:
  python sync_laws.py
  python sync_laws.py --limit 5
  python sync_laws.py --metadata-only
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

BASE = "https://legalinfo.mn"
LIST_URL = f"{BASE}/mn/ajaxList/"
DETAIL_URL = f"{BASE}/mn/detail"
WORD_URL = f"{BASE}/mn/wordExport"
CONNECTED_URL = f"{BASE}/mn/lawConnectedTypeData"

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "laws.sqlite"
WORD_DIR = DATA_DIR / "laws"
TEXT_DIR = ROOT / "legal"
STATE_PATH = DATA_DIR / "last_sync.json"

REPEAL_PHRASE = "ХҮЧИНГҮЙ БОЛСОНД ТООЦОХ"
PAGE_DELAY = 0.6
FILE_DELAY = 1.2
CHECK_DELAY = 0.35
MAX_RETRIES = 3

HEADERS = {
    "User-Agent": "MongolianLawSync/1.0 (public legal archive; monthly refresh)",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Origin": BASE,
    "Referer": f"{BASE}/mn/law?page=law&cate=27&active=1",
    "X-Requested-With": "XMLHttpRequest",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def is_repeal(title: str) -> bool:
    return REPEAL_PHRASE in normalize_ws(title).upper()


def connect() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    WORD_DIR.mkdir(parents=True, exist_ok=True)
    TEXT_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS laws (
            law_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            detail_url TEXT NOT NULL,
            word_url TEXT NOT NULL,
            enacted_date TEXT,
            enforcement_date TEXT,
            journal TEXT,
            word_path TEXT,
            content_sha256 TEXT,
            byte_size INTEGER,
            status TEXT NOT NULL DEFAULT 'active',
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            last_changed_at TEXT,
            last_fetched_at TEXT,
            dvid TEXT,
            amendment_fp TEXT,
            amendment_count INTEGER,
            latest_amendment_date TEXT
        );

        CREATE TABLE IF NOT EXISTS sync_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ran_at TEXT NOT NULL,
            law_id TEXT,
            action TEXT NOT NULL,
            detail TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_laws_status ON laws(status);
        CREATE INDEX IF NOT EXISTS idx_events_ran ON sync_events(ran_at);
        """
    )
    existing = {row[1] for row in conn.execute("PRAGMA table_info(laws)")}
    for column, typedef in (
        ("dvid", "TEXT"),
        ("amendment_fp", "TEXT"),
        ("amendment_count", "INTEGER"),
        ("latest_amendment_date", "TEXT"),
    ):
        if column not in existing:
            conn.execute(f"ALTER TABLE laws ADD COLUMN {column} {typedef}")
    return conn


def session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def request_with_retry(s: requests.Session, method: str, url: str, **kwargs: Any) -> requests.Response:
    last: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = s.request(method, url, timeout=kwargs.pop("timeout", 60), **kwargs)
            resp.raise_for_status()
            return resp
        except Exception as exc:  # noqa: BLE001 — boundary retry
            last = exc
            time.sleep(2 * attempt)
            kwargs.setdefault("timeout", 60)
    raise RuntimeError(f"{method} {url} failed: {last}")


def fetch_list_page(s: requests.Session, page: int) -> str:
    data = {
        "filtercategorytypeid": "27",
        "filterrelationtypeid": "",
        "filtertopicid": "",
        "filtercityid": "",
        "filterstartdate": "",
        "filterenddate": "",
        "filterdepartmentid": "",
        "useg": "",
        "code": "1",
        "filtersubtypeid": "",
        "title": "",
        "enforcementdate": "",
        "enacteddate": "",
        "filteractive": "1",
        "page": str(page),
        "isactive": "1",
        "sort": "title",
        "sortType": "asc",
        "filteractnumber": "",
        "filterregisternumber": "",
    }
    resp = request_with_retry(
        s,
        "POST",
        LIST_URL,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"},
        timeout=40,
    )
    payload = resp.json()
    return payload.get("Html") or ""


def parse_list_html(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    laws: list[dict[str, str]] = []
    for row in soup.select("div.legal-list-component"):
        anchor = row.select_one("a.act-name")
        if not anchor or not anchor.get("href"):
            continue
        match = re.search(r"lawId=([^&\"']+)", anchor["href"])
        if not match:
            continue
        law_id = match.group(1)
        title = normalize_ws(anchor.get_text(" ", strip=True))
        enacted = ""
        enforcement = ""
        dates = row.select(
            "div[data-block='enacteddate'] span, div[data-block='enforcementdate'] span"
        )
        if dates:
            enacted = normalize_ws(dates[0].get_text())
        if len(dates) > 1:
            enforcement = normalize_ws(dates[1].get_text())
        journal_el = row.select_one("span[style*='italic']")
        journal = normalize_ws(journal_el.get_text()) if journal_el else ""
        laws.append(
            {
                "law_id": law_id,
                "title": title,
                "detail_url": f"{DETAIL_URL}?lawId={law_id}",
                "word_url": f"{WORD_URL}?fileid={law_id}",
                "enacted_date": enacted,
                "enforcement_date": enforcement,
                "journal": journal,
            }
        )
    return laws


def fetch_all_active(s: requests.Session) -> list[dict[str, str]]:
    collected: list[dict[str, str]] = []
    seen: set[str] = set()
    empty = 0
    for page in range(1, 80):
        html = fetch_list_page(s, page)
        batch = parse_list_html(html)
        print(f"  жагсаалт {page}: {len(batch)}", flush=True)
        if not batch:
            empty += 1
            if empty >= 2:
                break
        else:
            empty = 0
            for item in batch:
                if item["law_id"] not in seen:
                    seen.add(item["law_id"])
                    collected.append(item)
        time.sleep(PAGE_DELAY)
    kept = [item for item in collected if not is_repeal(item["title"])]
    print(
        f"Нийт идэвхтэй: {len(collected)}, хүчингүй болгосон нэртэйг хассан: {len(collected) - len(kept)}, үлдсэн: {len(kept)}",
        flush=True,
    )
    return kept


def safe_stem(law_id: str, title: str) -> str:
    slug = re.sub(r"[\\/*?:\"<>|\r\n]+", " ", title)
    slug = normalize_ws(slug)[:80]
    return f"{law_id}_{slug}"


def replace_same_law(directory: Path, law_id: str, keep: Path) -> None:
    """Тухайн хавтсанд зөвхөн энэ хуулийн хуучин нэртэй файлыг солино. Нөгөө хавтаст хүрэхгүй."""
    for old in directory.glob(f"{law_id}_*"):
        if old != keep:
            old.unlink(missing_ok=True)


def html_to_law_text(html: str, title: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "head"]):
        tag.decompose()
    supers = str.maketrans("0123456789", "⁰¹²³⁴⁵⁶⁷⁸⁹")
    for sup in soup.find_all("sup"):
        sup.replace_with(sup.get_text().translate(supers))
    for br in soup.find_all("br"):
        br.replace_with("\n")
    blocks = [b for b in soup.select("div.w-100") if b.find_parent("div", class_="w-100") is None]
    lines: list[str] = []
    if blocks:
        for block in blocks:
            line = normalize_ws(block.get_text("", strip=True))
            line = re.sub(r"([⁰¹²³⁴⁵⁶⁷⁸⁹])(?=[А-Яа-яӨөҮүЁё])", r"\1 ", line)
            if line:
                lines.append(line)
    else:
        for raw in soup.get_text("\n").splitlines():
            line = normalize_ws(raw)
            if not line:
                if lines and lines[-1] != "":
                    lines.append("")
                continue
            lines.append(line)
    while lines and lines[0] == "":
        lines.pop(0)
    while lines and lines[-1] == "":
        lines.pop()
    body = "\n".join(lines)
    if title and not body.startswith(title):
        body = f"{title}\n\n{body}"
    return body + "\n"


def download_word(s: requests.Session, law: dict[str, str]) -> tuple[Path, str, int]:
    # Сайт текстийг Word HTML экспортоор өгдөг. Үүнээс зүйл, заалттай .txt гаргана.
    resp = request_with_retry(
        s,
        "POST",
        WORD_URL,
        data={
            "fileid": law["law_id"],
            "orientation": "portrait",
            "size": "a4",
            "top": "1cm",
            "left": "1.5cm",
            "bottom": "1cm",
            "right": "0.5cm",
            "width": "",
            "height": "",
            "fontfamily": "Arial, Helvetica, sans-serif",
        },
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Referer": law["detail_url"],
        },
        timeout=90,
    )
    html = resp.content.decode("utf-8", "replace")
    if len(html) < 1500 or not re.search(r"[А-Яа-яӨөҮүЁё]", html):
        raise RuntimeError(f"Хуулийн текст хоосон байна: lawId={law['law_id']} ({len(html)} bytes)")
    text = html_to_law_text(html, law["title"])
    if not re.search(r"[А-Яа-яӨөҮүЁё]", text) or len(text) < 200:
        raise RuntimeError(f"Текст хэт богино: lawId={law['law_id']}")
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    stem = safe_stem(law["law_id"], law["title"])
    WORD_DIR.mkdir(parents=True, exist_ok=True)
    TEXT_DIR.mkdir(parents=True, exist_ok=True)
    word_path = WORD_DIR / f"{stem}.doc"
    text_path = TEXT_DIR / f"{stem}.txt"
    replace_same_law(WORD_DIR, law["law_id"], word_path)
    replace_same_law(TEXT_DIR, law["law_id"], text_path)
    word_path.write_bytes(html.encode("utf-8"))
    text_path.write_text(text, encoding="utf-8")
    return text_path, digest, len(text.encode("utf-8"))


def fetch_dvid(s: requests.Session, law_id: str) -> str:
    resp = request_with_retry(s, "GET", f"{DETAIL_URL}?lawId={law_id}", timeout=60)
    match = re.search(rf'data-unid="(\d+)"[^>]*data-lawid="{re.escape(law_id)}"', resp.text)
    if not match:
        match = re.search(r'data-unid="(\d+)"', resp.text)
    if not match:
        raise RuntimeError(f"Нэмэлт өөрчлөлтийн dvid олдсонгүй: {law_id}")
    return match.group(1)


def amendment_fingerprint(s: requests.Session, law_id: str, dvid: str) -> tuple[str, int, str]:
    """«Нэмэлт өөрчлөлт» табын батлагдсан огноонууд. Буцаах: хэш, тоо, хамгийн сүүлийн огноо."""
    resp = request_with_retry(
        s,
        "POST",
        CONNECTED_URL,
        data={"dvid": dvid, "type": "2", "lawId": law_id, "annexId": ""},
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Referer": f"{DETAIL_URL}?lawId={law_id}",
        },
        timeout=40,
    )
    html = resp.json().get("Html") or ""
    soup = BeautifulSoup(html, "html.parser")
    rows: list[str] = []
    dates: list[str] = []
    for block in soup.select("div[data-id]"):
        amend_id = block.get("data-id") or ""
        if not amend_id or amend_id == law_id:
            continue
        anchor = block.select_one("a")
        title = normalize_ws(anchor.get_text(" ", strip=True) if anchor else "")
        enacted = ""
        for span in block.select("span"):
            text = normalize_ws(span.get_text())
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
                enacted = text
                break
        rows.append(f"{amend_id}|{enacted}|{title}")
        if enacted:
            dates.append(enacted)
    rows = sorted(set(rows))
    digest = hashlib.sha256("\n".join(rows).encode("utf-8")).hexdigest()
    latest = max(dates) if dates else ""
    return digest, len(rows), latest


def log_event(conn: sqlite3.Connection, ran_at: str, action: str, law_id: str | None, detail: str) -> None:
    conn.execute(
        "INSERT INTO sync_events (ran_at, law_id, action, detail) VALUES (?, ?, ?, ?)",
        (ran_at, law_id, action, detail),
    )


def metadata_changed(row: sqlite3.Row, law: dict[str, str]) -> bool:
    return (
        row["title"] != law["title"]
        or (row["enacted_date"] or "") != law["enacted_date"]
        or (row["enforcement_date"] or "") != law["enforcement_date"]
        or (row["journal"] or "") != law["journal"]
    )


def upsert_fetched(
    conn: sqlite3.Connection,
    law: dict[str, str],
    path: Path,
    digest: str,
    size: int,
    now: str,
    first_seen: str,
) -> None:
    conn.execute(
        """
        INSERT INTO laws (
            law_id, title, detail_url, word_url, enacted_date, enforcement_date, journal,
            word_path, content_sha256, byte_size, status,
            first_seen_at, last_seen_at, last_changed_at, last_fetched_at,
            dvid, amendment_fp, amendment_count, latest_amendment_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(law_id) DO UPDATE SET
            title=excluded.title,
            detail_url=excluded.detail_url,
            word_url=excluded.word_url,
            enacted_date=excluded.enacted_date,
            enforcement_date=excluded.enforcement_date,
            journal=excluded.journal,
            word_path=excluded.word_path,
            content_sha256=excluded.content_sha256,
            byte_size=excluded.byte_size,
            status='active',
            last_seen_at=excluded.last_seen_at,
            last_changed_at=excluded.last_changed_at,
            last_fetched_at=excluded.last_fetched_at,
            dvid=excluded.dvid,
            amendment_fp=excluded.amendment_fp,
            amendment_count=excluded.amendment_count,
            latest_amendment_date=excluded.latest_amendment_date
        """,
        (
            law["law_id"],
            law["title"],
            law["detail_url"],
            law["word_url"],
            law["enacted_date"],
            law["enforcement_date"],
            law["journal"],
            str(path.relative_to(ROOT)),
            digest,
            size,
            first_seen,
            now,
            now,
            now,
            law.get("dvid"),
            law.get("amendment_fp"),
            law.get("amendment_count"),
            law.get("latest_amendment_date"),
        ),
    )


def touch_seen(conn: sqlite3.Connection, law: dict[str, str], now: str) -> None:
    conn.execute(
        """
        UPDATE laws SET
            title=?, detail_url=?, word_url=?, enacted_date=?, enforcement_date=?, journal=?,
            status='active', last_seen_at=?,
            dvid=COALESCE(?, dvid),
            amendment_fp=COALESCE(?, amendment_fp),
            amendment_count=COALESCE(?, amendment_count),
            latest_amendment_date=COALESCE(?, latest_amendment_date)
        WHERE law_id=?
        """,
        (
            law["title"],
            law["detail_url"],
            law["word_url"],
            law["enacted_date"],
            law["enforcement_date"],
            law["journal"],
            now,
            law.get("dvid"),
            law.get("amendment_fp"),
            law.get("amendment_count"),
            law.get("latest_amendment_date"),
            law["law_id"],
        ),
    )


def sync(limit: int, metadata_only: bool, force: bool) -> dict[str, int]:
    now = utc_now()
    conn = connect()
    http = session()
    remote = fetch_all_active(http)
    if limit > 0:
        remote = remote[:limit]
        print(f"Туршилт: зөвхөн эхний {limit}", flush=True)

    stats = {"new": 0, "updated": 0, "unchanged": 0, "missing": 0, "failed": 0}
    remote_ids = {item["law_id"] for item in remote}

    for index, law in enumerate(remote, 1):
        row = conn.execute("SELECT * FROM laws WHERE law_id=?", (law["law_id"],)).fetchone()
        text_ready = False
        if row is not None and row["word_path"] and str(row["word_path"]).endswith(".txt"):
            text_ready = (ROOT / row["word_path"]).is_file()
        has_file = bool(row and row["content_sha256"] and text_ready)
        meta_dirty = row is not None and metadata_changed(row, law)
        amend_changed = False
        checked_fp = False

        if has_file and not force and not metadata_only:
            try:
                dvid = row["dvid"] or fetch_dvid(http, law["law_id"])
                fp, count, latest = amendment_fingerprint(http, law["law_id"], dvid)
                law["dvid"] = dvid
                law["amendment_fp"] = fp
                law["amendment_count"] = count
                law["latest_amendment_date"] = latest
                checked_fp = True
                stored = row["latest_amendment_date"] or ""
                if row["amendment_fp"]:
                    # Шинэ батлагдсан огноо гарсан, эсвэл таб дээр мөр нэмэгдсэн.
                    amend_changed = latest > stored or row["amendment_fp"] != fp
                    if amend_changed:
                        print(
                            f"[{index}/{len(remote)}] нэмэлт өөрчлөлт: {stored or '—'} → {latest or '—'}",
                            flush=True,
                        )
                time.sleep(CHECK_DELAY)
            except Exception as exc:  # noqa: BLE001
                print(f"[{index}/{len(remote)}] нэмэлт өөрчлөлтийн огноо уншигдсангүй, алгаслаа: {exc}", flush=True)

        should_download = (
            force
            or not has_file
            or meta_dirty
            or amend_changed
        )

        if not should_download:
            touch_seen(conn, law, now)
            if checked_fp and not row["amendment_fp"]:
                log_event(conn, now, "baseline", law["law_id"], law.get("amendment_fp", "")[:12])
            stats["unchanged"] += 1
            conn.commit()
            continue

        print(f"[{index}/{len(remote)}] татаж байна: {law['title'][:70]}", flush=True)
        try:
            if not law.get("dvid"):
                try:
                    law["dvid"] = fetch_dvid(http, law["law_id"])
                    fp, count, latest = amendment_fingerprint(http, law["law_id"], law["dvid"])
                    law["amendment_fp"] = fp
                    law["amendment_count"] = count
                    law["latest_amendment_date"] = latest
                except Exception as exc:  # noqa: BLE001
                    print(f"    нэмэлтийн жагсаалт аваагүй: {exc}", flush=True)
            path, digest, size = download_word(http, law)
        except Exception as exc:  # noqa: BLE001
            stats["failed"] += 1
            log_event(conn, now, "failed", law["law_id"], str(exc))
            conn.commit()
            print(f"    алдаа: {exc}", flush=True)
            time.sleep(FILE_DELAY)
            continue

        content_changed = row is None or row["content_sha256"] != digest
        if row is not None and not content_changed and not meta_dirty:
            touch_seen(conn, law, now)
            stats["unchanged"] += 1
            log_event(conn, now, "unchanged", law["law_id"], digest[:12])
        else:
            first_seen = row["first_seen_at"] if row else now
            final_action = "new" if row is None else "updated"
            upsert_fetched(conn, law, path, digest, size, now, first_seen)
            stats[final_action] += 1
            log_event(
                conn,
                now,
                final_action,
                law["law_id"],
                json.dumps({"title": law["title"], "sha256": digest, "bytes": size}, ensure_ascii=False),
            )
        conn.commit()
        time.sleep(FILE_DELAY)

    if limit == 0 and remote_ids:
        conn.execute("CREATE TEMP TABLE seen_ids (law_id TEXT PRIMARY KEY)")
        conn.executemany("INSERT INTO seen_ids (law_id) VALUES (?)", [(i,) for i in remote_ids])
        missing = conn.execute(
            """
            SELECT laws.law_id, laws.title FROM laws
            WHERE laws.status='active'
              AND NOT EXISTS (SELECT 1 FROM seen_ids WHERE seen_ids.law_id = laws.law_id)
            """
        ).fetchall()
        for row in missing:
            conn.execute(
                "UPDATE laws SET status='inactive', last_seen_at=? WHERE law_id=?",
                (now, row["law_id"]),
            )
            log_event(conn, now, "missing", row["law_id"], row["title"])
            stats["missing"] += 1
        conn.commit()

    summary = {
        "ran_at": now,
        "remote": len(remote),
        **stats,
        "db": str(DB_PATH),
        "files": str(TEXT_DIR),
        "word_files": str(WORD_DIR),
    }
    STATE_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    log_event(conn, now, "summary", None, json.dumps(stats, ensure_ascii=False))
    conn.commit()
    conn.close()
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="legalinfo.mn хуулийн санг шинэчлэх")
    parser.add_argument("--limit", type=int, default=0, help="Зөвхөн N хууль (туршилт)")
    parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Нэмэлт өөрчлөлтийн табыг алгасаж, зөвхөн шинэ хууль болон нэр/огноо өөрчлөгдсөнийг татана.",
    )
    parser.add_argument("--force", action="store_true", help="Бүх идэвхтэй хуулийг дахин татах")
    args = parser.parse_args()
    sync(limit=args.limit, metadata_only=args.metadata_only, force=args.force)


if __name__ == "__main__":
    main()
