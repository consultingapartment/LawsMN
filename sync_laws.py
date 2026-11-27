#!/usr/bin/env python3
"""Монгол Улсын хүчин төгөлдөр хууль, тогтоол, тушаалыг legalinfo.mn-ээс татаж SQLite-д бүртгэнэ.

Ангилал (CATEGORIES): хууль (27), УИХ-ын тогтоол (28), Засгийн газрын тогтоол (33), Сайдын тушаал (34).

Сар бүр (эсвэл гараар) дахин ажиллуулахад:
- өмнө байгаагүй шинэ хуулийг татна
- «Нэмэлт өөрчлөлт» табын жагсаалт өөрчлөгдсөн хуулийг дахин татна
- нэр, батлагдсан/хүчин төгөлдөр огноо, эмхэтгэл өөрчлөгдсөн хуулийг дахин татна
- жагсаалтаас алга болсон хуулийг inactive гэж тэмдэглэнэ (файлыг устгахгүй)
- --rclone-root өгсөн бол локал репозиторт биш, Google Drive-ийн 4 дэд хавтас руу шууд хуулна

Жишээ:
  python sync_laws.py
  python sync_laws.py --limit 5
  python sync_laws.py --metadata-only
  python sync_laws.py --category 27,28
  python sync_laws.py --time-budget 300 --rclone-root gdrive:LawsMN
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import subprocess
import tempfile
import time
from dataclasses import dataclass
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
STATE_PATH = DATA_DIR / "last_sync.json"

REPEAL_PHRASE = "ХҮЧИНГҮЙ БОЛСОНД ТООЦОХ"
PAGE_DELAY = 0.6
FILE_DELAY = 1.2
CHECK_DELAY = 0.35
MAX_RETRIES = 3
MAX_LIST_PAGES = 400
UPLOAD_BATCH_SIZE = 50
# Идэвхтэй жагсаалт өмнөхөөсөө энэ хувиас их буурвал сайтын доголдол гэж үзэж inactive тэмдэглэхгүй.
MISSING_GUARD_RATIO = 0.9
NO_AMENDMENTS_FP = hashlib.sha256(b"").hexdigest()


@dataclass(frozen=True)
class Category:
    code: str
    name: str
    folder: str
    # Нэмэлт өөрчлөлтийн табыг хэдэн сард нэг удаа шалгах вэ (1 = сар бүр).
    # Олон мянган баримттай ангиллыг ээлжилж шалгаснаар сайтын ачааллыг бууруулна.
    amend_rotation: int

    @property
    def text_dir(self) -> Path:
        return ROOT / self.folder


# Жижгээс нь эхэлж боловсруулна — хамгийн том ангилал (33) сүүлд.
CATEGORIES: dict[str, Category] = {
    "27": Category("27", "Монгол Улсын хууль", "legal", 1),
    "28": Category("28", "Улсын Их Хурлын тогтоол", "uih_togtool", 3),
    "34": Category("34", "Сайдын тушаал", "said_tushaal", 3),
    "33": Category("33", "Засгийн газрын тогтоол", "zg_togtool", 3),
}
DEFAULT_CATEGORY = "27"

HEADERS = {
    "User-Agent": "MongolianLawSync/1.0 (public legal archive; monthly refresh)",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Origin": BASE,
    "Referer": f"{BASE}/mn/law?page=law&cate=27&active=1",
    "X-Requested-With": "XMLHttpRequest",
}


class RcloneRemote:
    """rclone ашиглан Google Drive-ийн LawsMN/<folder> дэд хавтсуудтай ажиллана."""

    def __init__(self, root: str) -> None:
        self.root = root.rstrip("/")

    def list_laws(self, folder: str) -> dict[str, list[str]]:
        """Дэд хавтсанд байгаа {law_id}_*.txt файлуудыг law_id-аар нь бүлэглэнэ."""
        remote_path = f"{self.root}/{folder}"
        last_err = ""
        for attempt in range(1, MAX_RETRIES + 1):
            proc = subprocess.run(
                [
                    "rclone",
                    "lsf",
                    remote_path,
                    "--files-only",
                    "--include",
                    "*.txt",
                    "--fast-list",
                ],
                capture_output=True,
                text=True,
            )
            if proc.returncode == 0:
                index: dict[str, list[str]] = {}
                for line in proc.stdout.splitlines():
                    name = line.strip()
                    if name.endswith(".txt") and "_" in name:
                        law_id = name.split("_", 1)[0]
                        index.setdefault(law_id, []).append(name)
                return index
            stderr = (proc.stderr or "").strip()
            low = stderr.lower()
            if proc.returncode == 3 or "directory not found" in low or "not found" in low:
                return {}
            last_err = stderr or proc.stdout.strip()
            if attempt < MAX_RETRIES:
                time.sleep(2 * attempt)
        raise RuntimeError(f"rclone lsf алдаа ({remote_path}): {last_err}")

    def upload_dir(self, local_dir: Path, folder: str) -> None:
        """Түр хавтсанд хураагдсан .txt файлуудыг Drive дэд хавтас руу хуулна."""
        if not local_dir.exists() or not any(local_dir.glob("*.txt")):
            return
        remote_path = f"{self.root}/{folder}"
        cmd = [
            "rclone",
            "copy",
            str(local_dir),
            remote_path,
            "--include",
            "*.txt",
            "--transfers",
            "4",
            "--checkers",
            "8",
            "--drive-chunk-size",
            "16M",
            "--stats",
            "1m",
            "--stats-one-line",
        ]
        last_err = ""
        for attempt in range(1, MAX_RETRIES + 1):
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode == 0:
                return
            last_err = (proc.stderr or proc.stdout).strip()
            if attempt < MAX_RETRIES:
                time.sleep(3 * attempt)
        raise RuntimeError(f"rclone copy алдаа ({remote_path}): {last_err}")

    def delete_file(self, folder: str, filename: str) -> None:
        """Нэр эсвэл ангилал нь солигдсон хуучин файлыг Drive-аас цэвэрлэнэ."""
        remote_file = f"{self.root}/{folder}/{filename}"
        proc = subprocess.run(
            ["rclone", "deletefile", remote_file],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            stderr = (proc.stderr or "").strip()
            if "not found" not in stderr.lower():
                print(f"    хуучин Drive файл устгасангүй ({remote_file}): {stderr}", flush=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def is_repeal(title: str) -> bool:
    return REPEAL_PHRASE in normalize_ws(title).upper()


def connect() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
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
            latest_amendment_date TEXT,
            category TEXT
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
        ("category", "TEXT"),
    ):
        if column not in existing:
            conn.execute(f"ALTER TABLE laws ADD COLUMN {column} {typedef}")
    # Олон ангилал нэмэгдэхээс өмнөх мөрүүд бүгд хууль (27) байсан.
    conn.execute("UPDATE laws SET category=? WHERE category IS NULL", (DEFAULT_CATEGORY,))
    conn.execute("CREATE INDEX IF NOT EXISTS idx_laws_category ON laws(category, status)")
    conn.commit()
    return conn


def session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def request_with_retry(s: requests.Session, method: str, url: str, **kwargs: Any) -> requests.Response:
    last: Exception | None = None
    timeout = kwargs.pop("timeout", 60)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = s.request(method, url, timeout=timeout, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.HTTPError as exc:
            last = exc
            status = exc.response.status_code if exc.response is not None else 0
            # 4xx (429-өөс бусад) дахин оролдоод нэмэргүй.
            if 400 <= status < 500 and status != 429:
                break
        except Exception as exc:  # noqa: BLE001 — boundary retry
            last = exc
        if attempt < MAX_RETRIES:
            time.sleep(2 * attempt)
    raise RuntimeError(f"{method} {url} failed: {last}")


def fetch_list_page(s: requests.Session, cate: str, page: int) -> str:
    data = {
        "filtercategorytypeid": cate,
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
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Referer": f"{BASE}/mn/law?page=law&cate={cate}&active=1",
        },
        timeout=40,
    )
    payload = resp.json()
    return payload.get("Html") or ""


def parse_list_html(html: str, cate: str = DEFAULT_CATEGORY) -> list[dict[str, str]]:
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
                "category": cate,
            }
        )
    return laws


def fetch_all_active(s: requests.Session, cate: str = DEFAULT_CATEGORY) -> list[dict[str, str]]:
    collected: list[dict[str, str]] = []
    seen: set[str] = set()
    empty = 0
    finished = False
    for page in range(1, MAX_LIST_PAGES + 1):
        html = fetch_list_page(s, cate, page)
        batch = parse_list_html(html, cate)
        if page == 1 or page % 25 == 0:
            print(f"  [{cate}] жагсаалт {page}: {len(batch)}", flush=True)
        if not batch:
            empty += 1
            if empty >= 2:
                finished = True
                break
        else:
            empty = 0
            fresh = [item for item in batch if item["law_id"] not in seen]
            if not fresh:
                # Сүүлийн хуудсыг давтан буцааж байвал жагсаалт дууссан гэж үзнэ.
                finished = True
                break
            for item in fresh:
                seen.add(item["law_id"])
                collected.append(item)
        time.sleep(PAGE_DELAY)
    if not finished:
        raise RuntimeError(
            f"[{cate}] жагсаалт {MAX_LIST_PAGES} хуудсанд дуусаагүй — дутуу жагсаалтаар inactive тэмдэглэхгүйн тулд зогсоолоо."
        )
    kept = [item for item in collected if not is_repeal(item["title"])]
    print(
        f"[{cate}] нийт идэвхтэй: {len(collected)}, хүчингүй болгосон нэртэйг хассан: {len(collected) - len(kept)}, үлдсэн: {len(kept)}",
        flush=True,
    )
    return kept


def find_law_file(directory: Path, law_id: str, suffix: str) -> Path | None:
    if not directory.exists():
        return None
    matches = sorted(directory.glob(f"{law_id}_*{suffix}"))
    return matches[0] if matches else None


def safe_stem(law_id: str, title: str) -> str:
    slug = re.sub(r"[\\/*?:\"<>|\r\n]+", " ", title)
    slug = normalize_ws(slug)[:80]
    return f"{law_id}_{slug}"


def replace_same_law(directory: Path, law_id: str, keep: Path) -> None:
    """Тухайн хавтсанд зөвхөн энэ хуулийн хуучин нэртэй файлыг солино. Нөгөө хавтаст хүрэхгүй."""
    for old in directory.glob(f"{law_id}_*"):
        if old != keep:
            old.unlink(missing_ok=True)


def write_txt_to_dir(directory: Path, law: dict[str, str], text: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    text_path = directory / f"{safe_stem(law['law_id'], law['title'])}.txt"
    replace_same_law(directory, law["law_id"], text_path)
    text_path.write_text(text, encoding="utf-8")
    return text_path


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


def download_text(s: requests.Session, law: dict[str, str]) -> tuple[str, str, int]:
    # Сайт текстийг Word HTML экспортоор өгдөг. Текстийг цэвэрлэж .txt болгоно.
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
    # Тогтоол, тушаал богино байж болох тул доод хязгаарыг гарчгаас арай урт гэж тавина.
    if not re.search(r"[А-Яа-яӨөҮүЁё]", text) or len(text) < len(law["title"]) + 60:
        raise RuntimeError(f"Текст хэт богино: lawId={law['law_id']}")
    raw = text.encode("utf-8")
    return text, hashlib.sha256(raw).hexdigest(), len(raw)


def fetch_dvid(s: requests.Session, law_id: str) -> str:
    """Нэмэлт өөрчлөлтийн табын dvid. Таб байхгүй (нэмэлт өөрчлөлт ороогүй) бол "" буцаана."""
    resp = request_with_retry(s, "GET", f"{DETAIL_URL}?lawId={law_id}", timeout=60)
    match = re.search(rf'data-unid="(\d+)"[^>]*data-lawid="{re.escape(law_id)}"', resp.text)
    if not match:
        match = re.search(r'data-unid="(\d+)"', resp.text)
    if not match:
        if law_id not in resp.text:
            raise RuntimeError(f"Хуулийн хуудас буруу ирлээ: {law_id}")
        return ""
    return match.group(1)


def amendment_fingerprint(s: requests.Session, law_id: str, dvid: str) -> tuple[str, int, str]:
    """«Нэмэлт өөрчлөлт» табын батлагдсан огноонууд. Буцаах: хэш, тоо, хамгийн сүүлийн огноо."""
    if not dvid:
        return NO_AMENDMENTS_FP, 0, ""
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
    rel_path: str,
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
            dvid, amendment_fp, amendment_count, latest_amendment_date, category
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            latest_amendment_date=excluded.latest_amendment_date,
            category=excluded.category
        """,
        (
            law["law_id"],
            law["title"],
            law["detail_url"],
            law["word_url"],
            law["enacted_date"],
            law["enforcement_date"],
            law["journal"],
            rel_path,
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
            law.get("category") or DEFAULT_CATEGORY,
        ),
    )


def touch_seen(
    conn: sqlite3.Connection,
    law: dict[str, str],
    now: str,
    word_path: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE laws SET
            title=?, detail_url=?, word_url=?, enacted_date=?, enforcement_date=?, journal=?,
            status='active', last_seen_at=?,
            word_path=COALESCE(?, word_path),
            dvid=COALESCE(?, dvid),
            amendment_fp=COALESCE(?, amendment_fp),
            amendment_count=COALESCE(?, amendment_count),
            latest_amendment_date=COALESCE(?, latest_amendment_date),
            category=COALESCE(?, category)
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
            word_path,
            law.get("dvid"),
            law.get("amendment_fp"),
            law.get("amendment_count"),
            law.get("latest_amendment_date"),
            law.get("category"),
            law["law_id"],
        ),
    )


def due_for_amendment_check(category: Category, law_id: str, now: datetime) -> bool:
    """Ээлжлэх ангилалд баримт бүрийг amend_rotation сард нэг удаа шалгана."""
    if category.amend_rotation <= 1:
        return True
    bucket = int(hashlib.sha1(law_id.encode("utf-8")).hexdigest(), 16) % category.amend_rotation
    month_index = now.year * 12 + now.month
    return bucket == month_index % category.amend_rotation


def mark_missing(
    conn: sqlite3.Connection,
    category: Category,
    remote_ids: set[str],
    now: str,
    stats: dict[str, int],
) -> bool:
    """Жагсаалтаас алга болсныг inactive болгоно. Огцом буурсан бол алгасаж True буцаана."""
    active_before = conn.execute(
        "SELECT COUNT(*) FROM laws WHERE status='active' AND category=?", (category.code,)
    ).fetchone()[0]
    if len(remote_ids) < MISSING_GUARD_RATIO * active_before:
        msg = (
            f"[{category.code}] идэвхтэй жагсаалт {active_before} → {len(remote_ids)} болж огцом буурлаа. "
            "Сайтын доголдол байж болзошгүй тул inactive тэмдэглэлийг алгаслаа."
        )
        print(msg, flush=True)
        log_event(conn, now, "missing_guard", None, msg)
        conn.commit()
        return True
    conn.execute("DROP TABLE IF EXISTS temp.seen_ids")
    conn.execute("CREATE TEMP TABLE seen_ids (law_id TEXT PRIMARY KEY)")
    conn.executemany("INSERT INTO seen_ids (law_id) VALUES (?)", [(i,) for i in remote_ids])
    missing = conn.execute(
        """
        SELECT laws.law_id, laws.title FROM laws
        WHERE laws.status='active' AND laws.category=?
          AND NOT EXISTS (SELECT 1 FROM seen_ids WHERE seen_ids.law_id = laws.law_id)
        """,
        (category.code,),
    ).fetchall()
    for row in missing:
        conn.execute("UPDATE laws SET status='inactive', last_seen_at=? WHERE law_id=?", (now, row["law_id"]))
        log_event(conn, now, "missing", row["law_id"], row["title"])
        stats["missing"] += 1
    conn.commit()
    return False


def new_stats() -> dict[str, int]:
    return {
        "new": 0,
        "updated": 0,
        "uploaded": 0,
        "unchanged": 0,
        "missing": 0,
        "failed": 0,
        "check_failed": 0,
        "skipped_check": 0,
    }


def sync(
    limit: int,
    metadata_only: bool,
    force: bool,
    categories: list[str] | None = None,
    time_budget_min: float = 0,
    rclone_root: str = "",
    keep_local: bool = False,
) -> dict[str, Any]:
    started = time.monotonic()
    deadline = started + time_budget_min * 60 if time_budget_min > 0 else None
    now_dt = datetime.now(timezone.utc)
    now = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    rclone = RcloneRemote(rclone_root) if rclone_root.strip() else None
    conn = connect()
    http = session()
    codes = categories or list(CATEGORIES)
    seen_global: set[str] = set()
    per_category: dict[str, dict[str, Any]] = {}
    totals = new_stats()
    partial = False

    if rclone is not None:
        print(f"Google Drive горим идэвхтэй: {rclone.root}/<folder> руу шууд синк хийнэ.", flush=True)

    with tempfile.TemporaryDirectory(prefix="laws_stage_") as tmp_dir:
        stage_root = Path(tmp_dir)

        for code in codes:
            category = CATEGORIES[code]
            stats = new_stats()
            per_category[code] = {"name": category.name, "folder": category.folder, **stats}
            if partial:
                per_category[code]["status"] = "not_started"
                continue
            print(f"\n=== [{code}] {category.name} ===", flush=True)

            remote_index: dict[str, list[str]] = {}
            stage_cat_dir = stage_root / category.folder
            staged_count = 0
            pending_remote_deletes: list[tuple[str, str]] = []

            def flush_batch() -> None:
                nonlocal staged_count
                if rclone is None:
                    conn.commit()
                    return
                if staged_count > 0:
                    print(
                        f"  [{code}] {staged_count} файлыг {rclone.root}/{category.folder} руу хуулж байна...",
                        flush=True,
                    )
                    try:
                        rclone.upload_dir(stage_cat_dir, category.folder)
                    except Exception:
                        conn.rollback()
                        raise
                    for del_folder, del_name in pending_remote_deletes:
                        rclone.delete_file(del_folder, del_name)
                    pending_remote_deletes.clear()
                    for staged_file in stage_cat_dir.glob("*.txt"):
                        staged_file.unlink(missing_ok=True)
                    stats["uploaded"] += staged_count
                    staged_count = 0
                conn.commit()

            if rclone is not None:
                remote_index = rclone.list_laws(category.folder)
                print(
                    f"  [{code}] {rclone.root}/{category.folder} хавтсанд байгаа файл: {len(remote_index)}",
                    flush=True,
                )

            remote = fetch_all_active(http, code)
            # Нэг баримт хоёр ангилалд давхар гарвал эхнийхийг нь үлдээнэ.
            dupes = [item for item in remote if item["law_id"] in seen_global]
            for item in dupes:
                log_event(conn, now, "duplicate", item["law_id"], f"{code}: {item['title'][:200]}")
            remote = [item for item in remote if item["law_id"] not in seen_global]
            remote_ids = {item["law_id"] for item in remote}
            seen_global |= remote_ids
            if limit > 0:
                remote = remote[:limit]
                print(f"Туршилт: зөвхөн эхний {limit}", flush=True)

            for index, law in enumerate(remote, 1):
                if deadline is not None and time.monotonic() > deadline:
                    partial = True
                    print(
                        f"[{code}] цагийн төсөв дууслаа ({index - 1}/{len(remote)}). Дараагийн удаа үргэлжилнэ.",
                        flush=True,
                    )
                    break
                tag = f"[{code} {index}/{len(remote)}]"
                row = conn.execute("SELECT * FROM laws WHERE law_id=?", (law["law_id"],)).fetchone()

                remote_files = remote_index.get(law["law_id"], []) if rclone is not None else []
                if rclone is not None:
                    has_file = bool(remote_files)
                else:
                    txt_path = find_law_file(category.text_dir, law["law_id"], ".txt")
                    has_file = txt_path is not None

                if not has_file and row is not None and row["category"] != code and row["word_path"]:
                    if rclone is None:
                        old = ROOT / row["word_path"]
                        if old.exists():
                            old.unlink()

                meta_dirty = row is not None and metadata_changed(row, law)
                amend_changed = False
                checked_fp = False
                if meta_dirty:
                    print(f"{tag} нэр/огноо өөрчлөгдсөн: {law['title'][:70]}", flush=True)

                want_check = has_file and not force and not metadata_only and not meta_dirty
                if want_check and not due_for_amendment_check(category, law["law_id"], now_dt):
                    want_check = False
                    stats["skipped_check"] += 1

                if want_check:
                    try:
                        dvid = (row["dvid"] if row else None) or fetch_dvid(http, law["law_id"])
                        fp, count, latest = amendment_fingerprint(http, law["law_id"], dvid)
                        law["dvid"] = dvid
                        law["amendment_fp"] = fp
                        law["amendment_count"] = count
                        law["latest_amendment_date"] = latest
                        checked_fp = True
                        stored = (row["latest_amendment_date"] if row else "") or ""
                        stored_fp = row["amendment_fp"] if row else None
                        if stored_fp:
                            amend_changed = latest > stored or stored_fp != fp
                        elif row is not None and count > 0:
                            # Өмнө нь тэмдэг хадгалаагүй баримтад одоо нэмэлт өөрчлөлт харагдаж байна.
                            amend_changed = True
                        if amend_changed:
                            print(f"{tag} нэмэлт өөрчлөлт: {stored or '—'} → {latest or '—'}", flush=True)
                        time.sleep(CHECK_DELAY)
                    except Exception as exc:  # noqa: BLE001
                        stats["check_failed"] += 1
                        log_event(conn, now, "check_failed", law["law_id"], str(exc)[:500])
                        print(f"{tag} нэмэлт өөрчлөлтийн огноо уншигдсангүй, алгаслаа: {exc}", flush=True)

                should_download = force or amend_changed or meta_dirty or not has_file

                if not should_download:
                    touch_seen(conn, law, now)
                    if checked_fp and row is not None and not row["amendment_fp"]:
                        log_event(conn, now, "baseline", law["law_id"], law.get("amendment_fp", "")[:12])
                    stats["unchanged"] += 1
                    if staged_count == 0:
                        conn.commit()
                    continue

                print(f"{tag} татаж байна: {law['title'][:70]}", flush=True)
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
                    text, digest, size = download_text(http, law)
                    if rclone is not None:
                        written = write_txt_to_dir(stage_cat_dir, law, text)
                        if keep_local:
                            write_txt_to_dir(category.text_dir, law, text)
                        rel_path = f"{category.folder}/{written.name}"
                        # Нэр нь өөрчлөгдсөн бол хуучин Drive файлыг upload амжилттай болсны дараа устгана.
                        for old_name in remote_files:
                            if old_name != written.name:
                                pending_remote_deletes.append((category.folder, old_name))
                        # Ангилал нь өөрчлөгдсөн бол хуучин ангиллын Drive файлыг устгах дараалалд хийнэ.
                        if row is not None and row["category"] != code and row["word_path"]:
                            old_rel = row["word_path"].replace("\\", "/")
                            if "/" in old_rel:
                                old_folder, old_name = old_rel.split("/", 1)
                                if (old_folder, old_name) != (category.folder, written.name):
                                    pending_remote_deletes.append((old_folder, old_name))
                        remote_index[law["law_id"]] = [written.name]
                        staged_count += 1
                    else:
                        written = write_txt_to_dir(category.text_dir, law, text)
                        rel_path = written.relative_to(ROOT).as_posix()
                except Exception as exc:  # noqa: BLE001
                    stats["failed"] += 1
                    log_event(conn, now, "failed", law["law_id"], str(exc)[:500])
                    if staged_count == 0:
                        conn.commit()
                    print(f"    алдаа: {exc}", flush=True)
                    time.sleep(FILE_DELAY)
                    continue

                content_changed = row is None or row["content_sha256"] != digest
                if row is not None and not content_changed and not meta_dirty:
                    touch_seen(conn, law, now, word_path=rel_path)
                    stats["unchanged"] += 1
                    log_event(conn, now, "unchanged", law["law_id"], digest[:12])
                else:
                    first_seen = row["first_seen_at"] if row else now
                    final_action = "new" if row is None else "updated"
                    upsert_fetched(conn, law, rel_path, digest, size, now, first_seen)
                    stats[final_action] += 1
                    log_event(
                        conn,
                        now,
                        final_action,
                        law["law_id"],
                        json.dumps(
                            {"category": code, "title": law["title"], "sha256": digest, "bytes": size},
                            ensure_ascii=False,
                        ),
                    )

                if rclone is None:
                    conn.commit()
                elif staged_count >= UPLOAD_BATCH_SIZE:
                    flush_batch()
                time.sleep(FILE_DELAY)

            # Үлдсэн багцыг Drive руу хуулж, DB-г баталгаажуулна.
            flush_batch()

            guard = False
            if limit == 0 and remote_ids and not partial:
                guard = mark_missing(conn, category, remote_ids, now, stats)
            per_category[code].update(stats)
            per_category[code]["remote"] = len(remote_ids)
            per_category[code]["missing_guard_skipped"] = guard
            per_category[code]["status"] = "partial" if partial else "done"
            for key, value in stats.items():
                totals[key] += value

    summary: dict[str, Any] = {
        "ran_at": now,
        "partial": partial,
        "rclone_root": rclone.root if rclone else "",
        "minutes": round((time.monotonic() - started) / 60, 1),
        **totals,
        "categories": per_category,
        "db": str(DB_PATH.relative_to(ROOT)),
    }
    STATE_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    log_event(conn, now, "summary", None, json.dumps({"partial": partial, **totals}, ensure_ascii=False))
    conn.commit()
    conn.close()
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="legalinfo.mn хуулийн санг шинэчлэх")
    parser.add_argument("--limit", type=int, default=0, help="Зөвхөн N хууль (туршилт)")
    parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Нэмэлт өөрчлөлтийн табыг алгасаж, зөвхөн шинэ хууль болон нэр/огноо өөрчлөгдсөнийг татана.",
    )
    parser.add_argument("--force", action="store_true", help="Бүх идэвхтэй баримтыг дахин татах")
    parser.add_argument(
        "--category",
        default="",
        help=f"Таслалаар тусгаарласан ангиллын код (анхдагч: бүгд — {','.join(CATEGORIES)})",
    )
    parser.add_argument(
        "--time-budget",
        type=float,
        default=0,
        help="Минут. Хэтэрвэл зогсоож partial гэж тэмдэглэнэ; дараагийн ажиллалт үргэлжлүүлнэ. 0 = хязгааргүй.",
    )
    parser.add_argument(
        "--rclone-root",
        default="",
        help="Google Drive rclone зам (жишээ нь: gdrive:LawsMN). Хоосон бол локал хавтсанд хадгална.",
    )
    parser.add_argument(
        "--keep-local",
        action="store_true",
        help="--rclone-root горимд ажиллахдаа локал .txt хуулбарыг мөн хадгалах.",
    )
    args = parser.parse_args()
    codes = [c.strip() for c in args.category.split(",") if c.strip()] or None
    if codes:
        unknown = [c for c in codes if c not in CATEGORIES]
        if unknown:
            parser.error(f"Тодорхойгүй ангилал: {', '.join(unknown)}")
    sync(
        limit=args.limit,
        metadata_only=args.metadata_only,
        force=args.force,
        categories=codes,
        time_budget_min=args.time_budget,
        rclone_root=args.rclone_root,
        keep_local=args.keep_local,
    )


if __name__ == "__main__":
    main()
