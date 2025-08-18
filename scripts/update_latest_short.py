# scripts/update_latest_short.py
import os
import re
import json
import shutil
from pathlib import Path
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

# ---- Constants ----
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_JSON = BASE_DIR / "latest_shorts.json"
DOWNLOADS = BASE_DIR / "downloads"
DOWNLOADS.mkdir(exist_ok=True)

INDEX_URL = "https://www.jpx.co.jp/markets/public/short-selling/index.html"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; JPXShortDownloader/1.0)"}
TIMEOUT = 25


def find_latest_xls_on_index() -> tuple[str, str] | None:
    """
    Crawl the JPX short-selling index page and pick the latest *_Short_Positions.xls.
    Returns (YYYYMMDD, absolute_url) or None.
    """
    r = requests.get(INDEX_URL, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    latest = None  # (date_str, url)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.lower().endswith(".xls"):
            continue
        m = re.search(r"(\d{8})_Short_Positions\.xls$", href)
        if not m:
            continue
        date_str = m.group(1)
        abs_url = urljoin(INDEX_URL, href)
        if (latest is None) or (date_str > latest[0]):
            latest = (date_str, abs_url)
    return latest


def download(url: str, dest_dir: Path) -> Path:
    """Download a file into dest_dir and return the saved path."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    filename = url.split("/")[-1]
    path = dest_dir / filename
    with requests.get(url, headers=HEADERS, timeout=TIMEOUT, stream=True) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 15):
                if chunk:
                    f.write(chunk)
    return path


def to_hiragana_if_possible(s: str) -> str:
    """Convert to hiragana if pykakasi is available; otherwise, return original."""
    try:
        from pykakasi import kakasi  # type: ignore
    except Exception:
        return s
    kks = kakasi()
    kks.setMode("J", "H")  # Kanji -> Hiragana
    kks.setMode("K", "H")  # Katakana -> Hiragana
    conv = kks.getConverter()
    try:
        return conv.do(s)
    except Exception:
        return s


def normalize_reading(json_path: Path) -> None:
    """
    Ensure each item has a 'reading' in hiragana.
    If pykakasi is not installed, this becomes a no-op.
    """
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        changed = 0
        for item in data:
            base = (item.get("reading") or item.get("name") or "").strip()
            hira = to_hiragana_if_possible(base)
            if item.get("reading") != hira:
                item["reading"] = hira
                changed += 1
        if changed:
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"reading をひらがな化: {changed}件更新")
        else:
            print("reading 変更なし")
    except Exception as e:
        print(f"reading 正規化スキップ: {e}")


def main():
    # 1) Find latest .xls on JPX
    found = find_latest_xls_on_index()
    if not found:
        print("JPX index: *_Short_Positions.xls が見つかりませんでした。終了します。")
        return
    date_str, url = found
    print(f"最新検出: {date_str} -> {url}")

    # 3) Download
    xls_path = download(url, DOWNLOADS)
    print(f"ダウンロード完了: {xls_path}")

    # 4) Convert & merge using core logic
    from scripts.update_core import update_with_xls

    tmp_json = BASE_DIR / "_latest_shorts.tmp.json"
    prev_json = DATA_JSON if DATA_JSON.exists() else DATA_JSON  # same path OK for first run
    update_with_xls(str(prev_json), str(xls_path), str(tmp_json))

    # 5) If no diff, remove tmp and exit; else move to main and normalize 'reading'
    existing_text = ""
    if DATA_JSON.exists():
        existing_text = DATA_JSON.read_text("utf-8")

    new_text = tmp_json.read_text("utf-8")
    if new_text.strip() == existing_text.strip():
        print("内容に変化なし（コミット不要）")
        tmp_json.unlink(missing_ok=True)
        return

    shutil.move(str(tmp_json), str(DATA_JSON))
    normalize_reading(DATA_JSON)
    print(f"更新完了: {DATA_JSON}")


if __name__ == "__main__":
    main()
