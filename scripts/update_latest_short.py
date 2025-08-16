# scripts/update_latest_shorts.py
import os, re, json, shutil
from pathlib import Path
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

# ---- リポ直下を基準にする ----
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_JSON = BASE_DIR / "latest_shorts.json"
DOWNLOADS = BASE_DIR / "downloads"
DOWNLOADS.mkdir(exist_ok=True)

INDEX_URL = "https://www.jpx.co.jp/markets/public/short-selling/index.html"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; JPXShortDownloader/1.0)"}
TIMEOUT = 20

def find_latest_xls_on_index() -> tuple[str, str] | None:
    """JPXインデックスから一番新しい *_Short_Positions.xls を (date_str, url) で返す"""
    r = requests.get(INDEX_URL, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    latest = None  # (date_str, url)
    pat = re.compile(r"(\d{8})_Short_Positions\.xls$")
    for a in soup.find_all("a", href=True):
        m = pat.search(a["href"])
        if not m:
            continue
        ds = m.group(1)
        url = urljoin(INDEX_URL, a["href"])
        if (latest is None) or (ds > latest[0]):
            latest = (ds, url)
    return latest

def download(url: str, outdir: Path) -> Path:
    name = url.split("/")[-1]
    dest = outdir / name
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    dest.write_bytes(r.content)
    return dest

def to_hiragana_if_possible(text: str) -> str:
    try:
        import pykakasi
        kks = getattr(pykakasi, "kakasi", pykakasi.kakasi)()
        try:
            kks.setMode("J","H"); kks.setMode("K","H")
        except Exception:
            pass
        conv = getattr(kks, "get_converter", getattr(kks, "getConverter"))()
        return conv.do(text)
    except Exception:
        return text

def normalize_reading(json_path: Path):
    """stock.reading をひらがな化（pykakasi が無ければ何もしない）"""
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
    # 1) JPX インデックスから最新 .xls を特定
    found = find_latest_xls_on_index()
    if not found:
        print("JPX index: *_Short_Positions.xls が見つかりませんでした。終了します。")
        return
    date_str, url = found
    print(f"最新検出: {date_str} -> {url}")

    # 2) すでに latest_shorts.json 内に同日のデータが入っていればスキップ（軽い重複抑止）
    existing_text = DATA_JSON.read_text("utf-8") if DATA_JSON.exists() else ""
    # 軽く日時文字列の有無だけ見る（厳密な比較は生成後に git diff で担保）
    if date_str in existing_text:
        print(f"ヒント: {date_str} は JSON 内に既出の可能性があります（処理は継続）。")

    # 3) ダウンロード
    xls_path = download(url, DOWNLOADS)
    print(f"ダウンロード完了: {xls_path}")

    # 4) 変換＆マージ（あなたの高機能ロジックを呼ぶ）
    from scripts.update_core import update_with_xls

    tmp_json = BASE_DIR / "_latest_shorts.tmp.json"
    prev_json = DATA_JSON if DATA_JSON.exists() else DATA_JSON  # 初回も同じパスでOK
    update_with_xls(str(prev_json), str(xls_path), str(tmp_json))

    # 5) 差分がなければ終了 / あれば読み仮名正規化して本番ファイルへ
    new_text = tmp_json.read_text("utf-8")
    if new_text.strip() == existing_text.strip():
        print("内容に変化なし（コミット不要）")
        tmp_json.unlink(missing_ok=True)
        return

    # 読み仮名の正規化（pykakasi が入っていれば適用）
    shutil.move(str(tmp_json), str(DATA_JSON))
    normalize_reading(DATA_JSON)
    print(f"更新完了: {DATA_JSON}")

if __name__ == "__main__":
    main()
