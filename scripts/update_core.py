# -*- coding: utf-8 -*-
"""
日次更新スクリプト（翌日報告は銘柄別・複数日を shorts に別行で出力・2日分保持）
- 基準日＝銘柄ごとに当日XLS内の最小日
- 翌日報告 (is_advanced=True) は合計外、基準日 (False) と据え置きは合計に含める
- 同一機関でも日付ごとに shorts に1行（= 2日出ていれば2行）
- “翌日しかXLSに無い”機関は、前回JSONの dates から基準日を補完して2本揃える
- dates は前回＋今回で新しい順に最大2日保持
依存: pandas, xlrd
"""

import json, os, re, unicodedata
from collections import defaultdict
from typing import Dict, Tuple, List
import pandas as pd

# ====== 設定（環境に合わせて変更） ======
PREV_JSON = "/Users/masaki/shorts/latest_shorts.json"            # 前回JSON（latest_short.json でもOK）
CURR_XLS  = "/Users/masaki/shorts/20250815_Short_Positions.xls"  # 当日XLS
OUT_JSON  = "/Users/masaki/shorts/latest_short_update.json"      # 出力JSON

# 生成後に latest_shorts.json に置き換える場合 True
DO_REPLACE_AFTER = False
REPLACE_PATH = "/Users/masaki/shorts/latest_shorts.json"

# しきい値など
RATIO_THRESHOLD = 0.5   # % 未満で報告義務消失（タグ付けのみ。数量は潰さない）
KEEP_DATES_N    = 2     # dates は常に最大2日保持

# ====== ユーティリティ ======
def nfkc(s: str) -> str:
    s = unicodedata.normalize("NFKC", str(s))
    s = re.sub(r"[\r\n]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def find_header_row(path: str, max_rows: int = 25) -> int:
    df = pd.read_excel(path, header=None, engine="xlrd", nrows=max_rows)
    for i in range(min(max_rows, len(df))):
        row = " ".join(str(x) for x in df.iloc[i, :].tolist())
        if ("計算年月日" in row or "Date of Calculation" in row) and ("銘柄コード" in row or "Code of Stock" in row):
            return i
    return 6

def pick_col(columns, keywords, exclude=None):
    exclude = exclude or []
    for c in columns:
        s = str(c)
        if any(k in s for k in keywords) and not any(x in s for x in exclude):
            return c
    return None

def _parse_calc_date_cell(x):
    """Excelシリアル/日時文字列/Timestamp/和式を堅牢に処理"""
    import datetime as _dt
    if isinstance(x, (pd.Timestamp, _dt.datetime, _dt.date)):
        try: return pd.Timestamp(x)
        except Exception: return pd.NaT
    try:
        dt = pd.to_datetime(x, errors="coerce")
        if pd.notna(dt): return pd.Timestamp(dt)
    except Exception:
        pass
    s = str(x).strip()
    if re.fullmatch(r"\d+(?:\.0+)?", s):
        try: return pd.Timestamp("1899-12-30") + pd.to_timedelta(int(float(s)), unit="D")
        except Exception: pass
    s = s.replace("年","/").replace("月","/").replace("日","")
    s = s.replace("-", "/").replace(".", "/")
    m = re.search(r"(\d{4})/(\d{1,2})/(\d{1,2})", s)
    if m:
        y,mn,d = map(int, m.groups())
        try: return pd.Timestamp(year=y, month=mn, day=d)
        except Exception: return pd.NaT
    return pd.to_datetime(s, errors="coerce")

def load_curr_xls(path: str) -> pd.DataFrame:
    """
    当日 .xls を読み込み、標準化したDFを返す。
    columns: [calc_date, code, name_jp, seller, ratio(%), position(int)]
    """
    header = find_header_row(path)
    df = pd.read_excel(path, header=header, engine="xlrd")

    col_date   = pick_col(df.columns, ["計算年月日","Date of Calculation"], exclude=["直近"])
    col_code   = pick_col(df.columns, ["銘柄コード","Code of Stock"])
    col_name   = pick_col(df.columns, ["銘柄名","Name of Stock"])
    col_seller = pick_col(df.columns, ["商号・名称・氏名","Name of Short Seller"], exclude=["委託者"])
    col_ratio  = pick_col(df.columns, ["空売り残高割合","Ratio of Short Positions to Shares Outstanding","残高割合"], exclude=["直近"])
    col_pos    = pick_col(df.columns, ["空売り残高数量","Number of Short Positions in Shares","残高数量"], exclude=["直近"])

    need = dict(date=col_date, code=col_code, name=col_name, seller=col_seller, ratio=col_ratio, position=col_pos)
    miss = [k for k,v in need.items() if v is None]
    if miss:
        raise RuntimeError(f"必要列が見つかりません: {miss} in {os.path.basename(path)}\ncolumns={list(df.columns)}")

    use = df[[col_date, col_code, col_name, col_seller, col_ratio, col_pos]].copy()

    use["calc_date"] = use[col_date].map(_parse_calc_date_cell)
    if use["calc_date"].notna().sum() == 0:
        sample_vals = list(use[col_date].dropna().astype(str).head(8).values)
        raise RuntimeError(f"calc_dateの解釈に失敗（全てNaT）: サンプル={sample_vals}")

    use["code"] = use[col_code].astype(str).str.strip()
    name_series = use[col_name].astype(str).str.replace(r"\r\n|\r|\n", "\n", regex=True)
    use["name_jp"] = name_series.str.split("\n").str[0].map(nfkc)
    use["seller"] = use[col_seller].astype(str).map(nfkc)

    # ratio を “%” に正規化（0.0123 → 1.23）
    raw = use[col_ratio].astype(str).str.strip()
    has_pct = raw.str.contains("%")
    num = pd.to_numeric(raw.str.replace("%","",regex=False).str.replace(",","",regex=False), errors="coerce")
    ratio = num.where(has_pct, num.where((num.isna()) | (num > 1), num * 100))
    use["ratio"] = ratio

    use["position"] = pd.to_numeric(use[col_pos].astype(str).str.replace(",","",regex=False), errors="coerce").fillna(0).astype(int)

    use = use.dropna(subset=["calc_date"])
    use = use[use["seller"] != ""]
    return use[["calc_date","code","name_jp","seller","ratio","position"]]

# ====== 本体 ======
def update_with_xls(prev_json_path: str, curr_xls_path: str, out_json_path: str):
    # 前回JSONのパス解決（short / shorts どちらでも）
    path = prev_json_path
    if not os.path.exists(path):
        alt = prev_json_path.replace("latest_short.json", "latest_shorts.json")
        if os.path.exists(alt): path = alt
        else:
            alt2 = prev_json_path.replace("latest_shorts.json", "latest_short.json")
            if os.path.exists(alt2): path = alt2
    if not os.path.exists(path):
        raise FileNotFoundError(prev_json_path)

    with open(path, "r", encoding="utf-8") as f:
        prev_json = json.load(f)

    # 当日 XLS
    curr_df = load_curr_xls(curr_xls_path)

    # 銘柄別の「基準日」マップ（当日XLSに出ている最小日）
    base_ts_by_code = (
        curr_df.groupby("code")["calc_date"]
               .min()
               .apply(lambda x: pd.Timestamp(x).normalize())
               .to_dict()
    )
    base_date_str_by_code = {c: ts.strftime("%Y/%m/%d") for c, ts in base_ts_by_code.items()}

    # ---- 当日XLS: (code, seller, date) ごとに集計 ----
    cur_by_csd: Dict[Tuple[str, str, str], Dict] = {}  # (code, seller, date)-> dict(pos, ratio)
    name_by_code_curr: Dict[str, str] = {}
    for r in curr_df.itertuples(index=False):
        code = str(r.code)
        name_by_code_curr[code] = r.name_jp
        dstr = pd.Timestamp(r.calc_date).strftime("%Y/%m/%d")
        key = (code, r.seller, dstr)
        if key not in cur_by_csd:
            cur_by_csd[key] = {"position": 0, "ratio": None}
        cur_by_csd[key]["position"] += int(r.position)
        if pd.notna(r.ratio):
            if cur_by_csd[key]["ratio"] is None:
                cur_by_csd[key]["ratio"] = float(r.ratio)
            else:
                cur_by_csd[key]["ratio"] = (cur_by_csd[key]["ratio"] + float(r.ratio)) / 2.0

    # 当日XLSの日付別内訳（dates）
    dates_by_code_curr = defaultdict(lambda: defaultdict(lambda: {"date": None, "items": [], "total": 0}))
    g = curr_df.groupby(["code","calc_date","seller"], as_index=False).agg(position=("position","sum"))
    for r in g.itertuples(index=False):
        code = str(r.code)
        dts = pd.Timestamp(r.calc_date).normalize()
        dstr = dts.strftime("%Y/%m/%d")
        b = dates_by_code_curr[code][dstr]
        b["date"] = dstr
        b["items"].append({"name": r.seller, "position": int(r.position)})
        b["total"] += int(r.position)
    for code in dates_by_code_curr:
        for d in dates_by_code_curr[code]:
            dates_by_code_curr[code][d]["items"].sort(key=lambda x: x["position"], reverse=True)

    # ---- 前回JSONから前日残と dates を取得 ----
    prev_name_by_code, prev_reading_by_code = {}, {}
    prev_pos_by_cs, prev_reporting_lost = {}, set()
    prev_dates_by_code = defaultdict(dict)
    for stock in prev_json:
        code = str(stock.get("code"))
        prev_name_by_code[code] = stock.get("name") or code
        prev_reading_by_code[code] = stock.get("reading") or stock.get("name") or code
        for d in stock.get("shorts", []) or []:
            seller = nfkc(d.get("name",""))
            prev_pos_by_cs[(code, seller)] = int(d.get("position", 0))
            if d.get("reporting_lost"):
                prev_reporting_lost.add((code, seller))
        for d in stock.get("dates", []) or []:
            if not d or not d.get("date"): continue
            prev_dates_by_code[code][d["date"]] = {
                "date": d["date"],
                "items": list(d.get("items") or []),
                "total": int(d.get("total") or 0),
            }

    # ---- shorts を作成（(code,seller,date) ごと）----
    # まず、当日XLSに出ている (code, seller) について、その銘柄の全日付を昇順で並べる
    cs_dates_map = defaultdict(list)  # (code, seller)-> [date1<date2<...]
    for (code, seller, dstr) in cur_by_csd.keys():
        cs_dates_map[(code, seller)].append(dstr)
    for k in cs_dates_map:
        cs_dates_map[k] = sorted(cs_dates_map[k], key=lambda x: pd.to_datetime(x))

    # ★★★ 追加: “翌日だけ出た機関”に前日（基準日）行を補完して2本揃える
    for (code, seller), dates in list(cs_dates_map.items()):
        base_ts = base_ts_by_code.get(code)
        if base_ts is None:
            continue
        base_date = base_ts.strftime("%Y/%m/%d")
        if base_date not in dates:
            base_blk = (prev_dates_by_code.get(code) or {}).get(base_date)
            if not base_blk:
                continue
            pos0 = None
            for it in base_blk.get("items", []):
                # namesはNFKCで比較
                if nfkc(it.get("name","")) == seller:
                    pos0 = int(it.get("position", 0))
                    break
            if pos0 is None:
                continue
            # 補完 (code, seller, base_date)
            cur_by_csd[(code, seller, base_date)] = {"position": pos0, "ratio": None}
            cs_dates_map[(code, seller)].append(base_date)
            cs_dates_map[(code, seller)] = sorted(cs_dates_map[(code, seller)], key=lambda x: pd.to_datetime(x))
    # ★★★ 追加ここまで

    result_by_code: Dict[str, Dict] = {}
    def ensure_stock(code: str):
        return result_by_code.setdefault(code, {
            "code": code,
            "name": name_by_code_curr.get(code, prev_name_by_code.get(code, code)),
            "reading": prev_reading_by_code.get(code, name_by_code_curr.get(code, "")),
            "base_date": base_date_str_by_code.get(code),
            "shorts": [],
            "dates": [],
        })

    # 1) 当日（＋補完）に出ている (code, seller) を処理 —— 日付ごとに1行作る
    for (code, seller), dates in sorted(cs_dates_map.items()):
        base_ts = base_ts_by_code.get(code)
        prev_pos = prev_pos_by_cs.get((code, seller), 0)
        last_day_pos = None  # 翌日以降の change 計算用

        for dstr in dates:
            cur = cur_by_csd[(code, seller, dstr)]
            cur_pos = int(cur["position"])
            ratio = cur["ratio"]

            if last_day_pos is None:
                change = cur_pos - prev_pos           # 基準日(最初の日)
            else:
                change = cur_pos - last_day_pos       # 翌日以降

            # is_advanced（銘柄別基準日より後なら True）
            is_adv = False
            if base_ts is not None:
                try:
                    is_adv = pd.to_datetime(dstr).normalize() > base_ts
                except Exception:
                    is_adv = False

            status = "normal"
            reporting_lost = False
            if (ratio is not None) and (ratio < RATIO_THRESHOLD):
                status = "reporting_lost"
                reporting_lost = True

            stock = ensure_stock(code)
            stock["shorts"].append({
                "name": seller,
                "position": cur_pos,
                "change": int(change),
                "status": status,
                "reporting_lost": reporting_lost,
                "carried_forward": False,
                "calc_date": dstr,
                "is_advanced": is_adv
            })

            last_day_pos = cur_pos

    # 2) 据え置き：当日に1行も無い (code, seller) を追加
    all_cs_today = {(c,s) for (c,s,_) in cur_by_csd.keys()}
    for (code, seller), prev_pos in prev_pos_by_cs.items():
        if (code, seller) in all_cs_today:
            continue
        if (code, seller) in prev_reporting_lost:
            continue
        stock = ensure_stock(code)
        stock["shorts"].append({
            "name": seller,
            "position": int(prev_pos),
            "change": 0,
            "status": "carried_forward",
            "reporting_lost": False,
            "carried_forward": True,
            "calc_date": None,
            "is_advanced": False
        })

    # 並び：同銘柄内で position 降順・calc_date 昇順で安定化
    for code, s in result_by_code.items():
        s["shorts"].sort(key=lambda x: (-(x["position"]), x["calc_date"] or ""))

    # ---- dates を「前回＋今回」で最大2日分に保つ（銘柄ごと）----
    def is_adv_per_code(code: str, date_str: str) -> bool:
        base_ts = base_ts_by_code.get(code)
        if base_ts is None: return False
        return pd.to_datetime(date_str).normalize() > base_ts

    # 当日/前回どちらかに出た銘柄すべて
    codes_in_scope = set(result_by_code.keys()) | set(prev_dates_by_code.keys()) | set(dates_by_code_curr.keys())

    for code in sorted(codes_in_scope):
        merged = {}
        for d, blk in prev_dates_by_code.get(code, {}).items():
            merged[d] = {"date": d, "items": list(blk.get("items") or []), "total": int(blk.get("total") or 0)}
        for d, blk in dates_by_code_curr.get(code, {}).items():
            merged[d] = {"date": d, "items": list(blk["items"]), "total": int(blk["total"])}

        if not merged:
            continue

        ds_desc = sorted(merged.keys(), key=lambda x: pd.to_datetime(x), reverse=True)
        keep = sorted(ds_desc[:KEEP_DATES_N], key=lambda x: pd.to_datetime(x))

        kept_blocks = []
        for d in keep:
            kept_blocks.append({
                "date": d,
                "is_advanced": is_adv_per_code(code, d),
                "items": merged[d]["items"],
                "total": int(merged[d]["total"]),
            })

        if code not in result_by_code:
            result_by_code[code] = {
                "code": code,
                "name": name_by_code_curr.get(code, prev_name_by_code.get(code, code)),
                "reading": prev_reading_by_code.get(code, name_by_code_curr.get(code, "")),
                "base_date": base_date_str_by_code.get(code),
                "shorts": [],
                "dates": kept_blocks,
            }
        else:
            result_by_code[code]["base_date"] = base_date_str_by_code.get(code)
            result_by_code[code]["dates"] = kept_blocks

    # 出力
    result = [result_by_code[c] for c in sorted(result_by_code.keys())]
    os.makedirs(os.path.dirname(out_json_path), exist_ok=True)
    with open(out_json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"✅ 更新JSON: {out_json_path}  銘柄: {len(result)}")

    # 任意: latest_shorts.json に置き換え
    if DO_REPLACE_AFTER:
        try:
            if os.path.exists(REPLACE_PATH):
                os.remove(REPLACE_PATH)
                print(f"削除: {REPLACE_PATH}")
            os.rename(out_json_path, REPLACE_PATH)
            print(f"リネーム: {out_json_path} → {REPLACE_PATH}")
        except Exception as e:
            print(f"⚠️ 置き換えに失敗しました: {e}")

# ====== 実行 ======
if __name__ == "__main__":
    update_with_xls(PREV_JSON, CURR_XLS, OUT_JSON)

import os

# 元ファイルとリネーム先のパス
src = "/Users/masaki/shorts/latest_short_update.json"
dst = "/Users/masaki/shorts/latest_shorts.json"

# 既存の latest_shorts.json を削除
if os.path.exists(dst):
    os.remove(dst)
    print(f"削除: {dst}")

# src を dst にリネーム
os.rename(src, dst)
print(f"リネーム完了: {src} → {dst}")
