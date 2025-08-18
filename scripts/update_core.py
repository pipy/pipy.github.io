# -*- coding: utf-8 -*-
"""
Core logic to convert a JPX *_Short_Positions.xls into a normalized JSON.
This version is import-safe (no side effects at import time).

Output JSON format (list of objects):
[
  {
    "calc_date": "2025-08-18",
    "code": "1234",
    "name": "株式会社サンプル",
    "reading": "かぶしきがいしゃさんぷる",  # same as name if pykakasi not applied yet
    "seller": "ABC CAPITAL",
    "ratio": 1.23,    # percent (1.23 = 1.23%)
    "position": 10000 # integer shares
  },
  ...
]

Dependencies: pandas, xlrd (for legacy .xls)
"""
from __future__ import annotations

import json
import os
import re
from typing import Optional, List
import pandas as pd


def pick_col(columns, keywords: List[str], exclude: Optional[List[str]] = None):
    exclude = exclude or []
    for c in columns:
        s = str(c)
        if any(k in s for k in keywords) and not any(x in s for x in exclude):
            return c
    return None


def _parse_calc_date_cell(x) -> pd.Timestamp | pd.NaT:
    """Robust date parsing for Excel serials / strings / timestamps / Japanese format."""
    import datetime as _dt
    if isinstance(x, (pd.Timestamp, _dt.datetime, _dt.date)):
        try:
            return pd.Timestamp(x).normalize()
        except Exception:
            return pd.NaT
    # Try as Excel serial number
    try:
        xv = float(x)
        # Excel serial day origin 1899-12-30 for pandas
        return (pd.Timestamp("1899-12-30") + pd.to_timedelta(int(xv), unit="D")).normalize()
    except Exception:
        pass
    # Try general string parsing
    try:
        return pd.to_datetime(x, errors="coerce").normalize()
    except Exception:
        return pd.NaT


def _normalize_code(x) -> str:
    try:
        s = str(int(float(str(x).strip())))
    except Exception:
        s = str(x).strip()
    # zero-pad typical JP code (4 digits) if purely numeric and short
    if re.fullmatch(r"\d{1,5}", s):
        if len(s) < 4:
            s = s.zfill(4)
    return s


def _normalize_ratio(raw_val) -> float:
    """Return percent value. Accepts '1.23%', '1.23', '0.0123' (treated as 1.23%)."""
    s = str(raw_val).strip()
    if not s:
        return 0.0
    has_pct = "%" in s
    s = s.replace("%", "").replace(",", "")
    try:
        v = float(s)
    except Exception:
        return 0.0
    if has_pct:
        return v
    # if looks like fraction (<= 1 and not 0), treat as 0-1 => percent
    if 0 < v <= 1:
        return v * 100
    return v


def _normalize_int(raw_val) -> int:
    s = str(raw_val).strip().replace(",", "")
    try:
        return int(float(s))
    except Exception:
        return 0


def read_xls_normalized(xls_path: str) -> pd.DataFrame:
    """Read all sheets and return normalized DataFrame with expected columns."""
    # Read all sheets
    sheets = pd.read_excel(xls_path, sheet_name=None, engine="xlrd")
    frames = []
    for name, df in sheets.items():
        if df is None or df.empty:
            continue
        # drop entirely empty columns/rows
        df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
        if df.empty:
            continue

        # Detect columns by header names (Japanese/English)
        col_date   = pick_col(df.columns, ["計算", "基準", "Calculation", "Date"])
        col_code   = pick_col(df.columns, ["銘柄コード", "コード", "Security Code"])
        col_name   = pick_col(df.columns, ["銘柄名", "Issuer", "Name"])
        col_seller = pick_col(df.columns, ["商号", "名称", "氏名", "Name of Short Seller"], exclude=["委託者"])
        col_ratio  = pick_col(df.columns, ["空売り残高割合", "Ratio of Short Positions", "残高割合"])
        col_pos    = pick_col(df.columns, ["空売り残高数量", "Number of Short Positions", "残高数量"])

        need = dict(date=col_date, code=col_code, name=col_name, seller=col_seller, ratio=col_ratio, position=col_pos)
        miss = [k for k, v in need.items() if v is None]
        if miss:
            # skip this sheet if it doesn't contain expected headers
            continue

        use = df[[col_date, col_code, col_name, col_seller, col_ratio, col_pos]].copy()

        # Normalize fields
        use["calc_date"] = use[col_date].map(_parse_calc_date_cell)
        use["code"]      = use[col_code].map(_normalize_code)
        use["name"]      = use[col_name].astype(str).str.strip()
        use["seller"]    = use[col_seller].astype(str).str.strip()

        # ratio to percent
        raw_ratio = use[col_ratio].astype(str).str.strip()
        use["ratio"] = raw_ratio.map(_normalize_ratio)

        # position to int
        use["position"] = use[col_pos].map(_normalize_int)

        # Drop rows with no date or no seller
        use = use.dropna(subset=["calc_date"])
        use = use[use["seller"] != ""]

        frames.append(use[["calc_date", "code", "name", "seller", "ratio", "position"]])

    if not frames:
        raise RuntimeError("Excel から必要な列を抽出できませんでした。見出し名の変更により失敗した可能性があります。")

    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values(["calc_date", "code", "seller"]).reset_index(drop=True)
    return out


def update_with_xls(prev_json_path: str, xls_path: str, out_json_path: str) -> None:
    """
    Convert the JPX xls file into JSON. For simplicity we overwrite the data with the latest XLS content.
    The caller is responsible for detecting diffs and moving the file.
    """
    df = read_xls_normalized(xls_path)

    # Build JSON list
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "calc_date": str(pd.Timestamp(r["calc_date"]).date()),
            "code": str(r["code"]),
            "name": str(r["name"]),
            "reading": str(r["name"]),  # normalized to hiragana later by the caller
            "seller": str(r["seller"]),
            "ratio": float(r["ratio"]) if pd.notna(r["ratio"]) else 0.0,
            "position": int(r["position"]) if pd.notna(r["position"]) else 0,
        })

    with open(out_json_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


# If you want to test locally, run:
if __name__ == "__main__":
    # Example:
    # update_with_xls("./latest_shorts.json", "./downloads/20250818_Short_Positions.xls", "./_latest_shorts.tmp.json")
    pass
