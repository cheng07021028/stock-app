# -*- coding: utf-8 -*-
"""VNext Phase 6 市場領漲回補 / 漏選檢討引擎。

設計目的：
- 用 2026/06/12 漲停與強勢股檢討結果，抽象成可重複使用的「主流領漲相似」規則。
- 不連網、不讀寫 JSON、不覆蓋推薦紀錄；只根據既有掃描 DataFrame 欄位補分與分區。
- 避免把低流動性假強股升成買進清單；只把主流題材 / 高成交額 / 高族群同步股補回飆股雷達。
"""
from __future__ import annotations

from typing import Any
import re
import pandas as pd

MARKET_LEADER_REPLAY_VERSION = "phase6_market_leader_replay_20260612"

MARKET_LEADER_REPLAY_COLUMNS = [
    "市場領漲相似分",
    "漲停族群相似度",
    "主流領漲回補分",
    "錯失飆股警示",
    "錯失原因診斷",
    "領漲回補角色",
    "領漲回補分區",
    "隔夜催化需求",
    "市場領漲檢討版本",
]

NUMERIC_MARKET_LEADER_REPLAY_COLUMNS = {
    "市場領漲相似分",
    "漲停族群相似度",
    "主流領漲回補分",
}

_BLANKS = {"", "nan", "none", "null", "nat", "--", "-", "<na>"}

# 2026/06/12 漲停與 7~10% 強勢名單高度集中在半導體/記憶體/PCB/被動元件/光電面板/綠能等。
# 這裡不硬塞個股名單，而是把「主流資金題材」抽成文字特徵，避免未來只背答案。
_LEADER_THEME_PATTERNS: dict[str, list[str]] = {
    "記憶體/半導體": ["記憶體", "DRAM", "NAND", "Flash", "半導體", "IC設計", "晶圓", "矽晶圓", "封測", "測試", "探針", "設備", "材料", "晶片"],
    "PCB/載板/電子零組件": ["PCB", "載板", "軟板", "銅箔", "電子零組件", "被動元件", "電容", "電阻", "連接器", "導線架"],
    "光電/面板/太陽能": ["光電", "面板", "太陽能", "綠能", "電池", "光學", "光通訊", "光纖", "光模組"],
    "設備/廠務/工程": ["設備", "廠務", "工程", "無塵室", "自動化", "半導體設備"],
    "電纜/材料輪動": ["電纜", "電器電纜", "塑化", "化工", "材料"],
}


def _blank(v: Any) -> bool:
    try:
        if v is None or pd.isna(v):
            return True
    except Exception:
        pass
    return str(v).strip().lower() in _BLANKS


def _num(df: pd.DataFrame, names: list[str], default: float = 0.0) -> pd.Series:
    out = pd.Series([float("nan")] * len(df), index=df.index, dtype="float64")
    for name in names:
        if name not in df.columns:
            continue
        s = pd.to_numeric(df[name], errors="coerce")
        mask = out.isna() & s.notna()
        if mask.any():
            out.loc[mask] = s.loc[mask]
    return out.fillna(default).astype(float)


def _text_blob(df: pd.DataFrame) -> pd.Series:
    cols = [
        "股票名稱", "類別", "產業", "族群名稱", "資金流熱門族群", "推薦型態", "機會型態", "型態名稱",
        "族群攻擊說明", "資金攻擊摘要", "攻擊候選原因", "飆股雷達原因", "市場火種狀態", "今日大盤結論",
    ]
    series = pd.Series([""] * len(df), index=df.index, dtype="object")
    for c in cols:
        if c in df.columns:
            series = series + "｜" + df[c].fillna("").astype(str)
    return series.str.lower()


def _theme_score(blob: pd.Series) -> tuple[pd.Series, pd.Series]:
    score = pd.Series([35.0] * len(blob), index=blob.index, dtype="float64")
    label = pd.Series([""] * len(blob), index=blob.index, dtype="object")
    for theme, words in _LEADER_THEME_PATTERNS.items():
        pat = "|".join(re.escape(w.lower()) for w in words)
        hit = blob.str.contains(pat, na=False, regex=True)
        score.loc[hit] = score.loc[hit].clip(lower=78)
        label.loc[hit & label.eq("")] = theme
        # 同時命中多族群，代表題材聯動，額外加分但上限控制。
        label.loc[hit & label.ne("") & ~label.str.contains(theme, na=False)] = label.loc[hit & label.ne("") & ~label.str.contains(theme, na=False)] + "+" + theme
    return score.clip(0, 100), label.replace("", "非06/12主流領漲題材")


def _amount_score(amount_m: pd.Series) -> pd.Series:
    amount_m = pd.to_numeric(amount_m, errors="coerce").fillna(0.0)
    s = pd.Series([38.0] * len(amount_m), index=amount_m.index, dtype="float64")
    s = s.mask(amount_m >= 100, 52)
    s = s.mask(amount_m >= 250, 63)
    s = s.mask(amount_m >= 500, 72)
    s = s.mask(amount_m >= 1000, 82)
    s = s.mask(amount_m >= 3000, 92)
    return s


def _momentum_score(ret5: pd.Series, ret20: pd.Series) -> pd.Series:
    r5 = pd.to_numeric(ret5, errors="coerce").fillna(0.0)
    r20 = pd.to_numeric(ret20, errors="coerce").fillna(0.0)
    # 06/12 強勢股很多是「週跌或剛反彈 + 題材重新點火」，不是只抓穩健回檔。
    s5 = pd.Series([55.0] * len(r5), index=r5.index, dtype="float64")
    s5 = s5.mask((r5 >= -6) & (r5 <= 3), 76)
    s5 = s5.mask((r5 > 3) & (r5 <= 12), 82)
    s5 = s5.mask((r5 > 12) & (r5 <= 22), 64)
    s5 = s5.mask(r5 > 22, 42)
    s5 = s5.mask(r5 < -12, 44)
    s20 = pd.Series([58.0] * len(r20), index=r20.index, dtype="float64")
    s20 = s20.mask((r20 >= -18) & (r20 <= 8), 78)
    s20 = s20.mask((r20 > 8) & (r20 <= 28), 72)
    s20 = s20.mask(r20 > 38, 45)
    return (s5 * 0.65 + s20 * 0.35).clip(0, 100)


def apply_market_leader_replay_engine(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame(columns=MARKET_LEADER_REPLAY_COLUMNS)
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if out.empty:
        for c in MARKET_LEADER_REPLAY_COLUMNS:
            if c not in out.columns:
                out[c] = pd.Series(dtype="float64" if c in NUMERIC_MARKET_LEADER_REPLAY_COLUMNS else "object")
        return out

    blob = _text_blob(out)
    theme, theme_label = _theme_score(blob)
    amount = _num(out, ["成交額百萬", "今日成交額百萬", "成交金額百萬", "20日均成交額百萬"], 0)
    volume = _num(out, ["人氣量能分", "量能啟動分", "量價動能分數", "主力點火分"], 50).clip(0, 100)
    sector = _num(out, ["族群攻擊強度", "族群輪動分", "族群資金流分數", "類股熱度分數"], 50).clip(0, 100)
    money = _num(out, ["資金攻擊有效分", "籌碼續航分", "法人攻擊分", "主力點火分"], 50).clip(0, 100)
    mainstream = _num(out, ["主流資金分", "族群流動性分數"], 50).clip(0, 100)
    radar = _num(out, ["爆發雷達分", "飆股攻擊分", "隔日大漲機率分"], 50).clip(0, 100)
    market_fire = _num(out, ["局部題材火種分", "飆股適合度", "今日可追強度"], 50).clip(0, 100)
    ret5 = _num(out, ["近5日漲幅%", "5日漲幅%", "RET5"], 0)
    ret20 = _num(out, ["近20日漲幅%", "20日漲幅%", "RET20"], 0)
    amount_s = _amount_score(amount)
    momentum_s = _momentum_score(ret5, ret20)

    limitup_theme_similarity = (
        theme * 0.42 + sector * 0.20 + volume * 0.14 + amount_s * 0.14 + market_fire * 0.10
    ).clip(0, 100)
    leader_similarity = (
        theme * 0.24
        + amount_s * 0.20
        + volume * 0.14
        + sector * 0.16
        + money * 0.10
        + mainstream * 0.08
        + momentum_s * 0.08
    ).clip(0, 100)
    replay = (
        leader_similarity * 0.46
        + limitup_theme_similarity * 0.26
        + radar * 0.16
        + market_fire * 0.12
    ).clip(0, 100)

    role: list[str] = []
    bucket: list[str] = []
    warn: list[str] = []
    diagnosis: list[str] = []
    catalyst: list[str] = []
    for idx in out.index:
        rp = float(replay.loc[idx]); th = float(theme.loc[idx]); am = float(amount.loc[idx]); sc = float(sector.loc[idx]); vl = float(volume.loc[idx]); mf = float(market_fire.loc[idx])
        theme_txt = str(theme_label.loc[idx])
        if rp >= 84 and th >= 76 and am >= 500 and sc >= 64:
            role_i = "L+｜領漲回補雷達"
            bucket_i = "領漲回補雷達"
            warn_i = "06/12型主流領漲結構；不得因RR/停損遠而提前刪除，需盤前/盤中重新掃描。"
        elif rp >= 76 and th >= 70 and am >= 200 and (sc >= 58 or vl >= 68):
            role_i = "L｜主流強勢回補"
            bucket_i = "領漲回補雷達"
            warn_i = "具主流題材與成交額，應補進飆股雷達觀察，不等同直接買進。"
        elif rp >= 66 and th >= 64:
            role_i = "T｜題材轉強追蹤"
            bucket_i = "題材轉強追蹤"
            warn_i = "題材可能轉強，但成交額/族群同步仍需確認。"
        else:
            role_i = "N｜非領漲回補"
            bucket_i = "非領漲回補"
            warn_i = "未符合06/12領漲股特徵。"
        role.append(role_i)
        bucket.append(bucket_i)
        warn.append(warn_i)
        diagnosis.append(f"題材={theme_txt}｜回補{rp:.1f}｜相似{float(leader_similarity.loc[idx]):.1f}｜族群{sc:.1f}｜量能{vl:.1f}｜成交額{am:.1f}百萬｜火種{mf:.1f}")
        if th >= 70:
            catalyst.append("盤前需重掃：隔夜美股/費半/美光/半導體設備與同族群漲停家數可能改變隔日飆股方向。")
        else:
            catalyst.append("一般盤前重掃即可；未命中06/12主流領漲題材。")

    out["市場領漲相似分"] = leader_similarity.round(1)
    out["漲停族群相似度"] = limitup_theme_similarity.round(1)
    out["主流領漲回補分"] = replay.round(1)
    out["錯失飆股警示"] = warn
    out["錯失原因診斷"] = diagnosis
    out["領漲回補角色"] = role
    out["領漲回補分區"] = bucket
    out["隔夜催化需求"] = catalyst
    out["市場領漲檢討版本"] = MARKET_LEADER_REPLAY_VERSION
    return out
