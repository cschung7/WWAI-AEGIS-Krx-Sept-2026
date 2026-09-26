"""AEGIS KRX prospective record: a read-only daily observer of the production outputs.

Design: WWAI/WWAI-ETF-FABLESS/docs/aegis_recheck/PROSPECTIVE_RECORD_DESIGN.md.

Per KRX trading day it appends one hash-chained entry to ledger.jsonl with:
  A. the AEGIS state produced by the production run (basket, regime, w_final, signals, the theme map it used,
     its theme source and age, code hashes, and revisions of previously logged w_final values);
  B. an independent full Naver theme -> ticker membership snapshot (stock.naver.com API);
  G1. the KIND KOSPI/KOSDAQ listed-company roster and recent delistings;
  S. the v3.8-candidate shadow run (outputs/KRX_v38_shadow), logged next to production.
Files go to blobs/<sha256> (content-addressed, written once, read-only). Nothing here writes to AEGIS.

    uv run --no-project --with pandas==3.0.6 --with pyarrow python record.py --slot evening|morning [--no-git]
    uv run --no-project --with pandas==3.0.6 --with pyarrow python record.py --verify
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
LEDGER = HERE / "ledger.jsonl"
BLOBS = HERE / "blobs"
RAW = HERE / "raw_local"                      # gitignored: gzipped raw API responses kept on the NAS only
KST = timezone(timedelta(hours=9))
UA = {"User-Agent": "Mozilla/5.0"}
SCHEMA = "aegis-krx-record-v1"


def nas_root() -> Path:
    for base in (Path("/mnt/nas"), Path("/mnt/nas-8bay")):
        if (base / "WWAI/WWAI-AEGIS/WWAI-AEGIS-KRX/outputs/KRX").is_dir():
            return base
    raise SystemExit("AEGIS KRX outputs not found under /mnt/nas or /mnt/nas-8bay")


NAS = nas_root()
AEGIS = NAS / "WWAI/WWAI-AEGIS/WWAI-AEGIS-KRX"
OUT = AEGIS / "outputs/KRX"
THEME_SOURCE = NAS / "AutoGluon/AutoML_Krx/DB/naver_theme_tickers.json"
CODE_FILES = [f"wwai_regime_engine/{f}" for f in (
    "backtest_v37_entropy.py", "exposure_sigmoid.py", "backtest.py", "backtest_fullstack.py", "backtest_v6_statedep.py",
    "backtest_v7_convexity.py", "signals.py", "graph_metrics.py", "main.py", "io_themes.py", "classifier.py", "config.py",
    "paths.py")] + ["AEGIS-index/run_daily.sh"]


# ── helpers ────────────────────────────────────────────────────────────────


def sha_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def canonical(obj) -> bytes:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def put_blob(data: bytes) -> str:
    h = sha_bytes(data)
    p = BLOBS / h
    if not p.exists():
        BLOBS.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_bytes(data)
        tmp.replace(p)
        p.chmod(0o444)
    return h


def fetch(url: str, data: dict | None = None, timeout: int = 30, tries: int = 4) -> bytes:
    body = urllib.parse.urlencode(data).encode() if data else None
    for i in range(tries):
        try:
            return urllib.request.urlopen(urllib.request.Request(url, body, UA), timeout=timeout).read()
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(3 * (i + 1))
    raise AssertionError


def keep_raw(day: str, name: str, data: bytes) -> None:
    d = RAW / day
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{name}.gz").write_bytes(gzip.compress(data))


def read_ledger() -> list[dict]:
    if not LEDGER.exists():
        return []
    return [json.loads(line) for line in LEDGER.read_text(encoding="utf-8").splitlines() if line.strip()]


def append_entry(entry: dict, prev: list[dict]) -> dict:
    entry["prev_sha256"] = prev[-1]["entry_sha256"] if prev else None
    entry["entry_sha256"] = sha_bytes(canonical({k: v for k, v in entry.items() if k != "entry_sha256"}))
    with LEDGER.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    return entry


def verify() -> int:
    prev = None
    bad = 0
    for n, e in enumerate(read_ledger(), 1):
        want = sha_bytes(canonical({k: v for k, v in e.items() if k != "entry_sha256"}))
        if e["entry_sha256"] != want or e["prev_sha256"] != prev:
            print(f"entry {n}: chain broken")
            bad += 1
        for h in blob_refs(e):
            p = BLOBS / h
            if not p.exists() or sha_bytes(p.read_bytes()) != h:
                print(f"entry {n}: blob {h[:12]} missing or altered")
                bad += 1
        prev = e["entry_sha256"]
    print("ledger OK" if not bad else f"{bad} problems")
    return 1 if bad else 0


def blob_refs(obj) -> list[str]:
    out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.endswith("_blob") and isinstance(v, str):
                out.append(v)
            else:
                out += blob_refs(v)
    elif isinstance(obj, list):
        for v in obj:
            out += blob_refs(v)
    return out


# ── trading calendar ───────────────────────────────────────────────────────


def trading_days(today: date) -> list[str]:
    """KRX sessions from KODEX 200 (069500) daily bars on Naver, last ~60 days, up to today."""
    start = (today - timedelta(days=60)).strftime("%Y%m%d")
    raw = fetch(f"https://api.finance.naver.com/siseJson.naver?symbol=069500&requestType=1&startTime={start}&endTime={today:%Y%m%d}&timeframe=day")
    days = sorted(set(re.findall(r'\["(\d{8})"', raw.decode("utf-8", "ignore"))))
    if not days:
        raise RuntimeError("empty trading calendar")
    return [f"{d[:4]}-{d[4:6]}-{d[6:]}" for d in days]


# ── A. AEGIS state ─────────────────────────────────────────────────────────


def aegis_state(prev_entries: list[dict], sessions: list[str]) -> dict:
    import pandas as pd

    st: dict = {"missing": []}
    hold_p = OUT / "aegis_holdings.json"
    try:
        hb = hold_p.read_bytes()
        h = json.loads(hb)
        st["holdings_blob"] = put_blob(hb)
        st["computed_at"] = h.get("computed_at")
        st["basket_date"] = h.get("date")
        st["data_dates"] = h.get("data_dates")
        st["regime"] = h.get("regime")
        st["equity_exposure_pct"] = h.get("equity_exposure_pct")
        st["strategy_weights"] = h.get("strategy_weights")
        st["basket"] = [{"ticker": x.get("ticker"), "weight": x.get("weight"), "sector": x.get("sector")} for x in h.get("holdings", [])]
    except Exception as e:
        st["missing"].append(f"aegis_holdings.json: {type(e).__name__}")

    def last_rows(name: str, n: int = 3) -> None:
        p = OUT / name
        try:
            b = p.read_bytes()
            df = pd.read_parquet(p)
            st[name.replace(".parquet", "") + "_file_sha256"] = sha_bytes(b)
            st[name.replace(".parquet", "") + "_tail"] = json.loads(df.tail(n).to_json(orient="index", date_format="iso", default_handler=str))
        except Exception as e:
            st["missing"].append(f"{name}: {type(e).__name__}")

    for name in ("regime_pred_fixed.parquet", "signals.parquet"):
        last_rows(name)

    # w_final: the latest value and revisions of values logged on earlier days
    try:
        p = OUT / "backtest_v37_exposure.parquet"
        st["exposure_file_sha256"] = sha_bytes(p.read_bytes())
        ex = pd.read_parquet(p)
        ex.index = pd.to_datetime(ex.index).strftime("%Y-%m-%d")
        last = ex.iloc[-1]
        st["w_final"] = {"date": ex.index[-1], "w_final": float(last["w_final"]), "regime": str(last.get("regime")),
                         "w_ml": float(last["w_ml"]), "w_size": float(last["w_size"]), "z_score": float(last["z_score"]),
                         "entropy": None if pd.isna(last["entropy"]) else float(last["entropy"])}
        logged = {}
        for e in prev_entries:
            w = (e.get("aegis") or {}).get("w_final")
            if w and w.get("date"):
                logged[w["date"]] = w["w_final"]
        rev = [{"date": d, "logged": v, "now": float(ex.loc[d, "w_final"])} for d, v in sorted(logged.items())
               if d in ex.index and abs(float(ex.loc[d, "w_final"]) - v) > 1e-9]
        gone = [d for d in sorted(logged) if d not in ex.index]
        st["revisions"] = {"checked": len(logged), "changed": len(rev), "max_abs_change": max((abs(r["now"] - r["logged"]) for r in rev), default=0.0),
                           "changed_dates": rev[-20:], "dates_no_longer_present": gone}
    except Exception as e:
        st["missing"].append(f"backtest_v37_exposure.parquet: {type(e).__name__}")

    try:
        st["portfolio_history_blob"] = put_blob((OUT / "portfolio_history.parquet").read_bytes())
    except Exception as e:
        st["missing"].append(f"portfolio_history.parquet: {type(e).__name__}")
    try:
        st["theme_map_used_blob"] = put_blob((OUT / "theme_to_tickers.json").read_bytes())
    except Exception as e:
        st["missing"].append(f"theme_to_tickers.json: {type(e).__name__}")
    try:
        st["theme_source_blob"] = put_blob(THEME_SOURCE.read_bytes())
        mt = datetime.fromtimestamp(THEME_SOURCE.stat().st_mtime, KST)
        st["theme_source_mtime"] = mt.isoformat(timespec="seconds")
        st["theme_source_age_days"] = (datetime.now(KST) - mt).days
    except Exception as e:
        st["missing"].append(f"theme source: {type(e).__name__}")
    st["code_hashes"] = {}
    for f in CODE_FILES:
        try:
            st["code_hashes"][f] = sha_bytes((AEGIS / f).read_bytes())
        except Exception:
            st["code_hashes"][f] = None
    return st


def status_of(st: dict, day: str, sessions: list[str], prev_entries: list[dict]) -> tuple[str, list[str], int | None]:
    flags = []
    prev_code = next((e["aegis"].get("code_hashes") for e in reversed(prev_entries) if e.get("aegis", {}).get("code_hashes")), None)
    if prev_code and st.get("code_hashes") != prev_code:
        flags.append("CODE_CHANGED")
    if (st.get("theme_source_age_days") or 0) > 7:
        flags.append("THEME_SOURCE_STALE")
    if st.get("revisions", {}).get("changed"):
        flags.append("HISTORY_REVISED")
    if not st.get("computed_at") or st["missing"]:
        return "MISSING", flags, None
    computed = st["computed_at"][:10]
    price = (st.get("data_dates") or {}).get("price")
    if computed < day:
        return "STALE", flags, None
    lag = None
    if price in sessions:
        lag = len([s for s in sessions if price < s <= day])
    elif price:
        lag = len([s for s in sessions if s > price and s <= day])
    return ("FRESH" if lag == 0 else "LAGGED"), flags, lag


# ── S. v3.8-candidate shadow (decision 2026-09-26: corrections approved, not promoted) ──


def shadow_state() -> dict:
    import pandas as pd

    d = OUT.parent / "KRX_v38_shadow"
    st: dict = {"version": "v3.8-candidate", "status": "SHADOW_NOT_VALIDATED", "missing": []}
    try:
        hb = (d / "aegis_holdings_v38_candidate.json").read_bytes()
        h = json.loads(hb)
        st["holdings_blob"] = put_blob(hb)
        st.update({"computed_at": h.get("computed_at"), "basket_date": h.get("date"), "regime": h.get("regime"),
                   "equity_exposure_pct": h.get("equity_exposure_pct"),
                   "basket": [{"ticker": x.get("ticker"), "weight": x.get("weight")} for x in h.get("holdings", [])]})
    except Exception as e:
        st["missing"].append(f"holdings: {type(e).__name__}")
    try:
        ex = pd.read_parquet(d / "backtest_v38_exposure.parquet")
        ex.index = pd.to_datetime(ex.index).strftime("%Y-%m-%d")
        st["w_final"] = {"date": ex.index[-1], "w_final": float(ex["w_final"].iloc[-1])}
    except Exception as e:
        st["missing"].append(f"exposure: {type(e).__name__}")
    try:
        st["code_sha256"] = sha_bytes((AEGIS / "wwai_regime_engine/backtest_v38_candidate.py").read_bytes())
    except Exception as e:
        st["missing"].append(f"code: {type(e).__name__}")
    return st


# ── B. Naver theme membership ──────────────────────────────────────────────


def naver_membership(day: str) -> dict:
    api = "https://stock.naver.com/api/domestic/market/theme"
    themes, page = [], 0
    while True:                                   # startIdx is a page index
        b = fetch(f"{api}/list?startIdx={page}&pageSize=200&sortType=changeRate")
        keep_raw(day, f"theme_list_{page}", b)
        rows = json.loads(b)
        if not rows:
            break
        themes += rows
        page += 1
        if page > 20:
            raise RuntimeError("theme list did not terminate")
    by_no = {}
    for t in themes:
        by_no[str(t["no"])] = t
    members, problems = {}, []
    for no, t in sorted(by_no.items(), key=lambda kv: int(kv[0])):
        got, page = [], 0
        while True:
            b = fetch(f"{api}/{no}/stocklist?marketType=ALL&orderType=marketSum&startIdx={page}&pageSize=100")
            keep_raw(day, f"theme_{no}_{page}", b)
            rows = json.loads(b)
            if not rows:
                break
            got += rows
            if len(rows) < 100:
                break
            page += 1
        codes = sorted({r["itemcode"] for r in got})
        names = {r["itemcode"]: r.get("itemname") for r in got}
        members[no] = {"name": t["name"], "tickers": [{"ticker": c, "name": names[c]} for c in codes]}
        if str(len(codes)) != str(t.get("totalCnt")):
            problems.append({"no": no, "listed_total": t.get("totalCnt"), "fetched": len(codes)})
        time.sleep(0.1)
    doc = {"source": f"{api}/list + /<no>/stocklist (marketType=ALL)", "themes": members}
    return {"membership_blob": put_blob(canonical(doc)), "n_themes": len(members),
            "n_memberships": sum(len(v["tickers"]) for v in members.values()),
            "n_unique_tickers": len({x["ticker"] for v in members.values() for x in v["tickers"]}),
            "count_mismatches": problems, "list_snapshot_time": max((t.get("thistime") or "" for t in themes), default=None)}


# ── G1. KIND roster ────────────────────────────────────────────────────────


def kind_roster(day: str, today: date) -> dict:
    rows = []
    for mkt, label in (("stockMkt", "KOSPI"), ("kosdaqMkt", "KOSDAQ")):
        raw = fetch(f"https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType={mkt}")
        keep_raw(day, f"kind_corplist_{label}", raw)
        for tr in re.findall(r"<tr>(.*?)</tr>", raw.decode("euc-kr", "ignore"), re.S)[1:]:
            c = [re.sub(r"\s+", " ", re.sub("<[^>]+>", "", x)).strip() for x in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
            rows.append({"market": label, "code": c[2], "name": c[0], "listed": c[5], "industry": c[3]})
    rows.sort(key=lambda r: (r["market"], r["code"]))
    if len(rows) < 2000:
        raise RuntimeError(f"roster too short: {len(rows)}")
    raw = fetch("https://kind.krx.co.kr/investwarn/delcompany.do", {"method": "searchDelCompanySub", "currentPageSize": 500, "pageIndex": 1,
                                                                   "orderMode": 1, "orderStat": "D", "marketType": "", "searchCorpName": "",
                                                                   "fromDate": (today - timedelta(days=30)).isoformat(), "toDate": today.isoformat()})
    keep_raw(day, "kind_delisted_30d", raw)
    dl = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", raw.decode("utf-8", "ignore"), re.S):
        m = re.search(r"alt='([^']+)'.*?companysummary_open\('(\w+)'\).*?title='([^']*)'.*?<td[^>]*>(\d{4}-\d\d-\d\d)</td>\s*<td[^>]*>(.*?)</td>", tr, re.S)
        if m:
            mk, short, name, d, reason = m.groups()
            dl.append({"market": mk, "short_code": short, "name": name.strip(), "delisted": d, "reason": re.sub(r"\s+", " ", reason).strip()})
    return {"roster_blob": put_blob(canonical(rows)), "n_listed": len(rows), "delisted_30d_blob": put_blob(canonical(dl)), "n_delisted_30d": len(dl)}


# ── git ────────────────────────────────────────────────────────────────────


def git_commit_push(msg: str) -> str:
    def run(*a):
        return subprocess.run(["git", *a], cwd=HERE, capture_output=True, text=True, timeout=300)
    run("add", "ledger.jsonl", "blobs")
    c = run("commit", "-q", "-m", msg)
    if c.returncode not in (0, 1):
        return f"commit failed: {c.stderr.strip()[:200]}"
    if not run("remote").stdout.strip():
        return "committed; no remote configured"
    p = run("push", "-q", "origin", "HEAD")
    return "pushed" if p.returncode == 0 else f"push failed: {p.stderr.strip()[:200]}"


# ── main ───────────────────────────────────────────────────────────────────


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slot", choices=["evening", "morning"])
    ap.add_argument("--no-git", action="store_true")
    ap.add_argument("--verify", action="store_true")
    a = ap.parse_args()
    if a.verify:
        return verify()
    if not a.slot:
        ap.error("--slot is required")
    now = datetime.now(KST)
    today = now.date()
    prev = read_ledger()
    sessions = trading_days(today)
    # the evening run covers today's session; the morning run covers yesterday's evening
    target = today if a.slot == "evening" else today - timedelta(days=1)
    covered = [s for s in sessions if s <= target.isoformat()]
    day = covered[-1] if covered else None
    same = [e for e in prev if e.get("session") == day]
    if day is None or (a.slot == "evening" and day != today.isoformat()):
        print(f"{now:%F %T} {a.slot}: {target} is not a KRX session; nothing to record")
        return 0
    if same and a.slot == "evening":
        print(f"{now:%F %T} evening: {day} already recorded")
        return 0
    if a.slot == "morning":
        if not same:
            pass                                   # the evening run did not happen: record now
        elif same[-1]["status"] in ("FRESH", "LAGGED") and same[-1].get("membership_B", {}).get("ok") and same[-1].get("roster_G1", {}).get("ok"):
            print(f"{now:%F %T} morning: {day} complete, nothing to catch up")
            return 0

    entry: dict = {"schema": SCHEMA, "session": day, "slot": a.slot, "recorded_at": now.isoformat(timespec="seconds"),
                   "host": os.uname().nodename, "recorder_sha256": sha_bytes(Path(__file__).read_bytes()),
                   "supersedes": same[-1]["entry_sha256"] if same else None}
    entry["aegis"] = aegis_state(prev, sessions)
    entry["status"], entry["flags"], entry["price_lag_sessions"] = status_of(entry["aegis"], day, sessions, prev)
    entry["shadow_v38"] = shadow_state()
    if a.slot == "morning" and same and entry["aegis"].get("computed_at") == same[-1]["aegis"].get("computed_at") \
            and same[-1].get("membership_B", {}).get("ok") and same[-1].get("roster_G1", {}).get("ok"):
        print(f"{now:%F %T} morning: AEGIS output unchanged since the evening entry; nothing new")
        return 0
    for key, fn in (("membership_B", lambda: naver_membership(day)), ("roster_G1", lambda: kind_roster(day, today))):
        try:
            entry[key] = {"ok": True, **fn()}
        except Exception as e:
            entry[key] = {"ok": False, "error": f"{type(e).__name__}: {str(e)[:200]}"}
    append_entry(entry, prev)
    print(f"{now:%F %T} {a.slot} {day}: {entry['status']} {entry['flags']} lag={entry['price_lag_sessions']} "
          f"B={'ok' if entry['membership_B']['ok'] else 'FAIL'} G1={'ok' if entry['roster_G1']['ok'] else 'FAIL'}", flush=True)
    rc = 0 if entry["membership_B"]["ok"] and entry["roster_G1"]["ok"] and entry["status"] != "MISSING" else 2
    if not a.no_git:
        g = git_commit_push(f"record {day} {a.slot}: {entry['status']}")
        print(f"git: {g}", flush=True)
        if not g.startswith(("pushed", "committed")):
            rc = 3
    return rc


if __name__ == "__main__":
    sys.exit(main())
