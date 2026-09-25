import json

import record as rec


def test_chain_and_verify(tmp_path, monkeypatch):
    monkeypatch.setattr(rec, "LEDGER", tmp_path / "ledger.jsonl")
    monkeypatch.setattr(rec, "BLOBS", tmp_path / "blobs")
    h = rec.put_blob(b"x")
    e1 = rec.append_entry({"session": "2026-09-25", "a": {"x_blob": h}}, [])
    e2 = rec.append_entry({"session": "2026-09-28"}, [e1])
    assert e2["prev_sha256"] == e1["entry_sha256"]
    assert rec.verify() == 0
    lines = rec.LEDGER.read_text().splitlines()
    bad = json.loads(lines[0]); bad["session"] = "2026-09-24"
    rec.LEDGER.write_text(json.dumps(bad) + "\n" + lines[1] + "\n")
    assert rec.verify() == 1


def test_blob_is_read_only_and_deduplicated(tmp_path, monkeypatch):
    monkeypatch.setattr(rec, "BLOBS", tmp_path / "blobs")
    a, b = rec.put_blob(b"same"), rec.put_blob(b"same")
    assert a == b and len(list((tmp_path / "blobs").iterdir())) == 1
    assert not ((tmp_path / "blobs" / a).stat().st_mode & 0o222)


def test_status():
    sessions = ["2026-09-22", "2026-09-23", "2026-09-24", "2026-09-25"]
    base = {"missing": [], "computed_at": "2026-09-25 20:12:26 KST", "data_dates": {"price": "2026-09-25"}, "code_hashes": {"a": "1"}}
    assert rec.status_of(base, "2026-09-25", sessions, [])[0::2] == ("FRESH", 0)
    lag = dict(base, data_dates={"price": "2026-09-23"})
    assert rec.status_of(lag, "2026-09-25", sessions, [])[0::2] == ("LAGGED", 2)
    stale = dict(base, computed_at="2026-09-24 20:12:00 KST")
    assert rec.status_of(stale, "2026-09-25", sessions, [])[0] == "STALE"
    miss = dict(base, missing=["x"])
    assert rec.status_of(miss, "2026-09-25", sessions, [])[0] == "MISSING"
    prev = [{"aegis": {"code_hashes": {"a": "0"}}}]
    assert "CODE_CHANGED" in rec.status_of(base, "2026-09-25", sessions, prev)[1]
    assert "THEME_SOURCE_STALE" in rec.status_of(dict(base, theme_source_age_days=50), "2026-09-25", sessions, [])[1]
