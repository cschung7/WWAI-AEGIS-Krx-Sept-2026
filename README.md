# WWAI-AEGIS-RECORD

A prospective, append-only record of what the AEGIS KRX production pipeline produced each KRX trading day. It is read-only towards AEGIS. The design is in `WWAI/WWAI-ETF-FABLESS/docs/aegis_recheck/PROSPECTIVE_RECORD_DESIGN.md`.

Each entry in `ledger.jsonl` records one session. Entries are hash-chained: `entry_sha256` is the sha256 of the canonical JSON of the entry without that field, with sorted keys, `ensure_ascii=False` and separators `(',', ':')`, and each entry holds the previous entry's hash in `prev_sha256`.

- `aegis`: the basket, regime, `w_final`, the signals tail, the theme map AEGIS used and its source's age, code hashes, and `revisions` (earlier logged `w_final` values that the recomputed history has since changed).
- `membership_B`: the full Naver theme → ticker membership, taken independently of AEGIS from the stock.naver.com API.
- `shadow_v38`: the v3.8-candidate shadow run (`outputs/KRX_v38_shadow`). Since 2026-09-26, v3.7 is sealed as `LEGACY_INVALIDATED_BY_POST_FREEZE_EVIDENCE`, and the corrections run in parallel only.
- `roster_G1`: the KIND KOSPI/KOSDAQ listed roster and delistings over the last 30 days.
- `status`: one of `FRESH`, `LAGGED`, `STALE`, `MISSING`. `flags`: any of `CODE_CHANGED`, `THEME_SOURCE_STALE`, `HISTORY_REVISED`.

The files an entry refers to are stored in `blobs/<sha256>`, content-addressed and read-only. Gzipped raw API responses stay on the NAS in `raw_local/` and are not in git.

**Rule.** Never edit or delete ledger lines or blobs. A correction is a new entry.

    uv run --no-project --with pandas==3.0.6 --with pyarrow python record.py --verify

Schedule on aorus2, user `chae`:
- **22:30 KST Mon–Fri:** the `evening` slot.
- **07:30 KST Tue–Sat:** the `morning` slot, which catches up the previous evening.
