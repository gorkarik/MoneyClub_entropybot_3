"""Recording of large trades on Entropy: fills of one transaction add up,
small ones are dropped, rows reach the CSV."""
import csv
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb import flows  # noqa: E402


def _fill(h, px, sz, side="A", t=1_700_000_000_000):
    return {"coin": "io:ANTH", "px": str(px), "sz": str(sz), "side": side,
            "hash": h, "time": t, "users": ["0xbuyer", "0xseller"]}


def test_fills_of_one_hash_add_up_and_small_ones_drop():
    a = flows.Aggregator(min_usd=50_000)
    a.add(_fill("h1", 100.0, 300), 10.0)      # $30K
    a.add(_fill("h1", 101.0, 300), 10.2)      # +$30.3K -> $60.3K
    a.add(_fill("h2", 100.0, 10), 10.3)       # $1K, too small
    assert a.take_done(10.5) == []             # still collecting
    rows = a.take_done(12.0)
    assert len(rows) == 1 and rows[0]["hash"] == "h1"
    assert rows[0]["fills"] == 2 and abs(rows[0]["usd"] - 60_300) < 1e-6
    assert a.open == {}


def test_recorder_writes_csv(tmp_path):
    rec = flows.FlowRecorder(str(tmp_path), "lighter-rh", "ANTH", "ws://x",
                             "io:ANTH", 10_000)
    rec.agg.add(_fill("h1", 100.0, 200), 1.0)
    rec._write(rec.agg.take_done(5.0))
    f = tmp_path / "lighter-rh_ANTH_2023-11-14.csv"
    rows = list(csv.DictReader(open(f)))
    assert rows[0]["taker_side"] == "sell" and rows[0]["fills"] == "1"
    assert float(rows[0]["notional_usd"]) == 20_000.0
    assert rec.rows_written == 1
