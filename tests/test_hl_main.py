"""First leg on the main Hyperliquid exchange (BTC, ETH, SOL...) or on
Entropy (dex io), and "auto" choosing between them.

Run:  python3 -m pytest tests/
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.config import VenueConf, leg1_title  # noqa: E402
from entropy_arb.venue_hl import HLVenue  # noqa: E402

DEXS = [None, {"name": "xyz"}, {"name": "io"}]
META = {
    "": {"universe": [{"name": "BTC", "szDecimals": 5},
                      {"name": "ETH", "szDecimals": 4},
                      {"name": "AAPL", "szDecimals": 2}]},
    "io": {"universe": [{"name": "io:SNDK", "szDecimals": 3},
                        {"name": "io:AAPL", "szDecimals": 2},
                        {"name": "io:OLD", "szDecimals": 2,
                         "isDelisted": True}]},
}


def venue(dex, names, fee=0.0):
    conf = VenueConf(key="entropy", kind="hl", label="ENTROPY",
                     symbol=names[0], symbol_aliases=tuple(names),
                     fee_bps=fee, cap_usd=100, orders_per_min=60, hl_dex=dex)
    v = HLVenue(conf, "https://x", "wss://x", session=None,
                settle_timeout_sec=1)

    async def info(payload):
        if payload["type"] == "perpDexs":
            return DEXS
        return META[payload["dex"]]
    v._info = info
    return v


def load(v):
    asyncio.run(v.load_market())
    return v


def test_main_exchange_asset_id_is_the_universe_index():
    v = load(venue("", ["ETH"], fee=4.5))
    assert (v.coin, v.asset_id, v.size_decimals) == ("ETH", 1, 4)
    assert v.conf.hl_dex == "" and v.fee_bps == 4.5


def test_entropy_asset_id_keeps_the_builder_dex_formula():
    v = load(venue("io", ["SNDK"]))
    assert (v.coin, v.asset_id) == ("io:SNDK", 100000 + 2 * 10000 + 0)


def test_auto_prefers_entropy_then_main():
    v = load(venue("auto", ["AAPL"]))
    assert v.conf.hl_dex == "io" and v.coin == "io:AAPL"
    v = load(venue("auto", ["BTC"], fee=0.0))
    assert v.conf.hl_dex == "" and v.coin == "BTC" and v.asset_id == 0


def test_main_exchange_fee_floor():
    # a pair left at the Entropy fee must not trade main at 0 bps
    v = load(venue("auto", ["BTC"], fee=0.17))
    assert v.fee_bps == 4.5
    v = load(venue("", ["BTC"], fee=3.0))      # a real discounted tier stays
    assert v.fee_bps == 3.0


def test_not_found_names_both_places():
    try:
        load(venue("auto", ["NOPE"]))
    except RuntimeError as e:
        assert "io:NOPE" in str(e) and "NOPE" in str(e)
    else:
        raise AssertionError("expected not found")


def test_leg1_title():
    assert leg1_title("io") == "Entropy"
    assert leg1_title("") == leg1_title("main") == "Hyperliquid"


def test_ticker_picker_groups_entropy_then_main(tmp_path, monkeypatch):
    sys.path.insert(0, os.path.dirname(__file__))
    from test_tickers import BASE
    import club
    from rich.console import Console
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yaml").write_text(BASE)
    monkeypatch.setattr(club, "find_bots", lambda: [])
    club.ensure_ticker_files()
    ent = {"io": {"SNDK": False, "OAI": False, "NVDA": False},
           "main": {"BTC": False, "ETH": False, "AAPL": False,
                    "NVDA": False}}
    hed = {"SNDK", "OPENAI", "NVDA", "BTC", "ETH", "AAPL"}
    monkeypatch.setattr(club, "fetch_markets", lambda v: (ent, hed, None))
    rec = Console(record=True, width=120)
    monkeypatch.setattr(club, "console", rec)
    monkeypatch.setattr(club, "header", lambda *a, **k: None)
    answers = iter(["5"])
    monkeypatch.setattr(club, "ask", lambda *a, **k: next(answers))
    picked = club.choose_ticker("t", "lighter", check=True)
    out = rec.export_text()
    assert out.index("Entropy — активность") < out.index("Hyperliquid — основная")
    # Entropy: SNDK, OAI, NVDA (1-3); main: AAPL, BTC, ETH (4-6)
    assert "1  SNDK" in out and "3  NVDA" in out and "5  BTC" in out
    assert "Нет на биржах сейчас: ANTH" in out
    assert picked[0] == ("lighter", "BTC")
    assert club.cfg_values(("lighter", "NVDA"))["dex"] == "auto"   # not picked
