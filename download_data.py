# download_binance_2021.py
# Requires: pip install binance-connector-python pandas

import os
import time
import math
import pandas as pd
from datetime import datetime, timezone, timedelta

from binance.spot import Spot
from binance.cm_futures import CMFutures

# ------------------ CONFIG ------------------

START = "2021-01-01 00:00:00"
END = "2021-09-30 23:59:59"
TZ = timezone.utc

INTERVAL = "1d"  # 1h granularity for backtest
DATA_DIR = "data_2021"

SPOT_SYMBOL = "BTCUSDT"
# Coin-M quarterly delivery contracts in 2021
DELIVERY_SYMBOLS = ["BTCUSD_210326", "BTCUSD_210625", "BTCUSD_210924"]
# Optional perp
PERP_SYMBOL = "BTCUSD_PERP"
DOWNLOAD_PERP = True

# API keys are optional for public endpoints; env vars if you have them
API_KEY = os.getenv(
    "BINANCE_API_KEY",
    "0Lj7lMcerkFtSnCyaIYs6CJmxbqwrdWoPjhJLqBLhyuDkCtvztgxbluNQxOCKn7X",
)
API_SEC = os.getenv(
    "BINANCE_API_SECRET",
    "jNd2ld4ONKDmeuse9TPLDBdB8ZCnlUMuMPpKknMMwfxZb8QcmpStkSRLHSvZDCk1",
)

# ------------------ HELPERS ------------------


def to_ms(dt_str: str) -> int:
    return int(
        datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ).timestamp()
        * 1000
    )


def ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=TZ).isoformat()


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def chunk_timerange(
    start_ms: int, end_ms: int, interval_ms: int, max_points: int = 1500
):
    """
    Binance returns at most ~1500 points per request on most kline endpoints.
    Yields (chunk_start_ms, chunk_end_ms_exclusive).
    """
    chunk_span = interval_ms * max_points
    cur = start_ms
    while cur < end_ms:
        nxt = min(cur + chunk_span, end_ms)
        yield cur, nxt
        cur = nxt
        time.sleep(0.1)  # gentle pace


def interval_to_ms(interval: str) -> int:
    unit = interval[-1].lower()
    val = int(interval[:-1])
    if unit == "m":
        return val * 60 * 1000
    if unit == "h":
        return val * 60 * 60 * 1000
    if unit == "d":
        return val * 24 * 60 * 60 * 1000
    raise ValueError(f"Unsupported interval: {interval}")


# ------------------ DOWNLOADERS ------------------


def download_spot_klines(
    spot: Spot, symbol: str, interval: str, start_ms: int, end_ms: int
) -> pd.DataFrame:
    """
    Uses /api/v3/klines. We approximate 'mid' as (high+low)/2 per bar.
    """
    iv_ms = interval_to_ms(interval)
    rows = []
    for a, b in chunk_timerange(start_ms, end_ms, iv_ms):
        data = spot.klines(
            symbol=symbol, interval=interval, startTime=a, endTime=b, limit=1500
        )
        for k in data:
            # kline fields per Binance docs
            open_time, o, h, l, c, v, close_time = (
                k[0],
                k[1],
                k[2],
                k[3],
                k[4],
                k[5],
                k[6],
            )
            rows.append(
                {
                    "open_time_ms": open_time,
                    "open_time": ms_to_iso(open_time),
                    "close_time_ms": close_time,
                    "close_time": ms_to_iso(close_time),
                    "open": float(o),
                    "high": float(h),
                    "low": float(l),
                    "close": float(c),
                    "hl2_mid": (float(h) + float(l)) / 2.0,
                    "volume": float(v),
                }
            )
    df = (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["open_time_ms"])
        .sort_values("open_time_ms")
    )
    return df


def download_mark_klines(
    cm: CMFutures, symbol: str, interval: str, start_ms: int, end_ms: int
) -> pd.DataFrame:
    """
    Uses /dapi/v1/markPriceKlines for Coin-M futures mark prices.
    IMPORTANT: Binance requires (endTime - startTime) <= 200 days.
    """
    iv_ms = interval_to_ms(interval)
    MAX_DAYS = 200
    MAX_SPAN_MS = MAX_DAYS * 24 * 60 * 60 * 1000  # hard cap per API
    rows = []

    cur = start_ms
    while cur < end_ms:
        chunk_end = min(cur + MAX_SPAN_MS - 1, end_ms)  # inclusive window OK
        data = cm.mark_price_klines(
            symbol=symbol,
            interval=interval,
            startTime=cur,
            endTime=chunk_end,
            limit=1500,
        )
        if not data:
            # advance by one interval to avoid infinite loop on empty range
            cur = min(end_ms, cur + iv_ms)
            continue

        for k in data:
            # kline payload: [ openTime, open, high, low, close, ... , closeTime ]
            open_time = k[0]
            o, h, l, c = float(k[1]), float(k[2]), float(k[3]), float(k[4])
            close_time = k[6] if len(k) > 6 else open_time  # defensive
            rows.append(
                {
                    "open_time_ms": open_time,
                    "open_time": ms_to_iso(open_time),
                    "close_time_ms": close_time,
                    "close_time": ms_to_iso(close_time),
                    "mark_open": o,
                    "mark_high": h,
                    "mark_low": l,
                    "mark_close": c,
                    "mark_hl2": (h + l) / 2.0,
                }
            )

        # advance cursor to just after the last candle we received
        last_open = data[-1][0]
        cur = max(cur + iv_ms, last_open + iv_ms)
        time.sleep(0.1)

    df = (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["open_time_ms"])
        .sort_values("open_time_ms")
    )
    return df


def download_perp_funding(
    cm: CMFutures, symbol: str, start_ms: int, end_ms: int
) -> pd.DataFrame:
    """
    /dapi/v1/fundingRate with pagination by time.
    """
    out = []
    cur = start_ms
    while True:
        data = cm.funding_rate(symbol=symbol, startTime=cur, endTime=end_ms, limit=1000)
        if not data:
            break
        for r in data:
            out.append(
                {
                    "funding_time_ms": int(r["fundingTime"]),
                    "funding_time": ms_to_iso(int(r["fundingTime"])),
                    "funding_rate": float(r["fundingRate"]),
                }
            )
        # advance cursor
        last_ms = int(data[-1]["fundingTime"])
        nxt = last_ms + 1
        if nxt >= end_ms:
            break
        cur = nxt
        time.sleep(0.1)
    df = (
        pd.DataFrame(out)
        .drop_duplicates(subset=["funding_time_ms"])
        .sort_values("funding_time_ms")
    )
    return df


# ------------------ MAIN ------------------


def main():
    ensure_dir(DATA_DIR)

    spot = Spot(api_key=API_KEY, api_secret=API_SEC)
    cm = CMFutures(key=API_KEY, secret=API_SEC)

    start_ms = to_ms(START)
    end_ms = to_ms(END)

    # 1) Spot BTCUSDT
    print(f"[DL] Spot {SPOT_SYMBOL} {INTERVAL} {START} -> {END}")
    df_spot = download_spot_klines(spot, SPOT_SYMBOL, INTERVAL, start_ms, end_ms)
    spot_path = os.path.join(DATA_DIR, f"spot_{SPOT_SYMBOL}_{INTERVAL}_2021Q1Q3.csv")
    df_spot.to_csv(spot_path, index=False)
    print(f"[OK] {spot_path} rows={len(df_spot)}")

    # 2) Coin-M quarterlies mark prices
    for sym in DELIVERY_SYMBOLS:
        print(f"[DL] Coin-M mark {sym} {INTERVAL} {START} -> {END}")
        df_mark = download_mark_klines(cm, sym, INTERVAL, start_ms, end_ms)
        fut_path = os.path.join(DATA_DIR, f"cm_mark_{sym}_{INTERVAL}_2021Q1Q3.csv")
        df_mark.to_csv(fut_path, index=False)
        print(f"[OK] {fut_path} rows={len(df_mark)}")

    # 3) Optional PERP: mark + funding
    if DOWNLOAD_PERP:
        print(f"[DL] Coin-M mark {PERP_SYMBOL} {INTERVAL} {START} -> {END}")
        df_perp = download_mark_klines(cm, PERP_SYMBOL, INTERVAL, start_ms, end_ms)
        perp_path = os.path.join(
            DATA_DIR, f"cm_mark_{PERP_SYMBOL}_{INTERVAL}_2021Q1Q3.csv"
        )
        df_perp.to_csv(perp_path, index=False)
        print(f"[OK] {perp_path} rows={len(df_perp)}")

        print(f"[DL] Funding {PERP_SYMBOL} {START} -> {END}")
        df_fund = download_perp_funding(cm, PERP_SYMBOL, start_ms, end_ms)
        fund_path = os.path.join(DATA_DIR, f"cm_funding_{PERP_SYMBOL}_2021Q1Q3.csv")
        df_fund.to_csv(fund_path, index=False)
        print(f"[OK] {fund_path} rows={len(df_fund)}")

    print("[DONE] Data saved to", os.path.abspath(DATA_DIR))


if __name__ == "__main__":
    main()
