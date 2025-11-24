import os
import math
import pandas as pd
from datetime import datetime, timezone

# ---- small helpers ----
UTC = timezone.utc
CONTRACT_SIZE_USD = 100.0  # Binance coin-M BTCUSD contract size


def parse_expiry_from_symbol(sym: str):
    # BTCUSD_210924 -> 2021-09-24 00:00:00 UTC (Binance delivery is at a set hour; day is enough for backtest)
    if "_" not in sym:
        return None
    code = sym.split("_")[-1]
    if code.lower() == "perp":
        return 0
    y = int("20" + code[:2])
    m = int(code[2:4])
    d = int(code[4:6])
    return int(datetime(y, m, d, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)


def contract_type(sym: str):
    if sym.endswith("PERP") or sym.endswith("perp"):
        return "PERPETUAL"
    # For 2021 you only need specific deliveries; label them as DELIVERY (you don't rely on CURRENT/NEXT in scanner)
    return "DELIVERY"


def _filters_spot():
    # Minimal filters for your code paths
    return [
        {"filterType": "LOT_SIZE", "stepSize": "0.000001", "minQty": "0.000001"},
        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
    ]


def _filters_cm():
    return [
        {"filterType": "LOT_SIZE", "stepSize": "1"},
        {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
    ]


# ---- Backtest "clock" ----
class BacktestClock:
    def __init__(self, ts_ms: int):
        self.ts_ms = ts_ms

    def set_time(self, ts_ms: int):
        self.ts_ms = ts_ms

    def get_time(self) -> int:
        return self.ts_ms


# ---- Backtest Spot adapter ----
class BacktestSpot:
    """
    Emulates binance.spot.Spot for the subset you use:
      - exchange_info(symbol=...)
      - book_ticker(symbol=...)
      - margin_interest_rate_history(asset="USDT", limit=1)
    Pulls from a daily CSV produced by your downloader.
    """

    def __init__(
        self, clock: BacktestClock, spot_csv_path: str, assumed_spread_bps=2.0
    ):
        self.clock = clock
        self.spread_bps = float(assumed_spread_bps)
        # Load spot df (daily)
        self.df = pd.read_csv(spot_csv_path, parse_dates=["open_time"])
        # Index by date for fast lookups
        self.df["date"] = self.df["open_time"].dt.date

    # --- API shape compatibility ---
    def exchange_info(self, symbol: str):
        return {
            "symbols": [
                {
                    "symbol": symbol,
                    "filters": _filters_spot(),
                }
            ]
        }

    def book_ticker(self, symbol: str):
        # Use last available daily record at/<= current day
        ts = datetime.fromtimestamp(self.clock.get_time() / 1000, tz=UTC).date()
        row = self.df[self.df["date"] <= ts].tail(1)
        if row.empty:
            raise RuntimeError(f"No spot data at or before {ts} for {symbol}")
        # Mid ~ close; synthesize bid/ask using spread_bps
        close = float(row["close"].values[0])
        half_spread = close * (self.spread_bps / 1e4)  # bps -> fraction
        bid = close - half_spread
        ask = close + half_spread
        return {"symbol": symbol, "bidPrice": f"{bid:.2f}", "askPrice": f"{ask:.2f}"}

    def margin_interest_rate_history(self, asset="USDT", limit=1):
        # Backtest constant (your scanner already falls back to DEFAULT_USDT_BORROW_APR if this errors)
        daily_rate = 0.06 / 365.0  # 6% APR -> daily
        ts = self.clock.get_time()
        return [
            {"asset": asset, "dailyInterestRate": f"{daily_rate:.8f}", "timestamp": ts}
        ]


# ---- Backtest Coin-M Futures adapter ----
class BacktestCMFutures:
    """
    Emulates binance.cm_futures.CMFutures for:
      - exchange_info()  -> time-aware 'TRADING' status
      - mark_price(symbol=...)
      - funding_rate(symbol=..., limit=1)
    """

    def __init__(
        self,
        clock: BacktestClock,
        mark_csv_by_symbol: dict,
        funding_csv_perp: str | None,
    ):
        self.clock = clock
        self.mark = {}  # symbol -> df with 'open_time' parsed and 'date'
        self.first_date = {}  # symbol -> first trading date (date)
        self.last_date = {}  # symbol -> last trading date (date)
        for sym, path in mark_csv_by_symbol.items():
            df = pd.read_csv(path, parse_dates=["open_time"])
            df["date"] = df["open_time"].dt.date
            self.mark[sym] = df
            if not df.empty:
                self.first_date[sym] = df["date"].min()
                self.last_date[sym] = df["date"].max()
            else:
                self.first_date[sym] = None
                self.last_date[sym] = None

        self.funding = None
        if funding_csv_perp:
            fdf = pd.read_csv(funding_csv_perp)
            # Force to timezone-aware datetimes in UTC
            fdf["funding_time"] = pd.to_datetime(
                fdf["funding_time"], utc=True, errors="coerce"
            )
            fdf = fdf.sort_values("funding_time").dropna(subset=["funding_time"])
            self.funding = fdf

    def exchange_info(self):
        """
        Return only symbols that are 'TRADING' at the current backtest date:
        - current_date >= first_date in CSV
        - and (expiry not passed) & (we still have data)
        """
        now_date = datetime.fromtimestamp(self.clock.get_time() / 1000, tz=UTC).date()
        symbols = []
        for sym, df in self.mark.items():
            first_d = self.first_date.get(sym)
            last_d = self.last_date.get(sym)
            delivery_ms = parse_expiry_from_symbol(sym)
            expiry_date = (
                None
                if not delivery_ms
                else datetime.fromtimestamp(delivery_ms / 1000, tz=UTC).date()
            )

            is_live_by_data = (first_d is not None) and (first_d <= now_date <= last_d)
            is_before_expiry = (expiry_date is None) or (now_date <= expiry_date)

            status = "TRADING" if (is_live_by_data and is_before_expiry) else "PENDING"

            symbols.append(
                {
                    "symbol": sym,
                    "pair": sym.replace("_", ""),
                    "baseAsset": "BTC",
                    "contractStatus": status,
                    "contractType": (
                        "PERPETUAL" if sym.endswith("PERP") else "DELIVERY"
                    ),
                    "deliveryDate": delivery_ms or 0,
                    "contractSize": CONTRACT_SIZE_USD,
                    "filters": _filters_cm(),
                }
            )
        return {"symbols": symbols}

    def mark_price(self, symbol: str):
        ts_date = datetime.fromtimestamp(self.clock.get_time() / 1000, tz=UTC).date()
        if symbol not in self.mark:
            raise RuntimeError(f"No mark dataset for {symbol}")
        df = self.mark[symbol]
        row = df[df["date"] <= ts_date].tail(1)
        if row.empty:
            # Shouldn't happen once exchange_info() filters non-trading symbols,
            # but keep defensive raise to catch misuse.
            raise RuntimeError(f"No mark data at/before {ts_date} for {symbol}")
        mark_close = float(row["mark_close"].values[0])
        index_price = mark_close  # proxy; scanner only reads it
        return {
            "symbol": symbol,
            "markPrice": f"{mark_close:.2f}",
            "indexPrice": f"{index_price:.2f}",
        }

    def funding_rate(self, symbol: str, limit=1, **kwargs):
        if self.funding is None or "PERP" not in symbol:
            return []
        cur_ts = datetime.fromtimestamp(self.clock.get_time() / 1000, tz=UTC)
        # Select rows up to current time (both sides tz-aware)
        fdf = self.funding[self.funding["funding_time"] <= cur_ts].tail(limit)
        if fdf.empty:
            return []
        out = []
        for _, r in fdf.iterrows():
            out.append(
                {
                    "fundingTime": int(r["funding_time"].timestamp() * 1000),
                    "fundingRate": str(r["funding_rate"]),
                    "symbol": symbol,
                }
            )
        return out
