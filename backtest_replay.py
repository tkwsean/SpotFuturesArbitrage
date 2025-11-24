# backtest_replay.py
# Step 3: Replay timeline using your existing scanner + TWAP execution
# Assumes you've created data_2021/*.csv with download_data.py
# and added historical_feed.py from Step 2.

import os
import csv
from datetime import datetime, timedelta, timezone as tz
from ledger import PortfolioLedger

from historical_feed import (
    BacktestClock,
    BacktestSpot,
    BacktestCMFutures,
    parse_expiry_from_symbol,
)
from scanner import BasisScanner
from api import ExecutionTWAP

# ---------------- Config ----------------
DATA_DIR = "data_2021"
SPOT_CSV = os.path.join(DATA_DIR, "spot_BTCUSDT_1d_2021Q1Q3.csv")
MARK_MAP = {
    "BTCUSD_210326": os.path.join(DATA_DIR, "cm_mark_BTCUSD_210326_1d_2021Q1Q3.csv"),
    "BTCUSD_210625": os.path.join(DATA_DIR, "cm_mark_BTCUSD_210625_1d_2021Q1Q3.csv"),
    "BTCUSD_210924": os.path.join(DATA_DIR, "cm_mark_BTCUSD_210924_1d_2021Q1Q3.csv"),
    "BTCUSD_PERP": os.path.join(DATA_DIR, "cm_mark_BTCUSD_PERP_1d_2021Q1Q3.csv"),
}
FUND_CSV = os.path.join(DATA_DIR, "cm_funding_BTCUSD_PERP_2021Q1Q3.csv")

START_DATE = datetime(2021, 1, 1, tzinfo=tz.utc)
END_DATE = datetime(2021, 9, 30, tzinfo=tz.utc)

INVEST_USDT = 1_000_000
MIN_NET_ANN = 0.00  # require non-negative delivery basis after costs; adjust as needed
SLICES_PER_TRADE = 24
HEDGE_TOLERANCE = 5
ASSUMED_SPREAD_BPS = 2.0  # for spot bid/ask synthesis in historical feed
MIN_OPEN_DTE = 7.0

LOG_PATH = "replay_trades_2021.csv"


# Always-fill simulator for backtest (your live sim used random fills)
class AlwaysFillSim:
    def place_order(self, params):
        r = params.copy()
        r["status"] = "filled"
        return r


# -------------- Main replay --------------
def main():
    # Clock + feeds
    clock = BacktestClock(int(START_DATE.timestamp() * 1000))
    spot_bt = BacktestSpot(
        clock, spot_csv_path=SPOT_CSV, assumed_spread_bps=ASSUMED_SPREAD_BPS
    )
    cm_bt = BacktestCMFutures(
        clock, mark_csv_by_symbol=MARK_MAP, funding_csv_perp=FUND_CSV
    )

    # backtest_replay.py
    scanner = BasisScanner(spot_bt, cm_bt, clock=clock)

    execer = ExecutionTWAP(spot_bt, cm_bt, AlwaysFillSim(), scanner)

    # Trade state
    open_pos = None  # will hold exec report + metadata
    trade_id = 0

    # CSV log header
    if not os.path.exists(LOG_PATH):
        with open(LOG_PATH, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "trade_id",
                    "open_date",
                    "symbol",
                    "days_to_expiry_at_open",
                    "spot_bought",
                    "fut_contracts",
                    "invested_usdt_est",
                    "slices_filled",
                    "slices_total",
                    "close_date",
                    "sold_spot",
                    "covered_contracts",
                ]
            )

    cur = START_DATE

    ledger = PortfolioLedger("daily_pnl_2021.csv")

    prev_spot = None
    prev_fut = None

    while cur <= END_DATE:
        clock.set_time(int(cur.timestamp() * 1000))
        S = spot_bt.book_ticker("BTCUSDT")
        spot_px = (float(S["bidPrice"]) + float(S["askPrice"])) / 2

        if open_pos:
            fut_sym = open_pos["future_symbol"]
            mark = cm_bt.mark_price(fut_sym)
            fut_px = float(mark["markPrice"])

            if prev_spot is not None and prev_fut is not None:
                ledger.mark(
                    cur,
                    spot_px,
                    fut_px,
                    open_pos["spot_bought"],
                    open_pos["fut_contracts_sold"],
                    prev_spot,
                    prev_fut,
                )

            prev_spot, prev_fut = spot_px, fut_px

            # roll one day before expiry
            expiry_ms = parse_expiry_from_symbol(fut_sym)
            if expiry_ms and cur >= datetime.fromtimestamp(
                expiry_ms / 1000, tz=tz.utc
            ) - timedelta(days=1):
                execer.close_twap(open_pos, slices=SLICES_PER_TRADE, mode="fast")
                open_pos = None
                prev_spot, prev_fut = None, None

        else:
            # flat -> open a new trade
            opps = scanner.scan(base="BTC", spot_symbol="BTCUSDT")
            delivery = [
                o
                for o in opps
                if o.get("type") == "DELIVERY_BASIS"
                and (o["days_to_expiry"] or 0) >= MIN_OPEN_DTE
            ]
            if delivery:
                rep = execer.open_twap(
                    invest_usdt=INVEST_USDT,
                    base="BTC",
                    spot_symbol="BTCUSDT",
                    slices=SLICES_PER_TRADE,
                    mode="fast",
                    use_scanner=True,
                    min_net_ann=MIN_NET_ANN,
                    hedge_tolerance=HEDGE_TOLERANCE,
                )
                open_pos = rep
                prev_spot, prev_fut = spot_px, (
                    fut_px if "fut_px" in locals() else spot_px
                )

        cur += timedelta(days=1)

    print("[DONE] Daily PnL log -> daily_pnl_2021.csv")


if __name__ == "__main__":
    main()
