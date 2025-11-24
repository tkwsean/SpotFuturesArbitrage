# ledger.py
import csv
from datetime import datetime, timezone as tz


class PortfolioLedger:
    def __init__(self, log_path="daily_pnl.csv"):
        self.log_path = log_path
        with open(self.log_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "date",
                    "spot_px",
                    "fut_px",
                    "spot_pos",
                    "fut_pos",
                    "spot_pnl",
                    "fut_pnl",
                    "net_pnl",
                    "nav",
                ]
            )
        self.nav = 1_000_000  # start NAV

    def mark(self, date, spot_px, fut_px, spot_pos, fut_pos, prev_spot, prev_fut):
        spot_pnl = (spot_px - prev_spot) * spot_pos
        fut_pnl = -(fut_px - prev_fut) * (
            fut_pos * 100 / spot_px
        )  # coin-M sizing approx
        net_pnl = spot_pnl + fut_pnl
        self.nav += net_pnl
        with open(self.log_path, "a", newline="") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    date.isoformat(),
                    spot_px,
                    fut_px,
                    spot_pos,
                    fut_pos,
                    round(spot_pnl, 2),
                    round(fut_pnl, 2),
                    round(net_pnl, 2),
                    round(self.nav, 2),
                ]
            )
        return net_pnl, self.nav
