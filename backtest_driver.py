# backtest_driver.py (sketch)
from historical_feed import BacktestClock, BacktestSpot, BacktestCMFutures
from scanner import BasisScanner
from api import ExecutionTWAP  # your existing file

from datetime import datetime, timezone

clock = BacktestClock(
    ts_ms=int(datetime(2021, 1, 4, tzinfo=timezone.utc).timestamp() * 1000)
)

spot_csv = "data_2021/spot_BTCUSDT_1d_2021Q1Q3.csv"
mark_map = {
    "BTCUSD_210326": "data_2021/cm_mark_BTCUSD_210326_1d_2021Q1Q3.csv",
    "BTCUSD_210625": "data_2021/cm_mark_BTCUSD_210625_1d_2021Q1Q3.csv",
    "BTCUSD_210924": "data_2021/cm_mark_BTCUSD_210924_1d_2021Q1Q3.csv",
    "BTCUSD_PERP": "data_2021/cm_mark_BTCUSD_PERP_1d_2021Q1Q3.csv",
}
fund_csv = "data_2021/cm_funding_BTCUSD_PERP_2021Q1Q3.csv"

spot_bt = BacktestSpot(clock, spot_csv_path=spot_csv, assumed_spread_bps=2.0)
cm_bt = BacktestCMFutures(clock, mark_csv_by_symbol=mark_map, funding_csv_perp=fund_csv)

scanner = BasisScanner(spot_bt, cm_bt)
execer = ExecutionTWAP(
    spot_bt, cm_bt, sim=None, scanner=scanner
)  # you don't need BinanceSimulator in backtest

# advance the clock to any date you want, then run the same open_twap()
# (If your ExecutionTWAP expects sim.place_order, pass a tiny shim that returns "filled")
