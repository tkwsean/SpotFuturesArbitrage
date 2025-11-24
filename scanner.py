import math, time, datetime as dt
from binance.spot import Spot
from binance.cm_futures import CMFutures

HURDLE_ANN_BASIS = 0.08  # 8% annualized hurdle for delivery basis
FEE_BPS_SPOT = 10  # example taker in bps
FEE_BPS_FUT = 5  # example taker in bps
DEFAULT_USDT_BORROW_APR = 0.06  # fallback if you don't pull margin rate


class BasisScanner:
    def __init__(self, spot: Spot, cm: CMFutures, clock=None):
        self.spot, self.cm = spot, cm
        self.clock = clock  # optional BacktestClock

    def _spot_mid(self, spot_symbol="BTCUSDT"):
        t = self.spot.book_ticker(symbol=spot_symbol)  # /api/v3/ticker/bookTicker
        bid, ask = float(t["bidPrice"]), float(t["askPrice"])
        return (bid + ask) / 2.0

    def _cm_contracts(self, base="BTC"):
        info = self.cm.exchange_info()  # /dapi/v1/exchangeInfo
        out = []
        for s in info["symbols"]:
            # Example filters: only COIN-M contracts for this base, active trading
            if (
                s["baseAsset"] != base or s["contractStatus"] != "TRADING"
            ):  # something wrong here
                continue
            # contractType in {"PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER", "DELIVERY"}
            out.append(
                {
                    "symbol": s["symbol"],
                    "pair": s["pair"],
                    "contractType": s["contractType"],
                    "deliveryDate": s.get("deliveryDate", 0),  # ms; 0 for PERP
                    "contractSize": float(
                        s.get("contractSize", 100.0)
                    ),  # USD per contract
                }
            )
        return out

    def _mark_index(self, fut_symbol):
        # /dapi/v1/premiumIndex returns markPrice and indexPrice
        methods = [m for m in dir(self.cm) if callable(getattr(self.cm, m))]

        pi = self.cm.mark_price(symbol=fut_symbol)
        # If API returns a list, take first; connector usually returns dict for single symbol
        if isinstance(pi, list):
            pi = pi[0]
        return float(pi["markPrice"]), float(pi["indexPrice"])

    def _latest_funding_rate(self, perp_symbol):
        # /dapi/v1/fundingRate?symbol=...&limit=1
        fr = self.cm.funding_rate(symbol=perp_symbol, limit=1)
        if fr:
            return float(fr[0]["fundingRate"])
        return 0.0

    def _days_to_expiry(self, delivery_ms):
        if not delivery_ms:
            return None
        now_ms = (
            self.clock.get_time()
            if getattr(self, "clock", None)
            else int(time.time() * 1000)
        )
        dte = (delivery_ms - now_ms) / (1000 * 60 * 60 * 24)
        # never allow exactly 0
        return max(1e-6, dte)

    def _spot_usdt_borrow_apr(self):
        # Optional: pull current USDT borrow APR; else fallback
        try:
            # pick latest record
            h = self.spot.margin_interest_rate_history(asset="USDT", limit=1)
            if h:
                return float(h[0]["dailyInterestRate"]) * 365.0
        except Exception:
            pass
        return DEFAULT_USDT_BORROW_APR

    def scan(self, base="BTC", spot_symbol="BTCUSDT"):
        S = self._spot_mid(spot_symbol)  # something wrong here
        contracts = self._cm_contracts(base)
        borrow_apr = self._spot_usdt_borrow_apr()

        opps = []
        for c in contracts:
            sym, ctype = c["symbol"], c["contractType"]
            mark, indexp = self._mark_index(sym)

            if ctype == "PERPETUAL":
                # funding carry snapshot: fundingRate * 3 (per day) as a crude daily; adjust horizon as needed
                fr = self._latest_funding_rate(sym)
                est_daily = fr * 3.0  # 8h intervals per day
                net_daily = (
                    est_daily
                    - (FEE_BPS_SPOT + FEE_BPS_FUT) / 10000.0
                    - borrow_apr / 365.0
                )
                opps.append(
                    {
                        "type": "PERP_FUNDING",
                        "symbol": sym,
                        "spot": S,
                        "mark": mark,
                        "funding_rate_last": fr,
                        "est_daily_after_costs": net_daily,
                    }
                )
            else:
                days = self._days_to_expiry(c["deliveryDate"])
                basis = (mark - S) / S
                ann = basis * 365.0 / days
                fee_drag = (FEE_BPS_SPOT + FEE_BPS_FUT) / 10000.0 * (365.0 / days)
                net_ann = ann - fee_drag - borrow_apr
                if net_ann >= HURDLE_ANN_BASIS:
                    # Hedge sizing: #contracts to short for 1 unit of base bought on spot
                    contracts_per_1_base = (S) / c["contractSize"]
                    opps.append(
                        {
                            "type": "DELIVERY_BASIS",
                            "symbol": sym,
                            "spot": S,
                            "mark": mark,
                            "days_to_expiry": days,
                            "ann_basis": ann,
                            "net_ann_after_costs": net_ann,
                            "contracts_per_1_base": round(contracts_per_1_base, 2),
                        }
                    )
        # Sort by best net economics
        return sorted(
            opps,
            key=lambda x: x.get(
                "net_ann_after_costs", x.get("est_daily_after_costs", -1)
            ),
            reverse=True,
        )
