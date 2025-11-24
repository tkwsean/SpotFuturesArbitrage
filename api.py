import random
import time

from binance.spot import Spot
from binance.cm_futures import CMFutures
from scanner import BasisScanner, HURDLE_ANN_BASIS


class BinanceSimulator(object):

    def __init__(
        self,
        api_key="0Lj7lMcerkFtSnCyaIYs6CJmxbqwrdWoPjhJLqBLhyuDkCtvztgxbluNQxOCKn7X",
        secret_key="jNd2ld4ONKDmeuse9TPLDBdB8ZCnlUMuMPpKknMMwfxZb8QcmpStkSRLHSvZDCk1",
    ):
        self.api_key = api_key
        self.secret_key = secret_key
        self.spot = Spot(api_key=api_key, api_secret=secret_key)
        self.cm_future = CMFutures(key=api_key, secret=secret_key)
        self.order_fill_prob = 0.9

    def place_order(self, order_params):
        response = order_params.copy()
        x = random.randint(1, 100)
        response["status"] = "filled" if x <= self.order_fill_prob * 100 else "canceled"
        return response


class ExecutionTWAP:
    """
    Opens (and can later close) a long-spot + short-coinM-future basis trade via
    a multi-slice TWAP. Uses BinanceSimulator.place_order(...) for simulated fills.
    """

    def __init__(
        self,
        spot_client: Spot,
        cm_client: CMFutures,
        sim: BinanceSimulator,
        scanner: BasisScanner,
    ):
        self.spot = spot_client
        self.cm = cm_client
        self.sim = sim
        self.scanner = scanner

    # ---------- helpers ----------

    @staticmethod
    def _parse_filters(filters, ftype):
        for f in filters:
            if f.get("filterType") == ftype:
                return f
        return {}

    @staticmethod
    def _round_to_step(qty, step):
        step = float(step)
        if step <= 0:
            return qty
        return (int(qty / step)) * step

    def _spot_filters(self, spot_symbol):
        info = self.spot.exchange_info(symbol=spot_symbol)
        s = info["symbols"][0]
        lot = self._parse_filters(s["filters"], "LOT_SIZE")
        pricef = self._parse_filters(s["filters"], "PRICE_FILTER")
        return {
            "qty_step": lot.get("stepSize", "0.000001"),
            "min_qty": lot.get("minQty", "0.000001"),
            "tick_size": pricef.get("tickSize", "0.01"),
        }

    def _cm_filters(self, fut_symbol):
        info = self.cm.exchange_info()
        for s in info["symbols"]:
            if s["symbol"] == fut_symbol:
                lot = self._parse_filters(s["filters"], "LOT_SIZE")
                pricef = self._parse_filters(s["filters"], "PRICE_FILTER")
                return {
                    "qty_step": lot.get(
                        "stepSize", "1"
                    ),  # coin-M contracts are integers
                    "tick_size": pricef.get("tickSize", "0.1"),
                    "contract_size": float(s.get("contractSize", 100.0)),
                    "delivery": s.get("deliveryDate", 0),
                    "contract_type": s.get("contractType"),
                }
        raise ValueError(f"Futures symbol {fut_symbol} not found in cm.exchange_info()")

    def _choose_quarterly(self, base="BTC"):
        cands = self.scanner._cm_contracts(base=base)
        for c in cands:
            c["days_to_expiry"] = (
                self.scanner._days_to_expiry(c["deliveryDate"])
                if c["deliveryDate"]
                else None
            )

        cur = [c for c in cands if c["contractType"] == "CURRENT_QUARTER"]
        nxt = [c for c in cands if c["contractType"] == "NEXT_QUARTER"]
        perp = [c for c in cands if c["contractType"] == "PERPETUAL"]

        if cur:
            dte = cur[0]["days_to_expiry"]
            if dte is None or dte > 1.0:
                return cur[0]
        if nxt:
            dte = nxt[0]["days_to_expiry"]
            if dte is None or dte > 1.0:
                return nxt[0]
        if perp:
            return perp[0]
        raise RuntimeError("No suitable coin-margined contract found")

    # ---------- core ----------

    def open_twap(
        self,
        invest_usdt=1_000_000,
        base="BTC",
        spot_symbol="BTCUSDT",
        slices=24,
        mode="fast",
        use_scanner=True,
        min_net_ann=None,
        hedge_tolerance=5,
    ):
        """
        Execute long spot + short coin-M futures via TWAP.
        - Dynamic per-slice sizing (adapts to partials)
        - Final sweep to hit target allocation
        - End reconciliation to align hedge
        """
        # Pick futures symbol
        if use_scanner:
            opps = self.scanner.scan(base=base, spot_symbol=spot_symbol)
            delivery = [o for o in opps if o.get("type") == "DELIVERY_BASIS"]
            perp = [o for o in opps if o.get("type") == "PERP_FUNDING"]

            if delivery:
                if min_net_ann is None:
                    best = delivery[0]
                else:
                    best = next(
                        (
                            o
                            for o in delivery
                            if o.get("net_ann_after_costs", -1) >= min_net_ann
                        ),
                        None,
                    )
                    if best is None:
                        best = delivery[0]
                        print(
                            f"[SCAN] No DELIVERY_BASIS >= {min_net_ann:.2%}. "
                            f"Taking best available: {best['symbol']} "
                            f"(net_ann_after_costs={best['net_ann_after_costs']:.2%})."
                        )
                fut_symbol = best["symbol"]
            elif perp:
                best = perp[0]
                if best["est_daily_after_costs"] < 0:
                    print("[SCAN] PERP carry negative; skipping PERP.")
                    fut = self._choose_quarterly(base=base)
                    fut_symbol = fut["symbol"]
                else:
                    fut_symbol = best["symbol"]
                    print(
                        f"[SCAN] Falling back to PERP_FUNDING: {fut_symbol} "
                        f"(est_daily_after_costs={best['est_daily_after_costs']:.6f})."
                    )
            else:
                print(
                    "[SCAN] No eligible contracts. Falling back to quarterly chooser."
                )
                fut = self._choose_quarterly(base=base)
                fut_symbol = fut["symbol"]
        else:
            fut = self._choose_quarterly(base=base)
            fut_symbol = fut["symbol"]

        fut_filters = self._cm_filters(fut_symbol)
        spot_filters = self._spot_filters(spot_symbol)

        # Prices
        S = self.scanner._spot_mid(spot_symbol)
        mark, _ = self.scanner._mark_index(fut_symbol)

        # Targets
        total_base = invest_usdt / S
        min_step_spot = float(spot_filters["qty_step"])

        # State
        bought_spot = 0.0
        shorted_contracts = 0
        filled_slices = 0
        canceled_slices = 0
        remaining_base = total_base
        carry_base = 0.0  # signed
        carry_cts = 0  # signed

        print(f"--- TWAP OPEN ({slices} slices) ---")
        print(
            f"Spot {spot_symbol}, Fut {fut_symbol} (via {'SCAN' if use_scanner else 'QUARTERLY PICK'})"
        )
        print(f"Spot mid ~ {S:.2f}, Mark ~ {mark:.2f}")
        print(
            f"Target invest: {invest_usdt:,.0f} USDT  -> buy ~{total_base:.6f} {base}"
        )

        # ---------- per-slice loop ----------
        for i in range(slices):
            S = self.scanner._spot_mid(spot_symbol)
            mark, _ = self.scanner._mark_index(fut_symbol)
            contracts_per_1_base = S / float(fut_filters["contract_size"])

            remaining_slices = max(1, slices - i)
            planned_base = self._round_to_step(
                remaining_base / remaining_slices, min_step_spot
            )
            slice_base = planned_base + carry_base
            slice_base = self._round_to_step(max(slice_base, 0.0), min_step_spot)

            target_cts = int(round(slice_base * contracts_per_1_base)) + carry_cts

            bt = self.spot.book_ticker(symbol=spot_symbol)
            ask = float(bt["askPrice"])

            spot_order = {
                "symbol": spot_symbol,
                "account": "SPOT",
                "side": "BUY",
                "type": "LIMIT",
                "timeInForce": "GTC",
                "quantity": float(f"{slice_base:.6f}"),
                "price": round(ask * 1.0005, 2),
            }
            fut_order = {
                "symbol": fut_symbol,
                "account": "COIN-M",
                "side": "SELL",
                "type": "LIMIT",
                "timeInForce": "GTC",
                "quantity": int(target_cts),
                "price": round(mark * 0.999, 1),
            }

            spot_resp = self.sim.place_order(spot_order)
            fut_resp = self.sim.place_order(fut_order)

            filled_base = (
                spot_order["quantity"] if spot_resp["status"] == "filled" else 0.0
            )
            filled_cts = fut_order["quantity"] if fut_resp["status"] == "filled" else 0

            bought_spot += filled_base
            shorted_contracts += filled_cts

            carry_base = slice_base - filled_base
            carry_cts = target_cts - filled_cts
            remaining_base = max(0.0, remaining_base - filled_base)

            if filled_base > 0 and filled_cts > 0:
                filled_slices += 1
                print(
                    f"[{i+1:02d}/{slices}] filled  spot + fut | +{filled_base:.6f} {base}, -{filled_cts} cts"
                )
            else:
                canceled_slices += 1
                print(
                    f"[{i+1:02d}/{slices}] partial/cancel; carry -> {carry_base:.6f} {base}, {carry_cts} cts"
                )

            if remaining_base <= 1e-9 and abs(carry_base) <= 1e-9 and carry_cts == 0:
                break

            time.sleep(0.2 if mode == "fast" else 3600)

        # ---------- final sweep to reach allocation ----------
        # Try once more to buy remaining spot (and matching futures) before re-hedge
        remaining_total = remaining_base + max(carry_base, 0.0)
        sweep_base = self._round_to_step(remaining_total, min_step_spot)
        if sweep_base > 0:
            S = self.scanner._spot_mid(spot_symbol)
            mark, _ = self.scanner._mark_index(fut_symbol)
            contracts_per_1_base = S / float(fut_filters["contract_size"])
            sweep_cts = int(round(sweep_base * contracts_per_1_base)) + max(
                carry_cts, 0
            )

            bt = self.spot.book_ticker(symbol=spot_symbol)
            ask = float(bt["askPrice"])

            spot_order = {
                "symbol": spot_symbol,
                "account": "SPOT",
                "side": "BUY",
                "type": "LIMIT",
                "timeInForce": "GTC",
                "quantity": float(f"{sweep_base:.6f}"),
                "price": round(ask * 1.001, 2),
            }
            fut_order = {
                "symbol": fut_symbol,
                "account": "COIN-M",
                "side": "SELL",
                "type": "LIMIT",
                "timeInForce": "GTC",
                "quantity": int(sweep_cts),
                "price": round(mark * 0.999, 1),
            }

            sresp = self.sim.place_order(spot_order)
            fresp = self.sim.place_order(fut_order)

            filled_base = spot_order["quantity"] if sresp["status"] == "filled" else 0.0
            filled_cts = fut_order["quantity"] if fresp["status"] == "filled" else 0

            bought_spot += filled_base
            shorted_contracts += filled_cts

            carry_base = sweep_base - filled_base
            carry_cts = sweep_cts - filled_cts
            remaining_base = max(0.0, remaining_base - filled_base)

            print(
                f"[SWEEP] spot {sresp['status']} + fut {fresp['status']} | +{filled_base:.6f} {base}, -{filled_cts} cts"
            )

        # ---------- end-of-loop reconciliation (re-hedge to delta-neutral) ----------
        S_final = self.scanner._spot_mid(spot_symbol)
        contracts_per_1_base_final = S_final / float(fut_filters["contract_size"])
        intended_cts = int(round(bought_spot * contracts_per_1_base_final))
        ct_diff = (
            intended_cts - shorted_contracts
        )  # + => need more shorts (SELL), - => too many shorts (BUY)

        if abs(ct_diff) > hedge_tolerance:
            mark, _ = self.scanner._mark_index(fut_symbol)
            side = "SELL" if ct_diff > 0 else "BUY"
            corr_order = {
                "symbol": fut_symbol,
                "account": "COIN-M",
                "side": side,
                "type": "LIMIT",
                "timeInForce": "GTC",
                "quantity": abs(int(ct_diff)),
                "price": round(mark * (0.999 if side == "SELL" else 1.001), 1),
            }
            corr_resp = self.sim.place_order(corr_order)
            if corr_resp["status"] == "filled":
                shorted_contracts += ct_diff  # move directly toward intended
                print(
                    f"[REHEDGE] {side} {abs(ct_diff)} cts filled. Now {shorted_contracts} vs intended {intended_cts}."
                )
            else:
                print(f"[REHEDGE] {side} {abs(ct_diff)} cts not filled (sim).")

        invested_usdt = bought_spot * S_final
        print("--- OPEN SUMMARY ---")
        print(
            f"Bought spot:       {bought_spot:.6f} {base}  (~{invested_usdt:,.0f} USDT)"
        )
        print(f"Shorted futures:   {shorted_contracts} contracts ({fut_symbol})")
        print(
            f"Slices filled:     {filled_slices}/{slices} (canceled {canceled_slices})"
        )

        return {
            "base": base,
            "spot_symbol": spot_symbol,
            "future_symbol": fut_symbol,
            "contract_type": "DELIVERY_OR_QUARTERLY",
            "days_to_expiry": None,
            "invest_target_usdt": invest_usdt,
            "invested_usdt_est": invested_usdt,
            "spot_bought": bought_spot,
            "fut_contracts_sold": shorted_contracts,
            "slices": slices,
            "filled_slices": filled_slices,
            "canceled_slices": canceled_slices,
            "hedge_ratio_contracts_per_base": contracts_per_1_base_final,
        }

    def close_twap(self, position_report, slices=24, mode="fast"):
        base = position_report["base"]
        spot_symbol = position_report["spot_symbol"]
        fut_symbol = position_report["future_symbol"]

        spot_qty_total = position_report["spot_bought"]
        fut_cts_total = position_report["fut_contracts_sold"]

        spot_per_slice = max(spot_qty_total / slices, 1e-6)
        cts_per_slice = max(int(round(fut_cts_total / slices)), 1)

        sold_spot = 0.0
        bought_back_cts = 0

        print(f"--- TWAP CLOSE ({slices} slices) ---")
        for i in range(slices):
            bt = self.spot.book_ticker(symbol=spot_symbol)
            bid = float(bt["bidPrice"])
            mark, _ = self.scanner._mark_index(fut_symbol)

            spot_order = {
                "symbol": spot_symbol,
                "account": "SPOT",
                "side": "SELL",
                "type": "LIMIT",
                "timeInForce": "GTC",
                "quantity": float(f"{spot_per_slice:.6f}"),
                "price": round(bid * 0.9995, 2),
            }
            fut_order = {
                "symbol": fut_symbol,
                "account": "COIN-M",
                "side": "BUY",
                "type": "LIMIT",
                "timeInForce": "GTC",
                "quantity": int(cts_per_slice),
                "price": round(mark * 1.001, 1),
            }

            sresp = self.sim.place_order(spot_order)
            fresp = self.sim.place_order(fut_order)

            if sresp["status"] == "filled":
                sold_spot += spot_order["quantity"]
            if fresp["status"] == "filled":
                bought_back_cts += fut_order["quantity"]

            print(
                f"[{i+1:02d}/{slices}] close slice -> spot {sresp['status']}, fut {fresp['status']}"
            )

            if mode == "real_time":
                time.sleep(3600)
            else:
                time.sleep(0.2)

        print("--- CLOSE SUMMARY ---")
        print(f"Sold spot:       {sold_spot:.6f} {base}")
        print(f"Covered futures: {bought_back_cts} contracts")

        return {
            "sold_spot": sold_spot,
            "covered_contracts": bought_back_cts,
            "slices": slices,
        }


if __name__ == "__main__":
    client = BinanceSimulator()
    spot = client.spot
    cm = client.cm_future
    scanner = BasisScanner(spot, cm)

    execer = ExecutionTWAP(spot, cm, client, scanner)
    report = execer.open_twap(
        invest_usdt=1_000_000,
        base="BTC",
        spot_symbol="BTCUSDT",
        slices=24,
        mode="fast",
        use_scanner=True,
        min_net_ann=None,  # set 0.0 to require positive EV; None to take best delivery
        hedge_tolerance=5,
    )
