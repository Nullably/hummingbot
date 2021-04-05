from decimal import Decimal
import logging
from typing import List, Dict, Any
import asyncio
from statistics import mean
import pandas as pd
import numpy as np
import time

from hummingbot.core.clock import Clock
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.event.events import OrderType, PositionMode, TradeType
from hummingbot.strategy.strategy_py_base import StrategyPyBase
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.derivative.binance_perpetual.binance_perpetual_utils import convert_from_exchange_trading_pair
from hummingbot.strategy.x_mm_on_steroids.data_types import Proposal, PriceAmount

NaN = float("nan")
s_decimal_zero = Decimal("0")
s_decimal_nan = Decimal("NaN")
lms_logger = None


class XMmOnSteroidsStrategy(StrategyPyBase):

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global lms_logger
        if lms_logger is None:
            lms_logger = logging.getLogger(__name__)
        return lms_logger

    def __init__(self,
                 market_infos: List[MarketTradingPairTuple],
                 parameters: Dict[str, Any]
                 ):
        super().__init__()
        self._exchange = market_infos[0].market
        self._markets = list(parameters["mm_markets"].split(","))
        self._markets = sorted(self._markets)
        self._market_infos = {}
        for market in self._markets:
            base, quote = market.split("-")
            self._market_infos[market] = MarketTradingPairTuple(self._exchange, market, base, quote)
        self._token = parameters["token"]
        self._position_size = Decimal(str(parameters["position_size"]))
        self._spread = Decimal(str(parameters["spread"])) / Decimal("100")
        self._order_refresh_time = float(str(parameters["order_refresh_time"]))
        self._order_refresh_tolerance_pct = Decimal(str(parameters["order_refresh_tolerance_pct"])) / Decimal("100")
        self._volatility_interval = int(str(parameters["volatility_interval"]))
        self._avg_volatility_period = int(str(parameters["avg_volatility_period"]))
        self._volatility_to_spread_multiplier = Decimal(str(parameters["volatility_to_spread_multiplier"]))
        self._steroids_level = int(str(parameters["steroids_level"]))

        self._short_budgets = {}
        self._long_budgets = {}
        self._refresh_times = {market: 0 for market in self._markets}
        self._mid_prices = {market: [] for market in self._markets}
        self._cur_mid_prices = {}
        self._volatility = {market: s_decimal_nan for market in self._markets}
        self._ready_to_trade = False
        self._mid_price_polling_task = None

        self._last_proposals_created = 0.

        self.add_markets([self._exchange])

    @property
    def active_orders(self):
        limit_orders = self.order_tracker.active_limit_orders
        return [o[1] for o in limit_orders]

    def tick(self, timestamp: float):
        """
        Clock tick entry point, is run every second (on normal tick setting).
        :param timestamp: current tick timestamp
        """
        if not self._ready_to_trade:
            # Check if there are restored orders, they should be canceled before strategy starts.
            self._ready_to_trade = self._exchange.ready and (len(self._exchange.limit_orders) == 0 and
                                                             len(self._cur_mid_prices) > 0)
            if not self._ready_to_trade:
                self.logger().warning(f"{self._exchange.name} is not ready. Please wait...")
                return
            else:
                self.logger().info(f"{self._exchange.name} is ready. Trading started.")
        self.update_mid_prices()
        self.update_volatility()
        self.update_budgets()
        proposals = self.create_base_proposals()
        # self.apply_budgets(proposals)
        self.apply_budget_constraint(proposals)
        self.cancel_active_orders(proposals)
        self.execute_orders_proposal(proposals)

        self._last_timestamp = timestamp

    @staticmethod
    def order_age(order: LimitOrder) -> float:
        if "//" not in order.client_order_id:
            return int(time.time()) - int(order.client_order_id[-16:]) / 1e6
        return -1.

    async def active_orders_df(self) -> pd.DataFrame:
        columns = ["Market", " Side", " Price", " Spread", " Size(USDT)", " Age"]
        data = []
        for order in self.active_orders:
            mid_price = self.get_mid_price(order.trading_pair)
            spread = 0 if mid_price == 0 else abs(order.price - mid_price) / mid_price
            size = order.quantity * mid_price
            age = self.order_age(order)
            # // indicates order is a paper order so 'n/a'. For real orders, calculate age.
            age_txt = "n/a" if age <= 0. else pd.Timestamp(age, unit='s').strftime('%H:%M:%S')
            data.append([
                order.trading_pair,
                "long" if order.is_buy else "short",
                float(order.price),
                f"{spread:.2%}",
                f"{size:.0f}",
                age_txt
            ])
        df = pd.DataFrame(data=data, columns=columns)
        df.sort_values(by=["Market", " Side"], inplace=True)
        return df

    def position_status_df(self) -> pd.DataFrame:
        data = []
        columns = ["Market", " Position", f" Size ({self._token})", " Unrealized PNL", " Entry Price", " Steroids"]
        for position in self.active_positions:
            data.append([
                position.trading_pair,
                "long" if position.amount > s_decimal_zero else "short",
                f"{position.amount * position.entry_price:.0f}",
                f"{position.unrealized_pnl:.2f}",
                position.entry_price,
                f"{position.leverage}x"
            ])
        df = pd.DataFrame(data=data, columns=columns).replace(np.nan, '', regex=True)
        df.sort_values(by=["Market"], inplace=True)
        return df

    def market_status_df(self) -> pd.DataFrame:
        data = []
        columns = ["Market", " Mid price", " Volatility"]
        for market, market_info in self._market_infos.items():
            mid_price = self.get_mid_price(market)
            data.append([
                market,
                float(mid_price),
                "" if self._volatility[market].is_nan() else f"{self._volatility[market]:.2%}",
            ])
        df = pd.DataFrame(data=data, columns=columns).replace(np.nan, '', regex=True)
        df.sort_values(by=["Market"], inplace=True)
        return df

    async def format_status(self) -> str:
        if not self._ready_to_trade:
            return "Market connectors are not ready."
        lines = []
        warning_lines = []
        warning_lines.extend(self.network_warning(list(self._market_infos.values())))

        lines.extend([f"Available Balance: {self._exchange.get_available_balance(self._token)} {self._token}"])

        pos_df = self.position_status_df()
        if not pos_df.empty:
            lines.extend(["", "  Positions:"] + ["    " + line for line in pos_df.to_string(index=False).split("\n")])

        market_df = self.market_status_df()
        lines.extend(["", "  Markets:"] + ["    " + line for line in market_df.to_string(index=False).split("\n")])

        # See if there're any open orders.
        if len(self.active_orders) > 0:
            df = await self.active_orders_df()
            lines.extend(["", "  Orders:"] + ["    " + line for line in df.to_string(index=False).split("\n")])
        else:
            lines.extend(["", "  No active maker orders."])

        warning_lines.extend(self.balance_warning(list(self._market_infos.values())))
        if len(warning_lines) > 0:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)
        return "\n".join(lines)

    def start(self, clock: Clock, timestamp: float):
        restored_orders = self._exchange.limit_orders
        for order in restored_orders:
            self._exchange.cancel(order.trading_pair, order.client_order_id)
        for market in self._market_infos:
            self._exchange.set_leverage(market, self._steroids_level)
        self._exchange.set_position_mode(PositionMode.ONEWAY)
        self._mid_price_polling_task = safe_ensure_future(self.mid_price_polling_loop())

    def stop(self, clock: Clock):
        if self._mid_price_polling_task is not None:
            self._mid_price_polling_task.cancel()
            self._mid_price_polling_task = None

    def create_base_proposals(self):
        proposals = []
        # if self._last_proposals_created > self.current_timestamp - 5:
        #     return proposals
        for market, market_info in self._market_infos.items():
            spread = self._spread
            if not self._volatility[market].is_nan():
                # volatility applies only when it is higher than the spread setting.
                spread = max(spread, self._volatility[market] * self._volatility_to_spread_multiplier)
            mid_price = self.get_mid_price(market)
            buy_price = mid_price * (Decimal("1") - spread)
            buy_price = self._exchange.quantize_order_price(market, buy_price)
            buy_amount = s_decimal_zero
            sell_price = mid_price * (Decimal("1") + spread)
            sell_price = self._exchange.quantize_order_price(market, sell_price)
            sell_amount = s_decimal_zero
            proposals.append(Proposal(market, PriceAmount(buy_price, buy_amount), PriceAmount(sell_price, sell_amount)))
        # self._last_proposals_created = self.current_timestamp
        return proposals

    def update_budgets(self):
        # Update buy and sell budgets for every market
        self._short_budgets = {m: s_decimal_zero for m in self._market_infos}
        self._long_budgets = {m: s_decimal_zero for m in self._market_infos}
        for market, market_info in self._market_infos.items():
            position = [p for p in self.active_positions if p.trading_pair == market]
            if len(position) > 1:
                raise Exception("This strategy supports only oneway position.")
            position_amount = s_decimal_zero
            if len(position) > 0:
                position_amount = position[0].amount * position[0].entry_price
            self._long_budgets[market] = self._position_size - position_amount
            self._short_budgets[market] = abs(- self._position_size - position_amount)

    def apply_budget_constraint(self, proposals: List[Proposal]):
        quote_bal = self._exchange.get_available_balance(self._token) * self._steroids_level
        # self.logger().info(f"Available balance: {self._exchange.get_available_balance(self._token)} ({quote_bal})")
        quote_bal = max(quote_bal, s_decimal_zero)
        for proposal in proposals:
            base, quote = proposal.market.split("-")
            fee = self._exchange.get_fee(base, quote, OrderType.LIMIT, TradeType.BUY, s_decimal_zero, s_decimal_zero)
            for prop_side in [proposal.buy, proposal.sell]:
                is_buy = True if prop_side == proposal.buy else False
                free_size = sum((o.price * o.quantity) * (Decimal("1") + fee.percent) for o in self.active_orders
                                if o.trading_pair == proposal.market and o.is_buy == is_buy)
                order_size = self._long_budgets[proposal.market] if is_buy else self._short_budgets[proposal.market]
                free_size = min(free_size, order_size)
                need_bal_size = order_size - free_size
                need_bal_size = min(need_bal_size, quote_bal)
                order_size = need_bal_size + free_size
                # min of 10 USDT per order
                order_size = s_decimal_zero if order_size < 10 else order_size
                prop_side.amount = order_size / (prop_side.price * (Decimal("1") + fee.percent))
                prop_side.amount = self._exchange.quantize_order_amount(proposal.market, prop_side.amount)
                quote_bal -= need_bal_size
        # self.logger().info("Proposals after applied budget")
        # for proposal in proposals:
        #     self.logger().info(proposal)

    def is_within_tolerance(self, cur_orders: List[LimitOrder], proposal: Proposal):
        cur_buy = [o for o in cur_orders if o.is_buy]
        cur_sell = [o for o in cur_orders if not o.is_buy]
        if (cur_buy and proposal.buy.amount <= 0) or (cur_sell and proposal.sell.amount <= 0):
            return False
        if cur_buy and \
                abs(proposal.buy.price - cur_buy[0].price) / cur_buy[0].price > self._order_refresh_tolerance_pct:
            return False
        if cur_sell and \
                abs(proposal.sell.price - cur_sell[0].price) / cur_sell[0].price > self._order_refresh_tolerance_pct:
            return False
        return True

    def cancel_active_orders(self, proposals: List[Proposal]):
        for proposal in proposals:
            to_cancel = False
            cur_orders = [o for o in self.active_orders if o.trading_pair == proposal.market]
            if self._refresh_times[proposal.market] <= self.current_timestamp and \
                    cur_orders and not self.is_within_tolerance(cur_orders, proposal):
                to_cancel = True
            if to_cancel:
                for order in cur_orders:
                    self.cancel_order(self._market_infos[proposal.market], order.client_order_id)
                    # To place new order on the next tick
                    self._refresh_times[order.trading_pair] = self.current_timestamp + 0.1

    def execute_orders_proposal(self, proposals: List[Proposal]):
        for proposal in proposals:
            cur_orders = [o for o in self.active_orders if o.trading_pair == proposal.market]
            if cur_orders or self._refresh_times[proposal.market] > self.current_timestamp:
                continue
            mid_price = self.get_mid_price(proposal.market)
            spread = s_decimal_zero
            if proposal.buy.amount > 0:
                spread = abs(proposal.buy.price - mid_price) / mid_price
                self.logger().info(f"({proposal.market}) Creating a bid order {proposal.buy} value: "
                                   f"{proposal.buy.amount * proposal.buy.price:.2f} {proposal.quote()} spread: "
                                   f"{spread:.2%}")
                self.buy_with_specific_market(
                    self._market_infos[proposal.market],
                    proposal.buy.amount,
                    order_type=OrderType.LIMIT,
                    price=proposal.buy.price
                )
            if proposal.sell.amount > 0:
                spread = abs(proposal.sell.price - mid_price) / mid_price
                self.logger().info(f"({proposal.market}) Creating an ask order at {proposal.sell} value: "
                                   f"{proposal.sell.amount * proposal.sell.price:.2f} {proposal.quote()} spread: "
                                   f"{spread:.2%}")
                self.sell_with_specific_market(
                    self._market_infos[proposal.market],
                    proposal.sell.amount,
                    order_type=OrderType.LIMIT,
                    price=proposal.sell.price
                )
            if proposal.buy.amount > 0 or proposal.sell.amount > 0:
                if not self._volatility[proposal.market].is_nan() and spread > self._spread:
                    adjusted_vol = self._volatility[proposal.market] * self._volatility_to_spread_multiplier
                    if adjusted_vol > self._spread:
                        self.logger().info(f"({proposal.market}) Spread is widened to {spread:.2%} due to high "
                                           f"market volatility")

                self._refresh_times[proposal.market] = self.current_timestamp + self._order_refresh_time

    @property
    def active_positions(self) -> List[Position]:
        return list(self._exchange._account_positions.values())

    def get_mid_price(self, trading_pair: str) -> Decimal:
        return self._cur_mid_prices[trading_pair]

    def update_mid_prices(self):
        for market in self._market_infos:
            mid_price = self.get_mid_price(market)
            self._mid_prices[market].append(mid_price)
            # To avoid memory leak, we store only the last part of the list needed for volatility calculation and spread bias
            max_len = self._volatility_interval * self._avg_volatility_period * 10
            self._mid_prices[market] = self._mid_prices[market][-1 * max_len:]

    def update_volatility(self):
        self._volatility = {market: s_decimal_nan for market in self._market_infos}
        for market, mid_prices in self._mid_prices.items():
            last_index = len(mid_prices) - 1
            atr = []
            first_index = last_index - (self._volatility_interval * self._avg_volatility_period)
            first_index = max(first_index, 0)
            for i in range(last_index, first_index, self._volatility_interval * -1):
                prices = mid_prices[i - self._volatility_interval + 1: i + 1]
                if not prices:
                    break
                atr.append((max(prices) - min(prices)) / min(prices))
            if atr:
                self._volatility[market] = mean(atr)

    async def mid_price_polling_loop(self):
        while True:
            try:
                ret_vals = await self._exchange.request(path="/fapi/v1/ticker/price")
                for ret_val in ret_vals:
                    try:
                        pair = convert_from_exchange_trading_pair(ret_val["symbol"])
                        self._cur_mid_prices[pair] = Decimal(str(ret_val["price"]))
                    # ignore errors in conversion
                    except Exception:
                        continue
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while fetching Binance future mid prices.", exc_info=True)
            finally:
                await asyncio.sleep(1)

    async def start_network(self):
        self._exchange.set_leverage()

    def did_fill_order(self, event):
        order_id = event.order_id
        market_info = self.order_tracker.get_shadow_market_pair_from_order_id(order_id)
        if market_info is not None:
            if event.trade_type is TradeType.BUY:
                msg = f"({market_info.trading_pair}) Maker BUY order (price: {event.price}) of {event.amount} " \
                      f"{market_info.base_asset} is filled."
                self.log_with_clock(logging.INFO, msg)
                self.notify_hb_app(msg)
            else:
                msg = f"({market_info.trading_pair}) Maker SELL order (price: {event.price}) of {event.amount} " \
                      f"{market_info.base_asset} is filled."
                self.log_with_clock(logging.INFO, msg)
                self.notify_hb_app(msg)

    def notify_hb_app(self, msg: str):
        from hummingbot.client.hummingbot_application import HummingbotApplication
        HummingbotApplication.main_application()._notify(msg)
