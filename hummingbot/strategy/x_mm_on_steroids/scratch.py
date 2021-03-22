from decimal import Decimal
import logging
from typing import List, Dict, Any, Set
import asyncio
from hummingbot.core.clock import Clock

from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.strategy_py_base import StrategyPyBase
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.core.event.events import TradeType, OrderType, PositionAction
from hummingbot.connector.derivative.binance_perpetual.binance_perpetual_derivative import BinancePerpetualDerivative
from hummingbot.connector.derivative.binance_perpetual.binance_perpetual_utils import convert_from_exchange_trading_pair

conn = BinancePerpetualDerivative("Nx00jeW2kiQHPLYmOI68kzMLYH1FnHdKVnj1yW3MvS0J2gPc3hJZSkNxxfx6TGW4",
                                  "PigBne0jl7hcumX7gDhTJ70hQvfEcASXxT9a627PWXZ8M79abnSWzD4YgskC4Ho3",
                                  "BTC-USDT")

cur_mid_prices = {}


async def async_main():
    await mid_price_polling_loop()


async def mid_price_polling_loop():
    while True:
        try:
            ret_vals = await conn.request(path="/fapi/v1/ticker/price")
            for ret_val in ret_vals:
                try:
                    pair = convert_from_exchange_trading_pair(ret_val["symbol"])
                    cur_mid_prices[pair] = Decimal(str(ret_val["price"]))
                except Exception:
                    continue
            print(cur_mid_prices)
        except asyncio.CancelledError:
            raise
        finally:
            await asyncio.sleep(0.5)

async def try_buy_sell():
    conn.buy("BTC-USDT", Decimal("20"), OrderType.LIMIT, Decimal("55000"), position_action=PositionAction.OPEN)
    asyncio.sleep(5)
    pass

def main():
    pass


if __name__ == "__main__":
    # main()
    asyncio.get_event_loop().run_until_complete(try_buy_sell())