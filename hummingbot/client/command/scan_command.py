import aiohttp
import pandas as pd
from statistics import mean
from typing import TYPE_CHECKING, List
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.connector.derivative.binance_perpetual.binance_perpetual_api_order_book_data_source import BinancePerpetualAPIOrderBookDataSource

if TYPE_CHECKING:
    from hummingbot.client.hummingbot_application import HummingbotApplication

OPTIONS = [
    "top_proximity"
]
kline_url = "https://fapi.binance.com/fapi/v1/klines"
pair_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"


class ScanCommand:
    def scan(self,  # type: HummingbotApplication
             option: str = None,
             args: List[str] = None
             ):
        self.app.clear_input()
        if option == OPTIONS[0]:
            safe_ensure_future(self.show_top_proximity())

    async def show_top_proximity(self):
        self._notify("This scan retrieves 500 day kline data from Binance Futures on every market to "
                     "calculate top proximity metrics.")
        self._notify("Retrieving data from Binance Futures... this will take about a minute.")
        lines = []
        df = await self.top_proximity_df()
        lines.extend(["", "Top Proximity:"] + [line for line in df.to_string(index=False).split("\n")])
        msg = "\n".join(lines)
        self._notify(msg)
        self._notify("Top proximity is (last_price - top)/top. The closer to 0, the closer it is to the top "
                     "(from 500 days data).")

    async def top_proximity_df(self) -> pd.DataFrame:
        data = await self.top_proximity_data()
        columns = ["Market", "Last Price", "*Top Proximity", "Avg 10d Vol(USDT)", "Avg 10d Volatility"]
        df = pd.DataFrame(data=data, columns=columns).replace(regex=True)
        return df

    async def top_proximity_data(self):
        pairs = await BinancePerpetualAPIOrderBookDataSource.fetch_trading_pairs()
        results = []
        for pair in pairs:
            params = {"limit": str(500), "interval": "1d", "symbol": pair.replace("-", "")}
            klines = await self.make_api_request(kline_url, params)
            ohlcs = [[float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])] for k in klines]
            max_high = max(o[1] for o in ohlcs)
            close = ohlcs[-1][3]
            close_to_max = (close - max_high) / max_high
            avg_10d_q_vol = mean(o[3] * o[4] for o in ohlcs) / 1e6
            avg_10d_volatility = mean((o[1] - o[2]) / o[2] for o in ohlcs)
            results.append([pair, close, close_to_max, avg_10d_q_vol, avg_10d_volatility])
        results.sort(key=lambda x: x[2], reverse=True)
        results = [[r[0], r[1], f"{r[2]:.2%}", f"{round(r[3])} M", f"{r[4]:.2%}"] for r in results]
        return results

    async def make_api_request(self, url, params):
        async with aiohttp.ClientSession() as client:
            resp = await client.get(url, params=params)
            resp_json = await resp.json()
        return resp_json
