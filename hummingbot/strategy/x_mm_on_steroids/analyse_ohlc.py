import aiohttp
import requests
import time
import asyncio
import aiohttp
from statistics import mean
import time
from hummingbot.connector.derivative.binance_perpetual.binance_perpetual_api_order_book_data_source import BinancePerpetualAPIOrderBookDataSource
from hummingbot.strategy.x_mm_on_steroids.ohlc_fixture import all_hlc

kline_url = "https://fapi.binance.com/fapi/v1/klines"
pair_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"


async def async_main():
    pairs = await BinancePerpetualAPIOrderBookDataSource.fetch_trading_pairs()
    # print(f"{len(pairs)} pairs: {pairs}")
    # return
    for pair in pairs:
        params = {"limit": str(500), "interval": "4h", "symbol": pair.replace("-","")}
        klines = await make_request(kline_url, params)
        hlc = [[k[2], k[3], k[4]] for k in klines]
        await asyncio.sleep(0.01)
        print(f"'{pair}': {hlc}" +",")
    await asyncio.sleep(1000)
    #     highs = [k[2] for k in klines]
    # max_high, m_indexes = max_n_index(highs)
    # print(max_high)
    # print(m_indexes)

def max_n_index(a_list):
    max_high = max(a_list)
    max_indexes = [i for i, j in enumerate(a_list) if j == max_high]
    return max_high, max_indexes

async def make_request(url, params):
    async with aiohttp.ClientSession() as client:
        resp = await client.get(url, params=params)
        resp_json = await resp.json()
    return resp_json



def top_apes():
    top_ape_ind = {}
    ape_2_ind = {}
    for pair, hlc in all_hlc.items():
        # hlc = hlc[:-2]
        # hlc = hlc[-200:]
        if len(hlc) < 100:
            continue
        top = max(float(p[0]) for p in hlc)
        bottom = min(float(p[1]) for p in hlc)
        cur_price = float(hlc[-1][2])
        p_to_top = (cur_price - top)/top
        p_to_bottom = (cur_price - bottom)/bottom
        prices = [(float(p[0]) + float(p[1]))/2.0 for p in hlc[-20:]]
        avg_price = mean(prices)
        p_to_avg = abs(cur_price - avg_price)/avg_price
        # p_to_bottom = min(1., p_to_bottom)
        ape_2_ind[pair] = (p_to_top * 100.) + (p_to_bottom * 1.) - (p_to_avg * 10.)

        prices = [(float(p[0]) + float(p[1])) / 2.0 for p in hlc[-100:]]
        avg_price = mean(prices)
        # print(pair)
        # print(f"avg_price: {avg_price}")
        first_low = float(hlc[0][1])
        avg_to_first = (avg_price - first_low)/first_low
        # print(f"avg_to_price: {avg_to_first}")
        last_to_avg = (float(hlc[-1][2]) - avg_price)/avg_price
        # print(f"last_to_avg: {last_to_avg}")
        top_ape_value = avg_to_first * 0.25 - (last_to_avg )
        # print(f"top_ape_value: {top_ape_value}")
        top_ape_ind[pair] = top_ape_value
    # sorted_apes = sorted(top_ape_ind.items(), key=lambda x: x[1], reverse=True)
    sorted_apes = dict(sorted(ape_2_ind.items(), key=lambda item: item[1], reverse=True))
    for pair, ape in sorted_apes.items():
        print(f"{pair}: {ape}")

if __name__ == "__main__":
    top_apes()
    # asyncio.get_event_loop().run_until_complete(async_main())

