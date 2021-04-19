import requests
from decimal import Decimal
from dataclasses import dataclass
import pandas as pd
from hummingbot.connector.exchange.binance.binance_utils import convert_from_exchange_trading_pair

bk_base_url = "https://api.bitkub.com"
bnb_base_url = "https://api.binance.com"


@dataclass
class Ticker:
    pair: str
    bid: Decimal
    ask: Decimal
    quote_volume: Decimal = None

    @property
    def spread(self):
        return (self.ask - self.bid) / self.bid


@dataclass
class ArbData:
    pair: str
    bk_ask: Decimal
    ex_rate: Decimal
    bnb_bid: Decimal
    arb_profit: Decimal


def get_binance_tickers():
    resp = requests.get(f"{bnb_base_url}/api/v3/ticker/bookTicker")
    # resp = requests.get(f"https://fapi.binance.com/fapi/v1/ticker/bookTicker")
    records = resp.json()
    results = {}
    for record in records:
        if record["bidPrice"] is not None and record["askPrice"] is not None:
            pair = convert_from_exchange_trading_pair(record["symbol"])
            bid = Decimal(str(record["bidPrice"]))
            ask = Decimal(str(record["askPrice"]))
            if bid > 0 and ask > 0:
                results[pair] = Ticker(pair, bid, ask)
    return results


def get_bitkub_tickers():
    resp = requests.get(f"{bk_base_url}/api/market/ticker")
    records = resp.json()
    results = {}
    for r_pair, r_ticker in records.items():
        quote, base = r_pair.split("_")
        pair = f"{base}-{quote}"
        bid = Decimal(str(r_ticker["highestBid"]))
        ask = Decimal(str(r_ticker["lowestAsk"]))
        print(f"{pair} bid: {bid} ask: {ask}")
        results[pair] = Ticker(pair, bid, ask, Decimal(str(r_ticker["quoteVolume"])))
    # for result in results.values():
    #     print(result)
    return results


def get_bitazza_tickers():
    btc_rate = Decimal(2e6)
    resp = requests.get("https://apexapi.bitazza.com:8443/AP/Summary")
    records = resp.json()
    results = {}
    for record in records:
        base, quote = record["trading_pairs"].split("_")
        pair = f"{base}-{quote}"
        bid = Decimal(str(record["highest_bid"]))
        ask = Decimal(str(record["lowest_ask"]))
        q_vol = Decimal(str(record["quote_volume"]))
        if quote == "BTC":
            q_vol *= btc_rate
        if bid == 0 or ask == 0:
            continue
        print(f"{pair} bid: {bid} ask: {ask}")
        results[pair] = Ticker(pair, bid, ask, q_vol)
    # for result in results.values():
    #     print(result)
    return results


def arb_df(thai_tickers, binance_tickers, th_ex_name):
    thb_usdt_ticker = thai_tickers["USDT-THB"]
    data = []
    not_on_binance = []
    for pair, th_ticker in thai_tickers.items():
        base, quote = pair.split("-")
        bnb_quote = "USDT" if quote == "THB" else quote
        bnb_pair = f"{base}-{bnb_quote}"
        if bnb_pair not in binance_tickers:
            not_on_binance.append(pair)
            continue
        bnb_ticker = binance_tickers[bnb_pair]

        # buy_at_bk_price = bk_ticker.ask / thb_usdt_ticker.ask
        # buy_diff = (bnb_ticker.bid - buy_at_bk_price)/buy_at_bk_price
        th_bid_maker_price = th_ticker.bid / thb_usdt_ticker.bid if quote == "THB" else th_ticker.bid
        buy_diff = (bnb_ticker.bid - th_bid_maker_price) / th_bid_maker_price
        buy_diff = round(buy_diff * 100, 2)

        th_ask_taker_price = th_ticker.ask / thb_usdt_ticker.ask if quote == "THB" else th_ticker.ask
        sell_diff = (th_ask_taker_price - bnb_ticker.ask) / bnb_ticker.ask
        sell_diff = round(sell_diff * 100, 2)

        # data.append([bnb_pair, bk_ticker.ask, bnb_ticker.bid, buy_diff,
        #              bk_ticker.bid, bnb_ticker.ask, sell_diff, round(bk_ticker.spread * 100, 2)])
        data.append([bnb_pair,
                     th_ticker.bid, bnb_ticker.bid, thb_usdt_ticker.bid, buy_diff,
                     th_ticker.ask, bnb_ticker.ask, thb_usdt_ticker.ask, sell_diff,
                     round(th_ticker.spread * 100, 2),
                     f"{round(th_ticker.quote_volume/Decimal('1e6'), 2)}M"
                     ])
    # columns = ["pair", "bk_ask", "bnb_bid", "buy_profit", "bk_bid", "bnb_ask", "sell_profit", "bk_spread"]
    columns = ["pair",
               f"{th_ex_name}_bid", "bnb_bid", "usdt_bid", "buy_profit",
               f"{th_ex_name}_ask", "bnb_ask", "usdt_ask", "sell_profit",
               f"{th_ex_name}_spread",
               "bk_vol"]
    data = sorted(data, key=lambda x: x[8])
    df = pd.DataFrame(columns=columns, data=data)

    return df, not_on_binance, thb_usdt_ticker


def show_result():
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('display.max_columns', None)
    bnb_tickers = get_binance_tickers()
    th_tickers = get_bitkub_tickers()
    # th_tickers = get_bitazza_tickers()
    df, not_on_binance, thb_usdt_ticker = arb_df(th_tickers, bnb_tickers, "ba")
    print(df)
    print(f"Not on Binance: {not_on_binance}")
    print(f"thb_usdt_ticker: {thb_usdt_ticker}")


# resp = requests.get(f"https://fapi.binance.com/fapi/v1/ticker/bookTicker")
# print(resp.json())
# show_result()
show_result()
