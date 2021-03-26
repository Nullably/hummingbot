#!/usr/bin/env python
from decimal import Decimal


class PriceAmount:
    def __init__(self, price: Decimal, amount: Decimal):
        self.price: Decimal = price
        self.amount: Decimal = amount

    def __repr__(self):
        return f"[ p: {self.price} a: {self.amount} ]"


class Proposal:
    def __init__(self, market: str, buy: PriceAmount, sell: PriceAmount):
        self.market: str = market
        self.buy: PriceAmount = buy
        self.sell: PriceAmount = sell

    def __repr__(self):
        return f"{self.market} buy: {self.buy} sell: {self.sell}"

    def base(self):
        return self.market.split("-")[0]

    def quote(self):
        return self.market.split("-")[1]
