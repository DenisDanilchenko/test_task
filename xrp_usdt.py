from __future__ import annotations
from enum import Enum
from datetime import datetime, timedelta
from aiohttp.client import ClientSession
from pydantic import parse_obj_as, BaseModel
from loguru import logger
import asyncio


class CoinSymbol(str, Enum):
    XRPUSDT = "XRPUSDT"


class SymbolPriceTick(BaseModel):
    symbol: CoinSymbol
    price: float
    time: int


class RequestError(Exception):
    pass


class CoinPriceTick:
    """
    Represent Binance symbol price tick. Using for set(), sorted() and __str__() functions
    """
    def __init__(self, price_tick: SymbolPriceTick) -> None:
        self.symbol = price_tick.symbol
        self.price = price_tick.price
        self.time = datetime.fromtimestamp(price_tick.time/1000)
    

    def __str__(self) -> str:
        time = self.time.strftime(r'%H:%M:%S.%f')[:-3:]
        return f"<Tick: {self.symbol.value} price {self.price}, time {time}"
    

    def __hash__(self) -> int:
        return hash((self.symbol, self.price, self.time))
    

    def __eq__(self, __o: object) -> bool:
        if type(self) != type(__o) or self.symbol != __o.symbol:
            raise NotImplementedError(f"Can not compare {type(self)} and {type(__o)}")
        else:
            return self.price == __o.price and self.time == __o.time
    

    def __lt__(self, __o: object) -> bool:
        if type(self) != type(__o) or self.symbol != __o.symbol:
            raise NotImplementedError(f"Can not compare {type(self)} and {type(__o)}")
        else:
            return self.time < __o.time
    

    def is_trigger(self, window_prices: HourWindowPrices) -> bool:
        return window_prices.max_price * 0.99 >= self.price


class HourWindowPrices:

    def __init__(self, symbol: CoinSymbol) -> None:
        self.sequence: list[CoinPriceTick] = []
        self._symbol = symbol
        self.max_price = float("nan")
    

    def _add_tick(self, price_tick: CoinPriceTick) -> bool:
        """
        Add tick to sequence, remove duplicate ticks, return true if tick updated sequence.
        """
        if price_tick.symbol != self._symbol:
            raise NotImplementedError(
                f"Price tick {price_tick.symbol.value} not equal window symbol {self._symbol.value}"
            )
        origin_lenght = len(self.sequence)
        self.sequence.append(price_tick)
        self.sequence = list(set(self.sequence))
        self.sequence.sort()
        new_lenght = len(self.sequence)
        return origin_lenght < new_lenght


    def _update_time_border(self) -> bool:
        """
        Check the lower time border. Remove out-of-bounds values from a sequence.
        Return True if lower time border updated.
        """
        last = self.sequence[-1]
        min_border = last.time - timedelta(hours=1)
        is_border_update = False
        for tick in self.sequence:
            if tick.time < min_border:
                self.sequence.pop(0)
                is_border_update = True
            else:
                break
        return is_border_update
    

    def update(self, price_tick: CoinPriceTick) -> bool:
        """
        Update sequence by tick, return true if tick updated sequence.
        """
        is_new_element = self._add_tick(price_tick=price_tick)
        self._update_time_border()
        self.max_price = max([tick.price for tick in self.sequence])
        if is_new_element:
            logger.debug(f"New sequence lenght is {len(self.sequence)}, max price is {self.max_price}")
            logger.debug(f"Updated by {price_tick}")
        return is_new_element


async def make_request(session: ClientSession, url, params, window_prices: HourWindowPrices):
    """
    Create request to exchange, check for condition.
    """
    async with session.get(url=url, params=params) as response:
        match response.status:
            case 200:
                payload = await response.json()
                price_tick = CoinPriceTick(parse_obj_as(SymbolPriceTick, payload))
                is_new_element = window_prices.update(price_tick)
                if is_new_element:
                    if price_tick.is_trigger(window_prices):
                        notification = f"{price_tick} "
                        notification += f"is below the hourly high = {window_prices.max_price} by 1%"
                        logger.warning(notification)
            case _:
                headers = response.headers
                payload = await response.text()
                logger.error(f"\n{headers=}\n{payload=}")
                raise RequestError()


async def main():
    """
    Create client session and infinite loop inside which the data from the exchange is updated.
    """
    async with ClientSession(base_url='https://fapi.binance.com') as session:
        url = "/fapi/v1/ticker/price"
        params = dict(symbol=CoinSymbol.XRPUSDT.value)
        window_prices = HourWindowPrices(symbol=CoinSymbol.XRPUSDT)
        loop = asyncio.get_running_loop()
        while True:
            coro = make_request(
                session=session, url=url, params=params, window_prices=window_prices
            )
            loop.call_later(0, asyncio.create_task, coro)
            await asyncio.sleep(0.025) # One request is weight eq. 1. Weight limit per minute is 2400


logger.add(sink='/home/homework_mashine/test_binance/logs.log', level='DEBUG')
asyncio.run(main())