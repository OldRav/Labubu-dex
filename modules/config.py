from collections import defaultdict
import asyncio


TOKENS_DATA = {
    "ETH": {
	    "market_id": 0,
		"min_base_amount": 0.005,
		"size_decimals": 4,
		"price_decimals": 2
  },
	"BTC": {
		"market_id": 1,
		"min_base_amount": 0.0002,
		"size_decimals": 5,
		"price_decimals": 1
  },
	"SOL": {
		"market_id": 2,
		"min_base_amount": 0.05,
		"size_decimals": 3,
		"price_decimals": 3
  },
	"DOGE": {
		"market_id": 3,
		"min_base_amount": 10.0,
		"size_decimals": 0,
		"price_decimals": 6
  },
	"1000PEPE": {
		"market_id": 4,
		"min_base_amount": 500.0,
		"size_decimals": 0,
		"price_decimals": 6
  },
	"WIF": {
		"market_id": 5,
		"min_base_amount": 5.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"WLD": {
		"market_id": 6,
		"min_base_amount": 5.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"XRP": {
		"market_id": 7,
		"min_base_amount": 20.0,
		"size_decimals": 0,
		"price_decimals": 6
  },
	"LINK": {
		"market_id": 8,
		"min_base_amount": 1.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"AVAX": {
		"market_id": 9,
		"min_base_amount": 0.5,
		"size_decimals": 2,
		"price_decimals": 4
  },
	"NEAR": {
		"market_id": 10,
		"min_base_amount": 2.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"DOT": {
		"market_id": 11,
		"min_base_amount": 2.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"TON": {
		"market_id": 12,
		"min_base_amount": 2.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"TAO": {
		"market_id": 13,
		"min_base_amount": 0.05,
		"size_decimals": 3,
		"price_decimals": 3
  },
	"POL": {
		"market_id": 14,
		"min_base_amount": 40.0,
		"size_decimals": 0,
		"price_decimals": 6
  },
	"TRUMP": {
		"market_id": 15,
		"min_base_amount": 0.2,
		"size_decimals": 2,
		"price_decimals": 4
  },
	"SUI": {
		"market_id": 16,
		"min_base_amount": 3.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"1000SHIB": {
		"market_id": 17,
		"min_base_amount": 500.0,
		"size_decimals": 0,
		"price_decimals": 6
  },
	"1000BONK": {
		"market_id": 18,
		"min_base_amount": 500.0,
		"size_decimals": 0,
		"price_decimals": 6
  },
	"1000FLOKI": {
		"market_id": 19,
		"min_base_amount": 100.0,
		"size_decimals": 0,
		"price_decimals": 6
  },
	"BERA": {
		"market_id": 20,
		"min_base_amount": 3.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"FARTCOIN": {
		"market_id": 21,
		"min_base_amount": 20.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"AI16Z": {
		"market_id": 22,
		"min_base_amount": 20.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"POPCAT": {
		"market_id": 23,
		"min_base_amount": 40.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"HYPE": {
		"market_id": 24,
		"min_base_amount": 0.5,
		"size_decimals": 2,
		"price_decimals": 4
  },
	"BNB": {
		"market_id": 25,
		"min_base_amount": 0.02,
		"size_decimals": 2,
		"price_decimals": 4
  },
	"JUP": {
		"market_id": 26,
		"min_base_amount": 15.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"AAVE": {
		"market_id": 27,
		"min_base_amount": 0.05,
		"size_decimals": 3,
		"price_decimals": 3
  },
	"MKR": {
		"market_id": 28,
		"min_base_amount": 0.005,
		"size_decimals": 4,
		"price_decimals": 2
  },
	"ENA": {
		"market_id": 29,
		"min_base_amount": 20.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"UNI": {
		"market_id": 30,
		"min_base_amount": 1.0,
		"size_decimals": 2,
		"price_decimals": 4
  },
	"APT": {
		"market_id": 31,
		"min_base_amount": 2.0,
		"size_decimals": 2,
		"price_decimals": 4
  },
	"SEI": {
		"market_id": 32,
		"min_base_amount": 50.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"KAITO": {
		"market_id": 33,
		"min_base_amount": 10.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"IP": {
		"market_id": 34,
		"min_base_amount": 2.0,
		"size_decimals": 2,
		"price_decimals": 4
  },
	"LTC": {
		"market_id": 35,
		"min_base_amount": 0.1,
		"size_decimals": 3,
		"price_decimals": 3
  },
	"CRV": {
		"market_id": 36,
		"min_base_amount": 20.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"PENDLE": {
		"market_id": 37,
		"min_base_amount": 3.0,
		"size_decimals": 2,
		"price_decimals": 4
  },
	"ONDO": {
		"market_id": 38,
		"min_base_amount": 10.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"ADA": {
		"market_id": 39,
		"min_base_amount": 10.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"S": {
		"market_id": 40,
		"min_base_amount": 20.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"VIRTUAL": {
		"market_id": 41,
		"min_base_amount": 5.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"SPX": {
		"market_id": 42,
		"min_base_amount": 5.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"TRX": {
		"market_id": 43,
		"min_base_amount": 40.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"SYRUP": {
		"market_id": 44,
		"min_base_amount": 20.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"PUMP": {
		"market_id": 45,
		"min_base_amount": 2000.0,
		"size_decimals": 0,
		"price_decimals": 6
  },
	"LDO": {
		"market_id": 46,
		"min_base_amount": 5.0,
		"size_decimals": 1,
		"price_decimals": 5
  },
	"PENGU": {
		"market_id": 47,
		"min_base_amount": 200.0,
		"size_decimals": 0,
		"price_decimals": 6
  }
}

address_locks = defaultdict(asyncio.Lock)

class MultiLock:
    def __init__(self, addresses: list[str]):
        self.locks = [address_locks[addr] for addr in sorted(addresses)]
        self.acquired: list[asyncio.Lock] = []

    async def __aenter__(self):
        while True:
            if all(not lock.locked() for lock in self.locks):
                for lock in self.locks:
                    await lock.acquire()
                    self.acquired.append(lock)
                return self
            await asyncio.sleep(1)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for lock in reversed(self.acquired):
            lock.release()
        self.acquired.clear()
