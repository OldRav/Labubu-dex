from curl_cffi import AsyncSession as AsyncWebSocket
from random import choice, uniform, shuffle, randint
from decimal import Decimal
from loguru import logger
from time import time
import asyncio
import lighter
import json

from .database import DataBase
from .retry import async_retry, CustomError, NotEnoughMargin
from .utils import sleeping, round_cut, make_border, async_sleep
from settings import (
    SLEEP_BETWEEN_CLOSE_ORDERS,
    SLEEP_BETWEEN_OPEN_ORDERS,
    SLEEP_AFTER_FUTURE,
    STOP_LOSS_SETTING,
    TOKENS_TO_TRADE,
    FUTURES_LIMITS,
    FUTURE_ACTIONS,
    PAIR_SETTINGS,
    TRADE_AMOUNTS,
    CANCEL_ORDERS,
    RETRY,
)


class LighterAccount:
    API_KEY_INDEX: int = 0
    BASE_URL: str = "https://mainnet.zklighter.elliot.ai"

    switch_params: dict = {
        "Long": "Short",
        "Short": "Long",
    }

    TOKENS_DATA: dict = {}

    def __init__(
            self,
            privatekey: str,
            encoded_privatekey: str,
            address: str,
            label: str,
            proxy: str | None,
            db: DataBase,
            group_data: dict = None
    ):
        self.privatekey = privatekey
        self.encoded_privatekey = encoded_privatekey
        self.address = address
        self.label = label
        self.db = db
        if group_data:
            self.group_number = group_data["group_number"]
            self.encoded_privatekey = group_data["group_index"]
            self.prefix = f"[<i>{self.label}</i>] "
        else:
            self.group_number = None
            self.prefix = ""

        if proxy in [None, "", " ", "\n", 'http://log:pass@ip:port']:
            self.proxy = None
        else:
            self.proxy = "http://" + proxy.removeprefix("https://").removeprefix("http://")

        config = lighter.Configuration(
            host=self.BASE_URL,
            proxy=self.proxy,
        )
        self.api_client = lighter.ApiClient(
            configuration=config,
        )

        self.signer_client = None
        self.account_api = None
        self.order_api = None
        self.transaction_api = None

        self.account_index = None
        self.lighter_private_key = None
        self.lighter_public_key = None

        self.auth_token = None
        self.account_info = None


    async def run_mode(self, mode: int):
        status = None
        await self.login()

        if mode == 1:
            status = await self.open_and_close_position()

        elif mode == 2:
            status = await self.cancel_all_orders()

        elif mode == 3:
            status = await self.parse_stats()

        return status


    @async_retry(source="Lighter")
    async def login(self):
        self.account_api = lighter.AccountApi(self.api_client)
        self.order_api = lighter.OrderApi(self.api_client)
        self.transaction_api = lighter.TransactionApi(self.api_client)

        try:
            response = await self.account_api.accounts_by_l1_address(l1_address=self.address)
        except lighter.ApiException as e:
            if e.data.message == "account not found":
                raise CustomError(f'Account not created')
            else:
                raise e.__class__(f"Failed to get accounts: {e}")

        self.account_index = response.sub_accounts[0].index
        self.signer_client, self.lighter_private_key, self.lighter_public_key = await self._get_api_key(account_index=self.account_index)
        self.account_info = (await self.account_api.account(by="index", value=str(self.account_index))).accounts[0]
        await self.load_tokens_data()


    async def open_and_close_position(self):
        possible_actions = [k for k, v in FUTURE_ACTIONS.items() if v]
        if not possible_actions:
            raise CustomError(f'You must enable Long/Short in settings!')

        if FUTURES_LIMITS["close_previous"]:
            positions_ = await self.get_account_info("positions", True)
            positions = [
                position
                for position in positions_
                if Decimal(position.position) and Decimal(position.avg_entry_price)
            ]
            if positions:
                await self.close_all_positions(positions)
                self.log_message(
                    f"Closed {len(positions)} position{'s' if len(positions) > 1 else ''} before opening new",
                    "+"
                )
                await self.db.append_report(
                    key=self.encoded_privatekey,
                    text=f"{self.prefix}closed {len(positions)} position{'s' if len(positions) > 1 else ''} before opening new",
                    success=True
                )

        futures_action = choice(possible_actions)
        token_name = choice(list(TOKENS_TO_TRADE.keys()))
        first_check = True
        while True:
            token_prices = await self.get_token_price(token_name=token_name)
            if TOKENS_TO_TRADE[token_name]["prices"][0] <= token_prices["average"] <= TOKENS_TO_TRADE[token_name]["prices"][1]:
                break
            elif first_check:
                first_check = False
                self.log_message(f'{token_name} current price {round(token_prices["average"])}. Waiting for price'
                                 f' in range {"-".join(str(v) for v in TOKENS_TO_TRADE[token_name]["prices"])}.')
            await async_sleep(5)

        # OPEN
        usdc_amount = await self.calculate_usdc_amount_for_order(token_name)
        try:
            opened_position = await self.create_order(side=futures_action, token_name=token_name, usdc_amount=usdc_amount)
        except NotEnoughMargin:
            self.log_message("Not enough margin for order, closing it...", "-", "ERROR")
            close_price, close_amount, close_usd_amount = await self.close_position(
                token_name=token_name,
                side=futures_action,
            )
            if [close_price, close_amount, close_usd_amount] == [0, 0, 0]:
                self.log_message(f"No {token_name} position found")
            else:
                self.log_message(f"Close {futures_action} {close_amount} {token_name} "
                                 f"(<green>{round(close_usd_amount, 2)}$</green>) at {close_price}", level="INFO")
            raise

        await async_sleep(randint(*SLEEP_AFTER_FUTURE))

        # CLOSE
        custom_price = self.calculate_limit_order_price(
            old_price=Decimal(opened_position.price),
            side=self.switch_params[futures_action]
        )
        closed_position = await self.create_order(
            side=self.switch_params[futures_action],
            token_name=token_name,
            token_amount=Decimal(opened_position.initial_base_amount),
            custom_price=custom_price,
            previous_pos=opened_position,
        )

        price_diff = Decimal(closed_position.price) - Decimal(opened_position.price)
        if futures_action == "Short":
            price_diff *= -1
        profit = round(Decimal(opened_position.initial_base_amount) * price_diff, 3)
        self.log_message(f"Profit: {profit}$", "+", "INFO")
        await self.db.append_report(
            key=self.encoded_privatekey,
            text=f'{self.prefix}💰 <b>profit {profit}$</b>',
        )
        return True


    async def calculate_usdc_amount_for_order(self, token_name: str):
        leverage = max(1, round(uniform(*TOKENS_TO_TRADE[token_name]["leverage"]), 1))
        balance = Decimal(await self.get_account_info("collateral", True))
        if TRADE_AMOUNTS["amount"] != [0, 0]:
            amounts = TRADE_AMOUNTS["amount"].copy()
            if amounts[0] > balance:
                raise CustomError(f'Not enough balance, need at least {amounts[0]}$, but have {round(balance, 2)}$')
            elif amounts[1] > balance:
                amounts[1] = float(balance)
            usdc_amount = Decimal(str(uniform(*amounts)))
        else:
            percent = uniform(*TRADE_AMOUNTS["percent"]) / 100
            usdc_amount = balance * Decimal(str(percent))

        return usdc_amount * Decimal(leverage)


    async def check_for_balance(self, needed_balance: float):
        balance = Decimal(await self.get_account_info("collateral", True))
        if needed_balance > balance:
            raise CustomError(f'Not enough balance, need at least {needed_balance}$, but have {round(balance, 2)}$')
        return balance


    @async_retry(source="Lighter", not_except=(CustomError, NotEnoughMargin))
    async def create_order(
            self,
            side: str,
            token_name: str,
            usdc_amount: float | Decimal = 0,
            token_amount: float | Decimal = 0,
            custom_price: float | Decimal = None,
            reduce_only: bool = False,
            previous_pos: lighter.models.Order = None,
            order_type: str = "limit",
            to_sleep: int = 0,
    ):
        token_data = self.TOKENS_DATA[token_name]
        if previous_pos:
            action_name = f"Sell {self.switch_params[side]}"

            token_position = (await self.get_open_positions()).get(str(token_data["market_id"]))
            if not token_position or float(token_position["position"]) == 0:
                self.log_message(
                    f"Failed to {action_name} order, no position found",
                    level="WARNING"
                )
                await self.db.append_report(
                    key=self.encoded_privatekey,
                    text=f"{self.prefix}failed to {action_name.lower()} order, no position found",
                    success=False
                )
                return lighter.models.order.CustomOrder(
                    initial_base_amount=previous_pos.initial_base_amount,
                    price=previous_pos.price,
                    is_ask=not previous_pos.is_ask
                )

        else:
            action_name = side

        if to_sleep:
            self.log_message(f"Sleep {to_sleep}s before {action_name}")
            await async_sleep(to_sleep)

        await self.wait_for_spread(token_name)

        if custom_price:
            token_price = round_cut(custom_price, token_data["price_decimals"])
        else:
            token_prices = await self.get_token_price(token_name)
            token_price = round_cut(token_prices[side], token_data["price_decimals"])

        if order_type == "market" and not custom_price:
            token_price = round_cut(token_prices[self.switch_params[side]], token_data["price_decimals"])

            if side == "Long":
                real_price = round_cut(token_price * Decimal("1.005"), token_data["price_decimals"])
            else:
                real_price = round_cut(token_price / Decimal("1.005"), token_data["price_decimals"])
        else:
            real_price = token_price
        token_base_price = int(real_price * Decimal(10 ** token_data["price_decimals"]))

        if usdc_amount:
            token_amount = round_cut(Decimal(str(usdc_amount)) / token_price, token_data["size_decimals"])
        elif token_amount:
            token_amount = round_cut(token_amount, token_data["size_decimals"])
        else:
            raise CustomError(f'Any of `usdc_amount` or `token_amount` must be provided!')

        if order_type == "limit" and token_amount < token_data["min_base_amount"]:
            raise CustomError(f'Minimum size is {token_data["min_base_amount"]} {token_name} but yours is {token_amount} {token_name}')

        usdc_amount = round(token_amount * token_price, 2)
        token_base_amount = int(token_amount * Decimal(10 ** token_data["size_decimals"]))

        open_price = Decimal(previous_pos.price) if previous_pos else 0
        trigger_base_price, trigger_price = self.calculate_trigger_price(
            side=side,
            token_price=open_price,
            decimals=token_data["price_decimals"],
            is_close=bool(previous_pos),
        )
        if order_type == "limit" and trigger_price:
            trigger_str = f" (Stop-Loss <red>{round_cut(trigger_price, token_data['price_decimals'])}</red>)"
        else:
            trigger_str = ""

        self.log_message(
            f"{action_name} {token_amount} {token_name} (<green>{usdc_amount}$</green>) at {token_price}{trigger_str}",
            "+", "INFO"
        )
        order_data = {
            "market_index": token_data["market_id"],
            "client_order_index": self.API_KEY_INDEX,
            "base_amount": token_base_amount,
            "price": token_base_price,
            "is_ask": side == "Short",
            "reduce_only": reduce_only,
            # "trigger_price": trigger_base_price,
        }
        if order_type == "limit":
            order_data.update({
                "order_expiry": int(time() * 1e3 + 60 * 60 * 24 * 28 * 1e3),
                "time_in_force": self.signer_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                "order_type": self.signer_client.ORDER_TYPE_LIMIT,
            })

        elif order_type == "market":
            order_data.update({
                "order_expiry": 0,
                "time_in_force": self.signer_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                "order_type": self.signer_client.ORDER_TYPE_MARKET,
            })

        else:
            raise Exception(f"Unexpected `order_type`: {order_type}")

        create_order_obj, tx_hash, error = await self.signer_client.create_order(**order_data)
        order_data.update({
            "token_amount": token_amount,
            "usdc_amount": usdc_amount,
            "token_price": token_price,
            "trigger_human_price": trigger_price,
            "token_name": token_name,
            "order_type": order_type,
        })
        if create_order_obj is None and tx_hash is None:
            raise Exception(f'Failed to create order: {error}')

        order_id = None
        if order_type == "market":
            for _ in range(10):
                filled_trade = await self.find_match_trade(order_data=order_data)
                if filled_trade:
                    break
                await async_sleep(3)
            if not filled_trade:
                raise Exception(f'Failed to found filled market order')

            self.log_message("Market order filled", "+", "SUCCESS")
            filled_order = lighter.models.order.CustomOrder(
                initial_base_amount=filled_trade["size"],
                price=filled_trade["price"],
                is_ask=order_data["is_ask"]
            )

        elif order_type == "limit":
            while True:
                filled_order, filled = await self.wait_for_limit_filled(
                    order_data=order_data,
                    is_close=bool(previous_pos),
                    order_id=order_id,
                    infinity_waiting=bool(custom_price or previous_pos),
                )
                if filled is not True:
                    if filled == "expired":
                        order_id = filled_order

                        token_prices = await self.get_token_price(token_name)
                        new_token_price = round_cut(token_prices[side], token_data["price_decimals"])
                        new_price = int(new_token_price * Decimal(10 ** token_data["price_decimals"]))
                        usdc_amount = round(token_amount * new_token_price, 2)

                        new_trigger_base_price, new_trigger_price = self.calculate_trigger_price(
                            side=side,
                            token_price=new_token_price,
                            decimals=token_data["price_decimals"],
                            is_close=bool(previous_pos),
                        )
                        if new_trigger_price:
                            trigger_str = (f" (Stop-Loss "
                                           f"<red>{round_cut(order_data['trigger_human_price'], token_data['price_decimals'])}</red> -> "
                                           f"<red>{round_cut(new_trigger_price, token_data['price_decimals'])})</red>")
                        else:
                            trigger_str = ""

                        self.log_message(f'Changing limit order price from <white>{order_data["token_price"]}</white> to <white>{new_token_price}</white>{trigger_str}')
                        order_data["token_price"], order_data["price"] = new_token_price, new_price
                        # order_data["trigger_price"], order_data["trigger_human_price"] = new_trigger_base_price, new_trigger_price
                        order_data["trigger_human_price"] = new_trigger_price

                        tx_info, api_response, error = await self.signer_client.modify_order(
                            market_index=order_data["market_index"],
                            order_index=int(order_id),
                            base_amount=order_data["base_amount"],
                            price=order_data["price"],
                            trigger_price=0,
                            # trigger_price=order_data["trigger_price"],
                        )
                        if error:
                            raise CustomError(f'Failed to change order price: {error}')

                    elif filled == "stop-loss":
                        current_price = filled_order
                        await self.close_open_orders()
                        close_price, close_amount, close_usd_amount = await self.close_position(
                            token_name=token_name,
                            side=self.switch_params[side],
                            amount=token_amount
                        )
                        await self.db.append_report(
                            key=self.encoded_privatekey,
                            text=f"{self.prefix}stop-loss: close {self.switch_params[side].lower()} {token_amount} {token_name} "
                                 f"({round(close_usd_amount, 2)}$) at {close_price}",
                            success="WARNING"
                        )
                        # return class with param `price` to calculate profit in main func
                        return lighter.transactions.create_order.CreateOrder.from_json(f'{{"Price":{close_price}}}')

                else:
                    break

        await self.db.append_report(
            key=self.encoded_privatekey,
            text=f"{self.prefix}{action_name.lower()} {token_amount} {token_name} ({usdc_amount}$) at {order_data['token_price']}",
            success=True
        )
        return filled_order


    @async_retry(source="Lighter", not_except=(CustomError, NotEnoughMargin))
    async def wait_for_limit_filled(
            self,
            order_data: dict,
            is_close: bool,
            order_id: str = None,
            infinity_waiting: bool = False
    ):
        if infinity_waiting:
            minutes_str = ""
            deadline = 0xfffffffff
        else:
            minutes_str = f"{FUTURES_LIMITS['to_wait']} minute{'s' if FUTURES_LIMITS['to_wait'] > 1 else ''} "
            deadline = int(time() + FUTURES_LIMITS['to_wait'] * 60)

        self.log_message(f"Waiting {minutes_str}for limit order filled...")

        if order_id is None:
            open_orders = await self.get_open_orders()
            token_open_orders = open_orders.get(str(order_data["market_index"]), [])
            order_id = next((
                open_order["order_id"]
                for open_order in token_open_orders
                if (
                    open_order["base_price"] == order_data["price"] and
                    open_order["client_order_index"] == order_data["client_order_index"] and
                    Decimal(open_order["initial_base_amount"]) == order_data["token_amount"] and
                    open_order["is_ask"] == order_data["is_ask"] and
                    open_order["market_index"] == order_data["market_index"] and
                    open_order["order_expiry"] == order_data["order_expiry"] and
                    open_order["owner_account_index"] == self.account_index and
                    open_order["reduce_only"] == order_data["reduce_only"] and
                    open_order["type"] == "limit"
                )
            ), None)

        while True:
            order = await self.find_match_order(order_data=order_data, order_id=order_id)
            if order:
                if order.status == "canceled-margin-not-allowed":
                    raise NotEnoughMargin('Not enough margin for order')
                elif order.status != "filled":
                    raise CustomError(f'Limit Order unexpected status: {order.status}')
                self.log_message("Limit order filled", "+", "SUCCESS")
                return order, True

            if is_close and order_data["trigger_human_price"]:
                token_prices = await self.get_token_price(order_data["token_name"])
                side = "Long" if order_data["is_ask"] else "Short" # reversed cuz closing pos
                if (
                    (side == "Short" and token_prices[side] >= order_data["trigger_human_price"]) or
                    (side == "Long" and token_prices[side] <= order_data["trigger_human_price"])
                ):
                    self.log_message(
                        f"Stop-Loss: Closing position, current price <red>{token_prices[side]}</red>",
                        level="WARNING"
                    )
                    return token_prices[side], "stop-loss"

            if time() > deadline:
                self.log_message(f"Order not filled in {minutes_str}", level="WARNING")
                return order_id, "expired"

            await async_sleep(2)


    async def find_match_order(self, order_data: dict, order_id: int = None):
        inactive_orders = await self.order_api.account_inactive_orders(
            account_index=self.account_index,
            limit=10,
            authorization=self._get_auth_token()
        )
        for order in inactive_orders.orders:
            if (order_id and order_id == order.order_id) or (
                    order.base_price == order_data["price"] and
                    order.client_order_index == order_data["client_order_index"] and
                    Decimal(order.initial_base_amount) == order_data["token_amount"] and
                    order.is_ask == order_data["is_ask"] and
                    order.market_index == order_data["market_index"] and
                    order.order_expiry == order_data["order_expiry"] and
                    order.owner_account_index == self.account_index and
                    order.reduce_only == order_data["reduce_only"] and
                    order.type == order_data["order_type"]
            ):
                return order


    async def find_match_trade(self, order_data: dict):
        last_trades = await self.order_api.trades(
            sort_by="timestamp",
            account_index=self.account_index,
            limit=10,
            authorization=self._get_auth_token()
        )
        summed_trades = {}
        for trade in last_trades.trades:
            if trade.is_maker_ask:
                trade_id = trade.bid_id
            else:
                trade_id = trade.ask_id
            if summed_trades.get(trade_id) is None:
                summed_trades[trade_id] = {"sizes": [], "prices": [], "is_maker_ask": trade.is_maker_ask, "market_id": trade.market_id}
            summed_trades[trade_id]["sizes"].append(Decimal(trade.size))
            summed_trades[trade_id]["prices"].append(Decimal(trade.price))

        for trade in summed_trades.values():
            if (
                    sum(trade["sizes"]) == order_data["token_amount"] and
                    trade["is_maker_ask"] == (not order_data["is_ask"]) and
                    trade["market_id"] == order_data["market_index"]
            ):
                token_data = self.TOKENS_DATA[order_data["token_name"]]
                avg_price = round_cut(sum(
                    size * price
                    for size, price in zip(trade["sizes"], trade["prices"])
                ) / sum(trade["sizes"]), token_data['price_decimals'])
                return {
                    "size": str(sum(trade["sizes"])),
                    "price": str(avg_price),
                }


    @async_retry(source="Lighter")
    async def cancel_all_orders(self):
        self.account_info = (await self.account_api.account(by="index", value=str(self.account_index))).accounts[0]

        positions = [
            position
            for position in self.account_info.positions
            if Decimal(position.position) and Decimal(position.avg_entry_price)
        ]

        if self.account_info.total_order_count and (CANCEL_ORDERS["orders"] or self.group_number):
            await self.close_open_orders()

            self.log_message(f"Canceled {self.account_info.total_order_count} open orders", "+", "INFO")
            await self.db.append_report(
                key=self.encoded_privatekey,
                text=f"{self.prefix}canceled {self.account_info.total_order_count} open orders",
                success=True
            )

        if positions and (CANCEL_ORDERS["positions"] or self.group_number):
            await self.close_all_positions(positions)

            self.log_message(f"Closed {len(positions)} positions", "+", "INFO")
            await self.db.append_report(
                key=self.encoded_privatekey,
                text=f"{self.prefix}closed {len(positions)} positions",
                success=True
            )

        if (
                not (self.account_info.total_order_count and CANCEL_ORDERS["orders"]) and
                not (positions and CANCEL_ORDERS["positions"])
        ):
            orders_positions_str = " & ".join([k for k, v in CANCEL_ORDERS.items() if v])
            self.log_message(f"No {orders_positions_str} to cancel found", "+", "INFO")
            await self.db.append_report(
                key=self.encoded_privatekey,
                text=f"{self.prefix}no {orders_positions_str} to cancel found",
                success=True
            )

        return True

    async def close_open_orders(self):
        tx_info, api_response, error = await self.signer_client.cancel_all_orders(
            time_in_force=self.signer_client.CANCEL_ALL_TIF_IMMEDIATE,
            time=0
        )
        if error:
            raise Exception(f"Failed to cancel all open orders: {error}")


    async def get_token_price(self, token_name: str):
        order_book_orders = await self.order_api.order_book_orders(
            market_id=self.TOKENS_DATA[token_name]["market_id"],
            limit=1,
        )
        short_price = Decimal(order_book_orders.asks[0].price)
        long_price = Decimal(order_book_orders.bids[0].price)
        return {
            "Short": short_price,
            "Long": long_price,
            "average": (short_price + long_price) / 2,
            "spread": (1 - long_price / short_price) * 100,
        }


    async def wait_for_spread(self, token_name: str):
        first_check = True
        while True:
            token_prices = await self.get_token_price(token_name)
            if token_prices["spread"] <= TOKENS_TO_TRADE[token_name]["max_spread"]:
                if not first_check:
                    self.log_message(
                        text=f'{token_name} spread {round(token_prices["spread"], 3)}%, creating order...',
                    )
                break

            elif first_check:
                first_check = False
                self.log_message(
                    text=f'{token_name} spread {round(token_prices["spread"], 3)}%. Waiting for max spread'
                         f' {TOKENS_TO_TRADE[token_name]["max_spread"]}%',
                )

            await async_sleep(1)


    async def get_account_info(self, param: str = None, update: bool = False):
        if update or self.account_info is None:
            self.account_info = (await self.account_api.account(by="index", value=str(self.account_index))).accounts[0]
        if param:
            return self.account_info.__getattribute__(param)
        else:
            return self.account_info


    async def close_position(self, token_name: str, side: str, amount: str | float = 0):
        price_multiplier = Decimal("1.005")
        token_data = self.TOKENS_DATA[token_name]

        if amount == 0:
            positions_ = await self.get_account_info("positions", True)
            amount = Decimal(next((
                pos.position for pos in positions_ if pos.market_id == token_data["market_id"]
            ), 0))
            if amount == 0:
                return 0, 0, 0

        token_prices = await self.get_token_price(token_name=token_name)
        if side == "Long":
            close_token_price = token_prices[side] / price_multiplier
        else:
            close_token_price = token_prices[side] * price_multiplier

        token_base_price = int(close_token_price * Decimal(10 ** token_data["price_decimals"]))

        create_order_obj, tx_hash, error = await self.signer_client.create_order(
            market_index=token_data["market_id"],
            client_order_index=self.API_KEY_INDEX,
            base_amount=int(Decimal(amount) * Decimal(10 ** token_data["size_decimals"])),
            price=token_base_price,
            is_ask=side != "Short",
            order_type=self.signer_client.ORDER_TYPE_MARKET,
            time_in_force=self.signer_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
            reduce_only=True,
            trigger_price=0,
            order_expiry=0,
        )
        if create_order_obj is None and tx_hash is None:
            raise Exception(f'Failed to close position: {error}')

        return token_prices[self.switch_params[side]], amount, token_prices[self.switch_params[side]] * Decimal(amount)


    async def close_all_positions(self, positions: list[lighter.models.account_position.AccountPosition]):
        tx_infos = []
        tx_types = [self.signer_client.TX_TYPE_CREATE_ORDER for _ in range(len(positions))]
        nonce = (await self.transaction_api.next_nonce(
            account_index=self.account_index, api_key_index=self.API_KEY_INDEX
        )).nonce
        price_multiplier = Decimal("1.005")

        for index, position in enumerate(positions):
            token_data = self.TOKENS_DATA[position.symbol]
            token_prices = await self.get_token_price(token_name=position.symbol)
            token_price = token_prices["average"]

            if (
                    (token_price > Decimal(position.avg_entry_price) and Decimal(position.unrealized_pnl) > 0) or
                    (token_price < Decimal(position.avg_entry_price) and Decimal(position.unrealized_pnl) < 0)
            ):
                side = "Long"
                close_token_price = token_prices[side] / price_multiplier
            else:
                side = "Short"
                close_token_price = token_prices[side] * price_multiplier

            token_base_price = int(close_token_price * Decimal(10 ** token_data["price_decimals"]))

            tx_info, error = self.signer_client.sign_create_order(
                market_index=position.market_id,
                client_order_index=self.API_KEY_INDEX,
                base_amount=int(Decimal(position.position) * Decimal(10 ** token_data["size_decimals"])),
                price=token_base_price,
                is_ask=side != "Short",
                order_type=self.signer_client.ORDER_TYPE_MARKET,
                time_in_force=self.signer_client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                reduce_only=True,
                trigger_price=0,
                order_expiry=0,
                nonce=nonce + index,
            )

            if error is not None:
                raise Exception(f"Error signing cancel position order: {error}")
            else:
                tx_infos.append(tx_info)

        try:
            await self.transaction_api.send_tx_batch(tx_types=json.dumps(tx_types), tx_infos=json.dumps(tx_infos))
        except Exception as err:
            raise Exception(f'Failed to cancel all positions: {err}')


    @async_retry(source="Lighter")
    async def parse_stats(self):
        balance = round(Decimal(self.account_info.collateral), 2)

        open_positions = len([
            position
            for position in self.account_info.positions
            if Decimal(position.position) and Decimal(position.avg_entry_price)
        ])

        open_orders = self.account_info.total_order_count

        current_ts = int(time())
        pnls = {}
        for pnl_data in [
            {"name": "daily", "resolution": "1h", "ts_diff": 86400},
            {"name": "weekly", "resolution": "1h", "ts_diff": 604800},
            {"name": "monthly", "resolution": "1d", "ts_diff": 2592000},
            {"name": "total", "resolution": "1d", "ts_diff": 315619200},
        ]:
            pnl = await self.account_api.pnl(
                by="index",
                value=str(self.account_index),
                resolution=pnl_data["resolution"],
                start_timestamp=current_ts - pnl_data["ts_diff"],
                end_timestamp=current_ts,
                count_back=0,
                ignore_transfers=False,
                authorization=self._get_auth_token()
            )
            pnls[pnl_data["name"]] = round(pnl.pnl[-1].trade_pnl - pnl.pnl[0].trade_pnl, 2)

        volume = await self.get_volume()
        points = await self.get_points()

        log_text = {
            f"{res.title()} {stat_data['name']}": f'{stat_data["stat"][res]}$'
            for res in ["daily", "weekly", "monthly", "total"]
            for stat_data in [{"name": "Volume", "stat": volume}, {"name": "PNL", "stat": pnls}]
        }
        log_text.update({
            "Positions": open_positions,
            "Open Orders": open_orders,
            "Total Balance": f"{balance}$",
            "Weekly Points": points["week"],
            "Total Points": points["total"],
        })
        self.log_message(f"Account statistics:\n{make_border(log_text)}", level="SUCCESS")

        tg_log = "\n\n".join([
            f"<b>{res.title()} Statistics</b>:\n📈 PNL: {pnls[res]}$\n💵 Volume: {volume[res]}$"
            for res in ["daily", "weekly", "monthly", "total"]
        ])
        tg_log += f"""

📌 Positions: {open_positions}
📎 Open Orders: {open_orders}
💰 Total Balance: {balance}$

🔅 Weekly Points: {points['week']}
🔆 Total Points: {points['total']}"""
        await self.db.append_report(
            key=self.encoded_privatekey,
            text=self.prefix + tg_log
        )

        return True


    async def get_points(self):
        response_data = await self.api_client.call_api(
            method="GET",
            url=self.BASE_URL + f"/api/v1/referral/points?account_index={self.account_index}",
            header_params={"Authorization": self._get_auth_token()},
        )
        resp = json.loads((await response_data.read()).decode())
        if (
                resp.get("user_total_points") is None or
                resp.get("user_last_week_points") is None
        ):
            self.log_message(f"Failed to get points: {resp}", '-', "WARNING")
            resp = {
                "user_total_points": 0,
                "user_last_week_points": 0,
            }

        return {"total": resp["user_total_points"], "week": resp["user_last_week_points"]}


    async def get_leverage(self, token_name: str):
        position = next((pos for pos in self.account_info.positions if pos.symbol == token_name), None)
        if position:
            return round(Decimal(self.account_info.collateral) / Decimal(position.initial_margin_fraction))
        else:
            return 10


    @async_retry(source="Lighter")
    async def change_leverage(self, token_name: str, leverage: int):
        tx_info, api_response, error = await self.signer_client.update_leverage(
            market_index=self.TOKENS_DATA[token_name]["market_id"],
            margin_mode=self.signer_client.CROSS_MARGIN_MODE,
            leverage=leverage
        )
        if error:
            raise Exception(f"Failed to change leverage to {leverage}x: {error}")
        self.log_message(f"Changed leverage to <white>{leverage}x</white>")


    async def get_volume(self):
        reply = await self._get_private_socket_info("account_all_trades")
        return {
            "daily": round(reply["daily_volume"], 2),
            "weekly": round(reply["weekly_volume"], 2),
            "monthly": round(reply["monthly_volume"], 2),
            "total": round(reply["total_volume"], 2),
        }

    async def get_open_orders(self):
        reply = await self._get_private_socket_info("account_all_orders")
        return reply["orders"]

    async def get_open_positions(self):
        reply = await self._get_private_socket_info("account_all_positions")
        return reply["positions"]

    async def _get_private_socket_info(self, channel: str):
        async with AsyncWebSocket(proxy=self.proxy, timeout=60) as s:
            ws = await s.ws_connect("wss://mainnet.zklighter.elliot.ai/stream")
            sub_data = {
                "type": "subscribe",
                "channel": f"{channel}/{self.account_index}",
                "auth": self._get_auth_token()
            }
            await ws.send_json(sub_data)

            while True:
                reply = await ws.recv_json()
                if reply["type"] == f"subscribed/{channel}":
                    return reply


    async def close_sessions(self):
        if self.api_client:
            await self.api_client.close()


    def _get_auth_token(self):
        if (
                self.auth_token is None or
                int(self.auth_token.split(':')[0]) - int(time()) <= 30
        ):
            self.auth_token = self.signer_client.create_auth_token_with_expiry()[0]
        return self.auth_token


    async def _get_api_key(self, account_index: int, force_create: bool = False):
        cached_api = await self.db.get_api_keys(self.address)

        if cached_api and not force_create:
            private_key, public_key = cached_api['private_key'], cached_api['public_key']
            signer_client = lighter.SignerClient(
                url=self.BASE_URL,
                private_key=private_key,
                account_index=self.account_index,
                api_key_index=self.API_KEY_INDEX,
                api_client=self.api_client
            )

        else:
            private_key, public_key, err = lighter.create_api_key(account_index=account_index)
            if err is not None:
                raise Exception(f"Failed to create api key: {err}")

            signer_client = lighter.SignerClient(
                url=self.BASE_URL,
                private_key=private_key,
                account_index=self.account_index,
                api_key_index=self.API_KEY_INDEX,
                api_client=self.api_client
            )

            response, err = await signer_client.change_api_key(
                eth_private_key=self.privatekey,
                new_pubkey=public_key,
            )
            if err is not None:
                raise Exception(f"Failed to change api key: {err}")

            await self.db.save_api_keys(
                address=self.address,
                new_api_keys={"public_key": public_key, "private_key": private_key}
            )
            await async_sleep(10)

        err = signer_client.check_client()
        if err and cached_api and not force_create:
            self.log_message("API key expired, creating new one...", "-", "WARNING")
            return await self._get_api_key(account_index=account_index, force_create=True)

        elif err:
            raise Exception(f"Failed to create new api key: {err}")

        elif not cached_api or force_create:
            self.log_message("Created new API key", "+")

        return signer_client, private_key, public_key


    async def load_tokens_data(self):
        if not self.TOKENS_DATA:
            response = await self.order_api.order_book_details()

            self.TOKENS_DATA = {
                detail.symbol: {
                    "market_id": detail.market_id,
                    "min_base_amount": float(detail.min_base_amount),
                    "size_decimals": detail.size_decimals,
                    "price_decimals": detail.price_decimals
                } for detail in response.order_book_details
            }


    async def remove_arch_file(self):
        if (
                self.account_index and
                self.signer_client and
                self.signer_client.signer and
                await self.db.get_modules_left(self.encoded_privatekey) <= 1
        ):
            lighter._remove_temp_arch_file(self.account_index, self.signer_client.signer._handle)


    def log_message(
            self,
            text: str,
            smile: str = "•",
            level: str = "DEBUG",
            colors: bool = True
    ):
        if self.group_number:
            if colors:
                label = f"<white>Group {self.group_number}</white> | <white>{self.label}</white>"
            else:
                label = f"Group {self.group_number} | {self.label}"
        else:
            label = f"<white>{self.label}</white>" if colors else self.label
        logger.opt(colors=colors).log(level.upper(), f'[{smile}] {label} | {text}')


    @classmethod
    def calculate_limit_order_price(cls, old_price: Decimal, side: str):
        new_percent, new_amount = cls.calculate_setting_difference(
            amounts=FUTURES_LIMITS["price_diff_amount"],
            percents=FUTURES_LIMITS["price_diff_percent"]
        )
        if side == "Short":
            if new_amount:
                token_price_raw = old_price + new_amount
            else:
                token_price_raw = old_price * new_percent
        else:
            if new_amount:
                token_price_raw = old_price - new_amount
            else:
                token_price_raw = old_price / new_percent

        return token_price_raw


    @classmethod
    def calculate_setting_difference(cls, amounts: list, percents: list):
        percent, amount = 0, 0
        if amounts != [0, 0]:
            amount = Decimal(str(uniform(*amounts)))
        else:
            random_percent = uniform(*percents)
            percent = Decimal("1") + Decimal(str(random_percent)) / 100

        return percent, amount

    @classmethod
    def calculate_trigger_price(cls, side: str, token_price: Decimal, decimals: int, is_close: bool):
        if is_close:
            side = cls.switch_params[side]  # reverse side for sell position (like reverse reversed)

        if STOP_LOSS_SETTING["enable"] and is_close:
            trigger_percent, trigger_amount = cls.calculate_setting_difference(
                amounts=STOP_LOSS_SETTING["loss_diff_amount"],
                percents=STOP_LOSS_SETTING["loss_diff_percent"]
            )
            if side == "Short":
                if trigger_amount:
                    trigger_price = token_price + abs(trigger_amount)
                else:
                    trigger_price = token_price * abs(trigger_percent)
            else:
                if trigger_amount:
                    trigger_price = token_price - abs(trigger_amount)
                else:
                    trigger_price = token_price / abs(trigger_percent)

        else:
            trigger_price = 0
        trigger_base_price = int(trigger_price * Decimal(10 ** decimals))

        return trigger_base_price, trigger_price


class PairAccounts:
    def __init__(self, accounts: list[LighterAccount], group_data: dict):
        self.accounts = accounts
        self.group_number = f"Group {group_data['group_number']}"
        self.group_index = group_data["group_index"]


    async def run(self):
        await asyncio.gather(*[
            acc.login()
            for acc in self.accounts
        ])

        if FUTURES_LIMITS["close_previous"]:
            await self.close_previous_positions()

        await self.open_and_close_position()
        return True


    @async_retry(source="Lighter")
    async def close_previous_positions(self):
        for account in self.get_randomized_accs(self.accounts):
            positions_ = await account.get_account_info("positions", True)
            positions = [
                position
                for position in positions_
                if Decimal(position.position) and Decimal(position.avg_entry_price)
            ]
            if positions:
                await account.close_all_positions(positions)
                self.log_group_message(
                    text=f"Closed {len(positions)} position{'s' if len(positions) > 1 else ''} before opening new",
                    smile="+",
                    account_label=account.label,
                )
                await account.db.append_report(
                    key=account.encoded_privatekey,
                    text=f"{account.prefix}closed {len(positions)} position{'s' if len(positions) > 1 else ''} before opening new",
                    success=True
                )


    async def open_and_close_position(self):
        token_name = choice(list(TOKENS_TO_TRADE.keys()))
        price_range_check = True
        while True:
            token_prices = await self.accounts[0].get_token_price(token_name=token_name)
            if TOKENS_TO_TRADE[token_name]["prices"][0] <= token_prices["average"] <= TOKENS_TO_TRADE[token_name]["prices"][1]:
                break

            elif price_range_check:
                price_range_check = False
                self.log_group_message(
                    text=f'{token_name} current price {round(token_prices["average"])}. Waiting for price'
                         f' in range {"-".join(str(v) for v in TOKENS_TO_TRADE[token_name]["prices"])}.',
                )

            await async_sleep(5)

        # OPEN
        force_leverage = max(1, randint(*TOKENS_TO_TRADE[token_name]["leverage"]))
        open_values = self.calculate_deltaneutral_amounts()
        await asyncio.gather(*[
            account.check_for_balance(needed_balance=open_values[account.address]["amount"])
            for account in self.accounts
        ])
        for acc in self.get_randomized_accs(self.accounts):
            await acc.change_leverage(token_name=token_name, leverage=force_leverage)

        tasks = []
        to_sleep_total = 0
        for acc_index, account in enumerate(self.accounts):
            random_sleep = randint(*SLEEP_BETWEEN_OPEN_ORDERS) if acc_index else 0
            to_sleep = to_sleep_total + random_sleep
            to_sleep_total += random_sleep
            tasks.append(
                account.create_order(
                    side=open_values[account.address]["side"],
                    token_name=token_name,
                    usdc_amount=round(open_values[account.address]["amount"] * force_leverage, 1),
                    order_type="market",
                    to_sleep=to_sleep,
                )
            )

        try:
            opened_positions = await asyncio.gather(*tasks)
            formatted_positions = {account.address: opened_positions[acc_index] for acc_index, account in enumerate(self.accounts)}
        except Exception as e:
            self.log_group_message(
                text=f"Failed to open {token_name} orders: {e}. Closing all positions...",
                smile="-",
                level="ERROR",
            )
            for account in self.get_randomized_accs(self.accounts):
                await account.cancel_all_orders()
            return False

        to_sleep = randint(*PAIR_SETTINGS["position_hold"])
        self.log_group_message(text=f"Sleeping {to_sleep}s before close positions...")
        await async_sleep(to_sleep)

        tasks = []
        to_sleep_total = 0
        randomized_accs = self.get_randomized_accs(self.accounts)
        for acc_index, account in enumerate(randomized_accs):
            random_sleep = randint(*SLEEP_BETWEEN_CLOSE_ORDERS) if acc_index else 0
            to_sleep = to_sleep_total + random_sleep
            to_sleep_total += random_sleep
            tasks.append(
                account.create_order(
                    side=account.switch_params[open_values[account.address]["side"]],
                    token_name=token_name,
                    token_amount=Decimal(formatted_positions[account.address].initial_base_amount),
                    order_type="market",
                    previous_pos=formatted_positions[account.address],
                    to_sleep=to_sleep,
                )
            )
        try:
            closed_positions_raw = await asyncio.gather(*tasks)
            closed_positions = [None for _ in range(len(self.accounts))]
            accs_addresses = [acc.address for acc in self.accounts]
            for random_acc_index, random_account in enumerate(randomized_accs):
                closed_positions[accs_addresses.index(random_account.address)] = closed_positions_raw[random_acc_index]

        except Exception as e:
            self.log_group_message(
                text=f"Failed to close {token_name} position: {e}. Closing all positions...",
                smile="-",
                level="ERROR",
            )
            for account in self.get_randomized_accs(self.accounts):
                await account.cancel_all_orders()
            return False

        total_profit = 0
        total_volume = 0
        for open_pos, close_pos in zip(opened_positions, closed_positions):
            open_amount = Decimal(open_pos.initial_base_amount) * Decimal(open_pos.price)
            close_amount = Decimal(close_pos.initial_base_amount) * Decimal(close_pos.price)
            if open_pos.is_ask:
                profit = open_amount - close_amount
            else:
                profit = close_amount - open_amount
            total_profit += profit
            total_volume += open_amount + close_amount

        total_profit = round(total_profit, 3)
        total_volume = round(total_volume, 1)
        hundred_thousand_cost = round(-total_profit / total_volume * 100000, 3)
        self.log_group_message(
            text=f"Profit: <green>{total_profit}$</green> | "
                f"Total Volume: <green>{total_volume}$</green> | "
                f"100k$ Volume Cost: <green>{hundred_thousand_cost}$</green>",
            smile="+",
            level="INFO"
        )
        await self.accounts[-1].db.append_report(
            key=self.accounts[-1].encoded_privatekey,
            text=f'\n💰 <b>profit {total_profit}$</b>'
                 f'\n💵 <b>volume {total_volume}$</b>'
                 f'\n💍 <b>100k$ volume cost: {hundred_thousand_cost}$</b>',
        )
        return True


    def get_randomized_accs(self, lst: list):
        randomized_accounts = lst[:]
        shuffle(randomized_accounts)
        return randomized_accounts


    def calculate_deltaneutral_amounts(self):
        def _distribute_sum(total_sum: float, num_parts: int, min_val: float, max_val: float):
            parts = []
            remaining_sum = total_sum

            for i in range(num_parts - 1):
                upper_bound = min(max_val, remaining_sum - (num_parts - 1 - i) * min_val)
                lower_bound = max(min_val, remaining_sum - (num_parts - 1 - i) * max_val)

                if lower_bound > upper_bound:
                    raise Exception(f"Failed to calculate amounts with {TRADE_AMOUNTS['amount']}")

                part = uniform(lower_bound, upper_bound)
                part = round(part, 2)

                parts.append(part)
                remaining_sum -= part

            parts.append(round(remaining_sum, 2))
            shuffle(parts)
            return parts

        min_amount, max_amount = TRADE_AMOUNTS["amount"]
        addresses = [acc.address for acc in self.get_randomized_accs(self.accounts)]
        if len(addresses) % 2:  # if odd
            if len(addresses) == 3:
                min_amount = min(min_amount, max_amount / 2.2)
            else:
                min_amount = min(min_amount, max_amount / 2)

        num_longs = randint(1, len(addresses) - 1)
        num_shorts = len(addresses) - num_longs

        long_accounts = addresses[:num_longs]
        short_accounts = addresses[num_longs:]

        min_total_sum = max(num_longs * min_amount, num_shorts * min_amount)
        max_total_sum = min(num_longs * max_amount, num_shorts * max_amount)

        if min_total_sum > max_total_sum:
            return self.calculate_deltaneutral_amounts()

        total_sum = uniform(min_total_sum, max_total_sum)

        long_amounts = _distribute_sum(total_sum, num_longs, min_amount, max_amount)
        short_amounts = _distribute_sum(total_sum, num_shorts, min_amount, max_amount)

        result = {}
        for addr, amount in zip(long_accounts, long_amounts):
            result[addr] = {"side": "Long", "amount": amount}

        for addr, amount in zip(short_accounts, short_amounts):
            result[addr] = {"side": "Short", "amount": amount}

        return result


    def log_group_message(
            self,
            text: str,
            smile: str = "•",
            level: str = "DEBUG",
            colors: bool = True,
            account_label: str = ""
    ):
        label = f"<white>{self.group_number}</white>" if colors else self.group_number
        if account_label:
            if colors:
                label += f" | <white>{account_label}</white>"
            else:
                label += f" | {account_label}"
        logger.opt(colors=colors).log(level.upper(), f'[{smile}] {label} | {text}')
