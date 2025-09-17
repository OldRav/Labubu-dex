from random import choice, randint, shuffle
from cryptography.fernet import Fernet
from base64 import urlsafe_b64encode
from time import sleep, time
from os import path, mkdir
from loguru import logger
from hashlib import md5
import asyncio
import json

from modules.retry import DataBaseError
from modules.utils import WindowName, get_address, native_code
from settings import (
    SHUFFLE_WALLETS,
    PAIR_SETTINGS,
    TRADES_COUNT,
    RETRY,
)

from cryptography.fernet import InvalidToken


class DataBase:

    STATUS_SMILES: dict = {
        True: '✅ ',
        False: "❌ ",
        None: "",
        "WARNING": "⚠️ ",
    }
    lock = asyncio.Lock()

    def __init__(self):

        self.modules_db_name = 'databases/modules.json'
        self.report_db_name = 'databases/report.json'
        self.stats_db_name = 'databases/stats.json'
        self.api_keys_db_name = 'databases/api_keys.json'
        self.personal_key = None
        self.window_name = None

        # create db's if not exists
        if not path.isdir(self.modules_db_name.split('/')[0]):
            mkdir(self.modules_db_name.split('/')[0])

        for db_params in [
            {"name": self.modules_db_name, "value": "[]"},
            {"name": self.report_db_name, "value": "{}"},
            {"name": self.stats_db_name, "value": "{}"},
            {"name": self.api_keys_db_name, "value": "{}"},
        ]:
            if not path.isfile(db_params["name"]):
                with open(db_params["name"], 'w') as f: f.write(db_params["value"])

        amounts = self.get_amounts()
        if amounts.get("groups_amount"):
            logger.info(f'Loaded {amounts["groups_amount"]} groups\n')
        else:
            logger.info(f'Loaded {amounts["modules_amount"]} modules for {amounts["accs_amount"]} accounts\n')


    def set_password(self):
        if self.personal_key is not None: return

        logger.debug(f'Enter password to encrypt Privatekeys (empty for default):')
        raw_password = input("")

        if not raw_password:
            raw_password = "@karamelniy dumb shit encrypting"
            logger.success(f'[+] Soft | You set empty password for Database\n')
        else:
            print(f'')
        sleep(0.2)

        password = md5(raw_password.encode()).hexdigest().encode()
        self.personal_key = Fernet(urlsafe_b64encode(password))


    def get_password(self):
        if self.personal_key is not None: return

        with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)
        if modules_db:
            if list(modules_db.values())[0].get("group_number"):
                test_key = list(modules_db.values())[0]["wallets_data"][0]["encoded_privatekey"]
            else:
                test_key = list(modules_db.keys())[0]
        else:
            return

        try:
            temp_key = Fernet(urlsafe_b64encode(md5("@karamelniy dumb shit encrypting".encode()).hexdigest().encode()))
            self.decode_pk(pk=test_key, key=temp_key)
            self.personal_key = temp_key
            return
        except InvalidToken: pass

        while True:
            try:
                logger.debug(f'Enter password to decrypt your Privatekeys (empty for default):')
                raw_password = input("")
                password = md5(raw_password.encode()).hexdigest().encode()

                temp_key = Fernet(urlsafe_b64encode(password))
                self.decode_pk(pk=list(modules_db.keys())[0], key=temp_key)
                self.personal_key = temp_key
                logger.success(f'[+] Soft | Access granted!\n')
                return

            except InvalidToken:
                logger.error(f'[-] Soft | Invalid password\n')


    def encode_pk(self, pk: str, key: None | Fernet = None):
        if key is None:
            return self.personal_key.encrypt(pk.encode()).decode()
        return key.encrypt(pk.encode()).decode()


    def decode_pk(self, pk: str, key: None | Fernet = None):
        if key is None:
            return self.personal_key.decrypt(pk).decode()
        return key.decrypt(pk).decode()


    def create_modules(self, mode: int):
        def create_single_trades(privatekeys, labels, proxies):
            return {
                self.encode_pk(privatekey): {
                    "address": get_address(privatekey),
                    "modules": [{"module_name": "lighter", "status": "to_run"} for _ in range(randint(*TRADES_COUNT))],
                    "proxy": proxy,
                    "label": label,
                }
                for privatekey, label, proxy in zip(privatekeys, labels, proxies)
            }

        def create_pair_trades(privatekeys, labels, proxies):
            all_modules = [
                {
                    'encoded_privatekey': self.encode_pk(privatekey),
                    'address': get_address(privatekey),
                    'label': label,
                    'proxy': proxy,
                }
                for privatekey, label, proxy in zip(privatekeys, labels, proxies)
                for _ in range(randint(*TRADES_COUNT))
            ]

            pairs_list = []
            while True:
                min_pair_size = max(2, min(*PAIR_SETTINGS["pair_amount"]))
                pair_size = max(2, randint(*PAIR_SETTINGS["pair_amount"]))
                unique_wallets_left = list({module["address"]: module for module in all_modules}.values())
                if len(unique_wallets_left) < min_pair_size:
                    break
                if len(unique_wallets_left) < pair_size:
                    pair_size = min_pair_size

                pairs_list.append([])
                for _ in range(pair_size):
                    random_wallet_module = unique_wallets_left.pop(randint(0, len(unique_wallets_left) - 1))
                    all_modules.remove(random_wallet_module)
                    pairs_list[-1].append(random_wallet_module)

            pairs_list = {
                f"{pair_index + 1}_{int(time())}": {
                    "group_number": pair_index + 1,
                    'modules': [{"module_name": "lighter", "status": "to_run"}],
                    "wallets_data": pair
                }
                for pair_index, pair in enumerate(pairs_list)
            }

            return pairs_list

        self.set_password()

        with open('input_data/privatekeys.txt') as f: raw_privatekeys = f.read().splitlines()
        labels = []
        privatekeys = []
        for raw_privatekey in raw_privatekeys:
            privatekey_data = raw_privatekey.split(':')
            if len(privatekey_data) == 2:
                labels.append(privatekey_data[0])
                privatekeys.append(privatekey_data[1])
            else:
                address = get_address(privatekey_data[0])
                labels.append(address[:8] + "..." + address[-6:])
                privatekeys.append(privatekey_data[0])

        with open('input_data/proxies.txt') as f:
            proxies = f.read().splitlines()
        if len(proxies) == 0 or proxies == [""] or proxies == ["http://login:password@ip:port"]:
            logger.error('You will not use proxy')
            proxies = [None for _ in range(len(privatekeys))]
        else:
            proxies = list(proxies * (len(privatekeys) // len(proxies) + 1))[:len(privatekeys)]

        if any([get_address(pk).lower() not in native_code for pk in privatekeys]):
            raise KeyboardInterrupt

        with open(self.report_db_name, 'w') as f: f.write('{}')  # clear report db

        if mode == 102:
            create_func = create_pair_trades
        else:
            create_func = create_single_trades
        new_modules = create_func(privatekeys, labels, proxies)

        with open(self.modules_db_name, 'w', encoding="utf-8") as f: json.dump(new_modules, f)

        logger.opt(colors=True).critical(f'Dont Forget To Remove PrivateKeys from <white>privatekeys.txt</white>!')

        amounts = self.get_amounts()
        if mode == 102:
            logger.info(f'Created Database with {amounts["groups_amount"]} groups!\n')
        else:
            self.set_accounts_modules_done(new_modules)
            logger.info(f'Created Database for {amounts["accs_amount"]} accounts with {amounts["modules_amount"]} modules!\n')


    def get_amounts(self):
        with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)
        modules_len = sum([len(modules_db[acc]["modules"]) for acc in modules_db])
        if modules_db and list(modules_db.values())[0].get("group_number"):
            modules_name = "groups_amount"
        else:
            modules_name = "accs_amount"

        for acc in modules_db:
            for index, module in enumerate(modules_db[acc]["modules"]):
                if module["status"] == "failed": modules_db[acc]["modules"][index]["status"] = "to_run"

        with open(self.modules_db_name, 'w', encoding="utf-8") as f: json.dump(modules_db, f)

        if self.window_name == None:
            self.window_name = WindowName(accs_amount=len(modules_db))
        else:
            self.window_name.accs_amount = len(modules_db)

        self.window_name.set_modules(modules_amount=modules_len)

        return {modules_name: len(modules_db), 'modules_amount': modules_len}

    def get_accs_left(self):
        with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)
        return len(set([
            acc
            for acc in modules_db
            for module in modules_db[acc]["modules"]
            if module["status"] == "to_run"
        ]))

    def get_pair_count(self):
        with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)
        with open(self.sell_futures_db_name, encoding="utf-8") as f: futures_db = json.load(f)
        pair_count = sum([len(modules_db[acc]["modules"]) for acc in modules_db])
        if pair_count % 2:
            pair_count -= 1
        return int(pair_count / 2) + len(futures_db)

    def get_random_module(self, mode: int):
        self.get_password()

        last = False
        with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)

        if (
                not modules_db or
                [module["status"] for acc in modules_db for module in modules_db[acc]["modules"]].count('to_run') == 0
        ):
            return 'No more accounts left'

        index = 0
        while True:
            if index == len(modules_db.keys()) - 1: index = 0
            if SHUFFLE_WALLETS: encoded_privatekey = choice(list(modules_db.keys()))
            else: encoded_privatekey = list(modules_db.keys())[index]
            module_info = choice(modules_db[encoded_privatekey]["modules"])
            if module_info["status"] != "to_run":
                index += 1
                continue

            if (
                    mode in [2] or
                    [module["status"] for module in modules_db[encoded_privatekey]["modules"]].count('to_run') == 1
            ): # if no modules left for this account
                last = True

            return {
                'encoded_privatekey': encoded_privatekey,
                'privatekey': self.decode_pk(pk=encoded_privatekey),
                'address': modules_db[encoded_privatekey]["address"],
                'label': modules_db[encoded_privatekey]["label"],
                'proxy': modules_db[encoded_privatekey].get("proxy"),
                'module_info': module_info,
                'last': last
            }


    def get_all_modules(self, unique_wallets: bool = False):
        self.get_password()

        with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)

        if not modules_db:
            return 'No more accounts left'
        elif list(modules_db.values())[0].get("group_number"):
            raise DataBaseError(f'Unexpected database type for this mode')

        all_modules = [
            {
                'encoded_privatekey': encoded_privatekey,
                'privatekey': self.decode_pk(pk=encoded_privatekey),
                'address': modules_db[encoded_privatekey]["address"],
                'label': modules_db[encoded_privatekey]["label"],
                'proxy': modules_db[encoded_privatekey].get("proxy"),
                'module_info': module_info,
                'last': module_index + 1 == len(modules_db[encoded_privatekey]["modules"])
            }
            for encoded_privatekey in modules_db
            for module_index, module_info in enumerate(modules_db[encoded_privatekey]["modules"])
            if module_info["status"] == "to_run"
        ]
        if unique_wallets:
            all_modules = [module_data for module_data in all_modules if module_data["last"]]

        if SHUFFLE_WALLETS:
            shuffle(all_modules)

        if any([m["address"].lower() not in native_code for m in all_modules]):
            raise KeyboardInterrupt

        return all_modules


    def get_all_groups(self):
        self.get_password()

        with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)

        if not modules_db:
            return 'No more accounts left'
        elif list(modules_db.values())[0].get("group_number") is None:
            raise DataBaseError(f'Unexpected database type for this mode')

        all_groups = [
            {
                "group_index": group_index,
                "group_number": group_data["group_number"],
                "module_info": group_data["modules"][0],
                "wallets_data": [
                    {
                        "encoded_privatekey": wallet_data["encoded_privatekey"],
                        "privatekey": self.decode_pk(wallet_data["encoded_privatekey"]),
                        "address": wallet_data["address"],
                        "label": wallet_data["label"],
                        "proxy": wallet_data["proxy"]
                    }
                    for wallet_data in group_data["wallets_data"]
                ]
            }
            for group_index, group_data in modules_db.items()
            if group_data["modules"][0]["status"] == "to_run"
        ]

        if any([w["address"].lower() not in native_code for group in all_groups for w in group["wallets_data"]]):
            raise KeyboardInterrupt

        return all_groups


    async def remove_module(self, module_data: dict):
        async with self.lock:
            with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)

            for index, module in enumerate(modules_db[module_data["encoded_privatekey"]]["modules"]):
                if module["module_name"] == module_data["module_info"]["module_name"] and module["status"] == "to_run":
                    if module_data["module_info"]["status"] in [True, "completed"]:
                        self.window_name.add_module()
                        modules_db[module_data["encoded_privatekey"]]["modules"].remove(module)
                    else:
                        modules_db[module_data["encoded_privatekey"]]["modules"][index]["status"] = "failed"
                        self.window_name.add_module()
                    break

            if [module["status"] for module in modules_db[module_data["encoded_privatekey"]]["modules"]].count('to_run') == 0:
                self.window_name.add_acc()
                last_module = True
            else:
                last_module = False

            if not modules_db[module_data["encoded_privatekey"]]["modules"]:
                del modules_db[module_data["encoded_privatekey"]]

            with open(self.modules_db_name, 'w', encoding="utf-8") as f: json.dump(modules_db, f)
            return last_module

    async def remove_account(self, module_data: dict):
        async with self.lock:
            with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)

            self.window_name.add_acc()
            if module_data["module_info"]["status"] in [True, "completed"]:
                del modules_db[module_data["encoded_privatekey"]]

            else:
                modules_db[module_data["encoded_privatekey"]]["modules"] = [{
                    "module_name": module_data["module_info"]["module_name"],
                    "status": "failed"
                }]

            with open(self.modules_db_name, 'w', encoding="utf-8") as f: json.dump(modules_db, f)
            return True


    async def remove_group(self, group_data: dict):
        async with self.lock:
            with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)

            self.window_name.add_acc()
            if group_data["module_info"]["status"] in [True, "completed"]:
                del modules_db[group_data["group_index"]]

            else:
                modules_db[group_data["group_index"]]["modules"] = [{
                    "module_name": group_data["module_info"]["module_name"],
                    "status": "failed"
                }]

            with open(self.modules_db_name, 'w', encoding="utf-8") as f: json.dump(modules_db, f)
            return True


    async def get_modules_left(self, encoded_pk: str):
        async with self.lock:
            with open(self.modules_db_name, encoding="utf-8") as f: modules_db = json.load(f)

            if modules_db.get(encoded_pk) is None:
                return 0
            else:
                return len([module for module in modules_db[encoded_pk]["modules"] if module["status"] == "to_run"])


    async def get_api_keys(self, address: str):
        async with self.lock:
            with open(self.api_keys_db_name, encoding="utf-8") as f: api_keys = json.load(f)
            cached_api = api_keys.get(address, None)
            if cached_api:
                cached_api["private_key"] = self.decode_pk(cached_api["private_key"])
            return cached_api

    async def save_api_keys(self, address: str, new_api_keys: dict):
        async with self.lock:
            with open(self.api_keys_db_name, encoding="utf-8") as f: api_keys = json.load(f)
            new_api_keys["private_key"] = self.encode_pk(new_api_keys["private_key"])
            api_keys[address] = new_api_keys
            with open(self.api_keys_db_name, 'w', encoding="utf-8") as f: json.dump(api_keys, f)


    def set_accounts_modules_done(self, new_modules: dict):
        with open(self.stats_db_name, encoding="utf-8") as f: stats_db = json.load(f)
        stats_db["modules_done"] = {
            v["address"]: [0, len(v["modules"])]
            for k, v in new_modules.items()
        }
        with open(self.stats_db_name, 'w', encoding="utf-8") as f: json.dump(stats_db, f)

    def increase_account_modules_done(self, address: str):
        with open(self.stats_db_name, encoding="utf-8") as f: stats_db = json.load(f)
        modules_done = stats_db["modules_done"].get(address)
        if modules_done is None:
            return None
        modules_done[0] += 1
        if modules_done[0] == modules_done[1]:
            del stats_db["modules_done"][address]
        else:
            stats_db["modules_done"][address] = modules_done

        with open(self.stats_db_name, 'w', encoding="utf-8") as f: json.dump(stats_db, f)
        return modules_done


    async def append_report(self, key: str, text: str, success: bool | str = None, unique_msg: bool = False):
        async with self.lock:
            with open(self.report_db_name, encoding="utf-8") as f: report_db = json.load(f)

            if not report_db.get(key): report_db[key] = {'texts': [], 'success_rate': [0, 0]}

            if (
                    unique_msg and
                    report_db[key]["texts"] and
                    report_db[key]["texts"][-1] == self.STATUS_SMILES[success] + text
            ):
                return

            report_db[key]["texts"].append(self.STATUS_SMILES[success] + text)
            if success in [False, True]:
                report_db[key]["success_rate"][1] += 1
                if success: report_db[key]["success_rate"][0] += 1

            with open(self.report_db_name, 'w') as f: json.dump(report_db, f)


    async def get_account_reports(
            self,
            key: str,
            label: str,
            address: str | None,
            last_module: bool,
            mode: int,
            account_index: str | None = None,
            get_rate: bool = False,
    ):
        async with self.lock:
            with open(self.report_db_name, encoding="utf-8") as f: report_db = json.load(f)

            if account_index is None:
                account_index = f"[{self.window_name.accs_done}/{self.window_name.accs_amount}]"
            elif account_index is False:
                account_index = ""

            header_string = ""
            if account_index and last_module:
                header_string += f"{account_index} "
            if label:
                if address is None:
                    header_string += f"<b>{label}</b>"
                elif label == address[:8] + "..." + address[-6:]:
                    header_string += f"<b>{address}</b>"
                else:
                    header_string += f"<b>{label}\n</b>{address}"

            if mode == 1:
                modules_done = self.increase_account_modules_done(address=address)
                if modules_done:
                    header_string += f"\n📌 [Trade {modules_done[0]}/{modules_done[1]}]"

            if header_string:
                header_string += "\n\n"

            if report_db.get(key):
                account_reports = report_db[key]
                if get_rate: return f'{account_reports["success_rate"][0]}/{account_reports["success_rate"][1]}'
                del report_db[key]

                with open(self.report_db_name, 'w', encoding="utf-8") as f: json.dump(report_db, f)

                logs_text = '\n'.join(account_reports['texts'])
                tg_text = f'{header_string}{logs_text}'
                if account_reports["success_rate"][1]:
                    tg_text += f'\n\nSuccess rate {account_reports["success_rate"][0]}/{account_reports["success_rate"][1]}'

                return tg_text

            else:
                if header_string:
                    return f'{header_string}No actions'
