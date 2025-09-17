from settings import RETRY
from time import sleep

from loguru import logger

from requests.exceptions import JSONDecodeError as json_error1
from json.decoder import JSONDecodeError as json_error2


class CustomError(Exception): pass

class DataBaseError(Exception): pass

class NotEnoughMargin(Exception): pass


def have_json(func):
    async def wrapper(*args, **kwargs):
        response = await func(*args, **kwargs)
        try:
            response.json()
        except (json_error1, json_error2):
            error_msg = response.text[:350].replace("\n", " ")
            raise Exception(f'bad json response: {error_msg}')

        return response
    return wrapper


def retry(
        source: str,
        module_str: str,
        exceptions,
        retries: int = RETRY,
        not_except=CustomError,
        to_raise: bool = True
):
    def decorator(f):
        def newfn(*args, **kwargs):
            attempt = 0
            while attempt < retries:
                try:
                    return f(*args, **kwargs)

                except not_except as e:
                    if to_raise: raise CustomError(f'{module_str}: {e}')
                    else: return False

                except exceptions as e:
                    try:
                        error_owner = args[0].label
                    except:
                        error_owner = "Soft"

                    logger.error(f'[-] {error_owner} | {source} | {module_str} | {e} [{attempt+1}/{retries}]')
                    attempt += 1
                    if attempt == retries:
                        if to_raise: raise ValueError(f'{module_str}: {e}')
                        else: return False
                    else:
                        sleep(2)
        return newfn
    return decorator


def async_retry(
        source: str,
        module_str: str = None,
        exceptions = Exception,
        retries: int = RETRY,
        not_except=(CustomError,),
        to_raise: bool = True
):
    def decorator(f):
        custom_module_str = f.__name__.replace('_', ' ').title() if not module_str else module_str
        async def newfn(*args, **kwargs):
            attempt = 0
            while attempt < retries:
                try:
                    return await f(*args, **kwargs)

                except not_except as e:
                    if to_raise: raise e.__class__(f'{custom_module_str}: {e}')
                    else: return False

                except exceptions as e:
                    try:
                        error_owner = args[0].label
                    except:
                        error_owner = "Soft"

                    logger.opt(colors=True).error(f'[-] <white>{error_owner}</white> | {source} | {custom_module_str} | {e} [{attempt+1}/{retries}]')
                    attempt += 1
                    if attempt == retries:
                        if to_raise: raise CustomError(f'{custom_module_str}: {e}')
                        else: return False
                    else:
                        sleep(2)
        return newfn
    return decorator
