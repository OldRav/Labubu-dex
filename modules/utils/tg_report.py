from loguru import logger
from aiohttp import ClientSession

from settings import TG_BOT_TOKEN, TG_USER_ID


class TgReport:
    def __init__(self, logs=""):
        self.logs = logs


    def update_logs(self, text: str):
        self.logs += f'{text}\n'


    async def send_log(self, logs: str = None):
        notification_text = logs or self.logs

        texts = []
        while len(notification_text) > 0:
            texts.append(notification_text[:1900])
            notification_text = notification_text[1900:]

        if TG_BOT_TOKEN:
            async with ClientSession() as session:
                for tg_id in TG_USER_ID:
                    for text in texts:
                        # text = text.replace('+', '%2B')
                        try:
                            r = await session.post(
                                url=f'https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage',
                                json={
                                    'parse_mode': 'html',
                                    'disable_web_page_preview': True,
                                    'chat_id': tg_id,
                                    'text': text,
                                }
                            )
                            response = await r.json()
                            if response.get("ok") != True: raise Exception(str(response))
                        except Exception as err: logger.error(f'[-] TG | Send Telegram message error to {tg_id}: {err}\n{text}')
