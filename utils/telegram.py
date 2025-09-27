import logging
import os
import traceback

from aiogram import (
    Bot as AioGramBot,
)
from aiogram.client.default import (
    DefaultBotProperties as AioGramDefaultBotProperties,
)
from aiogram.enums import (
    ParseMode,
)

_CHAT_ID = -1002403348867


logger = logging.getLogger(
    __name__,
)


class TelegramUtils:
    __slots__ = ()

    @staticmethod
    def create_aiogram_bot(
        token: str,
    ) -> AioGramBot:
        return AioGramBot(
            token,
            default=AioGramDefaultBotProperties(
                parse_mode=ParseMode.MARKDOWN_V2,
            ),
        )

    @classmethod
    async def send_message_to_channel(
        cls,
        message_markdown_text: str,
    ) -> None:
        aiogram_bot_token = os.getenv(
            'TELEGRAM_BOT_TOKEN',
        )

        assert aiogram_bot_token is not None, None

        aiogram_bot = cls.create_aiogram_bot(
            aiogram_bot_token,
        )

        try:
            async with aiogram_bot:
                await aiogram_bot.send_message(
                    chat_id=_CHAT_ID,
                    text=message_markdown_text,
                )
        except Exception as exception:
            logger.error(
                'Could not send message'
                f' with markdown text {message_markdown_text!r}'
                ' to Telegram'
                ': handled exception'
                f': {"".join(traceback.format_exception(exception))}'
            )
