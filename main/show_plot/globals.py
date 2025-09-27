__all__ = ('g_globals',)

import asyncio

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from constants.common import (
    CommonConstants,
)
from settings import (
    settings,
)


class Globals:
    __slots__ = (
        '__postgres_db_engine',
        '__postgres_db_session_maker',
        '__postgres_db_task_queue',
    )

    def __init__(
        self,
    ) -> None:
        super().__init__()

        self.__postgres_db_engine = postgres_db_engine = create_async_engine(
            'postgresql+asyncpg'
            '://'
            f'{settings.POSTGRES_DB_USER_NAME}'
            ':'
            f'{settings.POSTGRES_DB_PASSWORD.get_secret_value()}'
            '@'
            f'{settings.POSTGRES_DB_HOST_NAME}'
            ':'
            f'{settings.POSTGRES_DB_PORT}'
            '/'
            f'{settings.POSTGRES_DB_NAME}',
            echo=True,  # TODO: enable only for debug mode
        )

        self.__postgres_db_session_maker = async_sessionmaker(
            postgres_db_engine,
            expire_on_commit=False,
        )

        self.__postgres_db_task_queue: asyncio.Queue[
            CommonConstants.AsyncFunctionType
        ] = asyncio.Queue()

    def get_postgres_db_engine(
        self,
    ) -> AsyncEngine:
        return self.__postgres_db_engine

    def get_postgres_db_session_maker(
        self,
    ) -> async_sessionmaker[AsyncSession]:
        return self.__postgres_db_session_maker

    def get_postgres_db_task_queue(
        self,
    ) -> asyncio.Queue[CommonConstants.AsyncFunctionType]:
        return self.__postgres_db_task_queue


g_globals = Globals()
