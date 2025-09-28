__all__ = ('g_globals',)

import asyncio
import typing
from collections import (
    defaultdict,
)
from datetime import (
    UTC,
)

import polars
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from constants.common import (
    CommonConstants,
)
from main.save_candles import (
    schemas,
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

    @staticmethod
    def fetch_candles_dataframe(
        interval_name: str,
        symbol_name: str,
        candles_count: int,
    ) -> polars.DataFrame | None:
        """Получить последние свечи для символа"""
        db_schema: (
            schemas.BinanceCandleData1H
            | schemas.BinanceCandleData4H
            | schemas.BinanceCandleData1D
        ) = getattr(schemas, f'BinanceCandleData{interval_name}')

        candles_dataframe = polars.read_database_uri(
            engine='connectorx',
            query=(
                'SELECT'
                # Primary key fields
                ' start_timestamp_ms'
                # Attribute fields
                ', close_price'
                ', high_price'
                ', low_price'
                ', open_price'
                ', taker_buy_volume_base_currency'
                ', taker_buy_volume_quote_currency'
                ', trades_count'
                ', volume'
                ', volume_quote_currency'
                f' FROM "{db_schema.__tablename__}"'
                ' WHERE'
                f' symbol_name = {symbol_name!r}'
                ' ORDER BY'
                ' start_timestamp_ms DESC'
                f' LIMIT {candles_count!r}'
                ';'
            ),
            uri=(
                'postgresql'
                '://'
                f'{settings.POSTGRES_DB_USER_NAME}'
                ':'
                f'{settings.POSTGRES_DB_PASSWORD.get_secret_value()}'
                '@'
                f'{settings.POSTGRES_DB_HOST_NAME}'
                ':'
                f'{settings.POSTGRES_DB_PORT}'
                '/'
                f'{settings.POSTGRES_DB_NAME}'
            ),
        )

        # Преобразуем типы данных
        candles_dataframe = candles_dataframe.with_columns(
            polars.col('start_timestamp_ms')
            .cast(
                polars.Datetime(
                    time_unit='ms',
                    time_zone=UTC,
                ),
            )
            .alias('start_datetime'),
            polars.col('close_price').cast(polars.Float64),
            polars.col('high_price').cast(polars.Float64),
            polars.col('low_price').cast(polars.Float64),
            polars.col('open_price').cast(polars.Float64),
            polars.col('volume').cast(polars.Float64),
        )

        # Сортируем по времени (от старых к новым)
        candles_dataframe = candles_dataframe.sort('start_datetime')

        return candles_dataframe

    @staticmethod
    def create_long_imbalances_dataframe(
        candles_dataframe: polars.DataFrame,
    ) -> polars.DataFrame | None:
        """Создать датафрейм с лонговыми имбалансами"""
        if candles_dataframe is None:
            return None

        max_timestamp_ms = candles_dataframe.get_column('start_timestamp_ms').max()

        # Получаем данные свечей
        candles_data = candles_dataframe.to_dicts()

        if len(candles_data) < 3:
            return None

        active_imbalance_raw_data_list_by_start_price_map: typing.DefaultDict[
            float, list[dict[str, typing.Any]]
        ] = defaultdict(list)

        inactive_imbalance_raw_data_list: list[dict[str, typing.Any]] = []

        # Проходим по всем возможным тройкам свечей
        for i in range(len(candles_data) - 2):
            candle1 = candles_data[i]
            candle2 = candles_data[i + 1]
            candle3 = candles_data[i + 2]

            low_price: float = candle3['low_price']
            high_price: float = candle3['high_price']

            # Определяем временные метки
            start_timestamp_ms = candle3['start_timestamp_ms']

            prices_to_remove: list[float] | None = None

            for (
                price,
                active_imbalance_raw_data_list,
            ) in active_imbalance_raw_data_list_by_start_price_map.items():
                if not (low_price <= price <= high_price):
                    continue

                for imbalance_raw_data in active_imbalance_raw_data_list:
                    imbalance_raw_data['end_timestamp_ms'] = start_timestamp_ms

                    inactive_imbalance_raw_data_list.append(imbalance_raw_data)

                active_imbalance_raw_data_list.clear()

                if prices_to_remove is None:
                    prices_to_remove = []

                prices_to_remove.append(price)

            if prices_to_remove is not None:
                for price in prices_to_remove:
                    active_imbalance_raw_data_list_by_start_price_map.pop(price)

                prices_to_remove.clear()
                prices_to_remove = None  # noqa

            # Проверяем, что вторая свеча бычья (close > open)
            if candle2['close_price'] <= candle2['open_price']:
                continue

            # Определяем цены X и Y
            start_price = candle1['high_price']  # X
            end_price: float = candle3['low_price']  # Y

            # Проверяем условие Y > X (есть разрыв)
            if end_price <= start_price:
                continue

            if end_price / start_price <= 1.05:  # 5%
                continue

            active_imbalance_raw_data_list = (
                active_imbalance_raw_data_list_by_start_price_map[start_price]
            )

            active_imbalance_raw_data_list.append(
                {
                    'start_timestamp_ms': start_timestamp_ms,
                    'start_price': start_price,
                    'end_price': end_price,
                    'end_timestamp_ms': None,
                }
            )

        for (
            imbalance_raw_data_list
        ) in active_imbalance_raw_data_list_by_start_price_map.values():
            for imbalance_raw_data in imbalance_raw_data_list:
                # imbalance_raw_data['end_timestamp_ms'] = max_timestamp_ms

                inactive_imbalance_raw_data_list.append(imbalance_raw_data)

            imbalance_raw_data_list.clear()

        active_imbalance_raw_data_list_by_start_price_map.clear()

        # Создаем датафрейм из списка имбалансов
        if inactive_imbalance_raw_data_list:
            long_imbalances_dataframe = polars.DataFrame(
                inactive_imbalance_raw_data_list,
            )

            return long_imbalances_dataframe.sort(by='start_timestamp_ms')
        else:
            return None


g_globals = Globals()
