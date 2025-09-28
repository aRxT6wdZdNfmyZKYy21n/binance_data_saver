import asyncio
import logging
import traceback
import typing
from collections import defaultdict
from datetime import (
    UTC,
)
from decimal import (
    Decimal,
)

import polars
import polars_talib
from chrono import (
    Timer,
)
from polars import (
    DataFrame,
    Series,
)
from sqlalchemy import (
    text,
)

import main.save_candles.schemas
from constants.symbol import (
    SymbolConstants,
)
from enumerations import (
    SymbolId,
)
from main.show_plot.globals import (
    g_globals,
)
from main.show_plot.gui.window import (
    ChartWindow,
)
from settings import (
    settings,
)

logger = logging.getLogger(
    __name__,
)

_DEBUG_SMOOTHING_LEVEL = None
# _DEBUG_SMOOTHING_LEVEL = 'Smoothed (2)'


_IS_ORDER_BOOK_VOLUME_ENABLED = False


class ChartProcessor:
    __slots__ = (
        '__candles_dataframe',
        '__candles_dataframe_update_lock',
        '__current_available_symbol_name_set',
        '__current_symbol_name',
        '__current_interval_name',
        '__long_imbalances_dataframe',
        '__max_price',
        '__min_price',
        '__rsi_series',
        '__velocity_series',
        '__window',
    )

    def __init__(
        self,
    ) -> None:
        super().__init__()

        self.__candles_dataframe: polars.DataFrame | None = None
        self.__candles_dataframe_update_lock = asyncio.Lock()
        self.__current_available_symbol_name_set: set[str] | None = None
        self.__current_symbol_name: str | None = None
        self.__current_interval_name: str | None = None
        self.__long_imbalances_dataframe: polars.DataFrame | None = None
        self.__max_price: Decimal | None = None
        self.__min_price: Decimal | None = None
        self.__rsi_series: Series | None = None
        self.__velocity_series: Series | None = None
        self.__window: ChartWindow | None = None

    # async def fini(
    #         self,
    # ) -> None:
    #     # TODO: fini the window
    #
    #     await super().fini()

    def get_candles_dataframe(
        self,
    ) -> polars.DataFrame | None:
        return self.__candles_dataframe

    async def get_current_available_symbol_names(
        self,
    ) -> list[str] | None:
        current_available_symbol_name_set = self.__current_available_symbol_name_set

        if current_available_symbol_name_set is None:
            return None

        return sorted(
            current_available_symbol_name_set,
        )

    def get_current_symbol_name(
        self,
    ) -> str | None:
        return self.__current_symbol_name

    def get_current_interval_name(
        self,
    ) -> str | None:
        return self.__current_interval_name

    def get_max_price(
        self,
    ) -> Decimal | None:
        return self.__max_price

    def get_min_price(
        self,
    ) -> Decimal | None:
        return self.__min_price

    def get_rsi_series(
        self,
    ) -> Series | None:
        return self.__rsi_series

    def get_long_imbalances_dataframe(
        self,
    ) -> polars.DataFrame | None:
        return self.__long_imbalances_dataframe

    async def init(
        self,
    ) -> None:
        # init the window

        window = ChartWindow(
            processor=self,
        )

        self.__window = window

        # show the window

        window.show()

        await window.plot(
            is_need_run_once=True,
        )

    async def start_updating_loop(
        self,
    ) -> None:
        while True:
            try:
                with Timer() as timer:
                    await self.__update()

                logger.info(
                    f'Processor was updated by {timer.elapsed:.3f}s',
                )
            except Exception as exception:
                logger.error(
                    'Could not update processor'
                    ': handled exception'
                    f': {"".join(traceback.format_exception(exception))}'
                )

            await asyncio.sleep(
                # 1.0  # s
                # 60.0  # s
                600.0  # s
            )

    async def update_current_symbol_name(
        self,
        value: str,
    ) -> bool:
        current_available_symbol_name_set = self.__current_available_symbol_name_set

        if current_available_symbol_name_set is None:
            return False

        if value not in current_available_symbol_name_set:
            return False

        if value == self.__current_symbol_name:
            return False

        self.__current_symbol_name = value

        self.__candles_dataframe = None
        self.__long_imbalances_dataframe = None
        self.__max_price = None
        self.__min_price = None
        self.__rsi_series = None

        await self.__update_candles_dataframe()

        window = self.__window

        await window.plot(
            is_need_run_once=True,
        )

        window.auto_range_candles_plot()

        return True

    async def update_current_interval_name(
        self,
        value: str,
    ) -> bool:
        if value == self.__current_interval_name:
            return False

        self.__current_interval_name = value

        self.__candles_dataframe = None
        self.__long_imbalances_dataframe = None
        self.__max_price = None
        self.__min_price = None
        self.__rsi_series = None

        await self.__update_current_available_symbol_name_set()
        await self.__update_candles_dataframe()

        window = self.__window

        await window.plot(
            is_need_run_once=True,
        )

        window.auto_range_candles_plot()

        return True

    async def __update(
        self,
    ) -> None:
        await self.__update_candles_dataframe()

    async def __update_candles_dataframe(
        self,
    ) -> None:
        current_interval_name = self.__current_interval_name

        if current_interval_name is None:
            return

        current_symbol_name = self.__current_symbol_name

        if current_symbol_name is None:
            return

        current_symbol_id = SymbolConstants.IdByName[current_symbol_name]

        async with self.__candles_dataframe_update_lock:
            with Timer() as timer:
                old_candles_dataframe = self.__candles_dataframe

                if old_candles_dataframe is not None:
                    min_start_timestamp_ms = old_candles_dataframe.get_column(
                        'start_timestamp_ms',
                    ).max()
                else:
                    min_start_timestamp_ms = 0

                new_candles_dataframe = self.__fetch_candles_dataframe(
                    interval_name=current_interval_name,
                    min_start_timestamp_ms=min_start_timestamp_ms,
                    symbol_id=current_symbol_id,
                )

                candles_dataframe: polars.DataFrame

                if old_candles_dataframe is not None:
                    candles_dataframe = old_candles_dataframe.update(
                        new_candles_dataframe,
                        on='start_timestamp_ms',
                    )

                    candles_dataframe = polars.concat(
                        [
                            candles_dataframe,
                            new_candles_dataframe.filter(
                                polars.col('start_timestamp_ms')
                                > min_start_timestamp_ms,
                            ),
                        ]
                    )
                else:
                    candles_dataframe = new_candles_dataframe

                self.__candles_dataframe = candles_dataframe

        logger.info(f'Candle dataframe was updated by {timer.elapsed:.3f}s')

        with Timer() as timer:
            self.__update_rsi_series()

        logger.info(
            f'RSI series were updated by {timer.elapsed:.3f}s',
        )

        with Timer() as timer:
            self.__update_long_imbalances()

        logger.info(
            f'Long imbalances were updated by {timer.elapsed:.3f}s',
        )

        await self.__window.plot(
            is_need_run_once=True,
        )

    @staticmethod
    def __fetch_candles_dataframe(
        interval_name: str,
        min_start_timestamp_ms: int,
        symbol_id: SymbolId,
    ) -> DataFrame | None:
        with Timer() as timer:
            db_schema: (
                main.save_candles.schemas.BinanceCandleData1H
                | main.save_candles.schemas.BinanceCandleData4H
                | main.save_candles.schemas.BinanceCandleData1D
            ) = getattr(main.save_candles.schemas, f'BinanceCandleData{interval_name}')

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
                    f' symbol_id = {symbol_id.name!r}'
                    f' AND start_timestamp_ms >= {min_start_timestamp_ms!r}'
                    ' ORDER BY'
                    ' symbol_id ASC'
                    ', start_timestamp_ms DESC'
                    # f' LIMIT {15_000_000!r}'
                    # f' LIMIT {10_000_000!r}'
                    # f' LIMIT {5_000_000!r}'
                    # f' LIMIT {2_000_000!r}'
                    # f' LIMIT {100_000!r}'
                    f' LIMIT {1_000!r}'
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

        print(f'Fetched candles dataframe by {timer.elapsed:.3f}s')

        candles_dataframe = candles_dataframe.with_columns(
            polars.col(
                'start_timestamp_ms',
            )
            .cast(
                polars.Datetime(
                    time_unit='ms',
                    time_zone=UTC,
                ),
            )
            .alias(
                'start_datetime',
            ),
            polars.col(
                'close_price',
            ).cast(
                polars.Float64,
            ),
            polars.col(
                'high_price',
            ).cast(
                polars.Float64,
            ),
            polars.col(
                'low_price',
            ).cast(
                polars.Float64,
            ),
            polars.col(
                'open_price',
            ).cast(
                polars.Float64,
            ),
            polars.col(
                'volume',
            ).cast(
                polars.Float64,
            ),
        )

        candles_dataframe = candles_dataframe.sort(
            'start_datetime',
        )

        return candles_dataframe

    async def __update_current_available_symbol_name_set(
        self,
    ) -> None:
        current_available_symbol_name_set: set[str] | None = None

        current_interval_name = self.__current_interval_name
        if current_interval_name is not None:
            db_schema: (
                main.save_candles.schemas.BinanceCandleData1H
                | main.save_candles.schemas.BinanceCandleData4H
                | main.save_candles.schemas.BinanceCandleData1D
            ) = getattr(
                main.save_candles.schemas, f'BinanceCandleData{current_interval_name}'
            )

            postgres_db_session_maker = g_globals.get_postgres_db_session_maker()

            table_name = f'"{db_schema.__tablename__}"'

            async with postgres_db_session_maker() as session:
                recursive_cte_full_query = text(
                    f"""
WITH RECURSIVE symbol_id_cte(symbol_id) AS 
(
  (
    SELECT {table_name}.symbol_id AS symbol_id 
    FROM {table_name} ORDER BY {table_name}.symbol_id ASC 
    LIMIT 1
  )
  UNION ALL
  SELECT (
    SELECT symbol_id
    FROM {table_name}
    WHERE symbol_id > cte.symbol_id
    ORDER BY symbol_id ASC
    LIMIT 1
  )
  FROM symbol_id_cte AS cte
  WHERE cte.symbol_id IS NOT NULL
)
SELECT symbol_id
FROM symbol_id_cte
WHERE symbol_id IS NOT NULL;
                    """
                )

                result = await session.execute(
                    recursive_cte_full_query,
                )

                for row in result:
                    symbol_id_raw: str = row.symbol_id

                    symbol_id = getattr(
                        SymbolId,
                        symbol_id_raw,
                    )

                    symbol_name = SymbolConstants.NameById[symbol_id]

                    if current_available_symbol_name_set is None:
                        current_available_symbol_name_set = set()

                    current_available_symbol_name_set.add(
                        symbol_name,
                    )

        self.__current_available_symbol_name_set = current_available_symbol_name_set

        window = self.__window

        await window.plot(
            is_need_run_once=True,
        )

        # window.auto_range_candles_plot()

    def __update_rsi_series(
        self,
    ) -> None:
        current_interval_name = self.__current_interval_name

        if current_interval_name is None:
            return

        candles_dataframe = self.__candles_dataframe

        rsi_series: polars.Series | None

        if candles_dataframe is not None:
            rsi_dataframe = candles_dataframe.with_columns(
                polars_talib.rsi(
                    real=polars.col(
                        'close_price',
                    ),
                    timeperiod=14,  # 6
                ).alias(
                    'rsi',
                ),
            )

            rsi_series = rsi_dataframe.get_column(
                'rsi',
            )
        else:
            rsi_series = None

        self.__rsi_series = rsi_series

    def __update_long_imbalances(
        self,
    ) -> None:
        candles_dataframe = self.__candles_dataframe

        if candles_dataframe is None:
            self.__long_imbalances_dataframe = None

            return

        max_timestamp_ms = candles_dataframe.get_column(
            'start_timestamp_ms',
        ).max()

        # Получаем данные свечей
        candles_data = candles_dataframe.to_dicts()

        if len(candles_data) < 3:
            self.__long_imbalances_dataframe = None

            return

        active_imbalance_raw_data_list_by_start_price_map: typing.DefaultDict[
            float, list[dict[str, typing.Any]]
        ] = defaultdict(
            list,
        )

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

                    inactive_imbalance_raw_data_list.append(
                        imbalance_raw_data,
                    )

                active_imbalance_raw_data_list.clear()

                if prices_to_remove is None:
                    prices_to_remove = []

                prices_to_remove.append(
                    price,
                )

            if prices_to_remove is not None:
                for price in prices_to_remove:
                    (active_imbalance_raw_data_list_by_start_price_map.pop(price),)

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

            active_imbalance_raw_data_list = (
                active_imbalance_raw_data_list_by_start_price_map[start_price]
            )

            active_imbalance_raw_data_list.append(
                {
                    'start_timestamp_ms': start_timestamp_ms,
                    'start_price': start_price,
                    'end_price': end_price,
                    'end_timestamp_ms': None,
                },
            )

        for (
            imbalance_raw_data_list
        ) in active_imbalance_raw_data_list_by_start_price_map.values():
            for imbalance_raw_data in imbalance_raw_data_list:
                imbalance_raw_data['end_timestamp_ms'] = max_timestamp_ms

                inactive_imbalance_raw_data_list.append(
                    imbalance_raw_data,
                )

            imbalance_raw_data_list.clear()

        active_imbalance_raw_data_list_by_start_price_map.clear()

        # Создаем датафрейм из списка имбалансов
        if inactive_imbalance_raw_data_list:
            long_imbalances_dataframe = polars.DataFrame(
                inactive_imbalance_raw_data_list,
            )

            self.__long_imbalances_dataframe = long_imbalances_dataframe.sort(
                by='start_timestamp_ms',
            )
        else:
            self.__long_imbalances_dataframe = None

    def __check_imbalance_filling(
        self,
        start_price: float,
        end_price: float,
        start_index: int,
        candles_data: list[dict],
    ) -> int | None:
        """
        Проверяет, закрыт ли имбаланс последующими свечами.
        Возвращает timestamp_ms свечи, которая закрыла имбаланс, или None если не закрыт.
        """
        for i in range(start_index, len(candles_data)):
            candle = candles_data[i]

            # Проверяем, пересекает ли свеча диапазон имбаланса
            # Имбаланс считается закрытым, если свеча пересекает диапазон [end_price, start_price]
            if candle['low_price'] <= start_price and candle['high_price'] >= end_price:
                return candle['start_timestamp_ms']

        return None
