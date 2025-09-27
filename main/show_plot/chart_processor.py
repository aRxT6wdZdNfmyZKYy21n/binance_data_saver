import asyncio
import logging
import traceback
import typing
from datetime import (
    UTC,
    datetime,
)
from decimal import (
    Decimal,
)

import numpy
import orjson
import polars
import polars_talib
import talib
from chrono import (
    Timer,
)
from polars import (
    DataFrame,
    Series,
)
from sqlalchemy import (
    and_,
    select,
    text,
)

from constants.common import (
    CommonConstants,
)
from constants.plot import (
    PlotConstants,
)
from constants.symbol import (
    SymbolConstants,
)
from enumerations import (
    OKXOrderBookActionId,
    SymbolId,
    TradingDirection,
)
from main.save_order_books.schemas import (
    OKXOrderBookData2,
)
from main.save_trades.schemas import (
    OKXTradeData2,
)
from main.show_plot.globals import (
    g_globals,
)
from main.show_plot.gui.window import (
    FinPlotChartWindow,
)
from settings import (
    settings,
)
from utils.trading import (
    TradingUtils,
)

logger = logging.getLogger(
    __name__,
)

_DEBUG_SMOOTHING_LEVEL = None
# _DEBUG_SMOOTHING_LEVEL = 'Smoothed (2)'


_IS_ORDER_BOOK_VOLUME_ENABLED = False


class ChartProcessor:
    __slots__ = (
        '__candle_dataframe_by_interval_name_map',
        '__current_available_symbol_name_set',
        '__current_rsi_interval_name',
        '__current_symbol_name',
        '__current_trades_smoothing_level',
        '__extreme_lines_array',
        '__extreme_lines_position',
        '__extreme_lines_scale',
        '__line_dataframe_by_level_map',
        '__max_trade_price',
        '__min_trade_price',
        '__order_book_volumes_asks_array',
        '__order_book_volumes_bids_array',
        '__order_book_volumes_position',
        '__order_book_volumes_scale',
        '__rsi_series',
        '__trades_dataframe',
        '__trades_dataframe_update_lock',
        '__trades_smoothed_dataframe_by_level_map',
        '__velocity_series',
        '__window',
    )

    def __init__(
        self,
    ) -> None:
        super().__init__()

        self.__candle_dataframe_by_interval_name_map: dict[str, polars.DataFrame] = {}
        self.__current_available_symbol_name_set: set[str] | None = None
        self.__current_rsi_interval_name: str | None = None
        self.__current_symbol_name: str | None = None
        self.__current_trades_smoothing_level: str | None = None
        self.__extreme_lines_array: numpy.ndarray | None = None
        self.__extreme_lines_position: tuple[float, float] | None = None
        self.__extreme_lines_scale: float | None = None
        self.__line_dataframe_by_level_map: dict[str, DataFrame | None] = {}
        self.__max_trade_price: Decimal | None = None
        self.__min_trade_price: Decimal | None = None
        self.__order_book_volumes_asks_array: numpy.ndarray | None = None
        self.__order_book_volumes_bids_array: numpy.ndarray | None = None
        self.__order_book_volumes_position: tuple[float, float] | None = None
        self.__order_book_volumes_scale: float | None = None
        self.__rsi_series: Series | None = None
        self.__trades_dataframe: DataFrame | None = None
        self.__trades_dataframe_update_lock = asyncio.Lock()
        self.__trades_smoothed_dataframe_by_level_map: dict[str, DataFrame | None] = {}
        self.__velocity_series: Series | None = None
        self.__window: FinPlotChartWindow | None = None

    # async def fini(
    #         self,
    # ) -> None:
    #     # TODO: fini the window
    #
    #     await super().fini()

    def get_candle_dataframe_by_interval_name_map(
        self,
    ) -> dict[str, polars.DataFrame]:
        return self.__candle_dataframe_by_interval_name_map

    async def get_current_available_symbol_names(
        self,
    ) -> list[str] | None:
        current_available_symbol_name_set = self.__current_available_symbol_name_set

        if current_available_symbol_name_set is None:
            return None

        return sorted(
            current_available_symbol_name_set,
        )

    def get_current_rsi_interval_name(
        self,
    ) -> str | None:
        return self.__current_rsi_interval_name

    def get_current_symbol_name(
        self,
    ) -> str | None:
        return self.__current_symbol_name

    def get_current_trades_smoothing_level(
        self,
    ) -> str | None:
        return self.__current_trades_smoothing_level

    def get_line_dataframe(
        self,
    ) -> DataFrame | None:
        return self.__line_dataframe_by_level_map.get(
            self.__current_trades_smoothing_level,
        )

    def get_max_trade_price(
        self,
    ) -> Decimal | None:
        return self.__max_trade_price

    def get_min_trade_price(
        self,
    ) -> Decimal | None:
        return self.__min_trade_price

    def get_extreme_lines_array(
            self,
    ) -> numpy.ndarray | None:
        return self.__extreme_lines_array

    def get_extreme_lines_position(
            self,
    ) -> tuple[float, float] | None:
        return self.__extreme_lines_position

    def get_extreme_lines_scale(
            self,
    ) -> float | None:
        return self.__extreme_lines_scale

    def get_order_book_volumes_asks_array(
            self,
    ) -> numpy.ndarray | None:
        return self.__order_book_volumes_asks_array

    def get_order_book_volumes_bids_array(
            self,
    ) -> numpy.ndarray | None:
        return self.__order_book_volumes_bids_array

    def get_order_book_volumes_position(
            self,
    ) -> tuple[float, float] | None:
        return self.__order_book_volumes_position

    def get_order_book_volumes_scale(
            self,
    ) -> float | None:
        return self.__order_book_volumes_scale

    def get_rsi_series(
        self,
    ) -> Series | None:
        return self.__rsi_series

    def get_trades_dataframe(
        self,
    ) -> DataFrame | None:
        return self.__trades_dataframe

    def get_smoothed_dataframe(
        self,
    ) -> DataFrame | None:
        level = self.__current_trades_smoothing_level

        if level == 'Raw (0)':
            return self.__trades_dataframe

        return self.__trades_smoothed_dataframe_by_level_map.get(
            level,
        )

    def get_velocity_series(
        self,
    ) -> Series | None:
        return self.__velocity_series

    async def init(
        self,
    ) -> None:
        # init the window

        window = FinPlotChartWindow(
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

    async def update_current_rsi_interval_name(
        self,
        value: str,
    ) -> bool:
        if value == self.__current_rsi_interval_name:
            return False

        self.__current_rsi_interval_name = value

        self.__rsi_series = None

        self.__update_rsi_series()

        window = self.__window

        await window.plot(
            is_need_run_once=True,
        )

        return True

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

        self.__candle_dataframe_by_interval_name_map.clear()
        self.__extreme_lines_array = None
        self.__extreme_lines_position = None
        self.__extreme_lines_scale = None
        self.__line_dataframe_by_level_map.clear()
        self.__max_trade_price = None
        self.__min_trade_price = None
        self.__order_book_volumes_asks_array = None
        self.__order_book_volumes_bids_array = None
        self.__order_book_volumes_position = None
        self.__order_book_volumes_scale = None
        self.__rsi_series = None
        self.__trades_dataframe = None
        self.__trades_smoothed_dataframe_by_level_map.clear()
        self.__velocity_series = None

        await self.__update_trades_dataframe()

        window = self.__window

        await window.plot(
            is_need_run_once=True,
        )

        window.auto_range_price_plot()

        return True

    async def update_current_trades_smoothing_level(
        self,
        value: str,
    ) -> bool:
        if value == self.__current_trades_smoothing_level:
            return False

        self.__current_trades_smoothing_level = value

        window = self.__window

        await window.plot(
            is_need_run_once=True,
        )

        return True

    async def __update(
        self,
    ) -> None:
        await self.__update_current_available_symbol_name_set()
        await self.__update_trades_dataframe()

    def __update_candle_dataframe_by_interval_name_map(
        self,
    ) -> None:
        trades_dataframe = self.__trades_dataframe

        assert trades_dataframe is not None, None

        candle_dataframe_by_interval_name_map = (
            self.__candle_dataframe_by_interval_name_map
        )

        for interval_name in PlotConstants.IntervalNames:
            candle_raw_data_list: list[dict[str, typing.Any]] = []

            interval_duration = CommonConstants.IntervalDurationByNameMap[interval_name]

            interval_duration_ms = int(
                interval_duration.total_seconds() * 1000,  # ms
            )

            last_candle_raw_data: dict[str, typing.Any] | None = None

            old_candle_dataframe = candle_dataframe_by_interval_name_map.get(
                interval_name,
            )

            min_trade_id: int

            if old_candle_dataframe is not None:
                min_polars_trade_id = old_candle_dataframe.get_column(
                    'start_trade_id'
                ).max()

                min_trade_id = int(
                    min_polars_trade_id,
                )
            else:
                min_trade_id = 0

            for trade_data in trades_dataframe.filter(
                polars.col('trade_id') >= min_trade_id
            ).iter_rows(named=True):
                trade_id: int = trade_data['trade_id']

                # if trade_id < min_trade_id:
                #     continue

                price: float = trade_data['price']
                quantity: float = trade_data['quantity']
                volume = price * quantity

                datetime_: datetime = trade_data['datetime']

                timestamp_ms = int(
                    datetime_.timestamp() * 1000,  # ms
                )

                candle_start_timestamp_ms = timestamp_ms - (
                    timestamp_ms % interval_duration_ms
                )

                candle_end_timestamp_ms = (
                    candle_start_timestamp_ms + interval_duration_ms
                )

                if last_candle_raw_data is not None:
                    if (
                        candle_start_timestamp_ms
                        == last_candle_raw_data['start_timestamp_ms']
                    ):
                        # Update candle raw data

                        if price > last_candle_raw_data['high_price']:
                            last_candle_raw_data['high_price'] = price

                        if price < last_candle_raw_data['low_price']:
                            last_candle_raw_data['low_price'] = price

                        last_candle_raw_data.update(
                            {
                                'trades_count': (
                                    last_candle_raw_data['trades_count'] + 1
                                ),
                                'end_trade_id': trade_id,
                                'close_price': price,
                                'volume': last_candle_raw_data['volume'] + volume,
                            }
                        )
                    else:
                        # Flush candle raw data

                        candle_raw_data_list.append(
                            last_candle_raw_data,
                        )

                        last_candle_raw_data = None

                if last_candle_raw_data is None:
                    last_candle_raw_data = {
                        'close_price': price,
                        'end_timestamp_ms': candle_end_timestamp_ms,
                        'end_trade_id': trade_id + 1,
                        'high_price': price,
                        'low_price': price,
                        'open_price': price,
                        'start_timestamp_ms': candle_start_timestamp_ms,
                        'start_trade_id': trade_id,
                        'trades_count': 1,
                        'volume': volume,
                    }

            if last_candle_raw_data is not None:
                # Flush candle raw data

                candle_raw_data_list.append(
                    last_candle_raw_data,
                )

                last_candle_raw_data = None  # noqa

            new_candle_dataframe = polars.DataFrame(candle_raw_data_list)

            assert new_candle_dataframe.height > 0, (
                min_trade_id,
                old_candle_dataframe,
            )

            new_candle_dataframe = new_candle_dataframe.with_columns(
                polars.col(
                    'end_timestamp_ms',
                )
                .cast(
                    polars.Datetime(
                        time_unit='ms',
                        time_zone=UTC,
                    ),
                )
                .alias(
                    'end_datetime',
                ),
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
            )

            new_candle_dataframe = new_candle_dataframe.sort(
                'start_trade_id',
            )

            candle_dataframe: polars.DataFrame

            if old_candle_dataframe is not None:
                candle_dataframe = polars.concat(
                    [old_candle_dataframe, new_candle_dataframe]
                )
            else:
                candle_dataframe = new_candle_dataframe

            candle_dataframe_by_interval_name_map[interval_name] = candle_dataframe

    async def __update_trades_dataframe(
        self,
    ) -> None:
        current_symbol_name = self.__current_symbol_name

        if current_symbol_name is None:
            return

        with Timer() as timer:
            self.__update_candle_dataframe_by_interval_name_map()

        logger.info(
            f'Candle dataframe by interval name map was updated by {timer.elapsed:.3f}s'
        )

        with Timer() as timer:
            self.__update_rsi_series()

        logger.info(
            f'RSI series were updated by {timer.elapsed:.3f}s',
        )

        with Timer() as timer:  # TODO
            self.__update_trades_smoothed_dataframe_by_level_map()

        logger.info(
            f'Trades smoothed dataframe by level map was updated by {timer.elapsed:.3f}s'
        )

        with Timer() as timer:
            self.__update_extreme_lines()

        logger.info(f'Extreme lines were updated by {timer.elapsed:.3f}s')

        with Timer() as timer:
            await self.__update_order_book_volumes()

        logger.info(f'Order book volumes were updated by {timer.elapsed:.3f}s')

        with Timer() as timer:
            self.__update_velocity_series()

        logger.info(
            f'Velocity series were updated by {timer.elapsed:.3f}s',
        )

        await self.__window.plot(
            is_need_run_once=True,
        )

    @staticmethod
    def __fetch_order_book_dataframe(
            max_timestamp_ms: int,
            min_timestamp_ms: int,
            symbol_id: SymbolId,
    ) -> DataFrame | None:
        with Timer() as timer:
            order_book_dataframe = polars.read_database_uri(
                query=(
                    'SELECT'
                    # Primary key fields
                    ' timestamp_ms'
                    # Attribute fields
                    ', action_id'
                    ', asks'
                    ', bids'
                    f' FROM {OKXOrderBookData2.__tablename__}'
                    f' WHERE symbol_id = {symbol_id.name!r}'
                    f' AND timestamp_ms >= {min_timestamp_ms!r}'
                    f' AND timestamp_ms <= {max_timestamp_ms!r}'
                    ' ORDER BY'
                    ' symbol_id ASC'
                    ', timestamp_ms ASC'
                    ';'
                ),
                engine='connectorx',
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

        print(
            f'Fetched order book dataframe by {timer.elapsed:.3f}s',
        )

        order_book_dataframe = order_book_dataframe.with_columns(
            polars.col(
                'timestamp_ms',
            )
            .cast(
                polars.Datetime(
                    time_unit='ms',
                    time_zone=UTC,
                ),
            )
            .alias(
                'datetime',
            )
        )

        order_book_dataframe = order_book_dataframe.sort(
            'datetime',
        )

        return order_book_dataframe

    @staticmethod
    def __fetch_trades_dataframe(
            min_trade_id: int,
            symbol_id: SymbolId,
    ) -> DataFrame | None:
        with Timer() as timer:
            trades_dataframe = polars.read_database_uri(
                engine='connectorx',
                query=(
                    'SELECT'
                    # Primary key fields
                    ' trade_id'
                    # Attribute fields
                    ', is_buy'
                    ', price'
                    ', quantity'
                    ', timestamp_ms'
                    f' FROM {OKXTradeData2.__tablename__}'
                    ' WHERE'
                    f' symbol_id = {symbol_id.name!r}'
                    f' AND trade_id >= {min_trade_id!r}'
                    ' ORDER BY'
                    ' symbol_id ASC'
                    ', trade_id DESC'
                    f' LIMIT {15_000_000!r}'
                    # f' LIMIT {10_000_000!r}'
                    # f' LIMIT {5_000_000!r}'
                    # f' LIMIT {2_000_000!r}'
                    # f' LIMIT {100_000!r}'
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

        print(f'Fetched trades dataframe by {timer.elapsed:.3f}s')

        trades_dataframe = trades_dataframe.with_columns(
            polars.col(
                'timestamp_ms',
            )
            .cast(
                polars.Datetime(
                    time_unit='ms',
                    time_zone=UTC,
                ),
            )
            .alias(
                'datetime',
            ),
            polars.col(
                'price',
            ).cast(
                polars.Float64,
            ),
            polars.col(
                'quantity',
            ).cast(
                polars.Float64,
            ),
        )

        trades_dataframe = trades_dataframe.sort(
            'trade_id',
        )

        return trades_dataframe

    async def __update_current_available_symbol_name_set(
        self,
    ) -> None:
        current_available_symbol_name_set: set[str] | None = None

        postgres_db_session_maker = g_globals.get_postgres_db_session_maker()

        async with postgres_db_session_maker() as session:
            recursive_cte_full_query = text(
                """
WITH RECURSIVE symbol_id_cte(symbol_id) AS 
(
  (
    SELECT okx_trade_data_2.symbol_id AS symbol_id 
    FROM okx_trade_data_2 ORDER BY okx_trade_data_2.symbol_id ASC 
    LIMIT 1
  )
  UNION ALL
  SELECT (
    SELECT symbol_id
    FROM okx_trade_data_2
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

        # window.auto_range_price_plot()

    def __update_extreme_lines(
            self,
    ) -> None:
        trades_dataframe = self.__trades_dataframe

        assert trades_dataframe is not None, None

        price_series = trades_dataframe.get_column(
            'price',
        )

        max_price = float(price_series.max())

        min_price = float(price_series.min())

        delta_price = max_price - min_price

        if not delta_price:
            return

        trade_id_series = trades_dataframe.get_column('trade_id')

        max_trade_id = int(trade_id_series.max())

        min_trade_id = int(trade_id_series.min())

        delta_trade_id = max_trade_id - min_trade_id

        if not delta_trade_id:
            return

        logger.info(f'delta_price: {delta_price}, delta_trade_id: {delta_trade_id})')

        # delta_price / delta_trade_id = height / width;
        # width = height * (delta_trade_id / delta_price);

        aspect_ratio = delta_trade_id / delta_price

        height = int(
            100  # delta_price / 10,
        )

        extreme_lines_scale = delta_price / height

        width = int(
            height * aspect_ratio,
            # delta_trade_id / 10,
        )

        logger.info(
            f'Creating extreme line array ({width} x {height}, scale {extreme_lines_scale}, aspect ratio {aspect_ratio})'
        )

        extreme_lines_array = numpy.zeros(
            (
                width,
                height,
            ),
        )

        logger.info('Filling extreme line array...')

        line_dataframe_by_level_map = self.__line_dataframe_by_level_map

        first_line_dataframe: DataFrame = line_dataframe_by_level_map['Smoothed (1)']

        active_extreme_line_raw_data_by_price_map: dict[
            float, dict[str, typing.Any]
        ] = {}
        extreme_line_raw_data_list: list[dict[str, typing.Any]] = []

        for line_data in first_line_dataframe.iter_rows(named=True):
            end_trade_id = int(line_data['end_trade_id'])

            start_trade_id = int(line_data['start_trade_id'])

            end_price = line_data['end_price']
            start_price = line_data['start_price']

            left_price = min(
                end_price,
                start_price,
            )

            right_price = max(
                end_price,
                start_price,
            )

            for price, extreme_line_raw_data in tuple(
                    active_extreme_line_raw_data_by_price_map.items(),
            ):
                if not (left_price <= price <= right_price):
                    continue

                active_extreme_line_raw_data_by_price_map.pop(
                    price,
                )

                extreme_line_raw_data.update(
                    {
                    'end_trade_id': start_trade_id,  # end_trade_id,
                    'price': price,
                    }
                )

                extreme_line_raw_data_list.append(
                    extreme_line_raw_data,
                )

            assert end_price not in active_extreme_line_raw_data_by_price_map, (
                end_price,
            )
            assert start_price not in active_extreme_line_raw_data_by_price_map, (
                start_price,
            )

            active_extreme_line_raw_data_by_price_map.update(
                {
                    end_price: {
                        'end_trade_id': None,
                        'start_trade_id': end_trade_id,
                    },
                    start_price: {
                        'end_trade_id': None,
                        'start_trade_id': start_trade_id,
                    },
                }
            )

        for (
            price,
            extreme_line_raw_data,
        ) in active_extreme_line_raw_data_by_price_map.items():
            extreme_line_raw_data.update(
                {
                'end_trade_id': max_trade_id,
                'price': price,
                }
            )

            extreme_line_raw_data_list.append(
                extreme_line_raw_data,
            )

        active_extreme_line_raw_data_by_price_map.clear()

        for extreme_line_raw_data in extreme_line_raw_data_list:
            end_trade_id: int = extreme_line_raw_data['end_trade_id']
            price: float = extreme_line_raw_data['price']
            start_trade_id: int = extreme_line_raw_data['start_trade_id']

            end_x = int((end_trade_id - min_trade_id) / extreme_lines_scale)

            start_x = int((start_trade_id - min_trade_id) / extreme_lines_scale)

            y = min(int((price - min_price) / extreme_lines_scale), height - 1)

            # logger.info(
            #     f'Price: {price}, start_trade_id: {start_trade_id}, end_trade_id: {end_trade_id}, start_x: {start_x}, end_x: {end_x}, y: {y}'
            # )

            # for x in range(start_x, end_x):
            #     extreme_lines_array[x, y] = x - start_x

            extreme_lines_array[start_x:end_x, y] = numpy.arange(
                end_x - start_x,
            )

        self.__extreme_lines_array = extreme_lines_array

        self.__extreme_lines_position = (
            float(
                min_trade_id,
            ),
            float(
                min_price,
            ),
        )

        self.__extreme_lines_scale = extreme_lines_scale

    async def __update_order_book_volumes(
            self,
    ) -> None:
        current_symbol_name = self.__current_symbol_name
        if current_symbol_name is None:
            return

        current_symbol_id = SymbolConstants.IdByName[current_symbol_name]

        trades_dataframe = self.__trades_dataframe

        assert trades_dataframe is not None, None

        price_series = trades_dataframe.get_column('price')

        max_price = float(price_series.max())

        min_price = float(price_series.min())

        delta_price = max_price - min_price

        if not delta_price:
            return

        trade_id_series = trades_dataframe.get_column('trade_id')

        max_trade_id = int(trade_id_series.max())

        min_trade_id = int(trade_id_series.min())

        delta_trade_id = max_trade_id - min_trade_id

        if not delta_trade_id:
            return

        logger.info(f'delta_price: {delta_price}, delta_trade_id: {delta_trade_id})')

        datetime_series = trades_dataframe.get_column(
            'datetime',
        )

        max_datetime: datetime = datetime_series.max()
        max_timestamp_ms = int(
            max_datetime.timestamp() * 1000,  # ms
        )

        min_datetime: datetime = datetime_series.min()
        min_timestamp_ms = int(
            min_datetime.timestamp() * 1000,  # ms
        )

        delta_timestamp_ms = max_timestamp_ms - min_timestamp_ms

        if not delta_timestamp_ms:
            return

        if _IS_ORDER_BOOK_VOLUME_ENABLED:
            postgres_db_session_maker = g_globals.get_postgres_db_session_maker()

            async with postgres_db_session_maker() as session:
                async with session.begin():
                    result = await session.execute(
                        select(
                            OKXOrderBookData2,
                        )
                        .where(
                            and_(
                                OKXOrderBookData2.symbol_id == current_symbol_id,
                                OKXOrderBookData2.timestamp_ms <= min_timestamp_ms,
                                    OKXOrderBookData2.action_id
                                    == OKXOrderBookActionId.Snapshot,
                            )
                        )
                        .order_by(
                            OKXOrderBookData2.symbol_id.asc(),
                            OKXOrderBookData2.timestamp_ms.desc(),  # TODO: make index
                        )
                        .limit(
                            1,
                        )
                    )

                    initial_order_book_snapshot_data = result.scalar_one_or_none()

                    if initial_order_book_snapshot_data is None:
                        logger.info(
                            'Could not find initial order book snapshot data'
                        f' for symbol ID {current_symbol_id.name} and max timestamp (ms) {min_timestamp_ms}'
                        )
                    else:
                        logger.info(
                            f'Found initial order book snapshot data: {initial_order_book_snapshot_data}'
                        )

                        new_order_book_dataframe: DataFrame = self.__fetch_order_book_dataframe(
                                max_timestamp_ms=max_timestamp_ms,
                            min_timestamp_ms=initial_order_book_snapshot_data.timestamp_ms,
                                symbol_id=current_symbol_id,
                        )

                        logger.info(f'new_order_book_dataframe: {new_order_book_dataframe}')

        # delta_price / delta_trade_id = height / width;
        # width = height * (delta_trade_id / delta_price);

        aspect_ratio = delta_trade_id / delta_price

        height = int(
            100  # delta_price / 10,
        )

        order_book_volumes_scale = delta_price / height

        width = int(
            height * aspect_ratio,
            # delta_trade_id / 10,
        )

        logger.info(
            f'Creating order book volume array ({width} x {height}, scale {order_book_volumes_scale}, aspect ratio {aspect_ratio})'
        )

        order_book_volumes_asks_array = numpy.zeros(
            (
            width,
            height,
            )
        )

        order_book_volumes_bids_array = numpy.zeros(
            (
                width,
                height,
            ),
        )

        if _IS_ORDER_BOOK_VOLUME_ENABLED:
            logger.info('Filling order book volumes asks and bids arrays...')

            order_book_ask_quantity_by_price_map: dict[Decimal, Decimal] = {}
            order_book_bid_quantity_by_price_map: dict[Decimal, Decimal] = {}

            for (
                timestamp_ms,
                action_id_raw,
                asks_raw,
                bids_raw,
                datetime_,
            ) in new_order_book_dataframe.iter_rows(
                named=False,
            ):
                action_id = getattr(
                    OKXOrderBookActionId,
                    action_id_raw,
                )

                if action_id == OKXOrderBookActionId.Snapshot:
                    order_book_ask_quantity_by_price_map.clear()
                    order_book_bid_quantity_by_price_map.clear()

                asks: list[str, str, str, str] = orjson.loads(
                    asks_raw,
                )

                for ask_list in asks:
                    price_raw, quantity_raw, _, _ = ask_list

                    price = Decimal(
                        price_raw,
                    )

                    quantity = Decimal(
                        quantity_raw,
                    )

                    if quantity:
                        order_book_ask_quantity_by_price_map[price] = quantity
                    else:
                        order_book_ask_quantity_by_price_map.pop(
                            price,
                            None,
                        )

                bids: list[str, str, str, str] = orjson.loads(
                    bids_raw,
                )

                for bid_list in bids:
                    price_raw, quantity_raw, _, _ = bid_list

                    price = Decimal(
                        price_raw,
                    )

                    quantity = Decimal(
                        quantity_raw,
                    )

                    if quantity:
                        order_book_bid_quantity_by_price_map[price] = quantity
                    else:
                        order_book_bid_quantity_by_price_map.pop(
                            price,
                            None,
                        )

                    if datetime_ < min_datetime:
                        continue

                logger.info(
                    f'{datetime_} / {max_datetime} ({100.0 * (timestamp_ms - min_timestamp_ms) / delta_timestamp_ms:.3f}%)'
                )

                idx = datetime_series.search_sorted(
                    element=datetime_,
                    side='left',
                )

                closest_trade_id = trade_id_series[idx]

                x = min(
                    int(
                        (closest_trade_id - min_trade_id) / order_book_volumes_scale,
                    ),
                    width - 1,
                )

                for price, quantity in order_book_ask_quantity_by_price_map.items():
                    volume = price * quantity

                    y = min(
                        int(
                            (float(price) - min_price) / order_book_volumes_scale,
                        ),
                        height - 1,
                    )

                    order_book_volumes_asks_array[x, y] = float(
                        volume,
                    )

                for price, quantity in order_book_bid_quantity_by_price_map.items():
                    volume = price * quantity

                    y = min(
                        int(
                            (float(price) - min_price) / order_book_volumes_scale,
                        ),
                        height - 1,
                    )

                    order_book_volumes_bids_array[x, y] = float(
                        volume,
                    )

        self.__order_book_volumes_asks_array = order_book_volumes_asks_array
        self.__order_book_volumes_bids_array = order_book_volumes_bids_array

        self.__order_book_volumes_position = (
            float(
                min_trade_id,
            ),
            float(
                min_price,
            ),
        )

        self.__order_book_volumes_scale = order_book_volumes_scale

    def __update_rsi_series(
        self,
    ) -> None:
        current_rsi_interval_name = self.__current_rsi_interval_name

        if current_rsi_interval_name is None:
            return

        candle_dataframe_by_interval_name_map = (
            self.__candle_dataframe_by_interval_name_map
        )

        candle_dataframe = candle_dataframe_by_interval_name_map.get(
            current_rsi_interval_name,
        )

        if candle_dataframe is None:
            return

        candle_dataframe = candle_dataframe.with_columns(
            polars_talib.rsi(
                real=polars.col(
                    'close_price',
                ),
            timeperiod=14,  # 6
            ).alias(
                'rsi',
            )
        )

        rsi_series = candle_dataframe.get_column(
            'rsi',
        )

        if rsi_series.is_empty():
            return

        self.__rsi_series = rsi_series

    def __update_trades_smoothed_dataframe_by_level_map(self) -> None:
        line_dataframe_by_level_map = self.__line_dataframe_by_level_map

        trades_dataframe = self.__trades_dataframe

        assert trades_dataframe is not None, None

        trades_smoothed_dataframe_by_level_map = (
            self.__trades_smoothed_dataframe_by_level_map
        )

        previous_line_dataframe: DataFrame | None = None

        for smoothing_level in PlotConstants.TradesSmoothingLevels:
            is_raw_level = smoothing_level == 'Raw (0)'

            if is_raw_level:
                continue

            is_first_level = smoothing_level == 'Smoothed (1)'

            line_dataframe: DataFrame
            line_raw_data: dict[str, typing.Any] | None = None
            line_raw_data_list: list[dict[str, typing.Any]] = []

            old_line_dataframe: DataFrame | None

            if is_first_level:
                old_line_dataframe = line_dataframe_by_level_map.get(
                    smoothing_level,
                )

                min_start_trade_id: int

                if old_line_dataframe is not None:
                    min_polars_start_trade_id = old_line_dataframe.get_column(
                        'start_trade_id',
                    ).max()

                    min_start_trade_id = int(
                        min_polars_start_trade_id,
                    )
                else:
                    min_start_trade_id = 0

                for trade_data in trades_dataframe.filter(
                    polars.col(
                        'trade_id',
                    )
                    >= min_start_trade_id
                ).iter_rows(
                    named=True,
                ):
                    trade_id: int = trade_data['trade_id']

                    # if trade_id < min_trade_id:
                    #     continue

                    price: float = trade_data['price']
                    quantity: float = trade_data['quantity']
                    volume = price * quantity

                    datetime_: datetime = trade_data['datetime']

                    # timestamp_ms = timestamp.value // 10 ** 6

                    if line_raw_data is None:
                        line_raw_data = {
                            'start_datetime': datetime_,
                            'start_price': price,
                            'start_trade_id': trade_id,
                            'quantity': quantity,
                            'trading_direction': None,
                            'volume': volume,
                        }
                    else:
                        old_trading_direction: TradingDirection | None = line_raw_data[
                            'trading_direction'
                        ]

                        if old_trading_direction is None:
                            start_price: float = line_raw_data['start_price']
                            end_price = price

                            new_trading_direction = TradingUtils.get_direction(
                                start_price,
                                end_price,
                            )

                            line_raw_data.update(
                                {
                                    'end_datetime': datetime_,
                                    'end_price': end_price,
                                    'end_trade_id': trade_id,
                                    'quantity': (line_raw_data['quantity'] + quantity),
                                    'trading_direction': new_trading_direction,
                                    'volume': (line_raw_data['volume'] + volume),
                                },
                            )
                        else:
                            new_end_price = price
                            old_end_price: float = line_raw_data['end_price']

                            new_trading_direction = TradingUtils.get_direction(
                                old_end_price,
                                new_end_price,
                            )

                            if (
                                old_trading_direction == new_trading_direction
                                or new_trading_direction == TradingDirection.Cross
                            ):
                                line_raw_data.update(
                                    {
                                        'quantity': (
                                            line_raw_data['quantity'] + quantity
                                        ),
                                        'volume': line_raw_data['volume'] + volume,
                                    },
                                )
                            else:
                                # Flush

                                line_raw_data_list.append(
                                    line_raw_data,
                                )

                                line_raw_data = {
                                    'start_datetime': line_raw_data['end_datetime'],
                                    'start_trade_id': line_raw_data['end_trade_id'],
                                    'start_price': old_end_price,
                                    'quantity': quantity,
                                    'trading_direction': new_trading_direction,
                                    'volume': volume,
                                }

                            line_raw_data.update(
                                {
                                    'end_price': new_end_price,
                                    'end_datetime': datetime_,
                                    'end_trade_id': trade_id,
                                }
                            )

                if line_raw_data is not None:
                    trading_direction: TradingDirection | None = line_raw_data[
                        'trading_direction'
                    ]

                    if trading_direction is not None:
                        # Flush

                        line_raw_data_list.append(
                            line_raw_data,
                        )

                        line_raw_data = None  # noqa
            else:
                break  # TODO: remove

                old_line_dataframe = None
                min_start_trade_id = 0

                assert previous_line_dataframe is not None, None

                line_raw_data_1: dict[str, typing.Any] | None = None
                line_raw_data_list_1: list[dict[str, typing.Any]] = []

                first_line_data: dict[str, typing.Any] | None = None
                second_line_data: dict[str, typing.Any] | None = None

                # for line_data in previous_line_dataframe.loc[
                #     previous_line_dataframe.get_column('start_trade_id',) >= min_start_trade_id
                # ].iter_rows(named=True):

                for line_data in previous_line_dataframe.iter_rows(named=True,):
                    high_price: float
                    trading_direction: TradingDirection
                    low_price: float

                    if line_raw_data_1 is None:
                        end_price: float = line_data['end_price']
                        high_price = end_price
                        start_price: float = line_data['start_price']
                        low_price: float = start_price

                        trading_direction: TradingDirection = line_data[
                            'trading_direction'
                        ]

                        line_raw_data_1 = {
                            'end_datetime': line_data['end_datetime'],
                            'end_trade_id': line_data['end_trade_id'],
                            'end_price': end_price,
                            'high_price': high_price,
                            'low_price': low_price,
                            'start_datetime': line_data['start_datetime'],
                            'start_trade_id': line_data['start_trade_id'],
                            'start_price': start_price,
                            'quantity': line_data['quantity'],
                            'trading_direction': trading_direction,
                            'volume': line_data['volume'],
                        }

                        if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                            print('Line raw data init', line_raw_data_1)

                        # if trading_direction == TradingDirection.Cross:
                        #     # Flush
                        #
                        #     if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                        #         print(
                        #             'Flushing 1',
                        #             line_raw_data_1,
                        #         )
                        #
                        #     line_raw_data_list_1.append(
                        #         line_raw_data_1,
                        #     )
                        #
                        #     line_raw_data_1 = None

                        continue

                    if line_data['trading_direction'] == TradingDirection.Cross:
                        # Combine

                        line_raw_data_1.update(
                            {
                                'end_datetime': line_data['end_datetime'],
                                'end_trade_id': line_data['end_trade_id'],
                                'end_price': line_data['end_price'],
                                # 'high_price': high_price,
                                # 'low_price': low_price,
                                'quantity': line_raw_data_1['quantity']
                                + line_data['quantity'],
                                'volume': line_raw_data_1['volume']
                                + line_data['volume'],
                            },
                        )

                        continue

                    if first_line_data is None:
                        first_line_data = line_data

                        continue
                    elif second_line_data is None:
                        second_line_data = line_data

                    high_price: float = line_raw_data_1['high_price']
                    low_price: float = line_raw_data_1['low_price']

                    trading_direction: TradingDirection = line_raw_data_1[
                        'trading_direction'
                    ]

                    if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                        print('Line data:', line_data)

                    assert first_line_data['trading_direction'] != trading_direction, (
                        first_line_data,
                        line_raw_data_1,
                        second_line_data,
                        smoothing_level,
                    )

                    if second_line_data['trading_direction'] != trading_direction:
                        # Flush

                        if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                            print('Flushing 2', line_raw_data_1)

                        line_raw_data_list_1.append(
                            line_raw_data_1,
                        )

                        end_price: float = first_line_data['end_price']
                        high_price = end_price
                        start_price: float = first_line_data['start_price']
                        low_price: float = start_price

                        trading_direction: TradingDirection = first_line_data[
                            'trading_direction'
                        ]

                        line_raw_data_1 = {
                            'end_datetime': first_line_data['end_datetime'],
                            'end_trade_id': first_line_data['end_trade_id'],
                            'end_price': end_price,
                            'high_price': high_price,
                            'low_price': low_price,
                            'start_datetime': first_line_data['start_datetime'],
                            'start_trade_id': first_line_data['start_trade_id'],
                            'start_price': start_price,
                            'quantity': first_line_data['quantity'],
                            'trading_direction': trading_direction,
                            'volume': first_line_data['volume'],
                        }

                        # if trading_direction == TradingDirection.Cross:
                        #     # Flush
                        #
                        #     if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                        #         print(
                        #             'Flushing 3',
                        #             line_raw_data_1,
                        #         )
                        #
                        #     line_raw_data_list_1.append(
                        #         line_raw_data_1,
                        #     )
                        #
                        #     line_raw_data_1 = None

                        first_line_data = second_line_data
                        second_line_data = None

                        continue

                    # Check low price update direction

                    new_low_price = first_line_data['end_price']

                    low_price_direction = TradingUtils.get_direction(
                        low_price,
                        new_low_price,
                    )

                    if (
                        low_price_direction != trading_direction
                        and low_price_direction != TradingDirection.Cross
                    ):
                        # Flush

                        if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                            print('Flushing 4', line_raw_data_1)

                        line_raw_data_list_1.append(
                            line_raw_data_1,
                        )

                        end_price: float = first_line_data['end_price']
                        high_price = end_price
                        start_price: float = first_line_data['start_price']
                        low_price: float = start_price

                        trading_direction: TradingDirection = first_line_data[
                            'trading_direction'
                        ]

                        line_raw_data_1 = {
                            'end_datetime': first_line_data['end_datetime'],
                            'end_trade_id': first_line_data['end_trade_id'],
                            'end_price': end_price,
                            'high_price': high_price,
                            'low_price': low_price,
                            'start_datetime': first_line_data['start_datetime'],
                            'start_trade_id': first_line_data['start_trade_id'],
                            'start_price': start_price,
                            'quantity': first_line_data['quantity'],
                            'trading_direction': trading_direction,
                            'volume': first_line_data['volume'],
                        }

                        # if trading_direction == TradingDirection.Cross:
                        #     # Flush
                        #
                        #     if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                        #         print(
                        #             'Flushing 5',
                        #             line_raw_data_1,
                        #         )
                        #
                        #     line_raw_data_list_1.append(
                        #         line_raw_data_1,
                        #     )
                        #
                        #     line_raw_data_1 = None

                        first_line_data = second_line_data
                        second_line_data = None

                        continue

                    # Check high price update direction

                    new_high_price = second_line_data['end_price']

                    high_price_direction = TradingUtils.get_direction(
                        high_price,
                        new_high_price,
                    )

                    if (
                        high_price_direction != trading_direction
                        and high_price_direction != TradingDirection.Cross
                    ):
                        # Flush

                        if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                            print('Flushing 6', line_raw_data_1)

                        line_raw_data_list_1.append(
                            line_raw_data_1,
                        )

                        end_price: float = first_line_data['end_price']
                        high_price = end_price
                        start_price: float = first_line_data['start_price']
                        low_price: float = start_price

                        trading_direction: TradingDirection = first_line_data[
                            'trading_direction'
                        ]

                        line_raw_data_1 = {
                            'end_datetime': first_line_data['end_datetime'],
                            'end_trade_id': first_line_data['end_trade_id'],
                            'end_price': end_price,
                            'high_price': high_price,
                            'low_price': low_price,
                            'start_datetime': first_line_data['start_datetime'],
                            'start_trade_id': first_line_data['start_trade_id'],
                            'start_price': start_price,
                            'quantity': first_line_data['quantity'],
                            'trading_direction': trading_direction,
                            'volume': first_line_data['volume'],
                        }

                        # if trading_direction == TradingDirection.Cross:
                        #     # Flush
                        #
                        #     if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                        #         print(
                        #             'Flushing 7',
                        #             line_raw_data_1,
                        #         )
                        #
                        #     line_raw_data_list_1.append(
                        #         line_raw_data_1,
                        #     )
                        #
                        #     line_raw_data_1 = None

                        first_line_data = second_line_data
                        second_line_data = None

                        continue

                    # Update low price

                    if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                        print('Update low price to', new_low_price)

                    line_raw_data_1.update(
                        {
                            'low_price': new_low_price,
                        }
                    )

                    # Combine line data

                    line_raw_data_1.update(
                        {
                            'end_datetime': first_line_data['end_datetime'],
                            'end_trade_id': first_line_data['end_trade_id'],
                            'end_price': first_line_data['end_price'],
                            'quantity': line_raw_data_1['quantity']
                            + first_line_data['quantity'],
                            'volume': line_raw_data_1['quantity']
                            + first_line_data['volume'],
                        },
                    )

                    # Update high price

                    if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                        print('Update high price to', new_high_price)

                    line_raw_data_1.update(
                        {
                            'high_price': new_high_price,
                        },
                    )

                    # Combine line data

                    line_raw_data_1.update(
                        {
                            'end_datetime': second_line_data['end_datetime'],
                            'end_trade_id': second_line_data['end_trade_id'],
                            'end_price': second_line_data['end_price'],
                            'quantity': line_raw_data_1['quantity']
                            + second_line_data['quantity'],
                            'volume': line_raw_data_1['quantity']
                            + second_line_data['volume'],
                        },
                    )

                    first_line_data = None
                    second_line_data = None

                if line_raw_data_1 is not None:
                    # Flush

                    if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                        print('Flushing 8', line_raw_data_1)

                    line_raw_data_list_1.append(
                        line_raw_data_1,
                    )

                    line_raw_data_1 = None  # noqa

                if first_line_data is not None:
                    end_price: float = first_line_data['end_price']
                    high_price = end_price
                    start_price: float = first_line_data['start_price']
                    low_price: float = start_price

                    trading_direction: TradingDirection = first_line_data[
                        'trading_direction'
                    ]

                    line_raw_data_1 = {
                        'end_datetime': first_line_data['end_datetime'],
                        'end_trade_id': first_line_data['end_trade_id'],
                        'end_price': end_price,
                        'high_price': high_price,
                        'low_price': low_price,
                        'start_datetime': first_line_data['start_datetime'],
                        'start_trade_id': first_line_data['start_trade_id'],
                        'start_price': start_price,
                        'quantity': first_line_data['quantity'],
                        'trading_direction': trading_direction,
                        'volume': first_line_data['volume'],
                    }

                    # Flush

                    if smoothing_level == _DEBUG_SMOOTHING_LEVEL:
                        print('Flushing 9', line_raw_data_1)

                    line_raw_data_list_1.append(
                        line_raw_data_1,
                    )

                    first_line_data = None  # noqa
                    line_raw_data_1 = None  # noqa

                for line_raw_data_1 in line_raw_data_list_1:
                    if line_raw_data is None:
                        line_raw_data = line_raw_data_1

                        continue

                    old_trading_direction: TradingDirection = line_raw_data[
                        'trading_direction'
                    ]

                    new_trading_direction: TradingDirection = line_raw_data_1[
                        'trading_direction'
                    ]

                    if old_trading_direction == new_trading_direction:
                        line_raw_data.update(
                            {
                                'end_datetime': line_raw_data_1['end_datetime'],
                                'end_trade_id': line_raw_data_1['end_trade_id'],
                                'end_price': line_raw_data_1['end_price'],
                                'quantity': (
                                    line_raw_data['quantity']
                                    + line_raw_data_1['quantity']
                                ),
                                'volume': (
                                    line_raw_data['volume'] + line_raw_data_1['volume']
                                ),
                            },
                        )
                    else:
                        # Flush

                        line_raw_data_list.append(
                            line_raw_data,
                        )

                        line_raw_data = line_raw_data_1

                if line_raw_data is not None:
                    # Flush

                    line_raw_data_list.append(line_raw_data)

                    line_raw_data = None  # noqa

            if line_raw_data_list:
                new_line_dataframe = polars.DataFrame(
                    line_raw_data_list,
                )

                assert new_line_dataframe.height > 0, None

                if old_line_dataframe is not None:
                    line_dataframe = polars.concat(
                        [old_line_dataframe, new_line_dataframe]
                    )
                else:
                    line_dataframe = new_line_dataframe

                line_dataframe = line_dataframe.sort(
                    'start_trade_id',
                )

                line_dataframe_by_level_map[smoothing_level] = line_dataframe
            else:
                line_dataframe = line_dataframe_by_level_map[smoothing_level]

            previous_line_dataframe = line_dataframe

            # Smoothed dataframe updating

            old_smoothed_dataframe = trades_smoothed_dataframe_by_level_map.get(
                smoothing_level,
            )

            # min_trade_id: int
            #
            # if old_line_dataframe is not None:
            #     min_polars_trade_id = old_smoothed_dataframe.get_column('trade_id',).max()
            #
            #     min_trade_id = int(
            #         min_polars_trade_id,
            #     )
            # else:
            #     min_trade_id = 0

            trades_smoothed_raw_data_list: list[dict[str, typing.Any]] = []

            last_line_data: dict[str, typing.Any] | None = None

            for line_data in line_dataframe.filter(
                polars.col(
                    'start_trade_id',
                )
                >= min_start_trade_id  # min_trade_id
            ).iter_rows(named=True):  # TODO
                trades_smoothed_raw_data_list.append(
                    {
                        'price': line_data['start_price'],
                        'datetime': line_data['start_datetime'],
                        'trade_id': line_data['start_trade_id'],
                    },
                )

                last_line_data = line_data

            if last_line_data is not None:
                trades_smoothed_raw_data_list.append(
                    {
                        'price': last_line_data['end_price'],
                        'datetime': last_line_data['end_datetime'],
                        'trade_id': last_line_data['end_trade_id'],
                    },
                )

            if trades_smoothed_raw_data_list:
                new_smoothed_dataframe = polars.DataFrame(
                    trades_smoothed_raw_data_list,
                )

                assert new_smoothed_dataframe.height > 0, None

                smoothed_dataframe: DataFrame

                if old_smoothed_dataframe is not None:
                    smoothed_dataframe = polars.concat(
                        [old_smoothed_dataframe, new_smoothed_dataframe]
                    )
                else:
                    smoothed_dataframe = new_smoothed_dataframe

                smoothed_dataframe = smoothed_dataframe.sort(
                    'trade_id',
                )

                trades_smoothed_dataframe_by_level_map[smoothing_level] = (
                    smoothed_dataframe
                )

    def __update_velocity_series(
        self,
    ) -> None:
        candle_dataframe_by_interval_name_map = (
            self.__candle_dataframe_by_interval_name_map
        )

        velocity_candle_dataframe = candle_dataframe_by_interval_name_map.get(
            PlotConstants.VelocityIntervalName,
        )

        if velocity_candle_dataframe is None:
            return

        self.__velocity_series = velocity_candle_dataframe.get_column(
            'trades_count',
        )
