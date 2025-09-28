import asyncio
import logging
import traceback
import typing
from decimal import Decimal

import httpx
import orjson

from constants.symbol import SymbolConstants

try:
    import uvloop
except ImportError:
    uvloop = asyncio

from sqlalchemy import (
    and_,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import (
    insert,
)

from main.save_candles import (
    schemas,
)
from main.save_candles.globals import (
    g_globals,
)
from utils.time import (
    TimeUtils,
)

logger = logging.getLogger(
    __name__,
)


_CANDLES_COUNT_PER_REQUEST = 500

_SYMBOL_NAMES = [
    'BTC-USDT',
    'ETH-USDT',
]


async def init_db_models():
    postgres_db_engine = g_globals.get_postgres_db_engine()

    async with postgres_db_engine.begin() as connection:
        # await connection.run_sync(
        #     models.Base.metadata.drop_all
        # )

        await connection.run_sync(
            schemas.Base.metadata.create_all,
        )


async def start_db_loop() -> None:
    postgres_db_task_queue = g_globals.get_postgres_db_task_queue()

    while True:
        task = await postgres_db_task_queue.get()

        try:
            await task
        except Exception as exception:
            logger.error(
                'Handled exception while awaiting DB task'
                f': {"".join(traceback.format_exception(exception))}',
            )


async def save_candles(
    api_session: httpx.AsyncClient,
) -> None:
    for symbol_name in _SYMBOL_NAMES:
        symbol_id = SymbolConstants.IdByName[symbol_name]

        for interval_name, interval_duration_ms in (
            (
                '1H',
                (
                    1000  # ms
                    * 60  # s
                    * 60  # m
                    * 1  # h
                ),
            ),
            (
                '4H',
                (
                    1000  # ms
                    * 60  # s
                    * 60  # m
                    * 4  # h
                ),
            ),
            (
                '1D',
                (
                    1000  # ms
                    * 60  # s
                    * 60  # m
                    * 24  # h
                    * 1  # d
                ),
            ),
        ):
            # Get last candle data

            postgres_db_session_maker = g_globals.get_postgres_db_session_maker()

            db_schema: (
                type[schemas.BinanceCandleData1H]
                | type[schemas.BinanceCandleData4H]
                | type[schemas.BinanceCandleData1D]
            ) = getattr(
                schemas,
                f'BinanceCandleData{interval_name}',
            )

            async with postgres_db_session_maker() as session:
                result = await session.execute(
                    select(
                        db_schema,
                    )
                    .where(
                        db_schema.symbol_id == symbol_id,
                    )
                    .order_by(
                        db_schema.start_timestamp_ms.desc(),
                    )
                    .limit(
                        1,
                    )
                )

                row_data = result.fetchone()

            is_last_candle_exists = row_data is not None

            now_timestamp_ms = TimeUtils.get_aware_current_timestamp_ms()

            if is_last_candle_exists:
                last_candle_data: (
                    schemas.BinanceCandleData1H
                    | schemas.BinanceCandleData4H
                    | schemas.BinanceCandleData1D
                )

                (last_candle_data,) = row_data

                last_candle_timestamp_ms = last_candle_data.start_timestamp_ms
            else:
                last_candle_timestamp_ms = now_timestamp_ms - (
                    1000  # ms
                    * 60  # s
                    * 60  # m
                    * 24  # h
                    * 365  # d
                    * 10  # y
                )

            logger.info(
                f'Saving {interval_name} candles for {symbol_name!r}'
                f' ({(now_timestamp_ms - last_candle_timestamp_ms) // (1000 * 60 * 60 * 1)} hours left)'
            )

            response = await api_session.get(
                url='/fapi/v1/klines',
                params={
                    'symbol': symbol_name.replace('-', ''),
                    'interval': interval_name.lower(),
                    # 'endTime': (
                    #     last_candle_timestamp_ms
                    #     + (
                    #         interval_duration_ms  # ms
                    #         * _CANDLES_COUNT_PER_REQUEST
                    #     )
                    # ),
                    'startTime': last_candle_timestamp_ms,
                    'limit': _CANDLES_COUNT_PER_REQUEST,
                },
            )

            response.raise_for_status()

            response_body = await response.aread()

            candle_tuples: list[
                tuple[int, str, str, str, str, str, int, str, int, str, str, str]
            ] = orjson.loads(
                response_body,
            )

            candle_raw_data_list_to_insert: list[dict[str, typing.Any]] = []
            candle_raw_data_to_update: dict[str, typing.Any] | None = None

            for candle_tuple in candle_tuples:
                (
                    candle_start_timestamp_ms,
                    candle_open_price_raw,
                    candle_high_price_raw,
                    candle_low_price_raw,
                    candle_close_price_raw,
                    candle_volume_raw,
                    candle_close_timestamp_ms,
                    candle_volume_quote_currency_raw,
                    candle_trades_count,
                    candle_taker_buy_volume_base_currency_raw,
                    candle_taker_buy_volume_quote_currency_raw,
                    _,
                ) = candle_tuple

                candle_open_price = Decimal(
                    candle_open_price_raw,
                )

                candle_high_price = Decimal(
                    candle_high_price_raw,
                )

                candle_low_price = Decimal(
                    candle_low_price_raw,
                )

                candle_close_price = Decimal(
                    candle_close_price_raw,
                )

                candle_taker_buy_volume_base_currency = Decimal(
                    candle_taker_buy_volume_base_currency_raw,
                )

                candle_taker_buy_volume_quote_currency = Decimal(
                    candle_taker_buy_volume_quote_currency_raw,
                )

                candle_volume = Decimal(
                    candle_volume_raw,
                )

                candle_volume_quote_currency = Decimal(
                    candle_volume_quote_currency_raw,
                )

                candle_raw_data = dict(
                    # Primary key fields
                    symbol_id=int(
                        symbol_id.value,
                    ),
                    start_timestamp_ms=candle_start_timestamp_ms,
                    # Attribute fields
                    close_price=candle_close_price,
                    high_price=candle_high_price,
                    open_price=candle_open_price,
                    low_price=candle_low_price,
                    taker_buy_volume_base_currency=candle_taker_buy_volume_base_currency,
                    taker_buy_volume_quote_currency=candle_taker_buy_volume_quote_currency,
                    trades_count=candle_trades_count,
                    volume=candle_volume,
                    volume_quote_currency=candle_volume_quote_currency,
                )

                if last_candle_timestamp_ms is not None and (
                    candle_start_timestamp_ms == last_candle_timestamp_ms
                ):
                    candle_raw_data_to_update = candle_raw_data
                else:
                    candle_raw_data_list_to_insert.append(
                        candle_raw_data,
                    )

            if is_last_candle_exists:
                assert candle_raw_data_to_update is not None, (
                    symbol_name,
                    interval_name,
                )

            if not (
                candle_raw_data_to_update is not None or candle_raw_data_list_to_insert
            ):
                logger.info(
                    'Nothing to update',
                )

                continue

            async with postgres_db_session_maker() as session:
                async with session.begin():
                    if candle_raw_data_to_update is not None:
                        # Remove primary key

                        candle_raw_data_to_update.pop(
                            'symbol_id',
                        )

                        start_timestamp_ms: int = candle_raw_data_to_update.pop(
                            'start_timestamp_ms',
                        )

                        await session.execute(
                            update(
                                db_schema,
                            )
                            .values(
                                candle_raw_data_to_update,
                            )
                            .where(
                                and_(
                                    db_schema.symbol_id == symbol_id,
                                    (
                                        db_schema.start_timestamp_ms
                                        == start_timestamp_ms
                                    ),
                                ),
                            )
                        )

                    if candle_raw_data_list_to_insert:
                        await session.execute(
                            insert(
                                db_schema,
                            ),
                            candle_raw_data_list_to_insert,
                        )

            logger.info(
                f'Added {len(candle_tuples)} {interval_name} candles',
            )

            await asyncio.sleep(
                1.0  # s
            )


async def start_candles_saving_loop() -> None:
    async with httpx.AsyncClient(
        base_url='https://fapi.binance.com',
    ) as api_session:
        while True:
            try:
                await save_candles(
                    api_session,
                )
            except Exception as exception:
                logger.error(
                    'Could not save candles'
                    ': handled exception'
                    f': {"".join(traceback.format_exception(exception))}'
                )

            await asyncio.sleep(
                15.0  # s
            )


async def main() -> None:
    # Set up logging

    logging.basicConfig(
        encoding='utf-8',
        format='[%(levelname)s][%(asctime)s][%(name)s]: %(message)s',
        level=(
            # logging.INFO
            logging.DEBUG
        ),
    )

    # Prepare DB

    await init_db_models()

    # Start loops

    await asyncio.gather(
        start_db_loop(),
        start_candles_saving_loop(),
    )


if __name__ == '__main__':
    uvloop.run(
        main(),
    )
