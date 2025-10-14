import asyncio
import logging
import traceback
import typing
from collections import (
    defaultdict,
)
from datetime import (
    UTC,
)

import polars
from aiogram.utils.text_decorations import (
    markdown_decoration,
)
from sqlalchemy import (
    select,
    text,
)
from sqlalchemy.ext.asyncio import (
    AsyncSession,
)

from utils.time import TimeUtils

try:
    import uvloop
except ImportError:
    uvloop = asyncio

import main.monitor_imbalances.schemas
import main.save_candles.schemas
from main.monitor_imbalances.constants import (
    CANDLES_COUNT_PER_REQUEST,
    INTERVAL_NAME,
)
from main.monitor_imbalances.globals import (
    g_globals,
)
from settings import (
    settings,
)
from utils.telegram import (
    TelegramUtils,
)

logger = logging.getLogger(
    __name__,
)


def fetch_candles_dataframe(
        interval_name: str,
        symbol_name: str,
        candles_count: int,
) -> polars.DataFrame | None:
    """Получить последние свечи для символа"""
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


def create_long_imbalances_dataframe(
        candles_dataframe: polars.DataFrame,
) -> polars.DataFrame | None:
    """Создать датафрейм с лонговыми имбалансами"""
    if candles_dataframe is None:
        return None

    # max_timestamp_ms = candles_dataframe.get_column('start_timestamp_ms').max()

    # Получаем данные свечей
    candles_data = candles_dataframe.to_dicts()[:-1]

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

    if not inactive_imbalance_raw_data_list:
        return None

    # Создаем датафрейм из списка имбалансов
    long_imbalances_dataframe = polars.DataFrame(
        inactive_imbalance_raw_data_list,
    )

    return long_imbalances_dataframe.sort(by='start_timestamp_ms')


async def process_symbol(
        session: AsyncSession,
        symbol_name: str,
) -> None:
    logger.info(f'Processing symbol with name {symbol_name!r}...')

    # 1. Забираем последние свечи
    candles_dataframe = fetch_candles_dataframe(
        interval_name=INTERVAL_NAME,
        symbol_name=symbol_name,
        candles_count=CANDLES_COUNT_PER_REQUEST,
    )

    if candles_dataframe is None:
        logger.warning(
            f'Could not fetch candles for symbol {symbol_name!r}',
        )
        return

    # 2. Создаем датафрейм с имбалансами
    long_imbalances_dataframe = create_long_imbalances_dataframe(
        candles_dataframe,
    )

    # 3. Получаем текущую высоту датафрейма
    current_dataframe_height = long_imbalances_dataframe.height if long_imbalances_dataframe is not None else 0

    # 4. Получаем сохраненную высоту датафрейма из БД
    async with session.begin():
        stored_height_result = await session.execute(
            select(
                main.monitor_imbalances.schemas.SymbolDataframeHeight,
            ).where(
                main.monitor_imbalances.schemas.SymbolDataframeHeight.symbol_name
                == symbol_name,
            )
        )

        stored_height_record = stored_height_result.scalar_one_or_none()

    # 5. Сравниваем высоты и отправляем уведомление при изменении
    if stored_height_record is None:
        # Первый раз обрабатываем этот символ - сохраняем высоту
        current_timestamp_ms = TimeUtils.get_aware_current_timestamp_ms()

        async with session.begin():
            new_height_record = main.monitor_imbalances.schemas.SymbolDataframeHeight(
                symbol_name=symbol_name,
                dataframe_height=current_dataframe_height,
                last_update_timestamp_ms=current_timestamp_ms,
            )
            session.add(new_height_record)
            await session.commit()

        logger.info(f'First time processing symbol {symbol_name!r}, saved height: {current_dataframe_height}')

    elif stored_height_record.dataframe_height != current_dataframe_height:
        # Высота изменилась - отправляем уведомление
        logger.info(
            f'Dataframe height changed for symbol {symbol_name!r}: '
            f'{stored_height_record.dataframe_height} -> {current_dataframe_height}'
        )

        # Отправляем уведомление
        is_notification_sent = await send_telegram_notification(
            symbol_name,
            long_imbalances_dataframe,
            current_dataframe_height,
        )

        # Если уведомление отправлено успешно, обновляем высоту в БД
        if is_notification_sent:
            current_timestamp_ms = TimeUtils.get_aware_current_timestamp_ms()

            async with session.begin():
                stored_height_record.dataframe_height = current_dataframe_height
                stored_height_record.last_update_timestamp_ms = current_timestamp_ms
                await session.commit()

            logger.info(f'Updated dataframe height for symbol {symbol_name!r} to {current_dataframe_height}')
        else:
            logger.warning(f'Failed to send notification for symbol {symbol_name!r}, height not updated')
    else:
        # Высота не изменилась - ничего не делаем
        logger.debug(f'Dataframe height unchanged for symbol {symbol_name!r}: {current_dataframe_height}')


async def send_telegram_notification(
        symbol_name: str,
        long_imbalances_dataframe: polars.DataFrame | None,
        dataframe_height: int,
) -> bool:
    """Отправить уведомление в Telegram об изменении высоты датафрейма"""
    try:
        message_parts = []

        tradingview_url = f'https://ru.tradingview.com/chart/?symbol=BINANCE%3A{symbol_name.replace("-", "")}.P'

        message_parts.extend(
            [
                f'Символ: `{symbol_name}`\n',
                f'Интервал: `{INTERVAL_NAME}`\n',
                # f'Высота датафрейма: `{dataframe_height}`\n',
                f'TradingView: {markdown_decoration.quote(tradingview_url)}\n',
            ]
        )

        # Показываем детали имбалансов если они есть
        if long_imbalances_dataframe is not None and dataframe_height > 0:
            active_imbalances_data = long_imbalances_dataframe.filter(
                polars.col(
                    'end_timestamp_ms',
                ).is_null(),
            ).to_dicts()

            # Показываем только первые 5 активных имбалансов
            for i, imbalance in enumerate(active_imbalances_data[:5], 1):
                start_price = imbalance['start_price']
                end_price = imbalance['end_price']
                gap_percent = ((end_price - start_price) / start_price) * 100

                message_parts.append(
                    f'Разрыв: `{gap_percent:.2f}%`\n'
                )

            if len(active_imbalances_data) > 5:
                message_parts.append(
                    f'\\.\\.\\. и ещё {len(active_imbalances_data) - 5} имбалансов'
                )

        message = ''.join(message_parts, )

        success = await TelegramUtils.send_message_to_channel(message, )

        if success:
            logger.info(f'Sent Telegram notification for symbol {symbol_name!r}')
            await asyncio.sleep(5.0)  # s
            return True
        else:
            logger.warning(f'Failed to send Telegram notification for symbol {symbol_name!r}')
            return False
    except Exception as exception:
        logger.error(
            f'Could not send Telegram notification for symbol {symbol_name!r}'
            f': {"".join(traceback.format_exception(exception))}'
        )

        return False


async def monitor_imbalances() -> None:
    db_schema: (
        main.save_candles.schemas.BinanceCandleData1H
        | main.save_candles.schemas.BinanceCandleData4H
        | main.save_candles.schemas.BinanceCandleData1D
    ) = getattr(
        main.save_candles.schemas,
        f'BinanceCandleData{INTERVAL_NAME}',
    )

    postgres_db_session_maker = g_globals.get_postgres_db_session_maker()

    table_name = f'"{db_schema.__tablename__}"'

    async with postgres_db_session_maker() as session:
        recursive_cte_full_query = text(
            f"""
    WITH RECURSIVE symbol_name_cte(symbol_name) AS 
    (
      (
        SELECT {table_name}.symbol_name AS symbol_name 
        FROM {table_name} ORDER BY {table_name}.symbol_name ASC 
        LIMIT 1
      )
      UNION ALL
      SELECT (
        SELECT symbol_name
        FROM {table_name}
        WHERE symbol_name > cte.symbol_name
        ORDER BY symbol_name ASC
        LIMIT 1
      )
      FROM symbol_name_cte AS cte
      WHERE cte.symbol_name IS NOT NULL
    )
    SELECT symbol_name
    FROM symbol_name_cte
    WHERE symbol_name IS NOT NULL;
                        """
        )

        async with session.begin():
            result = await session.execute(
                recursive_cte_full_query,
            )

            symbol_names: list[str] = []

            for row in result:
                symbol_name: str = row.symbol_name

                symbol_names.append(
                    symbol_name,
                )

        for symbol_name in symbol_names:
            try:
                await process_symbol(
                    session,
                    symbol_name,
                )
            except Exception as exception:
                logger.error(
                    'Handled exception while processing symbol'
                    f': {"".join(traceback.format_exception(exception))}'
                )


def monitor_imbalances_process() -> None:
    # Set up logging

    logging.basicConfig(
        encoding='utf-8',
        format='[%(levelname)s][%(asctime)s][%(name)s]: %(message)s',
        level=(
            # logging.INFO
            logging.DEBUG
        ),
    )

    uvloop.run(
        monitor_imbalances(),
    )