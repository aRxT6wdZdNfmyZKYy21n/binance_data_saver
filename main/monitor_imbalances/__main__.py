import asyncio
import logging
import traceback
from decimal import (
    Decimal,
)

import polars
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
from main.monitor_imbalances.globals import (
    g_globals,
)
from utils.telegram import (
    TelegramUtils,
)

logger = logging.getLogger(
    __name__,
)


_CANDLES_COUNT_PER_REQUEST = 30
_INTERVAL_NAME = '1D'


async def init_db_models():
    postgres_db_engine = g_globals.get_postgres_db_engine()

    async with postgres_db_engine.begin() as connection:
        await connection.run_sync(
            main.monitor_imbalances.schemas.Base.metadata.create_all,
        )

        await connection.run_sync(
            main.save_candles.schemas.Base.metadata.create_all,
        )


async def process_symbol(
    session: AsyncSession,
    symbol_name: str,
) -> None:
    logger.info(f'Processing symbol with name {symbol_name!r}...')

    # 1. Забираем последние свечи
    candles_dataframe = g_globals.fetch_candles_dataframe(
        interval_name=_INTERVAL_NAME,
        symbol_name=symbol_name,
        candles_count=_CANDLES_COUNT_PER_REQUEST,
    )

    if candles_dataframe is None:
        logger.warning(f'Could not fetch candles for symbol {symbol_name!r}')
        return

    # 2. Создаем датафрейм с имбалансами
    long_imbalances_dataframe = g_globals.create_long_imbalances_dataframe(
        candles_dataframe,
    )

    # 3. Получаем существующие открытые имбалансы из БД
    async with session.begin():
        existing_imbalances_result = await session.execute(
            select(
                main.monitor_imbalances.schemas.LongImbalanceData,
            ).where(
                main.monitor_imbalances.schemas.LongImbalanceData.symbol_name == symbol_name,
                main.monitor_imbalances.schemas.LongImbalanceData.is_closed == False,
            )
        )
        existing_imbalances = existing_imbalances_result.scalars().all()

    # 4. Обрабатываем новые имбалансы (grow-only подход)
    current_timestamp_ms = TimeUtils.get_aware_current_timestamp_ms()
    new_imbalances_added = 0
    existing_imbalances_closed = 0

    if long_imbalances_dataframe is not None:
        new_imbalances_data = long_imbalances_dataframe.to_dicts()
        
        # Создаем множество существующих имбалансов для быстрого поиска
        existing_imbalance_keys = {
            (imb.start_timestamp_ms, float(imb.start_price), float(imb.end_price))
            for imb in existing_imbalances
        }

        # Добавляем новые имбалансы
        async with session.begin():
            for imbalance_data in new_imbalances_data:
                imbalance_key = (
                    imbalance_data['start_timestamp_ms'],
                    imbalance_data['start_price'],
                    imbalance_data['end_price']
                )
                
                if imbalance_key not in existing_imbalance_keys:
                    # Это новый имбаланс - добавляем в БД
                    new_imbalance = main.monitor_imbalances.schemas.LongImbalanceData(
                        symbol_name=symbol_name,
                        start_timestamp_ms=imbalance_data['start_timestamp_ms'],
                        detection_timestamp_ms=current_timestamp_ms,
                        start_price=Decimal(str(imbalance_data['start_price'])),
                        end_price=Decimal(str(imbalance_data['end_price'])),
                        end_timestamp_ms=imbalance_data['end_timestamp_ms'],
                        is_closed=False if imbalance_data['end_timestamp_ms'] is None else True,
                        close_timestamp_ms=imbalance_data['end_timestamp_ms'],
                    )
                    session.add(new_imbalance)
                    new_imbalances_added += 1

            await session.commit()

        # Проверяем, какие существующие имбалансы нужно закрыть
        current_imbalance_keys = {
            (imb['start_timestamp_ms'], imb['start_price'], imb['end_price'])
            for imb in new_imbalances_data
        }

        async with session.begin():
            for existing_imbalance in existing_imbalances:
                existing_key = (
                    existing_imbalance.start_timestamp_ms,
                    float(existing_imbalance.start_price),
                    float(existing_imbalance.end_price)
                )
                
                if existing_key not in current_imbalance_keys:
                    # Этот имбаланс больше не активен - закрываем его
                    existing_imbalance.is_closed = True
                    existing_imbalance.close_timestamp_ms = current_timestamp_ms
                    existing_imbalances_closed += 1

            await session.commit()

    # 5. Отправляем уведомление в Telegram при изменениях
    if new_imbalances_added > 0 or existing_imbalances_closed > 0:
        await send_telegram_notification(
            symbol_name, 
            long_imbalances_dataframe,
            new_imbalances_added,
            existing_imbalances_closed
        )
        
        logger.info(
            f'Processed symbol {symbol_name!r}: +{new_imbalances_added} new, -{existing_imbalances_closed} closed'
        )


async def send_telegram_notification(
    symbol_name: str,
    long_imbalances_dataframe: polars.DataFrame | None,
    new_imbalances_added: int = 0,
    existing_imbalances_closed: int = 0,
) -> None:
    """Отправить уведомление в Telegram о изменениях в имбалансах"""
    try:
        message_parts = []
        
        if new_imbalances_added > 0 and existing_imbalances_closed > 0:
            # И новые, и закрытые
            message_parts.extend([
                f'🔄 *Обновление имбалансов*\n\n',
                f'Символ: `{symbol_name}`\n',
                f'Интервал: `{_INTERVAL_NAME}`\n',
                f'Новых: `+{new_imbalances_added}`\n',
                f'Закрытых: `-{existing_imbalances_closed}`\n\n',
            ])
        elif new_imbalances_added > 0:
            # Только новые
            message_parts.extend([
                f'🟢 *Новые лонговые имбалансы*\n\n',
                f'Символ: `{symbol_name}`\n',
                f'Интервал: `{_INTERVAL_NAME}`\n',
                f'Количество: `+{new_imbalances_added}`\n\n',
            ])
        elif existing_imbalances_closed > 0:
            # Только закрытые
            message_parts.extend([
                f'🔴 *Имбалансы закрыты*\n\n',
                f'Символ: `{symbol_name}`\n',
                f'Интервал: `{_INTERVAL_NAME}`\n',
                f'Закрыто: `-{existing_imbalances_closed}`\n\n',
            ])

        # Показываем детали только если есть новые имбалансы
        if new_imbalances_added > 0 and long_imbalances_dataframe is not None:
            imbalances_data = long_imbalances_dataframe.to_dicts()
            
            # Показываем только первые 5 новых имбалансов
            for i, imbalance in enumerate(imbalances_data[:5], 1):
                start_price = imbalance['start_price']
                end_price = imbalance['end_price']
                gap_percent = ((end_price - start_price) / start_price) * 100

                message_parts.append(
                    f'*{i}\\. Имбаланс:*\n'
                    f'   От: `{start_price:.4f}`\n'
                    f'   До: `{end_price:.4f}`\n'
                    f'   Разрыв: `{gap_percent:.2f}%`\n\n'
                )

            if len(imbalances_data) > 5:
                message_parts.append(
                    f'\\.\\.\\. и ещё {len(imbalances_data) - 5} имбалансов'
                )

        message = ''.join(message_parts)

        await TelegramUtils.send_message_to_channel(message)

        logger.info(f'Sent Telegram notification for symbol {symbol_name!r}')

    except Exception as exception:
        logger.error(
            f'Could not send Telegram notification for symbol {symbol_name!r}'
            f': {"".join(traceback.format_exception(exception))}'
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


async def monitor_imbalances() -> None:
    db_schema: (
        main.save_candles.schemas.BinanceCandleData1H
        | main.save_candles.schemas.BinanceCandleData4H
        | main.save_candles.schemas.BinanceCandleData1D
    ) = getattr(
        main.save_candles.schemas,
        f'BinanceCandleData{_INTERVAL_NAME}',
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


async def start_imbalances_monitoring_loop() -> None:
    while True:
        try:
            await monitor_imbalances()
        except Exception as exception:
            logger.error(
                'Could not monitor imbalances'
                ': handled exception'
                f': {"".join(traceback.format_exception(exception))}'
            )

        await asyncio.sleep(
            15.0  # s
        )


async def main_() -> None:
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
        start_imbalances_monitoring_loop(),
    )


if __name__ == '__main__':
    uvloop.run(
        main_(),
    )
