import asyncio
import logging
import multiprocessing
import traceback

try:
    import uvloop
except ImportError:
    uvloop = asyncio

import main.monitor_imbalances.schemas
import main.save_candles.schemas
from main.monitor_imbalances.globals import (
    g_globals,
)
from main.monitor_imbalances.monitor_imbalances_process import (
    monitor_imbalances_process,
)

logger = logging.getLogger(
    __name__,
)


async def init_db_models():
    postgres_db_engine = g_globals.get_postgres_db_engine()

    async with postgres_db_engine.begin() as connection:
        await connection.run_sync(
            main.monitor_imbalances.schemas.Base.metadata.create_all,
        )

        await connection.run_sync(
            main.save_candles.schemas.Base.metadata.create_all,
        )


async def start_imbalances_monitoring_loop() -> None:
    while True:
        try:
            # Polars has a memory leak https://github.com/pola-rs/polars/issues/22871 so we need to create separate process

            process = multiprocessing.Process(
                target=monitor_imbalances_process,
                name='monitor_imbalances_process',
                daemon=False,
            )

            logger.info(
                'Process was created'
            )

            process.start()
            process.join()
        except Exception as exception:
            logger.error(
                'Could not monitor imbalances'
                ': handled exception'
                f': {"".join(traceback.format_exception(exception))}'
            )

        await asyncio.sleep(
            60.0 *  # s
            10.0    # m
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

    # Set up multiprocessing

    multiprocessing.set_start_method(
        'spawn',
    )

    # Prepare DB

    await init_db_models()

    # Start loops

    await start_imbalances_monitoring_loop()


if __name__ == '__main__':
    uvloop.run(
        main_(),
    )
