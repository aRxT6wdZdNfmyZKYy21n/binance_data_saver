import asyncio
import logging
import sys

try:
    import uvloop
except ImportError:
    uvloop = asyncio

from PyQt6.QtWidgets import QApplication
from qasync import QEventLoop

from main.save_candles import (
    schemas,
)
from main.save_candles.globals import (
    g_globals,
)
from main.show_plot.chart_processor import (
    ChartProcessor,
)

logger = logging.getLogger(
    __name__,
)


async def init_db_models():
    postgres_db_engine = g_globals.get_postgres_db_engine()

    async with postgres_db_engine.begin() as connection:
        # await connection.run_sync(
        #     models.Base.metadata.drop_all
        # )

        await connection.run_sync(
            schemas.Base.metadata.create_all,
        )


async def start_processor_updating_loop() -> None:
    # Prepare DB

    await init_db_models()

    processor = ChartProcessor()

    await processor.init()

    await processor.start_updating_loop()


def main() -> None:
    # Set up logging

    logging.basicConfig(
        encoding='utf-8',
        format='[%(levelname)s][%(asctime)s][%(name)s]: %(message)s',
        level=(
            # logging.INFO
            logging.DEBUG
        ),
    )

    # create PyQt6 application

    application = QApplication(
        sys.argv,
    )

    py_qt_event_loop = QEventLoop(
        application,
    )

    # py_qt_event_loop.set_debug(
    #     True,
    # )

    asyncio.set_event_loop(
        py_qt_event_loop,
    )

    # Start loops

    py_qt_event_loop.run_until_complete(
        start_processor_updating_loop(),
    )


if __name__ == '__main__':
    main()
