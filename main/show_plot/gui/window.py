from __future__ import annotations

import asyncio
import traceback
import typing
from functools import (
    partial,
)

import pyqtgraph
from chrono import (
    Timer,
)
from PyQt6.QtCore import (
    Qt,
)
from PyQt6.QtGui import (
    QColor,
)
from PyQt6.QtWidgets import (
    QGridLayout,
    QMainWindow,
    QVBoxLayout,
    QWidget,
)
from qasync import (
    asyncSlot,
)

from constants.common import CommonConstants
from constants.plot import (
    PlotConstants,
)
from main.show_plot.gui.item.candlestick import (
    CandlestickItem,
)
from main.show_plot.gui.item.datetime_axis import (
    DateTimeAxisItem,
)
from main.show_plot.gui.item.rect import (
    RectItem,
)
from utils.async_ import (
    create_task_with_exceptions_logging,
)
from utils.qt import (
    QtUtils,
)

if typing.TYPE_CHECKING:
    from main.show_plot.chart_processor import ChartProcessor

    print(
        ChartProcessor.__name__,  # To do not remove import by ruff
    )


_IS_NEED_SHOW_RSI = False

_BOLLINGER_BANDS_FILL_COLOR = QColor(
    33,
    150,
    243,
    12,
)

_BOLLINGER_BASE_LINE_COLOR = '#2962ff'
_BOLLINGER_LOWER_BAND_COLOR = '#089981'
_BOLLINGER_UPPER_BAND_COLOR = '#f23645'

_PLOT_BACKGROUND_UPPER_COLOR = QColor(
    126,
    87,
    194,
)

_PLOT_BACKGROUND_LOWER_COLOR = QColor(
    67,
    70,
    81,
)

_RSI_PLOT_GRADIENT_UPPER_START_COLOR = QColor(
    76,
    175,
    80,
)

_RSI_PLOT_GRADIENT_UPPER_END_COLOR = QColor(
    76,
    175,
    80,
    0,
)

_RSI_PLOT_GRADIENT_LOWER_START_COLOR = QColor(
    255,
    82,
    82,
    0,
)

_RSI_PLOT_GRADIENT_LOWER_END_COLOR = QColor(
    255,
    82,
    82,
    255,
)

_RSI_LINE_COLOR = '#7e57c2'

_LONG_IMBALANCE_BRUSH_COLOR = QColor(
    76,  # R
    175,  # G
    80,  # B
    127,  # Alpha (полупрозрачный)
)

_LONG_IMBALANCE_PEN_COLOR = QColor(
    76,  # R
    175,  # G
    80,  # B
    255,  # Alpha (непрозрачный для контура)
)


class ChartWindow(QMainWindow):
    def __init__(
        self,
        processor,  # type: ChartProcessor
        parent=None,
    ):
        super().__init__(
            parent,
        )

        functionality_layout = QGridLayout()

        window_layout_widget = QWidget()

        window_layout = QVBoxLayout(
            window_layout_widget,
        )

        self.setCentralWidget(
            window_layout_widget,
        )

        # self.setLayout(
        #     window_layout
        # )

        self.setMinimumSize(
            1366,  # 1920
            768,  # 1080
        )

        self.setWindowFlags(
            Qt.WindowType(
                Qt.WindowType.WindowMaximizeButtonHint
                | Qt.WindowType.WindowMinimizeButtonHint
                | Qt.WindowType.WindowCloseButtonHint,
            )
        )

        self.setWindowTitle(
            'Chart',
        )

        graphics_layout_widget: pyqtgraph.GraphicsLayout = (  # noqa
            pyqtgraph.GraphicsLayoutWidget()
        )

        candles_plot = graphics_layout_widget.addPlot(
            title='Candles',
        )

        candles_date_axis = DateTimeAxisItem(
            orientation='bottom',
            processor=processor,
        )

        candles_plot.setAxisItems(
            {
                'bottom': candles_date_axis,
            },
        )

        candles_plot.showGrid(
            x=True,
            y=True,
        )

        candles_plot.sigXRangeChanged.connect(
            partial(
                self.__update_plots_x_range,
                candles_plot,
            ),
        )

        candles_plot.sigYRangeChanged.connect(
            partial(
                self.__update_plots_y_range,
                candles_plot,
            ),
        )

        graphics_layout_widget.nextRow()

        if _IS_NEED_SHOW_RSI:
            rsi_plot = graphics_layout_widget.addPlot(
                title='RSI',
            )

            rsi_date_axis = DateTimeAxisItem(
                orientation='bottom',
                processor=processor,
            )

            rsi_plot.setAxisItems(
                {
                    'bottom': rsi_date_axis,
                },
            )

            rsi_plot.showGrid(
                x=True,
                y=True,
            )

            rsi_plot.sigXRangeChanged.connect(
                partial(
                    self.__update_plots_x_range,
                    rsi_plot,
                ),
            )

            graphics_layout_widget.nextRow()

        # Create a linear gradient for any plot background

        """
        plot_background_gradient = (
            QLinearGradient(
                0,
                0,
                0,
                1
            )
        )

        plot_background_gradient.setColorAt(
            0.0,
            _PLOT_BACKGROUND_UPPER_COLOR
        )

        plot_background_gradient.setColorAt(
            1.0,
            _PLOT_BACKGROUND_LOWER_COLOR
        )

        plot_background_gradient.setCoordinateMode(
            QLinearGradient.CoordinateMode.ObjectMode
        )

        plot_background_brush = (
            QBrush(
                plot_background_gradient
            )
        )

        price_view_box.setBackgroundColor(
            plot_background_brush
        )

        rsi_view_box.setBackgroundColor(
            plot_background_brush
        )
        """

        (
            symbol_name_label,
            symbol_name_combo_box,
        ) = QtUtils.create_label_and_combo_box(
            'Символ',
            self.__on_symbol_name_changed,
            alignment=Qt.AlignmentFlag.AlignLeft,
        )

        (
            interval_name_label,
            interval_name_combo_box,
        ) = QtUtils.create_label_and_combo_box(
            'Interval name',
            self.__on_interval_name_changed,
            alignment=Qt.AlignmentFlag.AlignLeft,
            values=PlotConstants.IntervalNames,
        )

        self.__graphics_layout_widget = graphics_layout_widget

        self.__candles_plot = candles_plot

        self.__drawing_lock = asyncio.Lock()

        self.__price_candlestick_item_by_start_timestamp_ms_map: dict[
            int, CandlestickItem
        ] = {}

        self.__long_imbalance_rect_item_by_start_timestamp_ms_map: dict[
            int, RectItem
        ] = {}

        self.__processor = processor

        # self.__quantity_plot = (
        #     quantity_plot
        # )

        # self.__quantity_plot = (
        #     quantity_plot
        # )

        if _IS_NEED_SHOW_RSI:
            self.__rsi_plot = rsi_plot

            self.__rsi_plot_data_item = rsi_plot.plot(
                pen=_RSI_LINE_COLOR,
                name='RSI',
            )

        self.__interval_name_combo_box = interval_name_combo_box
        self.__interval_name_label = interval_name_label

        self.__symbol_name_combo_box = symbol_name_combo_box
        self.__symbol_name_label = symbol_name_label

        # self.__volume_plot = (
        #     volume_plot
        # )

        # self.__volume_plot = (
        #     volume_plot
        # )

        functionality_layout.addWidget(symbol_name_label, 0, 2, 2, 1)
        functionality_layout.addWidget(symbol_name_combo_box, 2, 2, 2, 1)

        functionality_layout.addWidget(interval_name_label, 0, 6, 2, 1)
        functionality_layout.addWidget(interval_name_combo_box, 2, 6, 2, 1)

        window_layout.addWidget(graphics_layout_widget)

        window_layout.addLayout(functionality_layout)

    def __update_plots_x_range(
        self,
        current_plot: pyqtgraph.PlotWidget,  # TODO: check typing
    ):
        x_range = current_plot.getViewBox().viewRange()[0]

        plots = [
            self.__candles_plot,
        ]

        if _IS_NEED_SHOW_RSI:
            plots.append(
                self.__rsi_plot,
            )

        for plot in plots:
            if plot is current_plot:
                continue

            plot.setXRange(
                *x_range,
                padding=0,
            )

    def __update_plots_y_range(
        self,
        current_plot: pyqtgraph.PlotWidget,  # TODO: check typing
    ):
        y_range = current_plot.getViewBox().viewRange()[1]

        for plot in (self.__candles_plot,):
            if plot is current_plot:
                continue

            plot.setYRange(
                *y_range,
                padding=0,
            )

    async def plot(
        self,
        delay: float | None = None,
        is_need_run_once: bool = True,
    ) -> None:
        drawing_lock = self.__drawing_lock

        if is_need_run_once and drawing_lock.locked():
            return

        async with drawing_lock:
            if delay is not None:
                await asyncio.sleep(
                    delay,
                )

            try:
                with Timer() as timer:
                    await self.__plot()

                print(
                    f'Plotted by {timer.elapsed:.3f}s',
                )
            except Exception as exception:
                print(
                    'Handled exception'
                    f': {"".join(traceback.format_exception(exception))}',
                )

            if not is_need_run_once:
                create_task_with_exceptions_logging(
                    self.plot(
                        delay=5.0,  # 5s  # 0.1  # 100ms  # TODO
                        is_need_run_once=False,
                    )
                )

    @asyncSlot()
    async def __on_interval_name_changed(
        self,
    ) -> None:
        current_interval_name = self.__interval_name_combo_box.currentText()

        processor = self.__processor

        if not current_interval_name or (
            current_interval_name == processor.get_current_interval_name()
        ):
            return

        print(
            f'Selected interval name: {current_interval_name!r}'
            # f' ({idx})'
        )

        if not await processor.update_current_interval_name(
            current_interval_name,
        ):
            # TODO: response to user UI

            return

    @asyncSlot()
    async def __on_symbol_name_changed(
        self,
        # idx: int
    ) -> None:
        current_symbol_name = self.__symbol_name_combo_box.currentText()

        processor = self.__processor

        if not current_symbol_name or (
            current_symbol_name == processor.get_current_symbol_name()
        ):
            return

        print(
            f'Selected symbol name: {current_symbol_name!r}'
            # f' ({idx})'
        )

        if not await processor.update_current_symbol_name(
            current_symbol_name,
        ):
            # TODO: response to user UI

            return

    async def __plot(
        self,
    ) -> None:
        candles_plot = self.__candles_plot
        processor = self.__processor

        current_available_symbol_names = (
            await processor.get_current_available_symbol_names()
        )

        QtUtils.update_combo_box_values(
            self.__symbol_name_combo_box,
            self.__symbol_name_label,
            current_available_symbol_names,
        )

        candles_dataframe = processor.get_candles_dataframe()

        price_candlestick_item_by_start_timestamp_ms_map = (
            self.__price_candlestick_item_by_start_timestamp_ms_map
        )

        if candles_dataframe is None:
            print(
                'candles_dataframe is None',
            )

            for (
                price_candlestick_item
            ) in price_candlestick_item_by_start_timestamp_ms_map.values():
                candles_plot.removeItem(
                    price_candlestick_item,
                )

            price_candlestick_item_by_start_timestamp_ms_map.clear()

            # Очищаем прямоугольники имбалансов
            long_imbalance_rect_item_by_start_timestamp_ms_map = (
                self.__long_imbalance_rect_item_by_start_timestamp_ms_map
            )

            for (
                long_imbalance_rect_item
            ) in long_imbalance_rect_item_by_start_timestamp_ms_map.values():
                candles_plot.removeItem(
                    long_imbalance_rect_item,
                )

            long_imbalance_rect_item_by_start_timestamp_ms_map.clear()

            return

        current_interval_name = processor.get_current_interval_name()

        assert current_interval_name is not None, None

        start_timestamp_ms_series = candles_dataframe.get_column(
            'start_timestamp_ms',
        )

        start_timestamp_ms_numpy_array = start_timestamp_ms_series.to_numpy()

        # plot.reset()
        # self.__plot_overlay.reset()

        candle_start_timestamp_ms_set: set[int] = set()

        interval_duration = CommonConstants.IntervalDurationByNameMap[
            current_interval_name
        ]

        interval_duration_ms = int(
            interval_duration.total_seconds() * 1000  # ms
        )

        for candle_row_data in candles_dataframe.iter_rows(named=True):
            start_timestamp_ms: int = candle_row_data['start_timestamp_ms']

            candle_close_price: float = candle_row_data['close_price']
            candle_high_price: float = candle_row_data['high_price']
            candle_low_price: float = candle_row_data['low_price']
            candle_open_price: float = candle_row_data['open_price']

            end_timestamp_ms = start_timestamp_ms + interval_duration_ms

            candle_start_timestamp_ms_set.add(
                start_timestamp_ms,
            )

            price_candlestick_item = (
                price_candlestick_item_by_start_timestamp_ms_map.get(
                    start_timestamp_ms,
                )
            )

            if price_candlestick_item is not None:
                price_candlestick_item.update_data(
                    candle_close_price,
                    end_timestamp_ms,
                    candle_high_price,
                    candle_low_price,
                    candle_open_price,
                    start_timestamp_ms,
                )
            else:
                price_candlestick_item = price_candlestick_item_by_start_timestamp_ms_map[
                    start_timestamp_ms
                ] = CandlestickItem(
                    candle_close_price,
                    end_timestamp_ms,
                    candle_high_price,
                    candle_low_price,
                    candle_open_price,
                    start_timestamp_ms,
                )

                candles_plot.addItem(
                    price_candlestick_item,
                )

        for start_timestamp_ms in tuple(
            price_candlestick_item_by_start_timestamp_ms_map,
        ):
            if start_timestamp_ms not in candle_start_timestamp_ms_set:
                print(
                    'Removing candlestick item'
                    f' by start timestamp (ms) {start_timestamp_ms}',
                )

                price_candlestick_item = (
                    price_candlestick_item_by_start_timestamp_ms_map.pop(
                        start_timestamp_ms,
                    )
                )

                candles_plot.removeItem(
                    price_candlestick_item,
                )

        long_imbalances_dataframe = processor.get_long_imbalances_dataframe()

        long_imbalance_rect_item_by_start_timestamp_ms_map = (
            self.__long_imbalance_rect_item_by_start_timestamp_ms_map
        )

        if long_imbalances_dataframe is not None:
            long_imbalance_start_timestamp_ms_set: set[int] = set()

            for long_imbalance_raw_data in long_imbalances_dataframe.iter_rows(
                named=True,
            ):
                start_timestamp_ms: int = long_imbalance_raw_data['start_timestamp_ms']
                start_price: float = long_imbalance_raw_data['start_price']
                end_price: float = long_imbalance_raw_data['end_price']
                end_timestamp_ms: int = long_imbalance_raw_data['end_timestamp_ms']

                long_imbalance_start_timestamp_ms_set.add(
                    start_timestamp_ms,
                )

                # Вычисляем размеры прямоугольника
                width = end_timestamp_ms - start_timestamp_ms
                height = end_price - start_price

                # Создаем или обновляем прямоугольник
                long_imbalance_rect_item = (
                    long_imbalance_rect_item_by_start_timestamp_ms_map.get(
                        start_timestamp_ms,
                    )
                )

                if long_imbalance_rect_item is not None:
                    # Обновляем существующий прямоугольник
                    long_imbalance_rect_item.setPos(
                        pyqtgraph.Point(
                            start_timestamp_ms,
                            start_price,
                        ),
                    )

                    long_imbalance_rect_item.set_size(
                        pyqtgraph.Point(
                            width,
                            height,
                        ),
                    )
                else:
                    # Создаем новый прямоугольник
                    long_imbalance_rect_item = long_imbalance_rect_item_by_start_timestamp_ms_map[
                        start_timestamp_ms
                    ] = RectItem(
                        brush_color=_LONG_IMBALANCE_BRUSH_COLOR,
                        pen_color=_LONG_IMBALANCE_PEN_COLOR,
                        position=pyqtgraph.Point(
                            start_timestamp_ms,
                            start_price,
                        ),
                        size=pyqtgraph.Point(
                            width,
                            height,
                        ),
                    )

                    candles_plot.addItem(
                        long_imbalance_rect_item,
                    )

            # Удаляем старые прямоугольники имбалансов
            for start_timestamp_ms in tuple(
                long_imbalance_rect_item_by_start_timestamp_ms_map,
            ):
                if start_timestamp_ms not in long_imbalance_start_timestamp_ms_set:
                    print(
                        'Removing long imbalance rect item'
                        f' by start timestamp (ms) {start_timestamp_ms}',
                    )

                    long_imbalance_rect_item = (
                        long_imbalance_rect_item_by_start_timestamp_ms_map.pop(
                            start_timestamp_ms,
                        )
                    )

                    candles_plot.removeItem(
                        long_imbalance_rect_item,
                    )
        else:
            # Если нет данных об имбалансах, очищаем все прямоугольники
            for (
                long_imbalance_rect_item
            ) in long_imbalance_rect_item_by_start_timestamp_ms_map.values():
                candles_plot.removeItem(
                    long_imbalance_rect_item,
                )

            long_imbalance_rect_item_by_start_timestamp_ms_map.clear()

        if _IS_NEED_SHOW_RSI:
            rsi_series = processor.get_rsi_series()

            if rsi_series is not None:
                self.__rsi_plot_data_item.setData(
                    start_timestamp_ms_numpy_array,
                    rsi_series.to_numpy(),
                )

    def auto_range_candles_plot(
        self,
    ) -> None:
        self.__candles_plot.getViewBox().autoRange()
