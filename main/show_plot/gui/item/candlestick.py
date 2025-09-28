import pyqtgraph
from PyQt6.QtGui import (
    QColor,
)
from pyqtgraph import (
    Point,
)

from main.show_plot.gui.item.rect import (
    RectItem,
)

_CANDLE_BEAR_BODY_COLOR = QColor(
    0xF2,
    0x36,
    0x45,
)

_CANDLE_BEAR_SHADOW_COLOR = QColor(
    0xF2,
    0x36,
    0x45,
)

_CANDLE_BULL_BODY_COLOR = QColor(
    0x08,
    0x99,
    0x81,
)

_CANDLE_BULL_SHADOW_COLOR = QColor(
    0x08,
    0x99,
    0x81,
)


class CandlestickItem(RectItem):
    def __init__(
        self,
        close_price: float,
        end_timestamp_ns: float,
        high_price: float,
        low_price: float,
        open_price: float,
        start_timestamp_ns: float,
    ) -> None:
        (
            body_color,
            shadow_color,
        ) = self.__generate_body_and_shadow_color_pair(
            close_price,
            open_price,
        )

        super().__init__(
            brush_color=body_color,
            pen_color=shadow_color,
            position=self.__generate_position(
                end_timestamp_ns,
                open_price,
                start_timestamp_ns,
            ),
            size=self.__generate_size(
                close_price,
                end_timestamp_ns,
                open_price,
                start_timestamp_ns,
            ),
        )

        self.__close_price = close_price
        self.__end_timestamp_ns = end_timestamp_ns
        self.__high_price = high_price
        self.__low_price = low_price
        self.__open_price = open_price
        self.__start_timestamp_ns = start_timestamp_ns

    def paint(self, painter, *args):
        painter.setPen(
            pyqtgraph.mkPen(
                self._get_pen_color(),
                width=1,
            ),
        )

        timestamp_ms_delta = (
            self.__end_timestamp_ns - self.__start_timestamp_ns
        ) * 0.25

        painter.drawLine(
            Point(
                timestamp_ms_delta,
                self.__high_price - self.__open_price,
            ),
            Point(
                timestamp_ms_delta,
                self.__low_price - self.__open_price,
            ),
        )

        super().paint(
            painter,
            *args,
        )

    def update_data(
        self,
        close_price: float,
        end_timestamp_ns: float,
        high_price: float,
        low_price: float,
        open_price: float,
        start_timestamp_ns: float,
    ) -> None:
        self.__close_price = close_price
        self.__end_timestamp_ns = end_timestamp_ns
        self.__high_price = high_price
        self.__low_price = low_price
        self.__open_price = open_price
        self.__start_timestamp_ns = start_timestamp_ns

        self.setPos(
            self.__generate_position(
                end_timestamp_ns,
                open_price,
                start_timestamp_ns,
            )
        )

        self.set_size(
            self.__generate_size(
                close_price,
                end_timestamp_ns,
                open_price,
                start_timestamp_ns,
            )
        )

        body_color, shadow_color = self.__generate_body_and_shadow_color_pair(
            close_price,
            open_price,
        )

        self.set_brush_color(
            body_color,
        )

        self.set_pen_color(
            shadow_color,
        )

    @staticmethod
    def __generate_body_and_shadow_color_pair(
        close_price: float,
        open_price: float,
    ) -> tuple[QColor, QColor]:
        is_bull = close_price >= open_price

        body_color: QColor
        shadow_color: QColor

        if is_bull:
            body_color = _CANDLE_BULL_BODY_COLOR
            shadow_color = _CANDLE_BULL_SHADOW_COLOR
        else:
            body_color = _CANDLE_BEAR_BODY_COLOR
            shadow_color = _CANDLE_BEAR_SHADOW_COLOR

        return body_color, shadow_color

    @staticmethod
    def __generate_position(
        end_timestamp_ns: float,
        open_price: float,
        start_timestamp_ns: float,
    ) -> Point:
        return Point(
            start_timestamp_ns + (end_timestamp_ns - start_timestamp_ns) * 0.25,
            open_price,
        )

    @staticmethod
    def __generate_size(
        close_price: float,
        end_timestamp_ns: float,
        open_price: float,
        start_timestamp_ns: float,
    ) -> Point:
        return Point(
            (end_timestamp_ns - start_timestamp_ns) * 0.5, close_price - open_price
        )
