import pyqtgraph
from PyQt6.QtCore import (
    QRectF,
)
from PyQt6.QtGui import (
    QColor,
)
from pyqtgraph import Point


class RectItem(pyqtgraph.GraphicsObject):
    def __init__(
        self,
        brush_color: QColor,
        pen_color: QColor,
        position: Point,
        size: Point,
    ) -> None:
        super().__init__()

        self.setPos(
            position,
        )

        self.__brush_color = brush_color
        self.__pen_color = pen_color
        self.__size = size

    def boundingRect(self):
        size = self.__size

        return QRectF(
            0,
            0,
            size[0],
            size[1],
        ).normalized()

    def paint(self, painter, *args):
        rect = self.boundingRect()

        painter.setPen(
            pyqtgraph.mkPen(
                self.__pen_color,
            ),
        )

        painter.setBrush(
            pyqtgraph.mkBrush(
                self.__brush_color,
            ),
        )

        painter.translate(
            rect.left(),
            rect.top(),
        )

        painter.scale(
            rect.width(),
            rect.height(),
        )

        painter.drawRect(
            0,
            0,
            1,
            1,
        )

    def set_brush_color(self, value: QColor) -> None:
        self.__brush_color = value

    def set_pen_color(self, value: QColor) -> None:
        self.__pen_color = value

    def set_size(self, value: Point) -> None:
        self.__size = value

    # def setPos(self, value: Point) -> None:
    #     super().setPos(value)

    def _get_pen_color(
        self,
    ) -> QColor:
        return self.__pen_color
