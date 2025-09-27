import logging
import typing

from PyQt6.QtCore import (
    Qt,
)
from PyQt6.QtWidgets import (
    QComboBox,
    QLabel,
)

logger = logging.getLogger(
    __name__,
)


class QtUtils:
    @staticmethod
    def create_label(
        label_text: str,
        alignment=Qt.AlignmentFlag.AlignCenter,
    ) -> QLabel:
        label = QLabel(
            label_text,
        )

        label.setAlignment(
            alignment,
        )

        label.adjustSize()

        return label

    @classmethod
    def create_label_and_combo_box(
        cls,
        label_text: str,
        handler: typing.Callable,
        values: list[str] | None = None,
        alignment=Qt.AlignmentFlag.AlignCenter,
    ) -> tuple[QLabel, QComboBox]:
        if values is None:
            values = []

        label = cls.create_label(
            label_text,
            alignment,
        )

        combo_box = QComboBox()

        combo_box.currentIndexChanged.connect(  # noqa
            handler
        )

        cls.update_combo_box_values(
            combo_box,
            label,
            values,
        )

        return (
            label,
            combo_box,
        )

    @staticmethod
    def update_combo_box_values(
        combo_box: QComboBox,
        label: QLabel,
        values: list[str] | None = None,
    ) -> None:
        if values:
            items_count = combo_box.count()

            is_need_update_items = items_count != len(
                values,
            )

            if not is_need_update_items:
                for value_idx, value in enumerate(
                    values,
                ):
                    item_value = combo_box.itemText(
                        value_idx,
                    )

                    if item_value != value:
                        is_need_update_items = True

                        break

            if is_need_update_items:
                combo_box.clear()

                combo_box.addItems(
                    values,
                )

            combo_box.show()
            label.show()
        else:
            combo_box.hide()
            combo_box.clear()
            label.hide()
