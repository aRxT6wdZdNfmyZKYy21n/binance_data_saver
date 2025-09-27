from __future__ import annotations

from datetime import (
    datetime,
)

import pyqtgraph


class DateTimeAxisItem(pyqtgraph.AxisItem):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setLabel(
            text='Time',
            units=None,
        )

        self.enableAutoSIPrefix(
            False,
        )

    def tickStrings(self, values, scale, spacing):
        tick_strings = []

        for value in values:
            try:
                tick_string = datetime.fromtimestamp(
                    value // 10**9,
                ).isoformat()
            except ValueError:
                tick_string = 'N/A'

            tick_strings.append(
                tick_string,
            )

        return tick_strings
