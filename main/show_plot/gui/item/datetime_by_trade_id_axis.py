import typing
from datetime import (
    datetime,
)

import pyqtgraph

if typing.TYPE_CHECKING:
    pass


class DateTimeByTradeIDAxisItem(pyqtgraph.AxisItem):
    __slots__ = ('__processor',)

    def __init__(
        self,
        processor,  # type: FinPlotChartProcessor
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.setLabel(
            text='Time by trade ID',
            units=None,
        )

        self.enableAutoSIPrefix(
            False,
        )

        self.__processor = processor

    def tickStrings(self, values, scale, spacing):
        processor = self.__processor

        trades_dataframe = processor.get_trades_dataframe()

        tick_strings: list[str] = []

        trade_id_float: float

        for trade_id_float in values:
            trade_id = int(
                trade_id_float,
            )

            tick_string = f'{trade_id}'

            if trades_dataframe is not None:
                trade_id_series = trades_dataframe.get_column(
                    'trade_id',
                )

                idx = trade_id_series.search_sorted(
                    element=trade_id,
                    side='any',
                )

                if idx < trade_id_series.len() and trade_id_series[idx] == trade_id:
                    datetime_series = trades_dataframe.get_column(
                        'datetime',
                    )

                    datetime_: datetime = datetime_series[idx]

                    try:
                        tick_string = '\n'.join(
                            (
                                datetime_.isoformat(),
                                tick_string,
                            )
                        )
                    except ValueError:
                        pass

            tick_strings.append(
                tick_string,
            )

        return tick_strings
