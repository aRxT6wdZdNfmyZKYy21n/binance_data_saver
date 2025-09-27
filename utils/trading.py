from decimal import (
    Decimal,
)

from enumerations import (
    TradingDirection,
)


class TradingUtils:
    @staticmethod
    def get_direction(
        price_1: float | Decimal,
        price_2: float | Decimal,
    ) -> TradingDirection:
        if price_2 > price_1:
            return TradingDirection.Bull
        elif price_2 < price_1:
            return TradingDirection.Bear
        else:
            return TradingDirection.Cross
