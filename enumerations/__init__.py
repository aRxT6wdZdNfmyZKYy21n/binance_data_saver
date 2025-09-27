"""
Перечисления для системы обработки данных.
"""

from enum import (
    IntEnum,
    StrEnum,
    auto,
)

from enumerations.compression import (
    CompressionAlgorithm,
)

__all__ = [
    'CompressionAlgorithm',
    'OKXOrderBookActionId',
    'SymbolId',
    'TradingDirection',
]


class OKXOrderBookActionId(IntEnum):
    Snapshot = auto()
    Update = auto()


class SymbolId(IntEnum):
    BTC_USDT = auto()
    ETH_USDT = auto()
    SOL_USDT = auto()


class TradingDirection(StrEnum):
    Bear = 'Bear'
    Bull = 'Bull'
    Cross = 'Cross'
