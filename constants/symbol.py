from enumerations import (
    SymbolId,
)


class SymbolConstants:
    __slots__ = ()

    NameById = {
        SymbolId.BTC_USDT: 'BTC-USDT',
        SymbolId.ETH_USDT: 'ETH-USDT',
        SymbolId.SOL_USDT: 'SOL-USDT',
    }

    IdByName = {value: key for key, value in NameById.items()}

    assert len(
        NameById,
    ) == len(
        IdByName,
    ), None
