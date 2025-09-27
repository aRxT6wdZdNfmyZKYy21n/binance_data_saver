from enumerations import (
    OKXOrderBookActionId,
)


class OKXConstants:
    __slots__ = ()

    OrderBookActionNameById = {
        OKXOrderBookActionId.Snapshot: 'snapshot',
        OKXOrderBookActionId.Update: 'update',
    }

    OrderBookActionIdByName = {
        value: key for key, value in OrderBookActionNameById.items()
    }

    assert len(
        OrderBookActionNameById,
    ) == len(
        OrderBookActionIdByName,
    ), None
