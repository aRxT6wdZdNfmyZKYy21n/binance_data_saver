from decimal import (
    Decimal,
)

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
)
from sqlalchemy.ext.asyncio import (
    AsyncAttrs,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    # mapped_column,
)
from sqlalchemy.types import (
    Enum,
)

from enumerations import (
    SymbolId,
)


class Base(AsyncAttrs, DeclarativeBase):
    pass


class BinanceCandleData1H(Base):
    __tablename__ = 'binance_candle_data_1H'
    __table_args__ = (
        PrimaryKeyConstraint(  # Explicitly define composite primary key
            'symbol_id',
            'start_timestamp_ms',
        ),
    )

    # Primary key fields

    symbol_id: Mapped[SymbolId] = Column(
        Enum(
            SymbolId,
        ),
    )

    start_timestamp_ms: Mapped[int] = Column(BigInteger)

    # Attribute fields

    is_closed: Mapped[bool] = Column(Boolean)

    close_price: Mapped[Decimal] = Column(Numeric)
    high_price: Mapped[Decimal] = Column(Numeric)
    low_price: Mapped[Decimal] = Column(Numeric)
    open_price: Mapped[Decimal] = Column(Numeric)

    taker_buy_volume_base_currency: Mapped[Decimal] = Column(Numeric)
    taker_buy_volume_quote_currency: Mapped[Decimal] = Column(Numeric)

    trades_count: Mapped[int] = Column(Integer)

    volume: Mapped[Decimal] = Column(Numeric)
    volume_quote_currency: Mapped[Decimal] = Column(Numeric)


class BinanceCandleData4H(Base):
    __tablename__ = 'binance_candle_data_4H'
    __table_args__ = (
        PrimaryKeyConstraint(  # Explicitly define composite primary key
            'symbol_id',
            'start_timestamp_ms',
        ),
    )

    # Primary key fields

    symbol_id: Mapped[SymbolId] = Column(
        Enum(
            SymbolId,
        ),
    )

    start_timestamp_ms: Mapped[int] = Column(BigInteger)

    # Attribute fields

    is_closed: Mapped[bool] = Column(Boolean)

    close_price: Mapped[Decimal] = Column(Numeric)
    high_price: Mapped[Decimal] = Column(Numeric)
    low_price: Mapped[Decimal] = Column(Numeric)
    open_price: Mapped[Decimal] = Column(Numeric)

    taker_buy_volume_base_currency: Mapped[Decimal] = Column(Numeric)
    taker_buy_volume_quote_currency: Mapped[Decimal] = Column(Numeric)

    trades_count: Mapped[int] = Column(Integer)

    volume: Mapped[Decimal] = Column(Numeric)
    volume_quote_currency: Mapped[Decimal] = Column(Numeric)


class BinanceCandleData1D(Base):
    __tablename__ = 'binance_candle_data_1D'
    __table_args__ = (
        PrimaryKeyConstraint(  # Explicitly define composite primary key
            'symbol_id',
            'start_timestamp_ms',
        ),
    )

    # Primary key fields

    symbol_id: Mapped[SymbolId] = Column(
        Enum(
            SymbolId,
        ),
    )

    start_timestamp_ms: Mapped[int] = Column(BigInteger)

    # Attribute fields

    is_closed: Mapped[bool] = Column(Boolean)

    close_price: Mapped[Decimal] = Column(Numeric)
    high_price: Mapped[Decimal] = Column(Numeric)
    low_price: Mapped[Decimal] = Column(Numeric)
    open_price: Mapped[Decimal] = Column(Numeric)

    taker_buy_volume_base_currency: Mapped[Decimal] = Column(Numeric)
    taker_buy_volume_quote_currency: Mapped[Decimal] = Column(Numeric)

    trades_count: Mapped[int] = Column(Integer)

    volume: Mapped[Decimal] = Column(Numeric)
    volume_quote_currency: Mapped[Decimal] = Column(Numeric)
