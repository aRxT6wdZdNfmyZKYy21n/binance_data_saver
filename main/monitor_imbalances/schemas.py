from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Column,
    PrimaryKeyConstraint,
    Text,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
)


class Base(DeclarativeBase):
    pass


class SymbolDataframeHeight(Base):
    """Схема для хранения высоты датафрейма по символам"""
    __tablename__ = 'symbol_dataframe_height'
    __table_args__ = (
        PrimaryKeyConstraint('symbol_name'),
    )

    # Primary key fields
    symbol_name: Mapped[str] = Column(Text)

    # Attribute fields
    dataframe_height: Mapped[int] = Column(BigInteger)  # Высота датафрейма
    last_update_timestamp_ms: Mapped[int] = Column(BigInteger)  # Время последнего обновления
