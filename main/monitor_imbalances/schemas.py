from __future__ import annotations

from decimal import (
    Decimal,
)

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Numeric,
    PrimaryKeyConstraint,
    Text,
    Boolean,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
)


class Base(DeclarativeBase):
    pass


class LongImbalanceData(Base):
    __tablename__ = 'long_imbalance_data'
    __table_args__ = (
        PrimaryKeyConstraint(  # Explicitly define composite primary key
            'symbol_name',
            'start_timestamp_ms',
            'detection_timestamp_ms',  # Добавляем время обнаружения для уникальности
        ),
    )

    # Primary key fields
    symbol_name: Mapped[str] = Column(Text)
    start_timestamp_ms: Mapped[int] = Column(BigInteger)
    detection_timestamp_ms: Mapped[int] = Column(
        BigInteger
    )  # Время обнаружения имбаланса

    # Attribute fields
    start_price: Mapped[Decimal] = Column(Numeric)
    end_price: Mapped[Decimal] = Column(Numeric)
    end_timestamp_ms: Mapped[int] = Column(
        BigInteger
    )  # None если имбаланс еще не закрыт

    # Метаданные
    is_closed: Mapped[bool] = Column(Boolean, default=False)  # Закрыт ли имбаланс
    close_timestamp_ms: Mapped[int] = Column(
        BigInteger, nullable=True
    )  # Время закрытия


class FailedTelegramNotification(Base):
    __tablename__ = 'failed_telegram_notification'
    __table_args__ = (
        PrimaryKeyConstraint(  # Explicitly define composite primary key
            'symbol_name',
            'notification_timestamp_ms',
        ),
    )

    # Primary key fields
    symbol_name: Mapped[str] = Column(Text)
    notification_timestamp_ms: Mapped[int] = Column(BigInteger)

    # Attribute fields
    message: Mapped[str] = Column(Text)
    new_imbalances_added: Mapped[int] = Column(BigInteger, default=0)
    existing_imbalances_closed: Mapped[int] = Column(BigInteger, default=0)
    
    # Метаданные
    last_retry_timestamp_ms: Mapped[int] = Column(BigInteger, nullable=True)  # Время последней попытки
