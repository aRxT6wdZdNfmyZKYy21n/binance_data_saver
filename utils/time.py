from datetime import (
    UTC,
    datetime,
)


class TimeUtils:
    __slots__ = ()

    @staticmethod
    def get_aware_current_datetime() -> datetime:
        return datetime.now(
            tz=UTC,
        )

    @classmethod
    def get_aware_current_timestamp_ms(
        cls,
    ) -> int:
        aware_current_datetime = cls.get_aware_current_datetime()

        return int(
            aware_current_datetime.timestamp() * 1000  # ms
        )
