"""
Перечисления для алгоритмов сжатия.
"""

from enum import StrEnum


class CompressionAlgorithm(StrEnum):
    """Алгоритмы сжатия данных."""

    XZ = 'xz'
    LZ4 = 'lz4'
    NONE = 'none'
