"""
Утилиты для сериализации и десериализации Polars DataFrame с использованием Apache Arrow IPC формата.
"""

import io
import lzma

import lz4.frame
import polars

from enumerations.compression import (
    CompressionAlgorithm,
)


def serialize_dataframe(
    df: polars.DataFrame,
    compression: CompressionAlgorithm,
) -> bytes:
    """
    Сериализация Polars DataFrame в сжатые IPC данные.

    Args:
        df: Polars DataFrame для сериализации
        compression: Алгоритм сжатия ("xz", "lz4", "none")

    Returns:
        Сжатые байты данных

    Raises:
        ValueError: Если указан неподдерживаемый алгоритм сжатия
    """
    # Сериализация в IPC формат
    buffer = io.BytesIO()

    df.write_ipc(
        buffer,
    )

    ipc_data = buffer.getvalue()

    # Применение сжатия

    if compression == CompressionAlgorithm.XZ:
        # xz обеспечивает лучшее сжатие для финансовых данных
        compressed_data = lzma.compress(
            ipc_data,
            # preset=6,  # баланс скорость/сжатие
            preset=lzma.PRESET_EXTREME,  # Максимальное сжатие
        )
    elif compression == CompressionAlgorithm.LZ4:
        # lz4 для быстрого сжатия когда размер не критичен
        compressed_data = lz4.frame.compress(
            ipc_data,
        )
    elif compression == CompressionAlgorithm.NONE:
        compressed_data = ipc_data
    else:
        raise ValueError(
            f'Unsupported compression: {compression}',
        )

    return compressed_data


def deserialize_dataframe(
    data: bytes,
    compression: CompressionAlgorithm,
) -> polars.DataFrame:
    """
    Десериализация сжатых IPC данных в Polars DataFrame.

    Args:
        data: Сжатые байты данных
        compression: Алгоритм сжатия ("xz", "lz4", "none")

    Returns:
        Восстановленный Polars DataFrame

    Raises:
        ValueError: Если указан неподдерживаемый алгоритм сжатия
    """
    # Распаковка сжатия
    if compression == CompressionAlgorithm.XZ:
        ipc_data = lzma.decompress(data)
    elif compression == CompressionAlgorithm.LZ4:
        ipc_data = lz4.frame.decompress(data)
    elif compression == CompressionAlgorithm.NONE:
        ipc_data = data
    else:
        raise ValueError(f'Unsupported compression: {compression.name}')

    # Десериализация из IPC формата
    buffer = io.BytesIO(ipc_data)
    return polars.read_ipc(buffer)


def split_dataframe_by_size(
    df: polars.DataFrame,
    max_size_bytes: int,
) -> list[polars.DataFrame]:
    """
    Разбивка DataFrame на части по размеру в байтах.

    Args:
        df: Polars DataFrame для разбивки
        max_size_bytes: Максимальный размер части в байтах (по умолчанию 400 МБ)

    Returns:
        Список частей DataFrame
    """
    if len(serialize_dataframe(df)) <= max_size_bytes:
        return [df]

    # Простая разбивка по количеству строк
    total_rows = df.height
    chunk_size = total_rows // 2  # Начинаем с половины

    chunks = []
    start = 0

    while start < total_rows:
        end = min(start + chunk_size, total_rows)
        chunk = df.slice(start, end - start)

        # Если чанк все еще слишком большой, уменьшаем размер
        while len(serialize_dataframe(chunk)) > max_size_bytes and chunk.height > 1:
            chunk = chunk.slice(0, chunk.height // 2)

        chunks.append(chunk)
        start += chunk.height

    return chunks


def merge_dataframe_chunks(
    dataframe_chunks: list[polars.DataFrame],
) -> polars.DataFrame:
    """
    Объединение частей DataFrame обратно в один.

    Args:
        dataframe_chunks: Список частей DataFrame

    Returns:
        Объединенный DataFrame
    """
    assert dataframe_chunks, None

    return polars.concat(dataframe_chunks)


def get_compression_ratio(
    original_size: int,
    compressed_size: int,
) -> float:
    """
    Вычисление коэффициента сжатия.

    Args:
        original_size: Исходный размер в байтах
        compressed_size: Сжатый размер в байтах

    Returns:
        Коэффициент сжатия (0.0 - 1.0, где 0.0 = 100% сжатие)
    """
    if original_size == 0:
        return 0.0

    return compressed_size / original_size


def estimate_dataframe_size(
    df: polars.DataFrame,
    compression: CompressionAlgorithm,
) -> int:
    """
    Оценка размера DataFrame после сжатия.

    Args:
        df: Polars DataFrame
        compression: Алгоритм сжатия

    Returns:
        Примерный размер в байтах после сжатия
    """
    return len(serialize_dataframe(df, compression))
