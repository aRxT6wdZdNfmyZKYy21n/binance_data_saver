"""
Утилиты для работы с Redis.
"""

import json
import logging
import traceback
from typing import Any

import polars
import redis.asyncio as redis
from redis.asyncio import Redis

from enumerations import CompressionAlgorithm
from settings import settings
from utils.serialization import (
    deserialize_dataframe,
    estimate_dataframe_size,
    get_compression_ratio,
    merge_dataframe_chunks,
    serialize_dataframe,
    split_dataframe_by_size,
)

logger = logging.getLogger(
    __name__,
)


class RedisManager:
    """Менеджер для работы с Redis."""

    def __init__(
        self,
    ) -> None:
        super().__init__()

        self.__redis: Redis | None = None

    async def connect(
        self,
    ) -> None:
        """Установка соединения с Redis."""
        try:
            self.__redis = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                password=settings.REDIS_PASSWORD.get_secret_value()
                if settings.REDIS_PASSWORD
                else None,
                db=settings.REDIS_DB,
                decode_responses=False,  # Работаем с бинарными данными
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
            )

            # Проверка соединения
            await self.__redis.ping()

            logger.info('Connected to Redis successfully')
        except Exception as exception:
            logger.error(
                'Failed to connect to Redis'
                f': {"".join(traceback.format_exception(exception))}',
            )

            raise exception

    async def disconnect(self) -> None:
        """Закрытие соединения с Redis."""
        if self.__redis:
            await self.__redis.close()
            logger.info('Disconnected from Redis')

    async def save_dataframe(
        self,
        key: str,
        dataframe: polars.DataFrame,
        compression: CompressionAlgorithm,
        max_size_bytes: int,
    ) -> dict[str, Any]:
        """
        Сохранение DataFrame в Redis с разбивкой на части при необходимости.

        Args:
            key: Ключ Redis
            dataframe: Polars DataFrame
            compression: Алгоритм сжатия
            max_size_bytes: Максимальный размер части

        Returns:
            Метаданные о сохранении
        """
        if not self.__redis:
            raise RuntimeError('Redis not connected')

        # Оценка размера
        estimated_size = estimate_dataframe_size(
            dataframe,
            compression,
        )

        if estimated_size <= max_size_bytes:
            # Сохраняем как единое целое
            compressed_data = serialize_dataframe(
                dataframe,
                compression,
            )
            await self.__redis.set(
                key,
                compressed_data,
            )

            metadata = {
                'compression': str(compression.value),
                'compression_ratio': get_compression_ratio(
                    len(dataframe),
                    len(compressed_data),
                ),
                'parts_count': 1,
                'total_size': len(compressed_data),
            }

            # Сохраняем метаданные
            await self.__redis.set(
                f'{key}:metadata',
                json.dumps(metadata),
            )

        else:
            # Разбиваем на части
            chunks = split_dataframe_by_size(
                dataframe,
                max_size_bytes,
            )

            # Сохраняем каждую часть
            for i, chunk in enumerate(chunks):
                chunk_key = f'{key}:part_{i}'

                compressed_data = serialize_dataframe(
                    chunk,
                    compression,
                )

                await self.__redis.set(
                    chunk_key,
                    compressed_data,
                )

            metadata = {
                'compression': str(compression.value),
                'compression_ratio': get_compression_ratio(
                    estimate_dataframe_size(
                        dataframe,
                        CompressionAlgorithm.NONE,
                    ),
                    sum(
                        estimate_dataframe_size(
                            chunk,
                            compression,
                        )
                        for chunk in chunks
                    ),
                ),
                'parts_count': len(
                    chunks,
                ),
                'total_size': sum(
                    len(
                        serialize_dataframe(
                            chunk,
                            compression,
                        ),
                    )
                    for chunk in chunks
                ),
            }

            # Сохраняем метаданные
            await self.__redis.set(
                f'{key}:metadata',
                json.dumps(metadata),
            )

        logger.info(
            f'Saved DataFrame to Redis: {key}, size: {metadata["total_size"]} bytes',
        )

        return metadata

    async def load_dataframe(
        self,
        key: str,
    ) -> Any | None:  # Optional[polars.DataFrame]
        """
        Загрузка DataFrame из Redis.

        Args:
            key: Ключ Redis

        Returns:
            Polars DataFrame или None если не найден
        """
        if not self.__redis:
            raise RuntimeError('Redis not connected')

        # Загружаем метаданные
        metadata_raw = await self.__redis.get(f'{key}:metadata')
        if not metadata_raw:
            return None

        metadata = json.loads(metadata_raw)
        compression_raw = metadata['compression']

        compression = CompressionAlgorithm(
            compression_raw,
        )

        parts_count = metadata['parts_count']

        if parts_count == 1:
            # Загружаем как единое целое
            data = await self.__redis.get(
                key,
            )

            if not data:
                return None

            return deserialize_dataframe(
                data,
                compression,
            )

        else:
            # Загружаем части и объединяем
            chunks = []
            for i in range(parts_count):
                chunk_key = f'{key}:part_{i}'

                chunk_data = await self.__redis.get(
                    chunk_key,
                )

                if not chunk_data:
                    logger.warning(f'Missing chunk {i} for key {key}')
                    continue

                chunk = deserialize_dataframe(
                    chunk_data,
                    compression,
                )

                chunks.append(
                    chunk,
                )

            if not chunks:
                return None

            return merge_dataframe_chunks(
                chunks,
            )

    async def delete_dataframe(
        self,
        key: str,
    ) -> None:
        """
        Удаление DataFrame из Redis.

        Args:
            key: Ключ Redis
        """
        if not self.__redis:
            raise RuntimeError('Redis not connected')

        # Загружаем метаданные для определения количества частей
        metadata_raw = await self.__redis.get(
            f'{key}:metadata',
        )

        if metadata_raw is not None:
            metadata = json.loads(metadata_raw)
            parts_count = metadata.get('parts_count', 1)

            # Удаляем все части
            keys_to_delete = [key, f'{key}:metadata']
            for i in range(parts_count):
                keys_to_delete.append(f'{key}:part_{i}')

            await self.__redis.delete(*keys_to_delete)
            logger.info(f'Deleted DataFrame from Redis: {key}')

    async def exists(self, key: str) -> bool:
        """
        Проверка существования ключа в Redis.

        Args:
            key: Ключ Redis

        Returns:
            True если ключ существует
        """
        if not self.__redis:
            raise RuntimeError('Redis not connected')

        return await self.__redis.exists(key) > 0

    async def get_metadata(self, key: str) -> dict[str, Any] | None:
        """
        Получение метаданных DataFrame.

        Args:
            key: Ключ Redis

        Returns:
            Метаданные или None если не найдены
        """
        if not self.__redis:
            raise RuntimeError('Redis not connected')

        metadata_raw = await self.__redis.get(
            f'{key}:metadata',
        )

        if not metadata_raw:
            return None

        return json.loads(
            metadata_raw,
        )

    async def set(
        self,
        key: str,
        value: str,
    ) -> None:
        redis_ = self.__redis
        if not redis_:
            raise RuntimeError('Redis not connected')

        await redis_.set(
            key,
            value,
        )

    async def set_metadata(
        self,
        key: str,
        metadata_raw_data_raw: str,
    ) -> None:
        """
        Запись метаданных DataFrame.

        Args:
            key: Ключ Redis
            metadata_raw_data_raw: JSON-представление метаданных
        Returns:
            Метаданные или None если не найдены
        """

        await self.set(
            f'{key}:metadata',
            metadata_raw_data_raw,
        )


# Глобальный экземпляр менеджера Redis
g_redis_manager = RedisManager()
