import json
from typing import Any, Optional

from core.config import KafkaConfig, load_config


def _import_kafka_producer():
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise ImportError("缺少 kafka-python 依赖，请先安装 kafka-python") from exc

    return KafkaProducer


def create_producer(kafka_config: KafkaConfig) -> Any:
    kafka_producer_cls = _import_kafka_producer()

    return kafka_producer_cls(
        bootstrap_servers=kafka_config.bootstrap_servers,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        retries=kafka_config.retries,
        acks=kafka_config.acks,
    )


def get_producer(kafka_config: Optional[KafkaConfig] = None) -> Any:
    config = kafka_config or load_config().kafka
    return create_producer(config)
