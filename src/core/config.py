import os
from dataclasses import dataclass
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - fallback for environments without python-dotenv
    def load_dotenv():
        return False


DEFAULT_PROJECT_ROOT = "/mnt/d/bigdata/myproject"
DEFAULT_WEATHER_LOCATION = "126.642464,45.756967"


def _to_bool(value: Optional[str], default: bool) -> bool:
    if value is None:
        return default

    normalized = value.strip().lower()
    return normalized in {"1", "true", "yes", "y", "on"}


def _to_int(value: Optional[str], default: int) -> int:
    if value is None:
        return default

    try:
        return int(value)
    except ValueError:
        return default


@dataclass(frozen=True)
class ApiConfig:
    baidu_ak: str
    weather_location: str
    timeout_seconds: int


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    weather_topic: str
    traffic_topic: str
    retries: int
    acks: str


@dataclass(frozen=True)
class CollectConfig:
    interval_seconds: int
    weather_interval_seconds: int
    save_raw_json: bool


@dataclass(frozen=True)
class PathConfig:
    project_root: str
    road_list_file: str
    raw_dir: str
    log_dir: str
    traffic_detail_output_path: str
    weather_output_path: str
    avg_speed_output_path: str
    traffic_detail_checkpoint: str
    weather_checkpoint: str
    avg_speed_checkpoint: str


@dataclass(frozen=True)
class AppConfig:
    api: ApiConfig
    kafka: KafkaConfig
    collect: CollectConfig
    paths: PathConfig


def load_config() -> AppConfig:
    load_dotenv()

    project_root = os.getenv("PROJECT_ROOT", DEFAULT_PROJECT_ROOT)

    paths = PathConfig(
        project_root=project_root,
        road_list_file=os.getenv("ROAD_LIST_FILE", f"{project_root}/data/config/road_list.csv"),
        raw_dir=os.getenv("RAW_DIR", f"{project_root}/data/raw"),
        log_dir=os.getenv("LOG_DIR", f"{project_root}/logs"),
        traffic_detail_output_path=os.getenv(
            "TRAFFIC_DETAIL_OUTPUT_PATH",
            "hdfs://localhost:9000/traffic/history/traffic_detail",
        ),
        weather_output_path=os.getenv(
            "WEATHER_OUTPUT_PATH",
            "hdfs://localhost:9000/traffic/history/weather",
        ),
        avg_speed_output_path=os.getenv(
            "AVG_SPEED_OUTPUT_PATH",
            "hdfs://localhost:9000/traffic/history/avg_speed",
        ),
        traffic_detail_checkpoint=os.getenv(
            "TRAFFIC_DETAIL_CHECKPOINT",
            f"file://{project_root}/checkpoints/traffic_detail",
        ),
        weather_checkpoint=os.getenv(
            "WEATHER_CHECKPOINT",
            f"file://{project_root}/checkpoints/weather",
        ),
        avg_speed_checkpoint=os.getenv(
            "AVG_SPEED_CHECKPOINT",
            f"file://{project_root}/checkpoints/avg_speed",
        ),
    )

    api = ApiConfig(
        baidu_ak=os.getenv("BAIDU_AK", "").strip(),
        weather_location=os.getenv("WEATHER_LOCATION", DEFAULT_WEATHER_LOCATION),
        timeout_seconds=_to_int(os.getenv("API_TIMEOUT_SECONDS"), 15),
    )

    kafka = KafkaConfig(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        weather_topic=os.getenv("KAFKA_WEATHER_TOPIC", "weather_raw"),
        traffic_topic=os.getenv("KAFKA_TRAFFIC_TOPIC", "traffic_raw"),
        retries=_to_int(os.getenv("KAFKA_RETRIES"), 3),
        acks=os.getenv("KAFKA_ACKS", "all"),
    )

    collect = CollectConfig(
        interval_seconds=_to_int(os.getenv("COLLECT_INTERVAL_SECONDS"), 60),
        weather_interval_seconds=_to_int(os.getenv("WEATHER_INTERVAL_SECONDS"), 600),
        save_raw_json=_to_bool(os.getenv("SAVE_RAW_JSON"), True),
    )

    return AppConfig(api=api, kafka=kafka, collect=collect, paths=paths)
