import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.config import AppConfig, load_config
from ingestion.adapters.baidu_adapters import adapt_traffic, adapt_weather
from ingestion.clients.baidu_api_client import BaiduApiClient, build_api_client
from ingestion.repositories.road_repository import load_active_roads
from messaging.kafka_producer import create_producer


@dataclass(frozen=True)
class CollectContext:
    now_ts: float
    collect_time: str
    dt: str


def setup_logger(log_dir: Optional[str] = None) -> logging.Logger:
    target_log_dir = log_dir or load_config().paths.log_dir
    os.makedirs(target_log_dir, exist_ok=True)

    logger = logging.getLogger("collector")
    logger.setLevel(logging.INFO)

    log_file = os.path.abspath(os.path.join(target_log_dir, "collector.log"))
    has_target_handler = any(
        isinstance(handler, logging.FileHandler)
        and os.path.abspath(handler.baseFilename) == log_file
        for handler in logger.handlers
    )

    if not has_target_handler:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def enrich_weather_record(record: Dict[str, Any], collect_time: str, dt: str) -> Dict[str, Any]:
    record["collect_time"] = collect_time
    record["dt"] = dt
    return record


def enrich_traffic_record(record: Dict[str, Any], road_meta: Dict[str, Any], collect_time: str, dt: str) -> Dict[str, Any]:
    record["collect_time"] = collect_time
    record["dt"] = dt
    record["road_id"] = road_meta.get("road_id")
    record["district"] = road_meta.get("district")
    record["center_lng"] = road_meta.get("center_lng")
    record["center_lat"] = road_meta.get("center_lat")
    record["free_flow_speed"] = road_meta.get("free_flow_speed")
    return record


class CollectorService:
    def __init__(self, config: Optional[AppConfig] = None, api_client: Optional[BaiduApiClient] = None):
        self.config = config or load_config()
        self.logger = setup_logger(self.config.paths.log_dir)
        self.api_client = api_client or build_api_client(self.config.api)
        self._last_weather_collect_ts: Optional[float] = None

    def should_collect_weather(self, now_ts: float) -> bool:
        if self._last_weather_collect_ts is None:
            return True

        return (now_ts - self._last_weather_collect_ts) >= self.config.collect.weather_interval_seconds

    def mark_weather_collected(self, now_ts: float):
        self._last_weather_collect_ts = now_ts

    def build_collect_context(self) -> CollectContext:
        now = datetime.now()
        return CollectContext(
            now_ts=time.time(),
            collect_time=now.strftime("%Y-%m-%d %H:%M:%S"),
            dt=now.strftime("%Y-%m-%d"),
        )

    def save_raw_json(self, data: Dict[str, Any], prefix: str):
        if not self.config.collect.save_raw_json:
            return

        os.makedirs(self.config.paths.raw_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(self.config.paths.raw_dir, f"{prefix}_{timestamp}.json")

        with open(file_path, "w", encoding="utf-8") as file_obj:
            json.dump(data, file_obj, ensure_ascii=False, indent=2)

    def publish_records(self, producer: Any, topic: str, records: List[Dict[str, Any]]):
        for record in records:
            producer.send(topic, value=record)

    def collect_weather_if_due(self, producer: Any, context: CollectContext):
        if not self.should_collect_weather(context.now_ts):
            elapsed = context.now_ts - (self._last_weather_collect_ts or 0)
            remaining = max(0, self.config.collect.weather_interval_seconds - int(elapsed))
            self.logger.info(f"本轮跳过天气采集，距下一次天气采集约 {remaining} 秒")
            return

        try:
            raw_weather = self.api_client.fetch_weather()
            self.save_raw_json(raw_weather, "weather")

            parsed_weather = adapt_weather(raw_weather)
            parsed_weather = enrich_weather_record(parsed_weather, context.collect_time, context.dt)

            producer.send(self.config.kafka.weather_topic, value=parsed_weather)
            self.mark_weather_collected(context.now_ts)
            self.logger.info(f"天气数据发送成功 -> {self.config.kafka.weather_topic}")
        except Exception as error:
            self.logger.exception(f"天气采集失败: {error}")

    @staticmethod
    def _safe_file_name(raw_name: str) -> str:
        safe = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in raw_name)
        return safe or "unknown_road"

    def collect_traffic_for_road(self, producer: Any, road: Dict[str, Any], context: CollectContext) -> int:
        road_name = road.get("road_name", "")
        city = road.get("city", "")

        if not road_name:
            self.logger.warning("跳过空道路名称记录")
            return 0

        try:
            raw_traffic = self.api_client.fetch_traffic(road_name=road_name, city=city)
            self.save_raw_json(raw_traffic, f"traffic_{self._safe_file_name(road_name)}")

            parsed_traffic_list = adapt_traffic(raw_traffic, city=city)
            if not parsed_traffic_list:
                self.logger.warning(f"道路无可发送记录: road_name={road_name}")
                return 0

            records = [
                enrich_traffic_record(item, road, context.collect_time, context.dt)
                for item in parsed_traffic_list
            ]
            self.publish_records(producer, self.config.kafka.traffic_topic, records)

            self.logger.info(f"路况发送成功: road_name={road_name}, message_count={len(records)}")
            return len(records)
        except Exception as error:
            self.logger.exception(f"路况采集失败: road_name={road_name}, error={error}")
            return 0

    def collect_once(self):
        roads = load_active_roads(self.config.paths.road_list_file)
        context = self.build_collect_context()

        self.logger.info(f"本轮采集开始，重点道路数量={len(roads)}")

        producer = create_producer(self.config.kafka)
        total_sent = 0

        try:
            self.collect_weather_if_due(producer, context)

            for road in roads:
                total_sent += self.collect_traffic_for_road(producer, road, context)
        finally:
            producer.flush()
            producer.close()

        self.logger.info(f"本轮采集结束，发送完成：traffic_count={total_sent}")


_DEFAULT_SERVICE: Optional[CollectorService] = None


def get_default_collector_service() -> CollectorService:
    global _DEFAULT_SERVICE

    if _DEFAULT_SERVICE is None:
        _DEFAULT_SERVICE = CollectorService()

    return _DEFAULT_SERVICE


def collect_once():
    get_default_collector_service().collect_once()


def load_road_list() -> List[Dict[str, Any]]:
    config = load_config()
    return load_active_roads(config.paths.road_list_file)
