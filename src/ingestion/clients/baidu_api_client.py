from typing import Optional

from core.config import ApiConfig, load_config


def _import_requests():
    try:
        import requests
    except ImportError as exc:
        raise ImportError("缺少 requests 依赖，请先安装 requests 或 requirements.txt") from exc

    return requests


class BaiduApiClient:
    def __init__(self, api_config: ApiConfig):
        self._config = api_config

    def _ensure_ak(self):
        if not self._config.baidu_ak:
            raise ValueError("没有读取到 BAIDU_AK，请检查 .env 文件")

    def fetch_weather(self, location: Optional[str] = None) -> dict:
        self._ensure_ak()

        requests = _import_requests()
        weather_location = location or self._config.weather_location

        response = requests.get(
            "https://api.map.baidu.com/weather/v1/",
            params={
                "location": weather_location,
                "coordtype": "wgs84",
                "data_type": "now",
                "output": "json",
                "ak": self._config.baidu_ak,
            },
            timeout=self._config.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def fetch_traffic(self, road_name: str, city: str) -> dict:
        self._ensure_ak()

        requests = _import_requests()

        response = requests.get(
            "https://api.map.baidu.com/traffic/v1/road",
            params={
                "road_name": road_name,
                "city": city,
                "ak": self._config.baidu_ak,
            },
            timeout=self._config.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()


def build_api_client(api_config: Optional[ApiConfig] = None) -> BaiduApiClient:
    config = api_config or load_config().api
    return BaiduApiClient(config)


_DEFAULT_CLIENT: Optional[BaiduApiClient] = None


def _get_default_client() -> BaiduApiClient:
    global _DEFAULT_CLIENT

    if _DEFAULT_CLIENT is None:
        _DEFAULT_CLIENT = build_api_client()

    return _DEFAULT_CLIENT


def fetch_weather(location: str = "126.642464,45.756967") -> dict:
    return _get_default_client().fetch_weather(location=location)


def fetch_traffic(road_name: str, city: str) -> dict:
    return _get_default_client().fetch_traffic(road_name=road_name, city=city)
