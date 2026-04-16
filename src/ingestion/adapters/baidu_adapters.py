from typing import Any, Dict, List, Optional


def safe_get(data: Dict[str, Any], *keys, default=None):
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def adapt_weather(raw: Dict[str, Any]) -> Dict[str, Any]:
    location = safe_get(raw, "result", "location", default={}) or {}
    now = safe_get(raw, "result", "now", default={}) or {}

    return {
        "source": "baidu_weather",
        "data_type": "weather",
        "api_status": raw.get("status"),
        "api_message": raw.get("message"),
        "collect_time": None,
        "dt": None,
        "country": location.get("country"),
        "province": location.get("province"),
        "city": location.get("city"),
        "district": location.get("name"),
        "district_id": location.get("id"),
        "weather_text": now.get("text"),
        "temperature": now.get("temp"),
        "feels_like": now.get("feels_like"),
        "humidity": now.get("rh"),
        "wind_class": now.get("wind_class"),
        "wind_dir": now.get("wind_dir"),
        "precipitation_1h": now.get("prec_1h"),
        "clouds": now.get("clouds"),
        "visibility": now.get("vis"),
        "aqi": now.get("aqi"),
        "pm25": now.get("pm25"),
        "pm10": now.get("pm10"),
        "pressure": now.get("pressure"),
        "raw_collect_time": now.get("uptime"),
        "raw_json": raw,
    }


def adapt_traffic(raw: Dict[str, Any], city: Optional[str] = None) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []

    api_status = raw.get("status")
    api_message = raw.get("message")
    description = raw.get("description")

    evaluation = raw.get("evaluation", {}) or {}
    overall_status = evaluation.get("status")
    overall_status_desc = evaluation.get("status_desc")

    road_traffic = raw.get("road_traffic", []) or []

    for road in road_traffic:
        road_name = road.get("road_name")
        congestion_sections = road.get("congestion_sections", []) or []

        if not congestion_sections:
            result.append(
                {
                    "source": "baidu_traffic",
                    "data_type": "traffic",
                    "api_status": api_status,
                    "api_message": api_message,
                    "collect_time": None,
                    "dt": None,
                    "road_id": None,
                    "city": city,
                    "district": None,
                    "center_lng": None,
                    "center_lat": None,
                    "road_name": road_name,
                    "description": description,
                    "overall_status": overall_status,
                    "overall_status_desc": overall_status_desc,
                    "section_desc": None,
                    "road_type": None,
                    "congestion_distance": None,
                    "speed": None,
                    "section_status": None,
                    "congestion_trend": None,
                    "raw_json_full": raw,
                    "raw_json_road": road,
                    "raw_json_section": None,
                }
            )
            continue

        for section in congestion_sections:
            result.append(
                {
                    "source": "baidu_traffic",
                    "data_type": "traffic",
                    "api_status": api_status,
                    "api_message": api_message,
                    "collect_time": None,
                    "dt": None,
                    "road_id": None,
                    "city": city,
                    "district": None,
                    "center_lng": None,
                    "center_lat": None,
                    "road_name": road_name,
                    "description": description,
                    "overall_status": overall_status,
                    "overall_status_desc": overall_status_desc,
                    "section_desc": section.get("section_desc"),
                    "road_type": section.get("road_type"),
                    "congestion_distance": section.get("congestion_distance"),
                    "speed": section.get("speed"),
                    "section_status": section.get("status"),
                    "congestion_trend": section.get("congestion_trend"),
                    "raw_json_full": raw,
                    "raw_json_road": road,
                    "raw_json_section": section,
                }
            )

    return result
