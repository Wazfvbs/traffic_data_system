import csv
from typing import Any, Dict, List


RoadMeta = Dict[str, Any]


def _safe_float(value):
    if value is None:
        return None

    normalized = str(value).strip()
    if not normalized:
        return None

    try:
        return float(normalized)
    except ValueError:
        return None


def load_active_roads(road_list_file: str) -> List[RoadMeta]:
    roads: List[RoadMeta] = []

    with open(road_list_file, "r", encoding="utf-8-sig") as file_obj:
        reader = csv.DictReader(file_obj)

        for row in reader:
            is_active = str(row.get("is_active", "1")).strip()
            if is_active != "1":
                continue

            roads.append(
                {
                    "road_id": row.get("road_id", "").strip(),
                    "road_name": row.get("road_name", "").strip(),
                    "city": row.get("city", "").strip(),
                    "district": row.get("district", "").strip(),
                    "center_lng": row.get("center_lng", "").strip(),
                    "center_lat": row.get("center_lat", "").strip(),
                    "display_order": row.get("display_order", "").strip(),
                    "remark": row.get("remark", "").strip(),
                    "free_flow_speed": _safe_float(row.get("free_flow_speed")),
                }
            )

    return roads
