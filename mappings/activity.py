# ground_accommodation_mapper.py
from utils import get_stripped, safe_float, safe_int
from .location import LOCATION_ALIASES, map_region_name_to_id
import pandas as pd

def map_activity_component(row, template_ids, COMPONENT_ID_MAP):

    # --- Regions ---
    regions = [map_region_name_to_id(get_stripped(row, "region"))]

    # --- Pricing ---
    price_val = None
    if pd.notna(row.get("Price")):
        try:
            price_val = int(float(row.get("Price")))
        except (ValueError, TypeError):
            price_val = None
    pricing = {}
    if price_val:
        pricing = {
            "amount": price_val,
            "currency": get_stripped(row, "Currency") or "USD"
        }

    # --- Media ---
    images = get_stripped(row, "images").split("\n")
    for i in images:
        i = i.strip()

    media = {
        "images": images,
        "videos": []
    }

    # start_location_name = get_stripped(row, "startLocation")
    # if start_location_name in LOCATION_ALIASES:
    #     start_location_name = LOCATION_ALIASES[start_location_name]
    # start_location_id = COMPONENT_ID_MAP.get(("location", start_location_name))
    # if not start_location_id:
    #     print(f"❌ WARNING NO location_id matching {start_location_name}")
    #     start_location_id = ""

    # end_location_name = get_stripped(row, "startLocation")
    # if end_location_name in LOCATION_ALIASES:
    #     end_location_name = LOCATION_ALIASES[end_location_name]
    # end_location_id = COMPONENT_ID_MAP.get(("location", end_location_name))
    # if not end_location_id:
    #     print(f"❌ WARNING NO location_id matching {end_location_name}")
    #     end_location_id = ""

    # ===== Level 0 → Base schema (empty) =====
    level_0 = {}

    # ===== Level 1 → Jounrey Details =====
    level_1 = {
        "journey": f"{get_stripped(row, "journey")} to {get_stripped(row, "endLocation")}",
        "difficulty": get_stripped(row, "difficulty") or "Other",
        "elevation": {
            "ascentm": safe_int(get_stripped(row, "elevationFieldsifApplicable.totalElevationGainmetres")) or -1,
            "descentm": safe_int(get_stripped(row, "elevationFieldsifApplicable.totalDescentmetres")) or -1
        }
    }

    component_fields = [
        {"templateId": template_ids[1], "data": level_1},
        {"templateId": template_ids[0], "data": level_0},
    ]

    return {
        "templateId": template_ids[1],
        "description":{
            "web":get_stripped(row, "description") or "",
            "quote":get_stripped(row, "description") or "",
            "final":get_stripped(row, "description") or ""
        },
        "partners": [p.strip() for p in get_stripped(row, "partner").split(",") if p.strip()],
        "regions": regions,
        "name": get_stripped(row, "name") or "Untitled",
        "pricing": pricing,
        "media": media,
        "componentFields": component_fields,
        "package": {},
    }
