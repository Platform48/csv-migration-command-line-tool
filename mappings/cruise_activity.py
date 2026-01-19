# activity_mapper.py
from utils import get_stripped, safe_float, safe_int, get_location_id
from .location import map_region_name_to_id
import pandas as pd

def map_cruise_activity_component(row, template_ids, COMPONENT_ID_MAP, context=None, row_index=-1, rooms_data=None, partner_map=None, destination_override=None):
    """
    Map cruise activity component with improved ID lookups and missing reference logging
    """

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
    images = get_stripped(row, "Images").split("\n")
    images = [img.strip() for img in images if img.strip()]  # clean empties

    media = {
        "images": images,
        "videos": []
    }


    # ===== Level 0 → Base schema (empty) =====
    level_0 = {}

    # ===== Level 1 → Activity Details =====
    level_1 = {
        "complementary": get_stripped(row, "At additional cost or Complimentary") == "At additional cost",
        "preBookingRequired": (get_stripped(row, "Pre-booking required") or "").startswith("Y"),
        "requirements":{
            "minimumAge": safe_int( get_stripped(row, "Min age")),
            "maximumAge": safe_int( get_stripped(row, "Max age")),
            "minimumHeightm": safe_int( get_stripped(row, "Lower Height Limit (m)") ),
            "gear": get_stripped(row, "Required gear (comma-list)").split("\n"),
            "Other":""
        }
    }

    component_fields = [
        {"templateId": template_ids[1], "data": level_1},
        {"templateId": template_ids[0], "data": level_0},
    ]

    return {
        "orgId":"swoop",
        "destination":"antarctica",
        "state": "Draft",
        "tripId": "",
        "pricing": {"amount":0,"currency":"gbp"},
        "package": None,
        "templateId": template_ids[1],
        "isBookable": True,
        "description": {
            "web": get_stripped(row, "description") or "",
            "quote": get_stripped(row, "description") or "",
            "final": get_stripped(row, "description") or ""
        },
        "partners": [
            partner_id
            for p in get_stripped(row, "Partner").split(",")
            if p.strip()
            for partner_id in [
                partner_map.get(
                    destination_override or get_stripped(row, "Destination") or "Patagonia",
                    {}
                ).get(p.strip())
            ]
            if partner_id  # only include if found
        ],
        "regions": [r for r in regions if r],  # filter out None values
        "name": get_stripped(row, "CODE") or "Untitled",
        "externalName": get_stripped(row, "name") or "Untitled",
        "media": media,
        "componentFields": component_fields,
    }
