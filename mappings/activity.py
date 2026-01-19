# activity_mapper.py
from utils import get_stripped, safe_float, safe_int, get_location_id
from .location import map_region_name_to_id
import pandas as pd

def map_activity_component(row, template_ids, COMPONENT_ID_MAP, context=None, row_index=-1, rooms_data=None, partner_map=None, destination_override=None):
    """
    Map activity component with improved ID lookups and missing reference logging
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
    images = get_stripped(row, "images").split("\n")
    images = [img.strip() for img in images if img.strip()]  # clean empties

    media = {
        "images": images,
        "videos": []
    }

    # --- Location lookups with improved logging ---
    start_location_name = get_stripped(row, "startLocation")
    start_location_id = get_location_id(
        location_name=start_location_name,
        component_id_map=COMPONENT_ID_MAP,
        context={
            **(context or {}),
            "field": "startLocation",
            "row_index": row_index,
            "additional_info": f"{get_stripped(row, 'name')}"
        }
    )

    end_location_name = get_stripped(row, "endLocation")
    end_location_id = get_location_id(
        location_name=end_location_name,
        component_id_map=COMPONENT_ID_MAP,
        context={
            **(context or {}),
            "field": "endLocation",
            "row_index": row_index,
            "additional_info": f"{get_stripped(row, 'name')}"
        }
    )

    # ===== Level 0 → Base schema (empty) =====
    level_0 = {}

    # ===== Level 1 → Activity Details =====
    level_1 = {
        # "journey": f"{start_location_name} to {end_location_name}",
        "difficulty": get_stripped(row, "difficulty") or "Other",
        "elevation": {
            "ascentm": safe_int(get_stripped(row, "elevationFieldsifApplicable.totalElevationGainmetres")) or -1,
            "descentm": safe_int(get_stripped(row, "elevationFieldsifApplicable.totalDescentmetres")) or -1
        },
        "startLocation": start_location_id or "",
        "endLocation": end_location_id or ""
    }

    component_fields = [
        {"templateId": template_ids[1], "data": level_1},
        {"templateId": template_ids[0], "data": level_0},
    ]

    return {
        "orgId":"swoop",
        "destination":(destination_override or get_stripped(row, "destination")).lower() or "patagonia",
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
            for p in get_stripped(row, "partner").split(",")
            if p.strip()
            for partner_id in [
                partner_map.get(
                    destination_override or get_stripped(row, "destination") or "Patagonia",
                    {}
                ).get(p.strip())
            ]
            if partner_id  # only include if found
        ],
        "regions": [r for r in regions if r],  # filter out None values
        "name": get_stripped(row, "name") or "Untitled",
        "externalName": get_stripped(row, "name") or "Untitled",
        "media": media,
        "componentFields": component_fields,
    }
