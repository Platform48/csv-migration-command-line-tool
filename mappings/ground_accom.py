from utils import get_stripped, safe_float, safe_int, get_location_id
from .location import LOCATION_ALIASES, map_region_name_to_id
import pandas as pd

def map_ground_accommodation_component(row, template_ids, COMPONENT_ID_MAP, context=None, row_index=-1):
    """
    Map ground accommodation component using consistent ID lookup utilities
    """

    # --- Regions ---
    regions = [map_region_name_to_id(get_stripped(row, "Region"))]

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
    images = [i.strip() for i in images if i.strip()]  # Clean empty strings
    media = {"images": images, "videos": []}

    # --- Location ID lookup with util ---
    location_name = get_stripped(row, "location")
    if location_name in LOCATION_ALIASES:
        location_name = LOCATION_ALIASES[location_name]

    location_id = get_location_id(
        location_name=location_name,
        component_id_map=COMPONENT_ID_MAP,
        context={
            **(context or {}),
            "field": "location",
            "row_index": row_index,
            "additional_info": f"{get_stripped(row, 'name')}"
        }
    )

    # ===== Level 0 → Base schema (empty) =====
    level_0 = {}

    # ===== Level 1 → Accommodation Details =====
    level_1 = {
        "location": location_id or "",

        "facilities": {
            "bar": get_stripped(row, "facilities.bar") == "TRUE",
            "elevator": get_stripped(row, "facilities.elevator") == "TRUE",
            "jacuzzi": get_stripped(row, "facilities.jacuzzi") == "Included",
            "library": get_stripped(row, "facilities.library") == "TRUE",
            "pool": get_stripped(row, "facilities.pool") == "Included",
            "spa": get_stripped(row, "facilities.spa") == "Included",
            "steamRoom": get_stripped(row, "facilities.steamRoom") == "Included",
            "laundry": get_stripped(row, "facilities.laundry") == "Included",
            "shop": get_stripped(row, "facilities.shop") == "TRUE",
            "restaurants": get_stripped(row, "facilities.restaurants") == "TRUE",
            "sauna": get_stripped(row, "facilities.sauna") == "Included",
            "gym": get_stripped(row, "facilities.gym") == "Included",
            "massage": get_stripped(row, "facilities.massage") == "Included",
            "roomService": get_stripped(row, "facilities.roomService") == "TRUE",
            "wiFi": get_stripped(row, "connectivity.wiFi") == "TRUE",
            "phoneSignal": get_stripped(row, "Phone Signal") == "TRUE"
        },
        "checkin": {
            "start": get_stripped(row, "Check in Time"),
            "end": "",
            "out": get_stripped(row, "Check Out Time")
        },
        "info": {
            "yearBuilt": safe_int(get_stripped(row, "facts.yearBuilt")),
            "capacity": safe_int(get_stripped(row, "facts.capacity"))
        },
        "rooms": [],
        "requirements": {
            "minimumAge": safe_int(get_stripped(row, "minimumAge"))
        },
        "inspections": [
            {
                "inspectedBy": get_stripped(row, "Inspected by 1"),
                "date":        get_stripped(row, "Date 1"),
                "notes":       get_stripped(row, "Inspection Notes 1")
            },
            {
                "inspectedBy": get_stripped(row, "Inspected by 2"),
                "date":        get_stripped(row, "Date 2"),
                "notes":       get_stripped(row, "Inspection Notes 2")
            },
        ]
    }

    # ===== Level 2 → Ground Accommodation =====
    level_2 = {
        "type": get_stripped(row, "Type"),
    }

    component_fields = [
        {"templateId": template_ids[2], "data": level_2},
        {"templateId": template_ids[1], "data": level_1},
        {"templateId": template_ids[0], "data": level_0},
    ]

    return {
        "templateId": template_ids[2],
        "description": {
            "web": get_stripped(row, "Description") or "",
            "quote": get_stripped(row, "Description") or "",
            "final": get_stripped(row, "Description") or ""
        },
        "partners": [p.strip() for p in get_stripped(row, "Partner").split(",") if p.strip()],
        "regions": [r for r in regions if r],
        "name": get_stripped(row, "name") or "Untitled",
        "pricing": pricing,
        "media": media,
        "componentFields": component_fields,
        "package": {},
    }
