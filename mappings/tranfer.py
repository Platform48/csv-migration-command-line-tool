# activity_mapper.py
from utils import get_journey_id, get_stripped, safe_float, safe_int, get_location_id
from .location import map_region_name_to_id
import pandas as pd

def map_transfer_component(row, template_ids, COMPONENT_ID_MAP, context=None, row_index=-1, rooms_data=None, partner_map=None, destination_override=None):
    """
    Map activity component with improved ID lookups and missing reference logging
    """

    # --- Regions ---
    regions = [map_region_name_to_id(get_stripped(row, "regions"))]

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
    journey_name = get_stripped(row, "journey")
    journey_id = get_journey_id(
        journey_name=journey_name,
        component_id_map=COMPONENT_ID_MAP,
        context={
            **(context or {}),
            "field": "journey",
            "row_index": row_index,
            "additional_info": f"{get_stripped(row, 'name')}"
        }
    )

    # ===== Level 0 → Base schema (empty) =====
    level_0 = {}

    # ===== Level 1 → Activity Details =====
    level_1 = {
        "journey": journey_id or "",
        "type": get_stripped(row, 'types'),
        "driver": get_stripped(row, 'types') == "Guided" or get_stripped(row, 'types') == "Driver Only",
        "guided": get_stripped(row, 'types') == "Guided",
        "operator": get_stripped(row, 'operator'),
        "notes": {
            "pickup": get_stripped(row, 'pickupNotes'),
            "schedule": get_stripped(row, 'Schedule Notes for Sales/CX')
        },
        "departureTime": get_stripped(row, 'Time') or "00:00:00",
        "arrivalTime": "00:00:00"
    }


    component_fields = [
        {"templateId": template_ids[1], "data": level_1},
        {"templateId": template_ids[0], "data": level_0},
    ]
    name = get_stripped(row, "Code") or "Untitled"
    external_name = get_stripped(row, "name")
    # print(f"Name: {get_stripped(row, "Code")}, {row.get("Code")}")
    # print(row)
    # name = f"{row.get( "name")} {row.get("partner")} {row.get("guidesDrivers")}"
    # print(row)
    # print(name)
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
            "web": get_stripped(row, "importantInformationWeb") or "",
            "quote": get_stripped(row, "importantInformationQuote") or "",
            "final": get_stripped(row, "importantInformationFinal") or ""
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
        "name": name,
        "externalName": external_name,
        "media": media,
        "componentFields": component_fields,
    }