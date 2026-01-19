# activity_mapper.py
from utils import get_component_id, get_stripped, safe_float, safe_int, get_location_id
from .location import map_region_name_to_id
import pandas as pd
                            
def map_excursion_component(row, template_ids, COMPONENT_ID_MAP, context=None, row_index=-1, rooms_data=None, partner_map=None, destination_override=None):
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


    package_span_items = []

    comp1_name = get_stripped(row, "Component 1")
    comp1_type = get_stripped(row, "Component Type 1")
    comp1_id = ""
    if comp1_name:
        comp1_id = get_component_id(
            component_type=comp1_type.lower(),
            component_name=comp1_name,
            component_id_map=COMPONENT_ID_MAP,
            context={
                **(context or {}),
                "field": "Component 1",
                "row_index": row_index,
                "additional_info": f"{get_stripped(row, 'name')}"
            },
            required=True
        )
        package_span_items.append( {
            "componentId": comp1_id,
            "allDay": True,
            # "startTime":"",
            # "endTime":""
        })

    comp2_name = get_stripped(row, "Component 2")
    comp2_type = get_stripped(row, "Component Type 2")
    comp2_id = ""
    if comp2_name:
        comp2_id = get_component_id(
            component_type=comp2_type.lower(),
            component_name=comp2_name,
            component_id_map=COMPONENT_ID_MAP,
            context={
                **(context or {}),
                "field": "Component 2",
                "row_index": row_index,
                "additional_info": f"{get_stripped(row, 'name')}"
            },
            required=True
        )
        package_span_items.append( {
            "componentId": comp2_id,
            "allDay": True,
            # "startTime":"",
            # "endTime":""
        })

    comp3_name = get_stripped(row, "Component 3")
    comp3_type = get_stripped(row, "Component Type 3")
    comp3_id = ""
    if comp3_name:
        comp3_id = get_component_id(
            component_type=comp3_type.lower(),
            component_name=comp3_name,
            component_id_map=COMPONENT_ID_MAP,
            context=context,
            required=True
        )
        package_span_items.append( {
            "componentId": comp3_id,
            "allDay": True,
            # "startTime":"",
            # "endTime":""
        })

    

    # ===== Level 0 → Base schema (empty) =====
    level_0 = {}

    # ===== Level 1 → Activity Details =====
    level_1 = {
        "private": False,
        "difficulty": get_stripped(row, "Difficulty") or "Other",
        "guided": get_stripped(row, "Guided?") == "Guided",
        "guideGuestRatio": -1,
        "requirements": {
            "gear": [],
            "minimumAge": -1,
            "maximumAge": -1,
            "lowerWeightLimitKg": "",
            "upperWeightLimitKg": -1,
            "lowerHeightLimitM": -1,
            "upperHeightLimitM": -1,
            "other":""
        },
        "additionalNotes":"",
        "facilities": {
            "isWheelChairAccessible": False,
            "isOkWhenPregnant": False,
            "isOkWithBreathingMachines": False,
            "hasDrinksIncluded": False,
            "hasComplementaryGifts": False,
            "hasNationalParkFee": False
        }
    }



    component_fields = [
        {"templateId": template_ids[2], "data": {}},
        {"templateId": template_ids[1], "data": level_1},
        {"templateId": template_ids[0], "data": level_0},
    ]

    # print(row)
    # print(f"name: {get_stripped(row, "name")}")

    return {        
        "orgId":"swoop",
        "destination":(destination_override or get_stripped(row, "Destination")).lower() or "patagonia",
        "state": "Draft",
        "tripId": "",
        "pricing": {"amount":0,"currency":"gbp"},
        
        "templateId": template_ids[2],
        "isBookable": True,
        "description": {
            "web": get_stripped(row, "Description - Quote") or "",
            "quote": get_stripped(row, "Description - Quote") or "",
            "final": get_stripped(row, "Description - Quote") or ""
        },
        "partners": ["PAT-" + id for id in get_stripped(row, "PartnerID").split("/")],
        "regions": [r for r in regions if r],  # filter out None values
        "name": get_stripped(row, "Code name") or "Untitled",
        "externalName": get_stripped(row, "name") or "Untitled",
        "media": media,
        "componentFields": component_fields,
        "package": {
            "spans": [
                {
                    "title": "Itinerary",
                    "description": get_stripped(row, "Description - Quote"),
                    "items": package_span_items,
                    "startDay": 1,
                    "endDay": 1,
                    "meals": []
                },
            ],
            "title": get_stripped(row, "name") or "NA",
            # "description": get_stripped(row, "Description - Quote"),
            # "startDate": "2025-08-01T00:00:00Z",
            # "endDate": "2025-08-10T00:00:00Z"
        },
    }
