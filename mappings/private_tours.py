# activity_mapper.py
from utils import get_component_id, get_stripped, safe_float, safe_int, get_location_id
from .location import map_region_name_to_id
import pandas as pd
                            
def map_private_tours_component(row, template_ids, COMPONENT_ID_MAP, context=None, row_index=-1):
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


    package_spans = []

    span_1_day = get_stripped(row, "Day")
    span_2_day = get_stripped(row, "Day.2")
    span_3_day = get_stripped(row, "Day.3")
    span_4_day = get_stripped(row, "Day.4")
    span_5_day = get_stripped(row, "Day.5")
    span_6_day = get_stripped(row, "Day.6")
    span_7_day = get_stripped(row, "Day.7")
    span_8_day = get_stripped(row, "Day.8")

    if span_1_day:
        
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
            })

        # This should continue as long as there exisits non null values for component 4, 5, 6, etc.


        package_spans.append({
            "title": get_stripped(row, "Day Title - Quote"),
            "description": get_stripped(row, "Day Description - Quote"),
            "items": package_span_items,
            "startDay": span_1_day,
            "endDay": span_1_day,
            "meals": []
        })

    if span_2_day:
        
        package_span_items = []

        comp1_name = get_stripped(row, "Component 1.2")
        comp1_type = get_stripped(row, "Component Type 1.2")
        comp1_id = ""
        if comp1_name:
            comp1_id = get_component_id(
                component_type=comp1_type.lower(),
                component_name=comp1_name,
                component_id_map=COMPONENT_ID_MAP,
                context={
                    **(context or {}),
                    "field": "Component 1.2",
                    "row_index": row_index,
                    "additional_info": f"{get_stripped(row, 'name')}"
                },
                required=True
            )
            package_span_items.append( {
                "componentId": comp1_id,
                "allDay": True,
            })

        comp2_name = get_stripped(row, "Component 2.2")
        comp2_type = get_stripped(row, "Component Type 2.2")
        comp2_id = ""
        if comp2_name:
            comp2_id = get_component_id(
                component_type=comp2_type.lower(),
                component_name=comp2_name,
                component_id_map=COMPONENT_ID_MAP,
                context={
                    **(context or {}),
                    "field": "Component 2.2",
                    "row_index": row_index,
                    "additional_info": f"{get_stripped(row, 'name')}"
                },
                required=True
            )
            package_span_items.append( {
                "componentId": comp2_id,
                "allDay": True,
            })

        comp3_name = get_stripped(row, "Component 3.2")
        comp3_type = get_stripped(row, "Component Type 3.2")
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
            })
        
        # This should continue as long as there exisits non null values for component 4, 5, 6, etc.

        package_spans.append({
            "title": get_stripped(row, "Day Title - Quote.2"),
            "description": get_stripped(row, "Day Description - Quote.2"),
            "items": package_span_items,
            "startDay": span_1_day,
            "endDay": span_1_day,
            "meals": []
        })

    
    # Keep adding spans as long as there keeps exisitng non-null Day.n values
    

    # ===== Level 0 → Base schema (empty) =====
    level_0 = {}

    # ===== Level 1 → Activity Details =====
    level_1 = {
        "private": False,
        "difficulty": ['Other', 'Easy', 'Medium', 'Hard', 'Advanced', 'Extreme'](int(get_stripped(row, "Difficulty")) or 0),
        "guided": False,
        "guideGuestRatio": -1,
        "requirements": {
            "gear": [],
            "minimumAge": -1,
            "maximumAge": -1,
            "lowerWeightLimitKg": "",
            "upperWeightLimitKg": -1,
            "lowerHeightLimitM": -1,
            "upperHeightLimitM": -1
        },
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
        {"templateId": template_ids[1], "data": level_1},
        {"templateId": template_ids[0], "data": level_0},
    ]

    return {
        "templateId": template_ids[1],
        "description": {
            "web": get_stripped(row, "Description") or "",
            "quote": get_stripped(row, "Description") or "",
            "final": get_stripped(row, "Description") or ""
        },
        "partners": [p.strip() for p in get_stripped(row, "partner").split(",") if p.strip()],
        "regions": [r for r in regions if r],  # filter out None values
        "name": get_stripped(row, "name") or "Untitled",
        "pricing": pricing,
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
            "title": get_stripped(row, "name"),
            "description": get_stripped(row, "Description - Quote"),
            # "startDate": "2025-08-01T00:00:00Z",
            # "endDate": "2025-08-10T00:00:00Z"
        },
    }
