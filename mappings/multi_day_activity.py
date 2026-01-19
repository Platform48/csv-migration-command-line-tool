# activity_mapper.py
from utils import get_component_id, get_stripped, safe_float, safe_int, get_location_id, parse_html_list
from .location import map_region_name_to_id
import pandas as pd
                            
def map_multi_day_activity_component(row, template_ids, COMPONENT_ID_MAP, context=None, row_index=-1, rooms_data=None, partner_map=None, destination_override=None):
    """
    Map activity component with improved ID lookups and missing reference logging
    """

    # --- Regions ---
    regions = [map_region_name_to_id(get_stripped(row, "Region Tags"))]

    media = {
        "images": [],
        "videos": []
    }

    # --- Inclusions / Exclusions ---
    inclusions = parse_html_list(get_stripped(row, "Inclusions"), "Inclusions")
    exclusions = parse_html_list(get_stripped(row, "Exclusions"), "Exclusions")

    # --- Dynamic Package Spans ---
    package_spans = []
    
    # Keep adding spans until we find an empty Day.n
    span_index = 1
    while True:
        # Determine the day column name
        if span_index == 1:
            day_col = f"Day {span_index}"
            title_col = "Day Title - Quote"
            desc_col = "Day Description - Quote"
            comp_suffix = ""
        else:
            day_col = f"Day {span_index}"
            title_col = f"Day Title - Quote.{span_index - 1}"
            desc_col = f"Day Description - Quote.{span_index - 1}"
            comp_suffix = f".{span_index - 1}"
        
        # Check if this day exists
        span_day = get_stripped(row, day_col)
        if not span_day:
            break  # No more days to process
        
        # Process components for this span
        package_span_items = []
        comp_index = 1
        
        while True:
            # Determine component column names
            comp_name_col = f"Component {comp_index}{comp_suffix}"
            comp_type_col = f"Component Type {comp_index}{comp_suffix}"
            
            comp_name = get_stripped(row, comp_name_col)
            comp_type = get_stripped(row, comp_type_col)
            pkg_aliases = {
                "Accommodation": "ground_accommodation"
                
            }
            comp_type = pkg_aliases.get(comp_type, comp_type)


            # If no component name, stop processing components for this span
            if not comp_name:
                break
            
            # Get component ID
            if comp_type:  # Only proceed if we have a component type
                comp_id = get_component_id(
                    component_type=comp_type.lower(),
                    component_name=comp_name,
                    component_id_map=COMPONENT_ID_MAP,
                    context={
                        **(context or {}),
                        "field": comp_name_col,
                        "row_index": row_index,
                        "additional_info": f"{get_stripped(row, 'Name')} - Day {span_index}, Component {comp_index}"
                    },
                    required=True
                )
                
                if True or comp_id:  # Only add if we got a valid ID
                    package_span_items.append({
                        "componentId": comp_id or "component_00000000000000000000000000000000",
                        "allDay": True,
                        # "startTime":"",
                        # "endTime":""
                    })
            
            comp_index += 1
        
        # Add the span (even if it has no valid components)
        package_spans.append({
            "title": get_stripped(row, title_col) or "Span Title",
            "description": get_stripped(row, desc_col),
            "items": package_span_items,
            "startDay": safe_int(span_day.split('-')[0]),
            "endDay": safe_int(span_day.split('-')[-1]),
            "meals": []
        })
        
        span_index += 1

    # ===== Level 0 → Base schema (empty) =====
    level_0 = {}

    # ===== Level 1 → Activity Details =====
    
    level_1 = {
        "private": False,
        "difficulty": "Other",
        "guided": False,
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
        },
        "inclusions": inclusions,
        "exclusions": exclusions
    }

    component_fields = [
        {"templateId": template_ids[2], "data": {}},
        {"templateId": template_ids[1], "data": level_1},
        {"templateId": template_ids[0], "data": level_0},
    ]

    return {
        "orgId":"swoop",
        "destination":(destination_override or get_stripped(row, "Destination")).lower() or "patagonia",
        "state": "Draft",
        "tripId": get_stripped(row, "TripID") or "",
        "pricing": {"amount":0,"currency":"gbp"},

        "templateId": template_ids[2],
        "isBookable": True,
        "description": {
            "web": get_stripped(row, "Description") or "",
            "quote": get_stripped(row, "Description") or "",
            "final": get_stripped(row, "Description") or ""
        },
        "partners": [
            get_stripped(row, "TripID")[:3]+"-"+get_stripped(row, "PartnerID")
        ],
        "regions": [r for r in regions if r],  # filter out None values
        "name": get_stripped(row, "Name") or "Untitled",
        "externalName": get_stripped(row, "Name") or "Untitled",
        "media": media,
        "componentFields": component_fields,
        "package": {
            "spans": package_spans,
            "title": get_stripped(row, "Name") or "NA",
            # "description": get_stripped(row, "Description - Quote"),
        },
    }