# activity_mapper.py
from utils import get_component_id, get_stripped, safe_float, safe_int, get_location_id
from .location import map_region_name_to_id
import pandas as pd

import json
import re

def normalize_day_string(s: str) -> str:
    if s.strip().upper() == "NULL":
        return ""
    
    numbers = re.findall(r"\d+", s)
    
    if not numbers:
        return ""
    elif len(numbers) == 1:
        return numbers[0]
    else:
        return f"{numbers[0]}-{numbers[1]}"

def map_cruise_component(row, template_ids, COMPONENT_ID_MAP, context=None, row_index=-1, rooms_data=None, partner_map=None, destination_override=None):
    """
    Map cruise component with improved ID lookups and missing reference logging
    """

    # --- Regions ---
    region_names = get_stripped(row, "Region").split("\n")
    for i, r in enumerate(region_names):
        if r == "FSG":
            region_names[i] = "Falklands"
    regions = [map_region_name_to_id(r) for r in region_names]
    regions = [r for r in regions if r]

    # --- Media ---
    images = get_stripped(row, "Trip Images").split(",")
    images = [img.strip() for img in images if img.strip()]  # clean empties

    media = {
        "images": images,
        "videos": []
    }

    # --- Dynamic Package Spans ---
    package_spans = []
    
    # Keep adding spans until we find an empty Day.n
    span_index = 1
    while True:
        # Determine the day column name
        if span_index == 1:
            day_col = "Day"
            title_col = "Day Title"
            desc_col = "Day Description"
            comp_suffix = ""
        else:
            day_col = f"Day.{span_index - 1}"
            title_col = f"Day Title.{span_index - 1}"
            desc_col = f"Day Description.{span_index - 1}"
            comp_suffix = f".{span_index - 1}"
        
        # Check if this day exists
        span_day = get_stripped(row, day_col)
        span_day = normalize_day_string(span_day)
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
                        "additional_info": f"{get_stripped(row, 'name')} - Day {span_index}, Component {comp_index}"
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

    # --- Add extra span for accommodation (Ground Accom) ---
    accom_name = get_stripped(row, "Ship")
    if accom_name:
        package_span_items = []
        comp_id = get_component_id(
            component_type="ship_accommodation",
            component_name=accom_name,
            component_id_map=COMPONENT_ID_MAP,
            context={
                **(context or {}),
                "field": "Ship",
                "row_index": row_index,
                "additional_info": f"{get_stripped(row, 'name')} - Full Trip Ship Accom"
            },
            required=True
        )
        package_span_items.append({
            "componentId": comp_id or "component_00000000000000000000000000000000",
            "allDay": True,
            # "startTime":"",
            # "endTime":""
        })

        # Determine last day from existing spans
        if package_spans:
            last_day = max(span.get("endDay", 1) for span in package_spans)
        else:
            last_day = 1

        # Add the accommodation span
        package_spans.append({
            "title": "Ship Accommodation",
            "description": f"{accom_name} for the entire trip",
            "items": package_span_items,
            "startDay": 1,
            "endDay": last_day,
            "meals": []
        })


    # ===== Level 0 → Base schema (empty) =====

    # ===== Level 1 → Package Details =====
    
    # ===== Level 2 → Cruise Details =====
    
    difficulty_levels = ['Other', 'Easy', 'Medium', 'Hard', 'Advanced', 'Extreme']
    difficulty_index = safe_int(get_stripped(row, "Difficulty"), 0)
    # Ensure difficulty_index is within valid range
    if difficulty_index < 0 or difficulty_index >= len(difficulty_levels):
        difficulty_index = 0
    

    inclusions_raw = re.split(r'[\n\r]*[•\-*•]\s*', get_stripped(row, "Inclusions"))
    exclusions_raw = re.split(r'[\n\r]*[•\-*•]\s*', get_stripped(row, "Exclusions"))

    # Strip whitespace and drop empty lines
    inclusions = [i.strip() for i in inclusions_raw if i.strip()]
    exclusions = [i.strip() for i in exclusions_raw if i.strip()]

    level_1 = {
        "private": False,
        "difficulty": difficulty_levels[difficulty_index],
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
        },
        "inclusions": inclusions,
        "exclusions": exclusions
    }

    activities = [a.strip() for a in get_stripped(row, "Activity").split("\n") if a.strip()]
    deref_activities = []

    for activity_name in activities:
        comp_id = get_component_id(
            component_type="cruise_activity",
            component_name=activity_name,
            component_id_map=COMPONENT_ID_MAP,
            context={
                **(context or {}),
                "field": "Activity",
                "row_index": row_index,
                "additional_info": f"{get_stripped(row, 'name')} - Cruise Activity: {activity_name}"
            },
            required=False  # Don't fail entire record if one activity is missing
        )

        deref_activities.append(comp_id or "component_00000000000000000000000000000000")

    level_2 = {
        "activities": deref_activities
    }

    component_fields = [
        {"templateId": template_ids[2], "data": level_2},
        {"templateId": template_ids[1], "data": level_1},
        {"templateId": template_ids[0], "data": {}},
    ]


    tripId = get_stripped(row, "Trip ID") or ""
    if tripId: 
        tripId = "ANT-"+tripId


    val = {
        "orgId":"swoop",
        "destination":(destination_override or get_stripped(row, "destination")).lower() or "patagonia",
        "state": "Draft",
        "tripId": tripId,
        "pricing": {"amount":0,"currency":"gbp"},


        "templateId": template_ids[2],
        "isBookable": True,
        "description": {
            "web": get_stripped(row, "Cruise Description") or "",
            "quote": get_stripped(row, "Cruise Description") or "",
            "final": get_stripped(row, "Cruise Description") or ""
        },
        "partners": [destination_override[:3].upper()+"-"+get_stripped(row, "Partner ID")],
        "regions": regions,  # filter out None values
        "name": get_stripped(row, "Name") or "Untitled",
        "externalName": get_stripped(row, "Name") or "Untitled",
        "media": media,
        "componentFields": component_fields,
        "package": {
            "spans": package_spans,
            "title": get_stripped(row, "Name") or "NA",
            # "description": get_stripped(row, "Cruise Description"),
            # "startDate": "2025-08-01T00:00:00Z",
            # "endDate": "2025-08-10T00:00:00Z"
        },
    }

    if get_stripped(row, "Name") == "Discover South Georgia, Antarctica and Falklands - 22 days reverse itinerary":
        pass

    return val