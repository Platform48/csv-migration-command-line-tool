# activity_mapper.py
from utils import get_component_id, get_stripped, safe_float, safe_int, get_location_id, parse_html_list
from .location import map_region_name_to_id
import pandas as pd
                            
def map_all_inclusive_hotels_component(row, template_ids, COMPONENT_ID_MAP, context=None, row_index=-1, rooms_data=None, partner_map=None, destination_override=None):
    """
    Map activity component with improved ID lookups and missing reference logging
    """

    # --- Regions ---
    regions = [map_region_name_to_id(get_stripped(row, "Region Tags"))]

    # --- Pricing ---
    # price_val = None
    # if pd.notna(row.get("Price")):
    #     try:
    #         price_val = int(float(row.get("Price")))
    #     except (ValueError, TypeError):
    #         price_val = None

    # pricing = {}
    # if price_val:
    #     pricing = {
    #         "amount": price_val,
    #         "currency": get_stripped(row, "Currency") or "USD"
    #     }

    # --- Media ---
    # images = get_stripped(row, "images").split("\n")
    # images = [img.strip() for img in images if img.strip()]  # clean empties

    media = {
        "images": [],
        "videos": []
    }

    # --- Dynamic Package Spans ---
    package_spans = []
    
    # For Debug
    if get_stripped(row, "Name") == "Explore Torres del Paine from a Luxury Lodge":
        pass

    # Keep adding spans until we find an empty Day.n
    span_index = 1
    while True:
        # Determine the day column name
        if span_index == 1:
            day_col = "Day"
            title_col = "Day Title - Quote"
            desc_col = "Day Description - Quote"
            comp_suffix = ""
        else:
            day_col = f"Day.{span_index - 1}"
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
        title_val = get_stripped(row, title_col) or get_stripped(row, title_col.replace("Quote", "Web"))
        desc_val = get_stripped(row, desc_col) or get_stripped(row, desc_col.replace("Quote", "Web"))

        package_spans.append({
            "title": title_val or "Span Title",
            "description": desc_val,
            "items": package_span_items,
            "startDay": safe_int(span_day.split('-')[0]),
            "endDay": safe_int(span_day.split('-')[-1]),
            "meals": []
        })
        
        span_index += 1

    # --- Add extra span for accommodation (Ground Accom) ---
    accom_name = get_stripped(row, "Accommodation")
    if accom_name:
        package_span_items = []
        comp_id = get_component_id(
            component_type="ground_accommodation",
            component_name=accom_name,
            component_id_map=COMPONENT_ID_MAP,
            context={
                **(context or {}),
                "field": "Accommodation",
                "row_index": row_index,
                "additional_info": f"{get_stripped(row, 'Name')} - Full Trip Ground Accom"
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
            "title": "Ground Accommodation",
            "description": f"{accom_name} for the entire trip",
            "items": package_span_items,
            "startDay": 1,
            "endDay": last_day,
            "meals": []
        })


    # ===== Level 0 → Base schema (empty) =====
    level_0 = {}

    # ===== Level 1 → Activity Details =====
    difficulty_levels = ['Other', 'Easy', 'Medium', 'Hard', 'Advanced', 'Extreme']
    difficulty_index = safe_int(get_stripped(row, "Difficulty"), 0)
    # Ensure difficulty_index is within valid range
    if difficulty_index < 0 or difficulty_index >= len(difficulty_levels):
        difficulty_index = 0
    
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
        "inclusions": parse_html_list(get_stripped(row, "Inclusions"), "Inclusions"),
        "exclusions": parse_html_list(get_stripped(row, "Exclusions"), "Exclusions"),
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
            # "description": get_stripped(row, "Description - Quote") or "NA",
            # "startDate":"2000-01-01T00:00:00Z",
            # "endDate":"2000-01-01T00:00:00Z",
            # "startDate": "2025-08-01T00:00:00Z",
            # "endDate": "2025-08-10T00:00:00Z"
        },
    }