from utils import get_stripped, safe_float, safe_int, get_location_id
from .location import LOCATION_ALIASES, map_region_name_to_id
import pandas as pd

def map_ship_accommodation_component(row, template_ids, COMPONENT_ID_MAP, context=None, row_index=-1):
    """
    Map ship accommodation component using consistent ID lookup utilities
    """

    # --- Regions ---
    raw_region = get_stripped(row, "Region")
    primary_region_id = map_region_name_to_id(raw_region)
    regions = [primary_region_id] if primary_region_id else []
    
    # Add additional regions if present
    for reg_field in ["Region 2"]:
        additional_region = get_stripped(row, reg_field)
        mapped_id = map_region_name_to_id(additional_region)
        if mapped_id and mapped_id not in regions:
            regions.append(mapped_id)

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
    images = [
        get_stripped(row, col)
        for col in ["Image 1", "Image 2", "Image 3", "Image 4", "Image 5"]
        if get_stripped(row, col)
    ]
    media = {"images": images, "videos": [get_stripped(row, "Video")] if get_stripped(row, "Video") else []}

    # ===== Level 0 → Ship =====
    level_2 = {
        "deckPlan": get_stripped(row, "Deck Plan"),
        "shipFacilities": {
            "observationLounge": get_stripped(row, "Observation Lounge").lower() == "true",
            "mudroom": get_stripped(row, "Mudroom").lower() == "true",
            "walkingTrackWraparoundDeck": get_stripped(row, "Walking Track/Wraparound Deck").lower() == "true",
            "openBridgePolicy": get_stripped(row, "Open Bridge Policy").lower() == "true",
            "igloos": get_stripped(row, "Igloos").lower() == "true",
            "scienceCentreLaboratory": get_stripped(row, "Science Centre/Laboratory").lower() == "true"
        },
        "type": get_stripped(row, "Type") or 'Other'
    }

    # ===== Level 1 → Accommodation =====
    level_1 = {
        "location": "",
        "type": get_stripped(row, "Type") or "Standard Ship", 
        "facilities": {
            "library": get_stripped(row, "Library") == "TRUE",
            "shop": get_stripped(row, "Shop") == "TRUE",
            "restaurant": get_stripped(row, "Restaurant") == "TRUE",
            "additionalRestaurants": get_stripped(row, "Additional restaurant") == "TRUE",
            "bar": get_stripped(row, "Bar") == "TRUE",
            "gym": get_stripped(row, "Gym") == "Included",
            "spa": get_stripped(row, "Spa") == "Included",
            "jacuzzi": get_stripped(row, "Jacuzzis") == "Included",
            "pool": get_stripped(row, "Pool") == "Included",
            "sauna": get_stripped(row, "Sauna") == "Included",
            "steamRoom": get_stripped(row, "Steam Room") == "Included",
            "massage": get_stripped(row, "Massage") == "Included",
            "elevator": get_stripped(row, "Elevator") == "TRUE",
            "laundry": get_stripped(row, "Laundry") == "Included",
            "roomService": get_stripped(row, "Room Service") == "TRUE"
        },
        "checkin": {
            "start": get_stripped(row, "Check in Time"),
            "end": "",
            "out": get_stripped(row, "Check Out Time")
        },
        "info": {
            "yearBuilt": safe_int(get_stripped(row, "Year Built")),
            "capacity": safe_int(get_stripped(row, "Capacity")),
        },
        "rooms": [
            # Note: Ship cabin data would need to be parsed from additional fields
            # This would require cabin-specific columns in your CSV
        ],
        "requirements": {
            "minimumAge": safe_int(get_stripped(row, "Minimum Age")),
        },
        "inspections": [
            {
                "inspectedBy": get_stripped(row, "Inspection 1 By"),
                "date": get_stripped(row, "Inspection 1 Date"),
                "notes": get_stripped(row, "Inspection 1 Notes")
            }
        ] if get_stripped(row, "Inspection 1 By") else []
    }

    # ===== Level 0 → Base schema (empty) =====
    level_0 = {}

    component_fields = [
        {"templateId": template_ids[2], "data": level_2},
        {"templateId": template_ids[1], "data": level_1},
        {"templateId": template_ids[0], "data": level_0},
    ]

    val = {
        "templateId": template_ids[2],
        "isBookable": False,
        "description": {
            "web": get_stripped(row, "Description") or "",
            "quote": get_stripped(row, "Description") or "",
            "final": get_stripped(row, "Description") or ""
        },
        "partners": [p.strip() for p in get_stripped(row, "Partners").split(",") if p.strip()],
        "regions": regions,
        "name": get_stripped(row, "Name") or "Untitled",
        "pricing": pricing,
        "media": media,
        "componentFields": component_fields,
        "package": {},
    }
    return val