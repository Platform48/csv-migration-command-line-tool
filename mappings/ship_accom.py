from utils import get_stripped, safe_float, safe_int
from .location import map_region_name_to_id
import pandas as pd
import re

def parse_room_size(val: str):
    """
    Extract numeric size in m² from messy strings like '25m2 - 32m2', '16.5m3', etc.
    Always returns the first valid number as float, or None.
    """
    if not val or not isinstance(val, str):
        return None
    
    match = re.findall(r"(\d+(?:\.\d+)?)m\d", val)
    if match:
        try:
            return float(match[0])
        except ValueError:
            return None
    return None


def map_ship_accommodation_component(row, template_ids, COMPONENT_ID_MAP, context=None, row_index=-1, rooms_data=None, partner_map=None, destination_override=None):
    """
    Map ship accommodation component, including cabin details from Rooms Cabins sheet
    """

    # --- Regions ---
    raw_region = get_stripped(row, "Region")
    primary_region_id = map_region_name_to_id(raw_region)
    regions = [primary_region_id] if primary_region_id else []
    
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

    # ===== Level 0 → Base schema (empty) =====
    level_0 = {}

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
            "start": get_stripped(row, "Check in Time") or "00:00:00",
            "end": "00:00:00",
            "out": get_stripped(row, "Check Out Time") or "00:00:00"
        },
        "info": {
            "yearBuilt": safe_int(get_stripped(row, "Year Built")),
            "capacity": safe_int(get_stripped(row, "Capacity")),
        },
        "rooms": [],
        "requirements": {
            "minimumAge": safe_int(get_stripped(row, "Minimum Age")),
        },
        "inspections": [
            {
                "inspectedBy": get_stripped(row, "Inspection 1 By"),
                # "date": get_stripped(row, "Inspection 1 Date") or "01-01-2000",
                "notes": get_stripped(row, "Inspection 1 Notes")
            }
        ] if get_stripped(row, "Inspection 1 By") else [],
        
        "whatWeLike":get_stripped(row, "What we like about this hotel"),
        "thingsToNote":get_stripped(row, "We think its worth noting"),
        "swoopSays":get_stripped(row, "Swoop Says"),

        "breakfastEndTime": "",
        "boardBasis":{
            "breakfast": "Breakfast" in get_stripped(row, "Meals Available") or "Breakfast" in get_stripped(row, "Board Basis"),
            "lunch":     "Lunch"     in get_stripped(row, "Meals Available") or "Lunch"     in get_stripped(row, "Board Basis"),
            "boxLunch":  "Box Lunch" in get_stripped(row, "Meals Available") or "Box Lunch" in get_stripped(row, "Board Basis"),
            "dinner":    "Dinner"    in get_stripped(row, "Meals Available") or "Dinner"    in get_stripped(row, "Board Basis"),
            "snacks":    "Snacks"    in get_stripped(row, "Meals Available") or "Snacks"    in get_stripped(row, "Board Basis"),
        },
        "guestToStaffRatio":""
    }

    # --- Map Cabins from rooms_data ---
    if rooms_data is not None and not rooms_data.empty:
        vessel_name = get_stripped(row, "Name")
        matching_rooms = rooms_data[rooms_data["Hotel/Vessel"].str.strip() == vessel_name]

        VALID_BEDS = {"Other","Double","Twin","Bunk Bed","Single","Triple","Quad","Quintuple"}
        VALID_BATHROOMS = {
            "En-suite","Shower","Sink","Shared Bathroom","WC",
            "Wetroom","Toilet","Composting Toilet","Combi Bath/Shower"
        }

        for _, r in matching_rooms.iterrows():
            room_obj = {
                "sizem2": parse_room_size(str(r.get("Size"))) or -1.0,
                "bedConfigurations": [
                    b.strip()
                    for b in str(r.get("Bed Configurations") if pd.notna(r.get("Bed Configurations")) else "").split(",")
                    if b.strip() in VALID_BEDS
                ],
                "bathroomConfigurations": [
                    b.strip()
                    for b in str(r.get("Bathroom Type") if pd.notna(r.get("Bathroom Type")) else "").split(",")
                    if b.strip() in VALID_BATHROOMS
                ],
                "energyType": (
                    ["Renewable"] if str(r.get("Do they use renewable energy?")).strip().lower() == "yes"
                    else ["Non-renewable"]
                ),
                "name": str(r.get("Room/Cabin name") or "Unnamed Cabin"),
                "type": "Cabin" if "Cabin" in str(r.get("Room/Cabin name")) else "Hotel",
                "description": str(r.get("Room/Cabin description") or "")
            }
            level_1["rooms"].append(room_obj)

    # ===== Level 2 → Ship Accommodation =====
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

    component_fields = [
        {"templateId": template_ids[2], "data": level_2},
        {"templateId": template_ids[1], "data": level_1},
        {"templateId": template_ids[0], "data": level_0},
    ]

    val = {
        "orgId":"swoop",
        "destination":(destination_override or get_stripped(row, "Destination")).lower() or "patagonia",
        "state": "Draft",
        "tripId": "",
        "pricing": {"amount":0,"currency":"gbp"},
        "package": None,

        "templateId": template_ids[2],
        "isBookable": False,
        "description": {
            "web": get_stripped(row, "Description") or get_stripped(row, "Ship overview (for Ship page)") or "",
            "quote": get_stripped(row, "Description") or get_stripped(row, "Ship overview (for Ship page)") or "",
            "final": get_stripped(row, "Description") or get_stripped(row, "Ship overview (for Ship page)") or "",
        },
        "partners": [
            partner_id
            for p in get_stripped(row, "Partners").split(",")
            if p.strip()
            for partner_id in [
                partner_map.get(
                    destination_override or get_stripped(row, "Destination") or "Patagonia",
                    {}
                ).get(p.strip())
            ]
            if partner_id  # only include if found
        ],
        "regions": regions,
        "name": get_stripped(row, "Name") or "Untitled",
        "externalName": get_stripped(row, "Name") or "Untitled",
        "media": media,
        "componentFields": component_fields,
    }
    return val
