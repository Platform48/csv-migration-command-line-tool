from utils import get_stripped, safe_float, safe_int, get_location_id
from .location import LOCATION_ALIASES, map_region_name_to_id
import pandas as pd
import re
from datetime import datetime


def parse_room_size(val: str):
    """
    Extract numeric size in m² from messy strings like '25m2 - 32m2', '16.5m3', etc.
    Always returns the first valid number as float, or None.
    """
    if not val or not isinstance(val, str):
        return None
    
    # Find all numbers (including decimals) followed by m + digit(s)
    match = re.findall(r"(\d+(?:\.\d+)?)m\d", val)
    if match:
        try:
            return float(match[0])  # Pick the first one if multiple
        except ValueError:
            return None
    return None

def map_ground_accommodation_component(row, template_ids, COMPONENT_ID_MAP, context=None, row_index=-1, rooms_data=None, partner_map=None, destination_override=None):
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
    images = [i.strip() for i in images if i.strip()]
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
        "type": get_stripped(row, "Type") or "Standard Hotel", 
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
            "start": get_stripped(row, "Check in Time") or "00:00:00",
            "end": "00:00:00",
            "out": get_stripped(row, "Check Out Time") or "00:00:00"
        },
        "info": {
            "yearBuilt": safe_int(get_stripped(row, "facts.yearBuilt")),
            "capacity": safe_int(get_stripped(row, "facts.capacity"))
        },
        "rooms": [],
        "requirements": {
            "minimumAge": safe_int(get_stripped(row, "minimumAge"))
        },
        "inspections": [],
        "whatWeLike":get_stripped(row, "whatWeLikeAboutThisAccommodation"),
        "thingsToNote":get_stripped(row, "thingsToNoteAboutThisAccommodation"),
        "recommendations":get_stripped(row, "Recommendations"),
        "swooper":"",
        "swoopSays":"",
        "importantInformation":"",
        "breakfastEndTime": get_stripped(row, "Breakfast End Time"),
        "boardBasis":{
            "breakfast":False,
            "lunch":False,
            "boxLunch":False,
            "dinner":False,
            "snacks":False
        }
    }


    for i in (1, 2):
        date = get_stripped(row, f"Date {i}")
        if not date:
            continue

        # Convert yyyy-mm-dd → dd-mm-yyyy
        try:
            parsed = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
            date_formatted = parsed.strftime("%Y-%m-%d")
        except ValueError:
            # Fallback if date is not in expected format
            date_formatted = date

        level_1["inspections"].append({
            "inspectedBy": get_stripped(row, f"Inspected by {i}") or "",
            "notes":       get_stripped(row, f"Inspection Notes {i}") or "",
            "date":        date_formatted
        })

        
    # --- Map Rooms from rooms_data ---
    if rooms_data is not None and not rooms_data.empty:
        accom_name = get_stripped(row, "name")
        matching_rooms = rooms_data[rooms_data["Hotel/Vessel"].str.strip() == accom_name]

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

            "name": str(r.get("Room/Cabin name") or "Unnamed Room"),
            "type": "Hotel" if "Room" in str(r.get("Room/Cabin name")) else "Cabin",
            "description": str(r.get("Room/Cabin description") or "")
        }

        level_1["rooms"].append(room_obj)

    # ===== Level 2 → Ground Accommodation =====
    level_2 = {
        "type": get_stripped(row, "Type") if get_stripped(row, "Type") in ['Luxury Lodge', 'Standard Hotel', 'Premium Hotel', 'Boutique Hotel', 'Premium Boutique Hotel', 'Refugio', 'Camping', 'Lodge', 'Glamping', 'Estancia'] else "Standard Hotel",
    }

    component_fields = [
        {"templateId": template_ids[2], "data": level_2},
        {"templateId": template_ids[1], "data": level_1},
        {"templateId": template_ids[0], "data": level_0},
    ]

    val = {
        "orgId":"swoop",
        "destination":(destination_override or get_stripped(row, "destination")).lower() or "patagonia",
        "state": "Draft",
        "tripId": "",
        "pricing": {"amount":0,"currency":"gbp"},
        "package": None,

        "templateId": template_ids[2],
        "isBookable": True,
        "description": {
            "web": get_stripped(row, "Description") or "",
            "quote": get_stripped(row, "Description") or "",
            "final": get_stripped(row, "Description") or ""
        },
        "partners": [
            partner_id
            for p in get_stripped(row, "Partner").split(",")
            if p.strip()
            for partner_id in [
                partner_map.get(
                    destination_override or get_stripped(row, "destination") or "Patagonia",
                    {}
                ).get(p.strip())
            ]
            if partner_id  # only include if found
        ],
        "regions": [r for r in regions if r],
        "name": get_stripped(row, "name") or "Untitled",
        "externalName": get_stripped(row, "name") or "Untitled",
        "media": media,
        "componentFields": component_fields,
    }
    return val
