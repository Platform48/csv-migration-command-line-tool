import pandas as pd
import json

with open("swoop.regions.json", "r", encoding="utf-8") as f:
    regions_data = json.load(f)

REGION_LOOKUP = {
    region["name"]: region["_id"]
    for region in regions_data
}

REGION_ALIASES = {
    "Glaciares": "Los Glaciares",
    "Torres": "Torres del Paine",
    "Ruta40": "Ruta 40",
    "Iguazu": "Iguazú",
    "Jujuy": "Salta & Jujuy",
    "Península": "Peninsula",
    "Circle Region": "Circle",
    "Santiago Region": "Santiago",
}

UNMAPPED_REGIONS = set()

def map_ship_accommodation_component(row, template_ids, COMPONENT_ID_MAP):

    def get_stripped(field):
        val = row.get(field)
        if pd.isna(val):
            return ""
        return str(val).strip()

    def map_region_name_to_id(region_name):
        if not region_name:
            return None
        region_name = region_name.strip()
        canonical = REGION_ALIASES.get(region_name, region_name)
        region_id = REGION_LOOKUP.get(canonical)
        if not region_id:
            UNMAPPED_REGIONS.add(region_name)
        return region_id

    # Images
    images = [
        get_stripped(col)
        for col in ["Image 1", "Image 2", "Image 3", "Image 4", "Image 5"]
        if get_stripped(col)
    ]

    # Map primary region
    raw_region = get_stripped("Region")
    region_id = map_region_name_to_id(raw_region)

    # ===== Level 0 Ship =====
    level_0 = {
        "deckPlan": get_stripped("Deck Plan"),
        "shipFacilities": {
            "observationLounge": get_stripped("Observation Lounge").lower() == "true",
            "mudroom": get_stripped("Mudroom").lower() == "true",
            "walkingTrackWraparoundDeck": get_stripped("Walking Track/Wraparound Deck").lower() == "true",
            "openBridgePolicy": get_stripped("Open Bridge Policy").lower() == "true",
            "igloos": get_stripped("Igloos").lower() == "true",
            "scienceCentreLaboratory": get_stripped("Science Centre/Laboratory").lower() == "true"
        },
        "type": get_stripped("Type") or 'Basic'
    }


    # ===== Level 1 Accommodation =====
    level_1 = {
        "description": get_stripped("Description") or "Description unavailable",
        "region": region_id,
        "images": images,
        "facilities": {
            "library": get_stripped("Library").lower() == "true",
            "shop": get_stripped("Shop").lower() == "true",
            "restaurant": get_stripped("Restaurant").lower() == "true",
            "additionalRestaurants": get_stripped("Additional restaurant").lower() == "true",
            "bar": get_stripped("Bar").lower() == "true",
            "gym": get_stripped("Gym")  if get_stripped("Gym") in ['Included', 'Additional Cost'] else "No",
            "spa": get_stripped("Spa") if get_stripped("Spa") in ['Included', 'Additional Cost'] else "No",
            "jacuzzi": get_stripped("Jacuzzis") if get_stripped("Jacuzzis") in ['Included', 'Additional Cost'] else "No",
            "pool": get_stripped("Pool") if get_stripped("Pool") in ['Included', 'Additional Cost'] else "No",
            "sauna": get_stripped("Sauna") if get_stripped("Sauna") in ['Included', 'Additional Cost'] else "No",
            "steamRoom": get_stripped("Steam Room") if get_stripped("Steam Room") in ['Included', 'Extra Cost'] else "No",
            "massage": get_stripped("Massage") if get_stripped("Massage") in ['Included', 'Additional Cost'] else "No",
            "elevator": get_stripped("Elevator").lower() == "true",
            "laundry": get_stripped("Laundry") if get_stripped("Laundry") in ['Included', 'Extra Cost'] else "No",
            "roomService": get_stripped("Room Service").lower() == "true"
        },
        "roomsCabinCategories": [
            # Note: Ship cabin data would need to be parsed from additional fields
            # This would require cabin-specific columns in your CSV
        ],
        "foodDrink": {
            "foodDrinkDescription": get_stripped("Food & Drink Description"),
            "boardBasis": {
                "bB": False,
                "halfBoard": False,
                "fullBoard": False,
                "allInclusive": False
            }
        },
        "connectivity": {
            "wiFi": get_stripped("Wifi").lower() == "true"
        },
        "facts": {
            "capacity": int(row.get("Capacity")) if pd.notna(row.get("Capacity")) else -1,
            "yearBuilt": int(row.get("Year Built")) if pd.notna(row.get("Year Built")) else None
        },
        "inspections": [
            {
                "inspectedBy": get_stripped("Inspection 1 By"),
                "date": get_stripped("Inspection 1 Date"),
                "inspectionNotes": get_stripped("Inspection 1 Notes")
            }
        ] if get_stripped("Inspection 1 By") else [],
        "swoopSays": get_stripped("Swoop Says"),
        "video": get_stripped("Video"),
        "minimumAge": int(row.get("Minimum Age")) if pd.notna(row.get("Minimum Age")) else -1,
        "emissionsPerNightPerPerson": -1,
        "importantInfo": None,
        "whatWeLikeAboutThisAccommodation": get_stripped("What we like about this hotel"),
        "thingsToNoteAboutThisAccommodation": get_stripped("We think its worth noting"),
        "hintsTips": get_stripped("Recommendations")
    }


    # ===== Level 2 (empty base schema) =====
    level_2 = {}

    # ===== Details =====
    details = {
        "regions": [region_id] if region_id else [],
        "price": int(row.get("Price")) if pd.notna(row.get("Price")) else None,
        "currency": get_stripped("Currency") if pd.notna(row.get("Currency")) else None
    }

    # Append additional mapped regions if present
    for reg_field in ["Region", "Region 2"]:
        raw_val = get_stripped(reg_field)
        mapped_id = map_region_name_to_id(raw_val)
        if mapped_id and mapped_id not in details["regions"]:
            details["regions"].append(mapped_id)

    component_fields = [
        {"templateId": template_ids[0], "data": level_0},
        {"templateId": template_ids[1], "data": level_1},
        {"templateId": template_ids[2], "data": level_2}
    ]

    return {
        "orgId": None,
        "templateId": template_ids[0],
        "revisionGroupId": None,
        "state": "unpublished",
        "name": get_stripped("Name"),
        "componentFields": component_fields,
        "partners": [
            p.strip()
            for p in get_stripped("Partners").split(",")
            if p.strip()
        ] if get_stripped("Partners") else [],
        "startDate": None,
        "endDate": None,
        "duration": None,
        "details": details,
        "bundle": {}
    }


import re
import pandas as pd

def map_cruise_bundle(row, template_ids, COMPONENT_ID_MAP):
    def get_stripped(field):
        """Return trimmed string or empty if NaN."""
        val = row.get(field)
        if pd.isna(val):
            return ""
        return str(val).strip()

    def parse_staff_guest_ratio(value):
        """Convert ratio 'a:b' into a/(a+b) or return float if numeric."""
        if not value or pd.isna(value):
            return None
        if isinstance(value, (int, float)):  # Already numeric
            return float(value)
        parts = str(value).split(":")
        if len(parts) == 2:
            try:
                a = float(parts[0].strip())
                b = float(parts[1].strip())
                return a / (a + b) if (a + b) != 0 else None
            except ValueError:
                return None
        try:
            return float(value)
        except ValueError:
            return None

    # ===== Level 0: Cruise =====
    level_0 = {
        "emmissionsPerDay": float(row.get("Emmissions per day")) if pd.notna(row.get("Emmissions per day")) else -1,
        "ship": get_stripped("Ship"),
        "staffGuestRatio": parse_staff_guest_ratio(row.get("Expedition Staff: Guest Ratio")) if pd.notna(row.get("Expedition Staff: Guest Ratio")) else -1,
        "itineraryType": "Flexible Itinerary" if "to" in str(get_stripped("Day")) else "Fixed Itinerary",
        "flexibleItineraryContent": {
            "embarkationDay": int(row.get("Embarkation Day")) if pd.notna(row.get("Embarkation Day")) else -1,
            "embarkationDescription": get_stripped("Day 1"),
            "voyageDescription": get_stripped("Day Description - Web"),
            "disembarkationDescription": get_stripped("Day 7"),
            "disembarkationDay": int(row.get("Disembarkation Day")) if pd.notna(row.get("Disembarkation Day")) else -1
        }
    }

    # ===== Level 1: Package =====
    level_1 = {
        "description": get_stripped("Cruise Description"),
        "durationdays": int(row.get("Duration (Days)")) if pd.notna(row.get("Duration (Days)")) else -1,
        "restrictions": {
            "minimumAge": int(row.get("Min age")) if pd.notna(row.get("Min age")) else -1,
            "maximumAge": int(row.get("Max age")) if pd.notna(row.get("Max age")) else -1,
            "oKWhenPregnant": get_stripped("Ok when pregnant?").lower() == "true",
            "wheelchairAccess": get_stripped("Wheelchair access?").lower() == "true",
            "upperWeightLimitkg": float(row.get("Upper Weight Limit (kg)")) if pd.notna(row.get("Upper Weight Limit (kg)")) else -1,
            "upperHeightLimitm": float(row.get("Upper Height Limit (m)")) if pd.notna(row.get("Upper Height Limit (m)")) else -1,
            "lowerHeightLimitm": float(row.get("Lower Height Limit (m)")) if pd.notna(row.get("Lower Height Limit (m)")) else -1,
            "oKWithBreathingMachines": get_stripped("OK with Breathing machines?").lower() == "true",
            "suitedForVisuallyImpaired": get_stripped("Suited for visually impaired").lower() == "true"
        },
        "inclusions": {
            "guided": get_stripped("Guided?").lower() == "true",
            "drinks": get_stripped("Drinks"),
            "complimentaryGifts": get_stripped("Complimentary gifts?"),
            "nationalParkFee": get_stripped("National Park fee") or 'Excluded ',
            "other": [x.strip() for x in get_stripped("Other").split(",") if x.strip()] if get_stripped("Other") else []
        },
        "requiredGear": [x.strip() for x in get_stripped("Required gear (comma-list)").split(",") if x.strip()] if get_stripped("Required gear (comma-list)") else [],
        "difficulty": get_stripped("Difficulty") or 'Moderate',
    }

    # ===== Level 2: Base =====
    level_2 = {}

    # ===== Build Bundle =====
    bundle_days = []

    # Find all matching columns dynamically, preserving order
    day_columns = [col for col in row.index if col.startswith("Day")]
    title_columns = [col for col in row.index if col.startswith("Day Title")]
    desc_columns = [col for col in row.index if col.startswith("Day Description - Web")]

    # Sort them in case deduplication added .1, .2, etc.
    day_columns.sort(key=lambda x: (len(x), x))
    title_columns.sort(key=lambda x: (len(x), x))
    desc_columns.sort(key=lambda x: (len(x), x))

    for i, day_col in enumerate(day_columns):
        day_val = get_stripped(day_col)
        if not day_val:
            continue

        # Handle simple formats: "3" or "3-6"
        start_day, end_day = None, None
        day_val_clean = get_stripped(day_columns[i]) if i < len(day_columns) else ""

        if "-" in day_val_clean:
            try:
                parts = [int(p.strip()) for p in day_val_clean.split("-")]
                if len(parts) == 2:
                    start_day, end_day = parts
            except ValueError:
                pass
        elif day_val_clean.isdigit():
            start_day = end_day = int(day_val_clean)

        title_val = get_stripped(title_columns[i]) if i < len(title_columns) else ""
        desc_val = get_stripped(desc_columns[i]) if i < len(desc_columns) else ""


        # Get component items for this day
        items = []
        comp_index = 1
        while True:
            comp_name_field = f"Component {comp_index}"
            comp_type_field = f"Component Type {comp_index}"
            if comp_name_field not in row:
                break
            comp_name = get_stripped(comp_name_field)
            comp_type = get_stripped(comp_type_field)
            if comp_name:
                comp_id = COMPONENT_ID_MAP.get((comp_type, comp_name))
                items.append({
                    "componentId": comp_id if comp_id else f"MISSING: {comp_type} {comp_name}",
                    "allDay": True,
                    "startTime": None,
                    "endTime": None
                })
            comp_index += 1

        bundle_days.append({
            "title": title_val,
            "description": desc_val,
            "items": items,
            "startDay": start_day if start_day else -1,
            "endDay": end_day if end_day else -1
        })

    bundle = {
        "days": bundle_days,
        "title": get_stripped("Name"),
        "description": get_stripped("Cruise Description"),
        "startTime": None,
        "endTime": None
    }

    component_fields = [
        {"templateId": template_ids[0], "data": level_0},
        {"templateId": template_ids[1], "data": level_1},
        {"templateId": template_ids[2], "data": level_2}
    ]

    return {
        "orgId": None,
        "templateId": template_ids[0],
        "revisionGroupId": None,
        "state": "unpublished",
        "name": get_stripped("Name"),
        "componentFields": component_fields,
        "partners": [p.strip() for p in get_stripped("Partner").split(",") if p.strip()] if get_stripped("Partner") else [],
        "startDate": None,
        "endDate": None,
        "duration": int(row.get("Duration (Days)")) if pd.notna(row.get("Duration (Days)")) else None,
        "details": {},
        "bundle": bundle
    }
