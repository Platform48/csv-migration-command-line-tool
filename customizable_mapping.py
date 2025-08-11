import pandas as pd
import json

# 👇 Load JSON and build REGION_LOOKUP dynamically
with open("swoop.regions.json", "r", encoding="utf-8") as f:
    regions_data = json.load(f)

REGION_LOOKUP = {
    region["name"]: region["_id"]
    for region in regions_data
}

# 👇 Optional: aliases for common variations/misspellings
REGION_ALIASES = {
    "Glaciares": "Los Glaciares",
    "Torres": "Torres del Paine",
    "Ruta40": "Ruta 40",
    "Iguazu": "Iguazú",
    "Jujuy": "Salta & Jujuy",
    "Península": "Peninsula",
    "Circle Region": "Circle",
    "Santiago Region": "Santiago",
    # Add more as needed
}

UNMAPPED_REGIONS = set()

def map_location_component(row, template_ids):

    def get_stripped(field):
        val = row.get(field)
        if pd.isna(val):
            return ""
        return str(val).strip()

    def map_region_name_to_id(region_name):
        if not region_name:
            return None
        region_name = region_name.strip()

        # Try alias first
        canonical = REGION_ALIASES.get(region_name, region_name)
        region_id = REGION_LOOKUP.get(canonical)

        if not region_id:
            UNMAPPED_REGIONS.add(region_name)

        return region_id

    images = [
        get_stripped(col)
        for col in ["Image 1", "Image 2", "Image 3", "Image 4", "Image 5"]
        if get_stripped(col)
    ]

    # Map primary region
    raw_region = get_stripped("Region")
    region_id = map_region_name_to_id(raw_region)

    level_0 = {
        "description": get_stripped("Description") or "Description unavailable",
        "descriptionwithHtml": get_stripped("Description (with html)"),
        "overrideUrl": get_stripped("Override url"),
        "type": get_stripped("Type"),
        "nEWCUSTOMADDRESSWHAT3WORDS": get_stripped("NEW CUSTOM ADDRESS/WHAT3WORDS"),
        "latitude": row.get("Latitude"),
        "longitude": row.get("Longitude"),
        "images": images,
        "componentName": get_stripped("Name"),
    }

    level_1 = {}

    details = {
        "regions": [region_id],
        "price": None,
        "currency": None
    }

    for reg_field in ["Region", "Region 2"]:
        raw_val = get_stripped(reg_field)
        mapped_id = map_region_name_to_id(raw_val)
        if mapped_id:
            details["regions"].append(mapped_id)

    if pd.notna(row.get("Price")):
        try:
            details["price"] = int(row.get("Price"))
        except:
            pass

    if pd.notna(row.get("Currency")):
        details["currency"] = get_stripped("Currency")


    component_fields = [
        {"templateId": template_ids[0], "data": level_0},
        {"templateId": template_ids[1], "data": level_1}
    ]

    return {
        "orgId": None,
        "templateId": template_ids[0],
        "revisionGroupId": None,
        "state": "unpublished",
        "name": get_stripped("Name"),
        "componentFields": component_fields,
        "partners": [],
        "startDate": None,
        "endDate": None,
        "duration": None,
        "details": details,
        "bundle": {},
    }


def map_ground_accommodation_component(row, template_ids):

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

    # ===== Level 0 → Schema 1 (Accommodation details) =====
    level_0 = {
        "description": get_stripped("Description") or "Description unavailable",
        "region": region_id,
        "images": images,
        "facilities": {
            "library": get_stripped("Library").lower() == "true",
            "shop": get_stripped("Shop").lower() == "true",
            "restaurant": get_stripped("Restaurant").lower() == "true",
            "additionalRestaurants": get_stripped("Additional Restaurants").lower() == "true",
            "bar": get_stripped("Bar").lower() == "true",
            "gym": get_stripped("Gym") or "No",
            "spa": get_stripped("Spa") or "No",
            "jacuzzi": get_stripped("Jacuzzi") or "No",
            "pool": get_stripped("Pool") or "No",
            "sauna": get_stripped("Sauna") or "No",
            "steamRoom": get_stripped("Steam Room") if get_stripped("Steam Room") in ['Included', 'Extra Cost'] else "No",
            "massage": get_stripped("Massage") or "No",
            "elevator": get_stripped("Elevator").lower() == "true",
            "laundry": get_stripped("Laundry") or "No",
            "roomService": get_stripped("Room Service").lower() == "true"
        },
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
            "wiFi": get_stripped("WiFi").lower() == "true"
        },
        "facts": {
            "capacity": int(row.get("Capacity")) if pd.notna(row.get("Capacity")) else None,
            "yearBuilt": int(row.get("Year Built")) if pd.notna(row.get("Year Built")) else None
        },
        "inspections": [
            {
                "inspectedBy": get_stripped("Inspected by 1"),
                "date": get_stripped("Date 1"),
                "inspectionNotes": get_stripped("Inspection Notes 1")
            }
        ] if get_stripped("Inspected by 1") else [],
        "swoopSays": get_stripped("What they say about this accommodation"),
        "video": get_stripped("Video URL"),
        "minimumAge": int(row.get("Minimum Age")) if pd.notna(row.get("Minimum Age")) else -1,
        "emissionsPerNightPerPerson": None,
        "importantInfo": None,
        "whatWeLikeAboutThisAccommodation": get_stripped("What we like about this hotel"),
        "thingsToNoteAboutThisAccommodation": get_stripped("We think its worth noting"),
        "hintsTips": get_stripped("Recommendations")
    }

    # ===== Level 1 → Schema 2 (Location/type/check-in) =====
    level_1 = {
        "location": get_stripped("Location\n(Ground Accommodation only)"),
        "type": get_stripped("Type"),
        "checkinTime": {
            "checkinStart": get_stripped("Check in Time"),
            "checkinCloses": None,  # You could parse from "(with ranges)" if provided
            "checkoutTime": get_stripped("Check Out Time")
        }
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


def map_ship_accommodation_component(row, template_ids):

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