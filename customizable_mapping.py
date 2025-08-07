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

# 👇 Track any non-matching region names
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
        "region": region_id,
    }

    level_1 = {}

    details = {
        "regions": [],
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

    start_date = get_stripped("StartDate")
    end_date = get_stripped("EndDate")

    duration = None
    if pd.notna(row.get("Duration")):
        try:
            duration = int(row.get("Duration"))
        except:
            pass

    state = get_stripped("State")

    component_fields = [
        {"templateId": template_ids[0], "data": level_0},
        {"templateId": template_ids[1], "data": level_1}
    ]

    return {
        "orgId": None,
        "templateId": template_ids[0],
        "revisionGroupId": None,
        "revision": 0,
        "name": get_stripped("Name"),
        "componentFields": component_fields,
        "partners": [],
        "startDate": start_date,
        "endDate": end_date,
        "duration": duration,
        "details": details,
        "bundle": {},
        "state": state
    }
