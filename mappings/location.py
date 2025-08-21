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

def map_location_component(row, template_ids, COMPONENT_ID_MAP):

    ALLOWED_TYPES = {
        "Other","Airport","Apartments","Bay","Bridge","Campsite","City","Estancia",
        "Fjord","Glacier","Glamping ","Site","Hotel","Island","Lake","Landing Site",
        "Lighthouse","Lodge","Mountain","Mountain Pass","National Park","Peninsula",
        "Port","Refugio","River","Town","Trailhead","Valley","Viewpoint","Vineyard",
        "Village","Volcano","Waterfall","Wildlife","Winery"
    }

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

    # --- Location schema fields ---
    raw_type = get_stripped("type")
    type_value = raw_type if raw_type in ALLOWED_TYPES else "Other"  # default to "Other"

    def safe_float(val):
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    latitude = safe_float(row.get("latitude"))
    longitude = safe_float(row.get("longitude"))

    level_1 = {
        "type": type_value,
        "latitude": latitude if latitude is not None else 0.0, 
        "longitude": longitude if longitude is not None else 0.0,
        "whatThreeWords": get_stripped("NEWCUSTOMADDRESSWHAT3WORDS") or "",
    }

    # --- Extra details ---
    price_val = None
    if pd.notna(row.get("price")):
        try:
            price_val = int(float(row.get("price")))
        except (ValueError, TypeError):
            price_val = None

    details = {
        "regions": [r for r in [map_region_name_to_id(get_stripped("regions"))] if r],
        "price": price_val,
        "currency": get_stripped("currency") if pd.notna(row.get("currency")) else None
    }

    # --- Media ---
    images_raw = get_stripped("images")
    images = [img.strip() for img in images_raw.split(',') if img.strip()] if images_raw else []

    # --- Component fields ---
    component_fields = [
        {"templateId": template_ids[1], "data": level_1},
        {"templateId": template_ids[0], "data": {}},
    ]

    # --- Final object ---
    return {
        "name": get_stripped("name") or "Untitled",
        "partners": [],
        "destination": get_stripped("destination") or "Unknown",
        "description": {
            "web":    get_stripped("descriptionWithHtml"),
            "quote":  get_stripped("description"),
            "booked": get_stripped("description")
        },
        "media": {
            "images": images,
            "videos": []
        },
        "requirements": {
            "minimumAge": -1
        },
        "details": details,
        "componentFields": component_fields,
        "templateId": template_ids[0],
        "state": "unpublished",
        "startDate": None,
        "endDate": None,
        "duration": None,
        "bundle": {},
    }