import sys
import pandas as pd
import json
import uuid
from datetime import datetime, timedelta

with open("swoop.regions.json", "r", encoding="utf-8") as f:
    regions_data = json.load(f)

REGION_LOOKUP = {region["name"]: region["_id"] for region in regions_data}

REGION_ALIASES = {
    # Existing
    "Glaciares": "Los Glaciares",
    "Torres": "Torres del Paine",
    "Ruta40": "Ruta 40",
    "Welsh Patagonia and Ruta 40": "Ruta 40",
    "Iguazu": "Iguazú",
    "Jujuy": "Salta & Jujuy",
    "Península": "Peninsula",
    "Circle Region": "Circle",
    "Santiago Region": "Santiago",
    "Santiago and Central Chile": "Santiago",
    "Tierra del Fuego": "Tierra del Feugo",  # note: your master list has "Feugo"
    "Chilean Lakes": "Chilean Lake District",
    "Argentine Lakes": "Argentine Lake District",
    "Aysen": "Aysén",

    # Fix accents & variations
    "Peninsula Valdes": "Peninsula Valdés",
    "Valdes": "Peninsula Valdés",
    "Los Glaciares NP": "Los Glaciares",

    # Atacama variations
    "Atacama": "Atacama Desert",
    "San Pedro de Atacama": "Atacama Desert",

    # Combined names → map to dominant region
    "Aysen, Torres del Paine": "Aysén",
    "Aysen, Los Glaciares": "Aysén",
    "Argentine Lakes, Chilean Lakes": "Argentine Lake District",

    # Other common alternates
    "North Argentina": "Salta & Jujuy",
    "Patagonia": "Ruta 40",

    "Uyuni": "Atacama Desert",
    "Uruguay": "Buenos Aires",
    "Brazil": "Iguazú",
    "Antarctica": "Interior South Pole",
    "Tepuhueico Park": "Chilean Lake District",
}

LOCATION_ALIASES = {
    "El calafate": "El Calafate"
}

def map_region_name_to_id(region_name):
    if not region_name:
        return None
    region_name = region_name.strip()
    canonical = REGION_ALIASES.get(region_name, region_name)
    region_id = REGION_LOOKUP.get(canonical)
    if not region_id:
        print(f"❌ ERROR: Region '{region_name}' (canonical: '{canonical}') not found in REGION_LOOKUP.")
    return region_id

def map_location_component(row, template_ids, COMPONENT_ID_MAP, context=None, row_index=-1):

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



    # --- Location schema fields ---
    raw_type = get_stripped("type")
    type_value = raw_type if raw_type in ALLOWED_TYPES else "Other"

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

    # --- Regions ---
    regions = [r for r in [map_region_name_to_id(get_stripped("regions"))] if r]

    # --- Pricing ---
    price_val = None
    if pd.notna(row.get("price")):
        try:
            price_val = int(float(row.get("price")))
        except (ValueError, TypeError):
            price_val = None

    pricing = {}
    if price_val:
        pricing = {
            "amount": price_val,
            "currency": get_stripped("currency") or "USD"
        }
    
    images = get_stripped("images").split("\n")
    for i in images:
        i = i.strip()

    media = {
        "images": images,
        "videos": []
    }

    # --- Component fields ---
    component_fields = [
        {"templateId": template_ids[1], "data": level_1},
        {"templateId": template_ids[0], "data": {}},
    ]

    # --- Final object ---
    return {
        "templateId": template_ids[1],
        "description":{
            "web":get_stripped("description") or "",
            "quote":get_stripped("description") or "",
            "final":get_stripped("description") or ""
        },
        "partners": [p.strip() for p in get_stripped("partners").split(",") if p.strip()],
        "regions": regions,
        "name": get_stripped("name") or "Untitled",
        "pricing": pricing,
        "media": media,
        "componentFields": component_fields,
        "package": {},
    }
