import math
import sys
import pandas as pd
import json
import uuid
from datetime import datetime, timedelta

from utils import get_stripped, safe_float

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
    # "El calafate": "El Calafate"
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

def map_location_component(row, template_ids, COMPONENT_ID_MAP, context=None, row_index=-1, rooms_data=None, partner_map=None, destination_override=None):

    ALLOWED_TYPES = {
        "Other","Airport","Apartments","Bay","Bridge","Campsite","City","Estancia",
        "Fjord","Glacier","Glamping ","Site","Hotel","Island","Lake","Landing Site",
        "Lighthouse","Lodge","Mountain","Mountain Pass","National Park","Peninsula",
        "Port","Refugio","River","Town","Trailhead","Valley","Viewpoint","Vineyard",
        "Village","Volcano","Waterfall","Wildlife","Winery"
    }


    # --- Location schema fields ---
    raw_type = get_stripped(row, "type")
    type_value = raw_type if raw_type in ALLOWED_TYPES else "Other"


    latitude = safe_float(row.get("latitude")) or 0.0
    longitude = safe_float(row.get("longitude")) or 0.0

    level_1 = {
        "type": type_value,
        "latitude": latitude,
        "longitude": longitude,
        "whatThreeWords": get_stripped(row, "NEWCUSTOMADDRESSWHAT3WORDS") or "",
    }

    # --- Regions ---
    regions = [r for r in [map_region_name_to_id(get_stripped(row, "regions"))] if r]

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
            "currency": get_stripped(row, "currency") or "USD"
        }
    
    images = get_stripped(row, "images").split("\n")
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
        
        "orgId":"swoop",
        "destination":(destination_override or get_stripped(row, "destination")).lower() or "patagonia",
        "state": "Draft",
        "tripId": "",
        "pricing": {"amount":0,"currency":"gbp"},

        "templateId": template_ids[1],
        "isBookable": False,
        "description":{
            "web":get_stripped(row, "description") or "",
            "quote":get_stripped(row, "description") or "",
            "final":get_stripped(row, "description") or ""
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
        "regions": regions,
        "name": get_stripped(row, "name") or "Untitled",
        "externalName": get_stripped(row, "name") or "Untitled",
        "media": media,
        "componentFields": component_fields,
        "package": None,
    }
