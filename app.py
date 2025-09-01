import os
import json
import pandas as pd
from datetime import datetime
import hashlib

from mappings.activity import map_activity_component
from mappings.cruise_pkg import map_cruise_bundle
from mappings.location import map_location_component
from mappings.ground_accom import map_ground_accommodation_component
from mappings.ship_accom import map_ship_accommodation_component
from mappings.journey import map_journey_component
from mappings.tranfer import map_transfer_component
from mappings.excursions import map_excursion_component
from mappings.private_tours import map_private_tours_component
from mappings.all_inclusive_hotels import map_all_inclusive_hotels_component

from validate_csv_dynamic import validate_csv
from core_data_services import CoreDataService
from utils import save_missing_references_log, clear_missing_references_session, get_missing_references_summary


DEBUG_MODE = False  # toggle this to switch between dry-run and real upload
FORCE_REUPLOAD = True

DEBUG_OUTPUT_FILE = "debug_output.ndjson"
COMPONENT_CACHE_FILE = "component_id_cache.json"

# Global map for later reference: (templateId, name) -> componentId
COMPONENT_ID_MAP = {}

# New: Component hash cache to detect changes
COMPONENT_HASH_CACHE = {}

SHEET_TEMPLATE_MAP = {
    "Location": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_0c105b25350647b096753b4f863ab06c", # Location
    ],
    "Journeys": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_14cc18c1408a4b73a16d4e1dad2efca9", # Journeys
    ],
    "Ground Accom": [
        "template_aca16a46ec3842ca85d182ee9348f627",  # Base
        "template_c265e31c0c2848fa8210050f452d3926",  # Accom
        "template_d32b51f46e7946faa5d3e2aa33e7d29a",  # Gy
    ],
    "All Activities - For Upload": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_e2f0e9e5343349358037a0564a3366a0"  # Activity
    ],
    "All Transfers - For Upload": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_901d40ac12214820995880915c5b62f5"
    ],
    "Copy of Excursions Package": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_3b7714dcfa374cd19b9dc97af1510204"  # Pkg
    ],
    "Copy of Private Tours Package": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_3b7714dcfa374cd19b9dc97af1510204"  # Pkg
    ],
    "Copy of All Inclusive Hotel Pac": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_3b7714dcfa374cd19b9dc97af1510204"  # Pkg
    ]
}

SHEET_ROW_MAPPERS = {
    "Location"                     : map_location_component,
    "Ground Accom"                 : map_ground_accommodation_component,
    "Journeys"                     : map_journey_component,
    "All Activities - For Upload"  : map_activity_component,
    "All Transfers - For Upload"   : map_transfer_component,
    "Copy of Excursions Package"   : map_excursion_component,
    "Copy of Private Tours Package": map_private_tours_component,
    "Copy of All Inclusive Hotel Pac": map_all_inclusive_hotels_component

}

TEMPLATE_TYPES = {
    "template_0c105b25350647b096753b4f863ab06c": "location",
    "template_c265e31c0c2848fa8210050f452d3926": "accommodation",
    "template_d32b51f46e7946faa5d3e2aa33e7d29a": "ground_accommodation",
    "template_14cc18c1408a4b73a16d4e1dad2efca9": "journey",
    "template_e2f0e9e5343349358037a0564a3366a0": "activity",
    "template_901d40ac12214820995880915c5b62f5": "transfer",
    "template_3b7714dcfa374cd19b9dc97af1510204": "package",
}

PAT_COMPONENTS_PATH = "pat_components.xlsx"
ANT_COMPONENTS_PATH = "ant_components.xlsx"
COMPONENTS_PATH = PAT_COMPONENTS_PATH

SHEET_PROCESS_ORDER = [
    # "Location",
    # "Ground Accom",
    # "Journeys",
    # "All Activities - For Upload",
    # "All Transfers - For Upload",
    "Copy of Excursions Package",
    "Copy of Private Tours Package",
    "Copy of All Inclusive Hotel Pac"
]

def load_component_cache():
    """Load existing component ID mappings from cache file"""
    global COMPONENT_ID_MAP, COMPONENT_HASH_CACHE
    
    if os.path.exists(COMPONENT_CACHE_FILE):
        try:
            with open(COMPONENT_CACHE_FILE, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                COMPONENT_ID_MAP = cache_data.get('component_map', {})
                COMPONENT_HASH_CACHE = cache_data.get('hash_cache', {})
                
                # Convert string keys back to tuples for component map
                COMPONENT_ID_MAP = {eval(k) if k.startswith('(') else k: v 
                                   for k, v in COMPONENT_ID_MAP.items()}
                
                print(f"📥 Loaded {len(COMPONENT_ID_MAP)} component mappings from cache")
                
        except Exception as e:
            print(f"⚠️ Error loading cache: {e}. Starting with empty cache.")
            COMPONENT_ID_MAP = {}
            COMPONENT_HASH_CACHE = {}
    else:
        print("📝 No cache file found. Starting with empty cache.")


def save_component_cache():
    """Save current component ID mappings to cache file"""
    try:
        # Convert tuple keys to strings for JSON serialization
        serializable_map = {str(k): v for k, v in COMPONENT_ID_MAP.items()}
        
        cache_data = {
            'component_map': serializable_map,
            'hash_cache': COMPONENT_HASH_CACHE,
            'last_updated': datetime.now().isoformat(),
            'total_components': len(COMPONENT_ID_MAP)
        }
        
        with open(COMPONENT_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, indent=2, ensure_ascii=False)
            
        print(f"💾 Saved {len(COMPONENT_ID_MAP)} component mappings to cache")
        
    except Exception as e:
        print(f"⚠️ Error saving cache: {e}")


def generate_component_hash(component_data):
    """Generate a hash for component data to detect changes"""
    # Remove fields that don't affect the component's core data
    hashable_data = component_data.copy()
    
    # Remove metadata that might change but doesn't affect the component
    hashable_data.pop('id', None)
    hashable_data.pop('createdAt', None) 
    hashable_data.pop('updatedAt', None)
    
    # Convert to stable string representation
    stable_json = json.dumps(hashable_data, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(stable_json.encode('utf-8')).hexdigest()


def check_component_exists(template_type, name, component_data):
    """Check if component already exists and hasn't changed"""
    cache_key = (template_type, name)
    
    if FORCE_REUPLOAD:
        print(f"🔁 Force re-upload enabled for: {name}")
        return False, COMPONENT_ID_MAP.get(cache_key)
    
    if cache_key not in COMPONENT_ID_MAP:
        return False, None
        
    component_id = COMPONENT_ID_MAP[cache_key]
    current_hash = generate_component_hash(component_data)
    cached_hash = COMPONENT_HASH_CACHE.get(f"{template_type}:{name}")
    
    if cached_hash == current_hash:
        print(f"♻️  Using cached component: {name} (ID: {component_id})")
        return True, component_id
    else:
        print(f"🔄 Component changed, will re-upload: {name}")
        return False, component_id


def filter_components_for_upload(components, template_type):
    """Filter out components that already exist and haven't changed"""
    components_to_upload = []
    cached_components = []
    
    for component in components:
        name = component.get('name', 'Untitled')
        exists, component_id = check_component_exists(template_type, name, component)
        
        if exists:
            cached_components.append({
                'name': name,
                'id': component_id,
                'component': component
            })
        else:
            components_to_upload.append(component)
    
    print(f"📊 Upload Summary:")
    print(f"   • Cached (skipping): {len(cached_components)}")
    print(f"   • New/Changed (uploading): {len(components_to_upload)}")
    
    return components_to_upload, cached_components


def run_loop():
    print("🔁 XLSX Validator and Migration App (Ctrl+C or type 'exit' to quit)")
    
    # Load existing cache and clear session missing references
    load_component_cache()
    clear_missing_references_session()

    try:
        while True:
            if not os.path.exists(COMPONENTS_PATH):
                print("❌ File not found.")
                continue

            try:
                xls = pd.read_excel(COMPONENTS_PATH, sheet_name=None)

                # Make duplicate column names unique
                def dedup_columns(columns):
                    seen = {}
                    new_cols = []
                    for col in columns:
                        if col not in seen:
                            seen[col] = 1
                            new_cols.append(col)
                        else:
                            seen[col] += 1
                            new_cols.append(f"{col}.{seen[col]}")
                    return new_cols

                for sheet, df_sheet in xls.items():
                    print(f"checking sheet: '{sheet}'")
                    df_sheet.columns = dedup_columns(df_sheet.columns)

                    if sheet == "All Transfers - For Upload":
                        # Treat row 2 as header (skip the first row)
                        df_sheet.columns = df_sheet.iloc[0]  # take row 2 as header
                        df_sheet = df_sheet.iloc[1:].reset_index(drop=True)
                    else:
                        # Normal behavior
                        df_sheet = df_sheet.iloc[1:].reset_index(drop=True)

                    xls[sheet] = df_sheet


            except Exception as e:
                print(f"❌ Error reading Excel file: {e}")
                continue

            for sheet_name in SHEET_PROCESS_ORDER:
                if sheet_name not in xls:
                    continue  # skip missing sheets
                df = xls[sheet_name]

                print(f"\n📄 Processing Sheet: {sheet_name}")

                template_ids = SHEET_TEMPLATE_MAP[sheet_name]
                row_mapper = SHEET_ROW_MAPPERS[sheet_name]
                core_data_service = CoreDataService(template_ids)

                schemas = core_data_service.getSchemaWithArrayLevel()

                try:
                    results, parsed_json = validate_csv(
                        df,
                        schemas,
                        template_ids,
                        lambda row, row_index: row_mapper(
                            row,
                            template_ids,
                            COMPONENT_ID_MAP,
                            {
                                "sheet_name": sheet_name,
                                "row_name": row.get("name", "Untitled")
                            },
                            row_index=row_index
                        )
                    )
                except Exception as e:
                    print(f"❌ Validation error in '{sheet_name}': {e}")
                    continue

                invalid = [r for r in results if not r["valid"]]
                if invalid:
                    print(f"\n❌ Validation failed for sheet '{sheet_name}' on the following rows:")
                    for r in invalid:
                        for err in r["errors"]:
                            print(f"  - {err}")
                    print("Please correct the above before retry.")
                    continue

                print(f"\n✅ All rows in sheet '{sheet_name}' are valid!")
                
                # Get template type for caching - use the component's actual templateId
                # We'll determine this from the first component since they should all have the same templateId
                template_type = "unknown"
                if parsed_json:
                    component_template_id = parsed_json[0].get("templateId")
                    template_type = TEMPLATE_TYPES.get(component_template_id, "unknown")
                    if template_type == "unknown":
                        print(f"⚠️ Warning: Unknown template type for templateId: {component_template_id}")
                
                # Filter components based on cache
                components_to_upload, cached_components = filter_components_for_upload(
                    parsed_json, template_type
                )
                
                if not components_to_upload:
                    print(f"🎉 All components in '{sheet_name}' already exist in cache. Skipping upload.")
                    continue
                insert = ""
                if FORCE_REUPLOAD: insert = "y"
                else: insert = input(f"Upload {len(components_to_upload)} new/changed components from '{sheet_name}'? (y/n): ").strip().lower()
                if insert == "y":
                    push = ""
                    if FORCE_REUPLOAD: push = "y"
                    else: push = input("Confirm upload to database? (y/n): ").strip().lower()
                    if push == "y":
                        print("🛜 Calling API ...")
                        # Upload only new/changed components
                        newly_uploaded = core_data_service.pushValidRowToDB(components_to_upload, template_type)
                        
                        # Update cache with newly uploaded components
                        for component in newly_uploaded:
                            name = component.get('name', 'Untitled')
                            component_hash = generate_component_hash(component)
                            COMPONENT_HASH_CACHE[f"{template_type}:{name}"] = component_hash
                        
                        # Save updated cache
                        save_component_cache()
                        
                        print(f"✅ {len(components_to_upload)} new components uploaded from '{sheet_name}'.")
                        print(f"♻️  {len(cached_components)} components were reused from cache.")
                    else:
                        print("⏩ Skipping database insert.")

            # Save missing references log after processing all sheets
            save_missing_references_log()
            
            print(f"\n📋 Session Summary:")
            print(f"Final Component ID Map Statistics: {len(COMPONENT_ID_MAP)} total components")
            by_type = {}
            for (template_type, name), comp_id in COMPONENT_ID_MAP.items():
                by_type[template_type] = by_type.get(template_type, 0) + 1
            
            for template_type, count in by_type.items():
                print(f"  {template_type}: {count} components")
            
            print(f"\n{get_missing_references_summary()}")

            print("✅ All sheets processed.")
            break

    except KeyboardInterrupt:
        print("\n👋 Exiting the app. Goodbye!")


import requests

class CoreDataService:
    def __init__(self, template_ids):
        self.template_ids = template_ids
        self.service_url = 'https://data-api-dev.swoop-adventures.com'
        self.headers = {
            "Authorization": "Bearer supercoolamazingtoken"
        }

    def getSchemaWithArrayLevel(self):
        schemas = []
        for idx, template_id in enumerate(self.template_ids):
            res = requests.get(
                f"{self.service_url}/core-data-service/v1/templates/{template_id}",
                headers=self.headers
            )
            
            # Debug: check content type and response text
            try:
                response = res.json()
            except json.JSONDecodeError:
                print("⚠️ JSON decode error. Raw response:")
                print(res.text)  # show first 500 chars for debugging
                raise  # re-raise so you can see the issue

            schema_str = response.get("validationSchemas", {}).get("componentSchema")
            if schema_str:
                schema = json.loads(schema_str)
                schemas.append(schema)
                if idx == len(self.template_ids) - 1:
                    self.template_name = response.get('name')
                    self.template_id = response.get('id')
            else:
                schemas.append({})
        return schemas


    def pushValidRowToDB(self, components, template_type):
        if DEBUG_MODE:
            print(f"📝 DEBUG MODE ON: Writing {len(components)} components to {DEBUG_OUTPUT_FILE}")
            with open(DEBUG_OUTPUT_FILE, "a", encoding="utf-8") as f:
                for comp in components:
                    f.write(json.dumps(comp, ensure_ascii=False) + "\n")
            return components
            
        uploaded_components = []
        
        for idx, component in enumerate(components):
            res = requests.post(
                f"{self.service_url}/core-data-service/v1/components",
                json=component,
                headers=self.headers
            )
            if res.status_code == 201:
                try:
                    data = res.json()
                    comp_id = data.get("id")
                    comp_name = component.get("name")
                    template_id = component.get("templateId")
                    
                    if comp_id and comp_name and template_id:
                        # Store in global cache
                        COMPONENT_ID_MAP[(template_type, comp_name)] = comp_id
                        print(f"✅ Row {idx + 1} - ({template_type}, {comp_name}) -> {comp_id}")
                        
                        # Add ID to component for hash caching
                        component['id'] = comp_id
                        uploaded_components.append(component)
                        
                except Exception as e:
                    print(f"⚠️ Row {idx + 1} - Could not parse returned ID for row {idx+1}: {e}")
            else:
                try:
                    error_msg = res.json()
                    print(f"❌ Failed to upload row {idx + 1}. Error: {error_msg}")
                except Exception:
                    print(f"❌ Failed to upload row {idx + 1}. HTTP Status: {res.status_code}")
        
        return uploaded_components


if __name__ == "__main__":
    run_loop()