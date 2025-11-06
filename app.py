import os
import json
import random
import hashlib
import requests
import openpyxl
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from validate_csv_dynamic import validate_csv
from utils import save_missing_references_log, clear_missing_references_session, get_missing_references_summary

from mappings.activity import map_activity_component
from mappings.location import map_location_component
from mappings.ground_accom import map_ground_accommodation_component
from mappings.ship_accom import map_ship_accommodation_component
from mappings.journey import map_journey_component
from mappings.tranfer import map_transfer_component
from mappings.excursions import map_excursion_component
from mappings.private_tours import map_private_tours_component
from mappings.all_inclusive_hotels import map_all_inclusive_hotels_component
from mappings.multi_day_activity import map_multi_day_activity_component
from mappings.cruise import map_cruise_component
from mappings.ship_accom import map_ship_accommodation_component

DEBUG_MODE = False
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
        "template_7546d5da287241629b5190f95346840e",  # Accom
        "template_68c8d409a9f7462aa528a1216cadf2b5",  # Gy
    ],
    "Ship Accom": [
        "template_aca16a46ec3842ca85d182ee9348f627",  # Base
        "template_7546d5da287241629b5190f95346840e",  # Accom
        "template_bb8caab1d3104257a75b7cb7dd958136",  # Gy
    ],
    "ANT Ship Accom": [
        "template_aca16a46ec3842ca85d182ee9348f627",  # Base
        "template_7546d5da287241629b5190f95346840e",  # Accom
        "template_bb8caab1d3104257a75b7cb7dd958136",  # Gy
    ],
    "All Activities - For Upload": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_e2f0e9e5343349358037a0564a3366a0"  # Activity
    ],
    "All Transfers - For Upload": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_901d40ac12214820995880915c5b62f5"
    ],
    "Excursions Package": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_3b7714dcfa374cd19b9dc97af1510204", # Pkg
        "template_a6a2dbfd478143de994dca40dc07e054"
    ],
    "Private Tours Package": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_3b7714dcfa374cd19b9dc97af1510204", # Pkg
        "template_d9081bfcc3b7461987a3728e57ca7363"
    ],
    "All Inclusive Hotel Package": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_3b7714dcfa374cd19b9dc97af1510204", # Pkg
        "template_ba7999ff957c4ca3a5e61496df6178ac"
    ],
    "Multi-day Activity Package": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_3b7714dcfa374cd19b9dc97af1510204", # Pkg
        "template_a64e161de5824fcb9515274b0f67d698"
    ],
    "PAT Cruise Packages ": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_3b7714dcfa374cd19b9dc97af1510204", # Pkg
        "template_63a57a90570c47b89f830d2c7618324f"
    ],
    "ANT Cruise Packages": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_3b7714dcfa374cd19b9dc97af1510204", # Pkg
        "template_63a57a90570c47b89f830d2c7618324f"
    ]
}

DUMMY_TEMPLATE_MAP = {
    "flights": [
        "template_aca16a46ec3842ca85d182ee9348f627",
        "template_4aec70add8e74467814fe7337f4e41b3"
    ],
    "independent_arrangements": [
        "template_aca16a46ec3842ca85d182ee9348f627",
        "template_932b514e6d804e248bf04a9fa1f836de"
    ]
}

SHEET_ROW_MAPPERS = {
    "Location"                     : map_location_component,
    "Ground Accom"                 : map_ground_accommodation_component,
    "Ship Accom"                   : map_ship_accommodation_component,
    "ANT Ship Accom"               : map_ship_accommodation_component,
    "Journeys"                     : map_journey_component,
    "All Activities - For Upload"  : map_activity_component,
    "All Transfers - For Upload"   : map_transfer_component,
    "Excursions Package"           : map_excursion_component,
    "Private Tours Package"        : map_private_tours_component,
    "All Inclusive Hotel Package"  : map_all_inclusive_hotels_component,
    "Multi-day Activity Package"   : map_multi_day_activity_component,
    "PAT Cruise Packages "         : map_cruise_component,
    "ANT Cruise Packages"          : map_cruise_component,

}

TEMPLATE_TYPES = {
    "template_0c105b25350647b096753b4f863ab06c": "location",
    "template_7546d5da287241629b5190f95346840e": "accommodation",
    "template_68c8d409a9f7462aa528a1216cadf2b5": "ground_accommodation",
    "template_bb8caab1d3104257a75b7cb7dd958136": "ship_accommodation",
    "template_14cc18c1408a4b73a16d4e1dad2efca9": "journey",
    "template_e2f0e9e5343349358037a0564a3366a0": "activity",
    "template_901d40ac12214820995880915c5b62f5": "transfer",
    "template_3b7714dcfa374cd19b9dc97af1510204": "package",

    "template_a6a2dbfd478143de994dca40dc07e054": "excursion",
    "template_d9081bfcc3b7461987a3728e57ca7363": "private_tour",
    "template_ba7999ff957c4ca3a5e61496df6178ac": "all_inclusive_hotel",
    "template_a64e161de5824fcb9515274b0f67d698": "multi_day_activity",
    "template_63a57a90570c47b89f830d2c7618324f": "cruise",
}

PAT_COMPONENTS_PATH = "pat_components.xlsx"
COMPONENTS_PATH = PAT_COMPONENTS_PATH

SHEET_PROCESS_ORDER = [
    "Location",
    "Ground Accom",
    "Ship Accom",
    "ANT Ship Accom",
    "Journeys",
    "All Activities - For Upload",
    "All Transfers - For Upload",

    "Excursions Package",
    "Private Tours Package",
    "All Inclusive Hotel Package",
    "Multi-day Activity Package",
    "PAT Cruise Packages ",
    "ANT Cruise Packages",
]

AUXILIARY_SHEETS = {
    "Rooms Cabins": ["Ground Accom", "Ship Accom", "ANT Ship Accom"]
}

def get_partners():
    url="https://data-test.swoop-adventures.com/api/partners?page=1&itemsPerPage=1000"
    headers = {"Authorization": "bearer 1|eaLBn270PQGlC1onbygdZZ8aptWAd8bU6Ux00RbW52bf7343"}
    ant_res = requests.get(url+"&region=antarctica", headers=headers)
    arc_res = requests.get(url+"&region=arctic", headers=headers)
    pat_res = requests.get(url+"&region=patagonia", headers=headers)
    ant_data = ant_res.json()
    arc_data = arc_res.json()
    pat_data = pat_res.json()

    partner_map = {
        "ant":{},
        "arc":{},
        "Patagonia":{}
    }

    for partner in ant_data:
        partner_map["ant"][partner["title"]] = partner["id"]
    for partner in arc_data:
        partner_map["arc"][partner["title"]] = partner["id"]
    for partner in pat_data:
        partner_map["Patagonia"][partner["title"]] = partner["id"]
    
    return partner_map

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

def generate_component_id(component: dict) -> str:
    """
    Generate a deterministic component ID in the format:
        component_<md5hash>
    based on name + template type.
    """
    try:
        name = component.get("name", "")
        template_type = TEMPLATE_TYPES.get(component.get("templateId"), "")
        base_str = name + template_type
        hash_str = hashlib.md5(base_str.encode("utf-8")).hexdigest()
        return f"component_{hash_str}"
    except:
        return f"component_error"

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

    partner_map = get_partners()

    try:
        while True:
            if not os.path.exists(COMPONENTS_PATH):
                print("❌ File not found.")
                continue

            try:
                def get_visible_sheets(path):
                    """Return only visible worksheet names (exclude hidden & veryHidden)."""
                    wb = openpyxl.load_workbook(path, read_only=False, data_only=True)
                    return [ws.title for ws in wb.worksheets if ws.sheet_state == "visible"]

                # Example usage
                visible_sheets = get_visible_sheets(COMPONENTS_PATH)
                print("Visible sheets:", visible_sheets)

                # Load only those into pandas
                xls = pd.read_excel(COMPONENTS_PATH, sheet_name=visible_sheets, dtype=str)
                
                def dedup_columns(columns):
                    seen = {}
                    new_cols = []
                    for col in list(columns):  # force left-to-right list iteration
                        col = str(col)  # flatten tuples if MultiIndex
                        if col not in seen:
                            seen[col] = 1
                            new_cols.append(col)
                        else:
                            seen[col] += 1
                            new_cols.append(f"{col}.{seen[col]}")
                    return new_cols

                # Store auxiliary data for later use
                auxiliary_data = {}

                for sheet, df_sheet in xls.items():
                    df_sheet.columns = dedup_columns(df_sheet.columns)
                    # print(f"Columns for {sheet}:")
                    # for i, col in enumerate(df_sheet.columns):
                    #     print(i, repr(col))

                    df_sheet = df_sheet.iloc[1:].reset_index(drop=True)
                    xls[sheet] = df_sheet
                    
                    # Store auxiliary data separately
                    if sheet in AUXILIARY_SHEETS:
                        auxiliary_data[sheet] = df_sheet
                        print(f"📋 Stored auxiliary data for {sheet}: {len(df_sheet)} rows")
                
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
                
                # Prepare auxiliary data for this sheet
                rooms_data_for_sheet = None
                if "Rooms Cabins" in auxiliary_data:
                    if sheet_name in AUXILIARY_SHEETS["Rooms Cabins"]:
                        rooms_data_for_sheet = auxiliary_data["Rooms Cabins"]
                        print(f"🏨 Including {len(rooms_data_for_sheet)} rooms for {sheet_name} processing")           
                
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
                            row_index=row_index,
                            rooms_data=rooms_data_for_sheet,  # Pass auxiliary data to mapper
                            partner_map=partner_map
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

            upload_dummy_components()

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

def upload_dummy_components():
    
    cds = CoreDataService([DUMMY_TEMPLATE_MAP["flights"]])

    flight_component_fields = [
        {"templateId": DUMMY_TEMPLATE_MAP["flights"][1], "data": {}},
        {"templateId": DUMMY_TEMPLATE_MAP["flights"][0], "data": {}},
    ]

    flight = {
        "orgId":"swoop",
        "destination":"patagonia",
        "state": "Draft",
        "pricing": {"amount":0,"currency":"gbp"},
        "package": None,
        "templateId": DUMMY_TEMPLATE_MAP["flights"][1],
        "isBookable": True,
        "description":{
            "web": "",
            "quote": "",
            "final": ""
        },
        "partners": ["NA"],
        "regions": [],
        "name": "Flight",
        "media": {
            "images": [],
            "videos": []
        },
        "componentFields": flight_component_fields,
    }
    flight_copy = flight.copy()
    cds.pushValidRowToDB([flight_copy], "Flight")

    independent_arrangements_component_fields = [
        {"templateId": DUMMY_TEMPLATE_MAP["independent_arrangements"][1], "data": {}},
        {"templateId": DUMMY_TEMPLATE_MAP["independent_arrangements"][0], "data": {}},
    ]

    independent_arrangement = flight
    independent_arrangement["name"] = "Independent Arrangement"
    independent_arrangement["templateId"] = DUMMY_TEMPLATE_MAP["independent_arrangements"][1]
    independent_arrangement["componentFields"] = independent_arrangements_component_fields
    
    cds = CoreDataService(DUMMY_TEMPLATE_MAP["independent_arrangements"])
    cds.pushValidRowToDB([independent_arrangement], "Independent Arrangement")


class CoreDataService:
    def __init__(self, template_ids):
        self.template_ids = template_ids
        self.service_url = 'https://data-api-dev.swoop-adventures.com'
        self.headers = {
            "Authorization": "Bearer supercoolamazingtoken",
        }

    def _fetch_schema(self, template_id):
        url = f"{self.service_url}/core-data-service/v1/template/{template_id}"
        url = f"{self.service_url}/core-data-service/v1/template/{template_id}"
        try:
            res = requests.get(url, headers=self.headers)
            res.raise_for_status()
            response = res.json()
        except Exception as e:
            print(f"⚠️ Failed fetching schema for {template_id}: {e}")
            return {}

        schema_str = response.get("validationSchemas", {}).get("componentSchema")
        if schema_str:
            try:
                return json.loads(schema_str)
            except Exception as e:
                print(f"⚠️ Failed parsing schema for {template_id}: {e}")
        return {}

    def getSchemaWithArrayLevel(self):
        schemas = []
        for tid in self.template_ids:
            schema = self._fetch_schema(tid)
            schemas.append(schema)
        return schemas


    def _upload_component(self, component, template_type, idx, overwrite_on_fail=True):
        pregenerated_id = generate_component_id(component)
        self.headers["x-datadog-trace-id"] = str(int(random.getrandbits(63)))
        self.headers["x-datadog-parent-id"] = str(int(random.getrandbits(63)))
        self.headers["x-datadog-sampling-priority"] = "1"
        url = ""
        try:
            if pregenerated_id:
                url = f"{self.service_url}/core-data-service/v1/component/{pregenerated_id}"
                url = f"{self.service_url}/core-data-service/v1/component/{pregenerated_id}"
                res = requests.post(url, json=component, headers=self.headers)
            else:
                url = f"{self.service_url}/core-data-service/v1/component"
                url = f"{self.service_url}/core-data-service/v1/component"
                res = requests.post(url, json=component, headers=self.headers)
        except Exception as e:
            print(f"❌ Request failed for row {idx+1}: {e}")
            return None

        if res.status_code in [200, 201, 202]:
            return self._process_success_response(res, component, template_type, idx)

        # 🔄 Retry with PUT if enabled
        if overwrite_on_fail and pregenerated_id:

            self.headers["x-datadog-trace-id"] = str(int(random.getrandbits(63)))
            self.headers["x-datadog-parent-id"] = str(int(random.getrandbits(63)))
            self.headers["x-datadog-sampling-priority"] = "1"

            print(f"🔁 POST failed for row {idx+1}, retrying with PATCH ...")
            try:
                del component['templateId']
                del component['name']
                del component['orgId']

                put_res = requests.patch(url, json=component, headers=self.headers)
                if put_res.status_code in [200, 201, 202]:
                    return self._process_success_response(put_res, component, template_type, idx)
                else:
                    try:
                        print(f"❌ PUT also failed for row {idx+1}. Error: {put_res.json()}")
                    except Exception:
                        print(f"❌ PUT also failed for row {idx+1}. HTTzP Status: {put_res.status_code}")
            except Exception as e:
                print(f"❌ PUT request failed for row {idx+1}: {e}")

        # ❌ Complete failure
        try:
            print(f"❌ Failed to upload row {idx+1}. Error: {res.json()}")
        except Exception:
            print(f"❌ Failed to upload row {idx+1}. HTTP Status: {res.status_code}")
        return None

    def _process_success_response(self, res, component, template_type, idx):
        """Helper to handle successful POST/PUT responses"""
        try:
            data = res.json()
            comp_id = data.get("id")
            comp_name = component.get("name")
            template_id = component.get("templateId")

            if comp_id and comp_name and template_id:
                COMPONENT_ID_MAP[(template_type, comp_name)] = comp_id
                print(f"✅ Row {idx+1} - ({template_type}, {comp_name}) -> {comp_id}")
                component['id'] = comp_id
                return component
        except Exception as e:
            print(f"⚠️ Could not parse returned ID for row {idx+1}: {e}")
        return None


    def pushValidRowToDB(self, components, template_type, max_workers=10):
        if DEBUG_MODE:
            print(f"📝 DEBUG MODE ON: Writing {len(components)} components to {DEBUG_OUTPUT_FILE}")
            with open(DEBUG_OUTPUT_FILE, "a", encoding="utf-8") as f:
                for comp in components:
                    f.write(json.dumps(comp, ensure_ascii=False) + "\n")
            return components

        uploaded_components = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_idx = {
                executor.submit(self._upload_component, comp, template_type, idx): idx
                for idx, comp in enumerate(components)
            }

            for future in as_completed(future_to_idx):
                result = future.result()
                if result:
                    uploaded_components.append(result)

        return uploaded_components

if __name__ == "__main__":
    run_loop()