import os
import json
import hashlib
import requests
import warnings
import openpyxl
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
import threading
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from mappings.cruise_activity import map_cruise_activity_component
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

ACCESS_TOKEN = ""

SHEET_PROCESS_ORDER = [
    # "Location",
    # "Ground Accom",
    # "Ship Accom",
    # "ANT Ship Accom",
    # "Journeys",
    # "All Activities - For Upload",
    # "ANT Activities",
    # "All Transfers - For Upload",
    # "ANT Transfers",
    # "Excursions Package",
    # "Private Tours Package",
    # "All Inclusive Hotel Package",
    # "Multi-day Activity Package",
    # "PAT Cruise Packages ",
    # "ANT Cruise Packages",
]

log_lock = threading.Lock()

def ts_print(msg):
    with log_lock:
        print(msg)

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
    "ANT Activities": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_12345678123456781234567812345678"  # Activity
    ],
    "All Transfers - For Upload": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_901d40ac12214820995880915c5b62f5"
    ],
    "ANT Transfers": [
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

SHEET_DESTINATION_OVERRIDE = {
    "Ship Accom": "Patagonia",
    "ANT Ship Accom": "Antarctica",
    "PAT Cruise Packages ": "Patagonia",
    "ANT Cruise Packages": "Antarctica",
}

DUMMY_TEMPLATE_MAP = {
    "flights": [
        "template_aca16a46ec3842ca85d182ee9348f627",
        "template_4aec70add8e74467814fe7337f4e41b3"
    ],
    "independent_arrangements": [
        "template_aca16a46ec3842ca85d182ee9348f627",
        "template_932b514e6d804e248bf04a9fa1f836de"
    ],
    "fee": [
        "template_aca16a46ec3842ca85d182ee9348f627",
        "template_d15cc4ab72034fb8a098d9a9ec791a7d"  
    ]
}

SHEET_ROW_MAPPERS = {
    "Location"                     : map_location_component,
    "Ground Accom"                 : map_ground_accommodation_component,
    "Ship Accom"                   : map_ship_accommodation_component,
    "ANT Ship Accom"               : map_ship_accommodation_component,
    "Journeys"                     : map_journey_component,
    "All Activities - For Upload"  : map_activity_component,
    "ANT Activities"               : map_cruise_activity_component,
    "All Transfers - For Upload"   : map_transfer_component,
    "ANT Transfers"                : map_transfer_component,
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
    "template_12345678123456781234567812345678": "cruise_activity",
    "template_901d40ac12214820995880915c5b62f5": "transfer",
    "template_3b7714dcfa374cd19b9dc97af1510204": "package",

    "template_a6a2dbfd478143de994dca40dc07e054": "excursion",
    "template_d9081bfcc3b7461987a3728e57ca7363": "private_tour",
    "template_ba7999ff957c4ca3a5e61496df6178ac": "all_inclusive_hotel",
    "template_a64e161de5824fcb9515274b0f67d698": "multi_day_activity",
    "template_63a57a90570c47b89f830d2c7618324f": "cruise",
}

COMPONENT_BLACKLIST = [
    "The Original Torres del Paine W Trek (Under 4 people)"
]

PAT_COMPONENTS_PATH = "pat_components.xlsx"
COMPONENTS_PATH = PAT_COMPONENTS_PATH


AUXILIARY_SHEETS = {
    "Rooms Cabins": ["Ground Accom", "Ship Accom", "ANT Ship Accom"]
}

def get_partners():
    url="https://data-test.swoop-adventures.com/api/partners?page=1&itemsPerPage=1000"
    headers = {"Authorization": "bearer 1|eaLBn270PQGlC1onbygdZZ8aptWAd8bU6Ux00RbW52bf7343"}
    ant_res = requests.get(url+"&region=antarctica", headers=headers)
    # arc_res = requests.get(url+"&region=arctic", headers=headers)
    pat_res = requests.get(url+"&region=patagonia", headers=headers)
    ant_data = ant_res.json()
    # arc_data = arc_res.json()
    pat_data = pat_res.json()

    partner_map = {
        "Antarctica":{},
        "Patagonia":{}
    }

    # for partner in arc_data:
    #     partner_map["Antarctica"][partner["title"]] = partner["id"]
    for partner in ant_data:
        partner_map["Antarctica"][partner["title"]] = partner["id"]
        #print(partner["title"] + " " + partner["id"])
    for partner in pat_data:
        partner_map["Patagonia"][partner["title"]] = partner["id"]
    
    partner_map["Rest of Chile/Argentina"] = partner_map["Patagonia"]

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
                
                ts_print(f"📥 Loaded {len(COMPONENT_ID_MAP)} component mappings from cache")
                
        except Exception as e:
            ts_print(f"⚠️ Error loading cache: {e}. Starting with empty cache.")
            COMPONENT_ID_MAP = {}
            COMPONENT_HASH_CACHE = {}
    else:
        ts_print("📝 No cache file found. Starting with empty cache.")


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
            
        ts_print(f"💾 Saved {len(COMPONENT_ID_MAP)} component mappings to cache")
        
    except Exception as e:
        ts_print(f"⚠️ Error saving cache: {e}")


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
        ts_print(f"🔁 Force re-upload enabled for: {name}")
        return False, COMPONENT_ID_MAP.get(cache_key)
    
    if cache_key not in COMPONENT_ID_MAP:
        return False, None
        
    component_id = COMPONENT_ID_MAP[cache_key]
    current_hash = generate_component_hash(component_data)
    cached_hash = COMPONENT_HASH_CACHE.get(f"{template_type}:{name}")
    
    if cached_hash == current_hash:
        ts_print(f"♻️  Using cached component: {name} (ID: {component_id})")
        return True, component_id
    else:
        ts_print(f"🔄 Component changed, will re-upload: {name}")
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
    
    ts_print(f"📊 Upload Summary:")
    ts_print(f"   • Cached (skipping): {len(cached_components)}")
    ts_print(f"   • New/Changed (uploading): {len(components_to_upload)}")
    
    return components_to_upload, cached_components


def run_loop():
    logging.info("🔁 Starting XLSX Validator and Migration")
    
    tracker = MigrationTracker()
    tracker.start()
    
    load_component_cache()
    clear_missing_references_session()
    partner_map = get_partners()

    try:
        while True:
            if not os.path.exists(COMPONENTS_PATH):
                logging.error("❌ File not found.")
                continue

            try:
                def get_visible_sheets(path):
                    """Return only visible worksheet names (exclude hidden & veryHidden)."""
                    wb = openpyxl.load_workbook(path, read_only=False, data_only=True)
                    return [ws.title for ws in wb.worksheets if ws.sheet_state == "visible"]

                visible_sheets = get_visible_sheets(COMPONENTS_PATH)
                logging.info(f"Visible sheets: {visible_sheets}")

                xls = pd.read_excel(COMPONENTS_PATH, sheet_name=visible_sheets, dtype=str)
                
                def dedup_columns(columns):
                    seen = {}
                    new_cols = []
                    for col in list(columns):
                        col = str(col)
                        if col not in seen:
                            seen[col] = 1
                            new_cols.append(col)
                        else:
                            seen[col] += 1
                            new_cols.append(f"{col}.{seen[col]}")
                    return new_cols

                auxiliary_data = {}

                for sheet, df_sheet in xls.items():
                    df_sheet.columns = dedup_columns(df_sheet.columns)
                    df_sheet = df_sheet.iloc[1:].reset_index(drop=True)
                    xls[sheet] = df_sheet
                    
                    if sheet in AUXILIARY_SHEETS:
                        auxiliary_data[sheet] = df_sheet
                        logging.info(f"📋 Stored auxiliary data for {sheet}: {len(df_sheet)} rows")
                
            except Exception as e:
                logging.error(f"❌ Error reading Excel file: {e}", exc_info=True)
                continue

            for sheet_name in SHEET_PROCESS_ORDER:
                if sheet_name not in xls:
                    continue
                
                sheet_start = datetime.now()
                df = xls[sheet_name]
                logging.info(f"📄 Processing Sheet: {sheet_name} ({len(df)} rows)")
                
                # Initialize sheet summary
                tracker.sheets[sheet_name] = SheetSummary(
                    sheet_name=sheet_name,
                )
            
                template_ids = SHEET_TEMPLATE_MAP[sheet_name]
                row_mapper = SHEET_ROW_MAPPERS[sheet_name]
                core_data_service = CoreDataService(template_ids, tracker=tracker, sheet_name=sheet_name)

                schemas = core_data_service.getSchemaWithArrayLevel()
                
                rooms_data_for_sheet = None
                if "Rooms Cabins" in auxiliary_data:
                    if sheet_name in AUXILIARY_SHEETS["Rooms Cabins"]:
                        rooms_data_for_sheet = auxiliary_data["Rooms Cabins"]
                        logging.info(f"🏨 Including {len(rooms_data_for_sheet)} rooms for {sheet_name} processing")           
                
                try:
                    destination_override = SHEET_DESTINATION_OVERRIDE.get(sheet_name, None)

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
                            rooms_data=rooms_data_for_sheet,
                            partner_map=partner_map,
                            destination_override=destination_override
                        ),
                        tracker=tracker,
                        sheet_name=sheet_name
                    )
                    
                    # Track validation results
                    for r in results:
                        if not r["valid"]:
                            for error in r["errors"]:
                                tracker.add_sheet_result(RowResult(
                                    sheet_name=sheet_name,
                                    row_number=r["row"],
                                    component_name=error.get("component_name", "Unknown"),
                                    status=OperationStatus.VALIDATION_ERROR,
                                    error_details=error,
                                    component={}
                                ))
                                logging.warning(f"Validation error at row {r['row']}: {error.get('message', 'Unknown error')}")
                        
                except Exception as e:
                    logging.error(f"❌ Validation error in '{sheet_name}': {e}", exc_info=True)
                    tracker.add_sheet_result(RowResult(
                        sheet_name=sheet_name,
                        row_number=0,
                        component_name="Sheet Validation",
                        status=OperationStatus.VALIDATION_ERROR,
                        error_details={"message": str(e), "type": type(e).__name__},
                        component={}
                    ))
                    continue

                invalid = [r for r in results if not r["valid"]]
                if invalid:
                    logging.error(f"\n❌ Validation failed for sheet '{sheet_name}' on {len(invalid)} rows")
                    for r in invalid[:5]:  # Log first 5 errors
                        for err in r["errors"]:
                            logging.error(f"  Row {r['row']}: {err}")
                    if len(invalid) > 5:
                        logging.error(f"  ... and {len(invalid) - 5} more errors")
                    continue

                logging.info(f"✅ All rows in sheet '{sheet_name}' are valid!")
                
                template_type = "unknown"
                if parsed_json:
                    component_template_id = parsed_json[0].get("templateId")
                    template_type = TEMPLATE_TYPES.get(component_template_id, "unknown")
                    if template_type == "unknown":
                        logging.warning(f"⚠️ Unknown template type for templateId: {component_template_id}")
                
                # Filter components and track cached ones
                components_to_upload, cached_components = filter_components_for_upload(
                    parsed_json, template_type
                )
                # Track cached components
                for cached in cached_components:
                    tracker.add_sheet_result(RowResult(
                        sheet_name=sheet_name,
                        row_number=0,  # We don't have row numbers here
                        component_name=cached['name'],
                        component_id=cached['id'],
                        status=OperationStatus.CACHED,
                        template_type=template_type,
                        component={}
                    ))
                
                if not components_to_upload:
                    logging.info(f"🎉 All components in '{sheet_name}' already exist in cache. Skipping upload.")
                    sheet_duration = (datetime.now() - sheet_start).total_seconds()
                    tracker.sheets[sheet_name].duration_seconds = sheet_duration
                    continue
                    
                insert = ""
                if FORCE_REUPLOAD: 
                    insert = "y"
                else: 
                    insert = input(f"Upload {len(components_to_upload)} new/changed components from '{sheet_name}'? (y/n): ").strip().lower()
                    
                if insert == "y":
                    push = ""
                    if FORCE_REUPLOAD: 
                        push = "y"
                    else: 
                        push = input("Confirm upload to database? (y/n): ").strip().lower()
                        
                    if push == "y":
                        logging.info("🛜 Calling API ...")
                        # Upload and track results
                        newly_uploaded = core_data_service.pushValidRowToDB(
                            components_to_upload, 
                            template_type
                        )
                        
                        # Update cache with newly uploaded components
                        for component in newly_uploaded:
                            name = component.get('name', 'Untitled')
                            component_hash = generate_component_hash(component)
                            COMPONENT_HASH_CACHE[f"{template_type}:{name}"] = component_hash
                        
                        save_component_cache()
                        
                        logging.info(f"✅ {len(newly_uploaded)} new components uploaded from '{sheet_name}'.")
                        logging.info(f"♻️  {len(cached_components)} components were reused from cache.")
                    else:
                        logging.info("⏩ Skipping database insert.")
                        # Track as skipped
                        for component in components_to_upload:
                            tracker.add_sheet_result(RowResult(
                                sheet_name=sheet_name,
                                row_number=0,
                                component_name=component.get('name', 'Untitled'),
                                status=OperationStatus.UPLOAD_ERROR,
                                error_details={"message": "Upload cancelled by user"},
                                component=component
                            ))
                
                sheet_duration = (datetime.now() - sheet_start).total_seconds()
                tracker.sheets[sheet_name].duration_seconds = sheet_duration
                logging.info(f"✅ Sheet '{sheet_name}' completed in {sheet_duration:.2f}s")

            upload_dummy_components()
            save_missing_references_log()
            
            # End tracking and generate reports
            tracker.end()
            
            # Generate and display text report
            report = tracker.generate_report()
            ts_print("\n" + "=" * 80)
            ts_print(report)
            logging.info("\n" + report)
            
            # Save JSON report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_report_path = f"logs/migration_report_{timestamp}.json"
            tracker.export_to_json(json_report_path)
            logging.info(f"📊 Detailed JSON report saved to: {json_report_path}")
            
            # # Save HTML report
            # html_report_path = f"logs/migration_report_{timestamp}.html"
            # tracker.export_to_html(html_report_path)
            # logging.info(f"📊 HTML report saved to: {html_report_path}")
            
            # Summary stats
            logging.info(f"\n📋 Final Statistics:")
            logging.info(f"   Component ID Map: {len(COMPONENT_ID_MAP)} total components")
            by_type = {}
            for (template_type, name), comp_id in COMPONENT_ID_MAP.items():
                by_type[template_type] = by_type.get(template_type, 0) + 1
            
            for template_type, count in sorted(by_type.items()):
                logging.info(f"   {template_type}: {count} components")
            
            logging.info(f"\n{get_missing_references_summary()}")
            logging.info("✅ All sheets processed successfully.")
            break

    except KeyboardInterrupt:
        logging.warning("\n👋 Migration interrupted by user")
        tracker.end()
        # Still generate reports on interrupt
        report = tracker.generate_report()
        ts_print(report)
    except Exception as e:
        logging.error(f"💥 Fatal error in main loop: {e}", exc_info=True)
        tracker.end()
    finally:
        save_component_cache()


def upload_dummy_components():
    
    cds = CoreDataService([DUMMY_TEMPLATE_MAP["flights"]])

    flight_component_fields = [
        {"templateId": DUMMY_TEMPLATE_MAP["flights"][1], "data": {}},
        {"templateId": DUMMY_TEMPLATE_MAP["flights"][0], "data": {}},
    ]

    base = {
        "orgId":"swoop",
        "destination":"unspecified",
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
        "partners": [],
        "regions": [],
        "name": "Flight",
        "externalName":"Flight",
        "media": {
            "images": [],
            "videos": []
        },
        "componentFields": flight_component_fields,
    }
    flight_copy = base.copy()
    cds.pushValidRowToDB([flight_copy], "Flight")

    independent_arrangements_component_fields = [
        {"templateId": DUMMY_TEMPLATE_MAP["independent_arrangements"][1], "data": {}},
        {"templateId": DUMMY_TEMPLATE_MAP["independent_arrangements"][0], "data": {}},
    ]

    independent_arrangement = base.copy()
    independent_arrangement["name"] = "Independent Arrangement"
    independent_arrangement["externalName"] = "Independent Arrangement"

    independent_arrangement["templateId"] = DUMMY_TEMPLATE_MAP["independent_arrangements"][1]
    independent_arrangement["componentFields"] = independent_arrangements_component_fields
    
    cds = CoreDataService(DUMMY_TEMPLATE_MAP["independent_arrangements"])
    cds.pushValidRowToDB([independent_arrangement], "Independent Arrangement")

    fee_component_fields = [
        {"templateId": DUMMY_TEMPLATE_MAP["fee"][1], "data": {}},
        {"templateId": DUMMY_TEMPLATE_MAP["fee"][0], "data": {}},
    ]

    independent_arrangement = base.copy()
    independent_arrangement["name"] = "Fee"
    independent_arrangement["externalName"] = "Fee"
    independent_arrangement["templateId"] = DUMMY_TEMPLATE_MAP["fee"][1]
    independent_arrangement["componentFields"] = fee_component_fields
    
    cds = CoreDataService(DUMMY_TEMPLATE_MAP["fee"])
    cds.pushValidRowToDB([independent_arrangement], "Fee")

import json
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tracker import MigrationTracker, OperationStatus, RowResult, SheetSummary

class CoreDataService:
    def __init__(self, template_ids, tracker=None, sheet_name=None):
        self.template_ids = template_ids
        self.service_url = 'https://data-api-dev.swoop-adventures.com'
        # self.service_url = 'http://localhost:8080'

        self.headers = {
            "Authorization": f"Bearer {ACCESS_TOKEN}",
        }
        self.tracker = tracker
        self.sheet_name = sheet_name

    def _fetch_schema(self, template_id):
        url = f"{self.service_url}/core-data-service/v1/template/{template_id}"
        try:
            res = requests.get(url, headers=self.headers)
            res.raise_for_status()
            response = res.json()
        except Exception as e:
            ts_print(f"⚠️ Failed fetching schema for {template_id}: {e}")
            return {}

        schema_str = response.get("validationSchemas", {}).get("componentSchema")
        if schema_str:
            try:
                return json.loads(schema_str)
            except Exception as e:
                ts_print(f"⚠️ Failed parsing schema for {template_id}: {e}")
        return {}

    def getSchemaWithArrayLevel(self):
        schemas = []
        for tid in self.template_ids:
            schema = self._fetch_schema(tid)
            schemas.append(schema)
        return schemas

    def _upload_component(self, component, template_type, idx, overwrite_on_fail=True):
        start_time = datetime.now()
        component_name = component.get("name", "Untitled")

        if component_name in COMPONENT_BLACKLIST:
            ts_print("Skipping blacklisted or empty row")
            if self.tracker and self.sheet_name:
                self.tracker.add_sheet_result(RowResult(
                    sheet_name=self.sheet_name,
                    row_number=idx + 2,
                    component_name=component_name,
                    status=OperationStatus.UPLOAD_ERROR,
                    error_details={"message": "Skipping blacklisted or empty row"},
                    template_type=template_type,
                    component=component
                ))
            return None

        if component.get("destination") == "antarctic":
            component["destination"] = "antarctica"

        pregenerated_id = generate_component_id(component)
        url = ""
        final_error = None

        try:
            if pregenerated_id:
                url = f"{self.service_url}/core-data-service/v1/component/{pregenerated_id}"
                res = requests.post(url, json=component, headers=self.headers)
            else:
                url = f"{self.service_url}/core-data-service/v1/component"
                res = requests.post(url, json=component, headers=self.headers)
        except Exception as e:
            final_error = f"Request failed: {e}"
            res = None

        # Success on POST
        if res and res.status_code in [200, 201, 202]:
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            return self._process_success_response(res, component, template_type, idx, duration_ms)

        # Retry with PATCH ONLY if status code is 409 (conflict)
        if (
            overwrite_on_fail
            and pregenerated_id
            and res is not None
            and res.status_code == 409
        ):
            try:
                component_copy = component.copy()
                component_copy.pop('templateId', None)
                component_copy.pop('name', None)
                component_copy.pop('orgId', None)

                patch_res = requests.patch(url, json=component_copy, headers=self.headers)

                if patch_res.status_code in [200, 201, 202]:
                    duration_ms = (datetime.now() - start_time).total_seconds() * 1000
                    return self._process_success_response(patch_res, component, template_type, idx, duration_ms)
                else:
                    try:
                        final_error = patch_res.json()
                    except:
                        final_error = f"PATCH HTTP {patch_res.status_code}"
            except Exception as e:
                final_error = f"PATCH exception: {e}"

        # FINAL FAILURE
        if final_error is None and res is not None:
            try:
                final_error = res.json()
            except:
                final_error = f"HTTP {res.status_code}"

        ts_print(f"❌ Failed to upload row {idx+1}. Error: {final_error}")

        if self.tracker and self.sheet_name:
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            self.tracker.add_sheet_result(RowResult(
                sheet_name=self.sheet_name,
                row_number=idx + 2,
                component_name=component_name,
                status=OperationStatus.UPLOAD_ERROR,
                error_details={"message": f"Upload failed: {final_error}"},
                duration_ms=duration_ms,
                template_type=template_type,
                component=component
            ))

        return None


    def _process_success_response(self, res, component, template_type, idx, duration_ms):
        """Helper to handle successful POST/PATCH responses"""
        
        component_name = component.get("name", "Untitled")
        
        try:
            data = res.json()
            comp_id = data.get("id")
            template_id = component.get("templateId")

            if comp_id and component_name and template_id:
                COMPONENT_ID_MAP[(template_type, component_name)] = comp_id
                ts_print(f"✅ Row {idx+1} - ({template_type}, {component_name}) -> {comp_id}")
                component['id'] = comp_id
                
                # Track successful upload
                if self.tracker and self.sheet_name:
                    self.tracker.add_sheet_result(RowResult(
                        sheet_name=self.sheet_name,
                        row_number=idx + 2,
                        component_name=component_name,
                        component_id=comp_id,
                        status=OperationStatus.SUCCESS,
                        duration_ms=duration_ms,
                        template_type=template_type,
                        component=component
                    ))
                
                return component
        except Exception as e:
            ts_print(f"⚠️ Could not parse returned ID for row {idx+1}: {e}")
            if self.tracker and self.sheet_name:
                self.tracker.add_sheet_result(RowResult(
                    sheet_name=self.sheet_name,
                    row_number=idx + 2,
                    component_name=component_name,
                    status=OperationStatus.UPLOAD_ERROR,
                    error_details={"message": f"Failed to parse response: {str(e)}"},
                    duration_ms=duration_ms,
                    template_type=template_type,
                    component=component
                ))
        return None

    def pushValidRowToDB(self, components, template_type, max_workers=10):
        uploaded_components = []


        def upload_and_update(comp_idx, comp):
            result = self._upload_component(comp, template_type, comp_idx)
            return result

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_idx = {
                executor.submit(upload_and_update, idx, comp): idx
                for idx, comp in enumerate(components)
            }
            for future in as_completed(future_to_idx):
                result = future.result()
                if result:
                    uploaded_components.append(result)

        return uploaded_components


import logging
from datetime import datetime

# Setup logging configuration
def setup_logging():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"{log_dir}/migration_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()  # Also print to console
        ]
    )
    return log_file

if __name__ == "__main__":
    run_loop()