import os
import json
import pandas as pd

from mappings.activity import map_activity_component
from mappings.cruise_pkg import map_cruise_bundle
from mappings.location import map_location_component
from mappings.ground_accom import map_ground_accommodation_component
from mappings.ship_accom import map_ship_accommodation_component
from mappings.journey import map_journey_component

from validate_csv_dynamic import validate_csv
from core_data_services import CoreDataService


DEBUG_MODE = False  # toggle this to switch between dry-run and real upload
DEBUG_OUTPUT_FILE = "debug_output.ndjson"

# Global map for later reference: (templateId, name) -> componentId
COMPONENT_ID_MAP = {}

SHEET_TEMPLATE_MAP = {
    "Location": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_d0904afaccec4ef69057572ff77e2790", # Location
    ],
    "Ground Accom": [
        "template_aca16a46ec3842ca85d182ee9348f627",  # Base
        "template_c265e31c0c2848fa8210050f452d3926",  # Accom
        "template_d32b51f46e7946faa5d3e2aa33e7d29a",  # Ground Accom
    ],
    "Journeys": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_fe4243df590147a09a546e2f177cdcf3", # Journeys
    ],
    "All Activities - For Upload": [
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
        "template_e2f0e9e5343349358037a0564a3366a0"
    ]
    # "Ship Accom": [
    #     "template_63766858b3f444a890574fd849d8e273",  # Ship
    #     "template_b70cd1388f5e49a4be344253215dd473",  # Accom
    #     "template_aca16a46ec3842ca85d182ee9348f627",  # Base
    # ],
    # "Cruise Packages": [
    #     "template_0c2ff80e37ab4632b808b45cfa79d2cd",  # Cruise
    #     "template_ee591e8d618542e2933819ac2a441af4",  # Packages
    #     "template_aca16a46ec3842ca85d182ee9348f627",  # Base
    # ]
}

SHEET_ROW_MAPPERS = {
    # Name of Sheet    : mapping function

    "Location"         : map_location_component,
    "Ground Accom"     : map_ground_accommodation_component,
    "Journeys"         : map_journey_component,
    "All Activities - For Upload": map_activity_component
    # "Ship Accom"     : map_ship_accommodation_component,
    # "Cruise Packages": map_cruise_bundle,

}

TEMPLATE_TYPES = {
    "template_d0904afaccec4ef69057572ff77e2790": "location",
    # "template_40f3b745b3f841caa2a7ee9631f21a26": "Accommodation", # Referenced by bundle "component type"
    # "template_0c2ff80e37ab4632b808b45cfa79d2cd": "Cruise"
}

PAT_COMPONENTS_PATH = "pat_components.xlsx"
ANT_COMPONENTS_PATH = "ant_components.xlsx"

COMPONENTS_PATH = PAT_COMPONENTS_PATH # Just using this for the moment - obv will need both!

def run_loop():
    print("🔁 XLSX Validator and Migration App (Ctrl+C or type 'exit' to quit)")

    try:
        while True:
            if not os.path.exists(COMPONENTS_PATH):
                print("❌ File not found.")
                continue

            try:
                xls = pd.read_excel(COMPONENTS_PATH, sheet_name=None)

                # Make duplicate column names unique (Day, Day.1, Day.2, etc.)
                def dedup_columns(columns):
                    seen = {}
                    new_cols = []
                    for col in columns:
                        if col not in seen:
                            seen[col] = 0
                            new_cols.append(col)
                        else:
                            seen[col] += 1
                            new_cols.append(f"{col}.{seen[col]}")
                    return new_cols

                for sheet, df_sheet in xls.items():
                    df_sheet.columns = dedup_columns(df_sheet.columns)

                    # drop the first data row (row index 0 is the header, row index 1 is metadata)
                    df_sheet = df_sheet.iloc[1:].reset_index(drop=True)

                    xls[sheet] = df_sheet  # overwrite back into the dict


            except Exception as e:
                print(f"❌ Error reading Excel file: {e}")
                continue

            for sheet_name, df in xls.items():
                if sheet_name not in SHEET_TEMPLATE_MAP or sheet_name not in SHEET_ROW_MAPPERS:
                    continue

                print(f"\n📄 Processing Sheet: {sheet_name}")

                template_ids = SHEET_TEMPLATE_MAP[sheet_name]
                row_mapper = SHEET_ROW_MAPPERS[sheet_name]
                core_data_service = CoreDataService(template_ids)

                schemas = core_data_service.getSchemaWithArrayLevel()


                # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                # df = df.head(20) # MUST TAKE THIS OUT (limits script to only read/upload first n records)
                # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

                try:
                    results, parsed_json = validate_csv(
                        df,
                        schemas,
                        template_ids,
                        lambda row: row_mapper(row, template_ids, COMPONENT_ID_MAP)
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
                insert = "y"#input(f"Do you want to insert '{sheet_name}' data into the database? (y/n): ").strip().lower()
                if insert == "y":
                    # print("📦 Preview JSON:")
                    # print(json.dumps(parsed_json, indent=2))

                    push = "y"#input("Are you sure to push these into database? (y/n): ").strip().lower()
                    if push == "y":
                        print("🛜 Calling API ...")
                        core_data_service.pushValidRowToDB(parsed_json)
                        print(f"✅ Data from '{sheet_name}' inserted into database.")
                    else:
                        print("⏩ Skipping database insert.")

            print("\n📋 Final Component ID Map (templateId, name) -> id")
            for k, v in COMPONENT_ID_MAP.items():
                print(f"{k} -> {v}")

            print("✅ All sheets processed.")
            break

    except KeyboardInterrupt:
        print("\n👋 Exiting the app. Goodbye!")


import requests

class CoreDataService:
    def __init__(self, template_ids):
        self.template_ids = template_ids
        self.service_url = 'https://data-api-dev.swoop-adventures.com'

    def getSchemaWithArrayLevel(self):
        schemas = []
        for idx, template_id in enumerate(self.template_ids):
            res = requests.get(f"{self.service_url}/core-data-service/v1/templates/{template_id}")
            response = res.json()

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

    def pushValidRowToDB(self, components):
        if DEBUG_MODE:
            print(f"📝 DEBUG MODE ON: Writing {len(components)} components to {DEBUG_OUTPUT_FILE} instead of uploading.")
            with open(DEBUG_OUTPUT_FILE, "a", encoding="utf-8") as f:
                for comp in components:
                    f.write(json.dumps(comp, ensure_ascii=False) + "\n")
            return
        for idx, component in enumerate(components):
            res = requests.post(f"{self.service_url}/core-data-service/v1/components", json=component)
            if res.status_code == 201:
                print(f"✅ Row {idx + 1} has been pushed!")
                try:
                    data = res.json()
                    comp_id = data.get("id")
                    comp_name = component.get("name")
                    template_id = component.get("templateId")
                    if comp_id and comp_name and template_id:
                        template_name = TEMPLATE_TYPES[template_id]
                        COMPONENT_ID_MAP[(template_name, comp_name)] = comp_id
                        print(f"   ↳ Stored in ID map: ({template_name}, {comp_name}) -> {comp_id}")
                except Exception as e:
                    print(f"⚠️ Could not parse returned ID for row {idx+1}: {e}")
            else:
                try:
                    error_msg = res.json()
                    print(f"❌ Failed to push Row {idx + 1}. Error: {error_msg}")
                except Exception:
                    print(f"❌ Failed to push Row {idx + 1}. HTTP Status: {res.status_code}")


if __name__ == "__main__":
    run_loop()
