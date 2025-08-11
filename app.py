import os
import json
import pandas as pd
from customizable_mapping import map_location_component, map_ground_accommodation_component, map_ship_accommodation_component, map_cruise_bundle
from validate_csv_dynamic import validate_csv
from core_data_services import CoreDataService

# Global map for later reference: (templateId, name) -> componentId
COMPONENT_ID_MAP = {}

SHEET_TEMPLATE_MAP = {
    # "Locations": [
    #     "template_6662df87de064104a81422a351d5ce1c", # Location
    #     "template_aca16a46ec3842ca85d182ee9348f627", # Base
    # ],
    # "Ground Accom": [
    #     "template_40f3b745b3f841caa2a7ee9631f21a26",  # Ground Accom
    #     "template_b70cd1388f5e49a4be344253215dd473",  # Accom
    #     "template_aca16a46ec3842ca85d182ee9348f627",  # Base
    # ],
    # "Ship Accom": [
    #     "template_63766858b3f444a890574fd849d8e273",  # Ship
    #     "template_b70cd1388f5e49a4be344253215dd473",  # Accom
    #     "template_aca16a46ec3842ca85d182ee9348f627",  # Base
    # ],
    "Cruise Packages": [
        "template_0c2ff80e37ab4632b808b45cfa79d2cd",  # Cruise
        "template_ee591e8d618542e2933819ac2a441af4",  # Packages
        "template_aca16a46ec3842ca85d182ee9348f627",  # Base
    ]
}

SHEET_ROW_MAPPERS = {
    # "Locations": map_location_component,
    # "Ground Accom": map_ground_accommodation_component,
    # "Ship Accom": map_ship_accommodation_component,
    "Cruise Packages": map_cruise_bundle,

}

PAT_COMPONENTS_PATH = "pat_components.xlsx"
ANT_COMPONENTS_PATH = "ant_components.xlsx"

COMPONENTS_PATH = ANT_COMPONENTS_PATH # Just using this for the moment - obv will need both!


def run_loop():
    print("🔁 XLSX Validator and Migration App (Ctrl+C or type 'exit' to quit)")

    try:
        while True:
            if not os.path.exists(COMPONENTS_PATH):
                print("❌ File not found.")
                continue

            try:
                xls = pd.read_excel(COMPONENTS_PATH, sheet_name=None)
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

                try:
                    results, parsed_json = validate_csv(
                        df,
                        schemas,
                        template_ids,
                        lambda row: row_mapper(row, template_ids)
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
                insert = input(f"Do you want to insert '{sheet_name}' data into the database? (y/n): ").strip().lower()
                if insert == "y":
                    print("📦 Preview JSON:")
                    print(json.dumps(parsed_json, indent=2))

                    push = input("Are you sure to push these into database? (y/n): ").strip().lower()
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
                        COMPONENT_ID_MAP[(template_id, comp_name)] = comp_id
                        print(f"   ↳ Stored in ID map: ({template_id}, {comp_name}) -> {comp_id}")
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
