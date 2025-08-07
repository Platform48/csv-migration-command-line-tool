import os
import json
import pandas as pd
from customizable_mapping import map_location_component
from validate_csv_dynamic import validate_csv
from core_data_services import CoreDataService

SHEET_TEMPLATE_MAP = {
    "Locations": [
        "template_6662df87de064104a81422a351d5ce1c", # Location
        "template_aca16a46ec3842ca85d182ee9348f627", # Base
    ],
}

SHEET_ROW_MAPPERS = {
    "Locations": map_location_component,
}

COMPONENTS_PATH = "components.xlsx"

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
                    results, parsed_json = validate_csv(df, schemas, template_ids, lambda row: row_mapper(row, template_ids))
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

            print("✅ All sheets processed.")
            break

    except KeyboardInterrupt:
        print("\n👋 Exiting the app. Goodbye!")

if __name__ == "__main__":
    run_loop()
