# Swoop Data Migration Tool

This tool is used to validate, transform, and upload component data from the pat_components.xlsx spreadsheet into the dev environment via the Core Data Service backend. It handles schema fetching, validation, mapping, ID generation, caching, and full upload tracking.

The orignal sheets can be found at https://docs.google.com/spreadsheets/d/1T2mMaDgjcgl68B_wwwxwemTKKJx_6ESyl5ujfzFRsNs

## Overview

This migration tool:

- Reads multiple sheets from the master Excel file.
- Maps each row into a Core Data Service component.
- Locally validates each row against the remote template schemas.
- Uploads components to the dev environment via API.
- Caches component IDs to avoid re-uploading unchanged components.
- Generates migration summary reports and missing-reference logs.

**Note:**
This tool only uploads to the DEV environment (https://data-api-dev.swoop-adventures.com).
If you want data in UAT, you must manually transfer MongoDB data. (Instructions below.)

## Authentication (VERY IMPORTANT)

Before running the script, you must generate an access token using gcloud and paste it into the ACCESS_TOKEN variable in the script.

1. Generate a token:
   gcloud auth print-access-token

2. Open the `app.py` and paste the token:

```python
ACCESS_TOKEN = "PASTE_YOUR_TOKEN_HERE"
```

Without this token, all schema lookups and uploads will fail.

**WARNING** Access Token Expiry

This token has a 1 hour expiry. Because the tool can take up to 20-30min (if migrating all sheets), it is strongly advised to generate a new token before a run.

Unfortunately `gcloud auth print-access-token` reprints any existing, unexpired token. To force-generate a new token, you must revoke your old one:

```b
gcloud auth revoke
gcloud auth login
gcloud auth print-access-token
```

## Selecting Which Sheets to Migrate

The tool does nothing by default because all sheets in SHEET_PROCESS_ORDER are commented out:

```python
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
```

### To migrate specific data:

Uncomment the sheet names you want. Ensure you keep the releative order the same, to ensure component dependencies are uploaded in the correct order.

```python
SHEET_PROCESS_ORDER = [
    "Location",
    "Ground Accom",
    "Journeys"
]
```

The tool will then process only those sheets in this exact order.

## Performing a Clean Run (Resetting the State)

Because the tool uses caching and persistent state, sometimes you need a completely fresh migration.
A clean run requires:

1. Delete the component cache file

   `rm component_id_cache.json`

   This forces the tool to treat all components as new.

2. Reset the DEV MongoDB database

   You must wipe the Core Data dev DB before running a clean migration:

   Drop the `components` collection in the dev environment in MondoDB

## Migrating Data to UAT (Manual Step)

This tool cannot upload to UAT.
Instead, the correct process is:

1. Run the migration script normally (which uploads to dev environment)
2. Export dev `components` collection as JSON
3. Drop the `components` collection in the UAT environment in MondoDB (**Note:** Make a backup first)
4. Import the exported dev JSON file into the matching UAT database

## Running the Tool

1. Ensure `ACCESS_TOKEN` is set, valid, and will not expire within 20-30min

2. Uncomment the sheet names you want to process

3. (Optional) Delete cache + reset DB for a clean run

4. Run:

   `python app.py`

## Files Generated After Every Run

Each run produces three important diagnostic files:

- `MISSING_COMPONENTS_BY_TYPE.txt`

  Lists missing references grouped by type.

- `MISSING_COMPONENTS_REPORT.txt`

  List missing references in order of appearance (i.e. grouped by sheet/row).

- `logs/migration_report_\<timestamp>.json`

  A full log of all uploads, and report of field completeness.

These files help diagnose missing dependencies or mismatched names.

## Component Cache System

To speed up re-runs, the tool writes:

`component_id_cache.json`

Stores previously uploaded component IDs

Stores hashes of component data to detect changes

## Dummy Components

At the end of every run, the tool automatically uploads 3 "dummy" components:

- Flight

- Independent Arrangement

- Fee
