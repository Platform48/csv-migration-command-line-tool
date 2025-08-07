import json
from jsonschema import validate, ValidationError



def validate_row(row: dict, schema: dict, row_number: int = None):
    try:
        validate(instance=row, schema=schema)
        return True, row
    except ValidationError as e:
        path = ".".join([str(p) for p in e.path]) or "root"
        return False, [f"Row {row_number}: {path}: {e.message}"]

def validate_csv(df, schemas, template_ids, row_mapper):
    UNMAPPED_REGIONS = set()
    
    mapped_data = df.apply(row_mapper, axis=1).tolist()
    results = []
    parsed_rows = []

    for idx, row in enumerate(mapped_data, start=2):
        final_res = []

        if isinstance(row, dict) and "componentFields" in row:
            valid = True
            for level, (schema, template_id) in enumerate(zip(schemas, template_ids)):
                nested = next((cf["data"] for cf in row["componentFields"] if cf["templateId"] == template_id), None)

                if nested is None:
                    is_valid = False
                    outcome = [f"Row {idx}: Missing data for template {template_id}"]
                else:
                    is_valid, outcome = validate_row(nested, schema, row_number=idx)

                results.append({
                    "row": idx,
                    "valid": is_valid,
                    "errors": outcome if not is_valid else []
                })

                if not is_valid:
                    valid = False

            if valid:
                parsed_rows.append(row)  # ✅ Append the full component

        else:
            # Fallback: handle as list of schema-aligned dicts
            for schema, nested in zip(schemas, row):
                is_valid, outcome = validate_row(nested, schema, row_number=idx)
                results.append({
                    "row": idx,
                    "valid": is_valid,
                    "errors": outcome if not is_valid else []
                })
                final_res.append(outcome if is_valid else {})

            parsed_rows.append(final_res)

    # 👇 ADD THIS before the return statement
    if UNMAPPED_REGIONS:
        print("\n⚠️ Unmapped region names detected:")
        for region in sorted(UNMAPPED_REGIONS):
            print(f"  - {region}")

    return results, parsed_rows

