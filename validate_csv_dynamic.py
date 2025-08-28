import json
from jsonschema import validate, ValidationError



from jsonschema import validate, ValidationError

def validate_row(row: dict, schema: dict, row_number: int = None):
    try:
        validate(instance=row, schema=schema)
        return True, row
    except ValidationError as e:
        path = ".".join([str(p) for p in e.path]) or "root"
        schema_path = " → ".join([str(p) for p in e.schema_path])
        
        error_details = {
            "row": row_number,
            "property": path,
            "message": e.message,
            "invalid_value": repr(e.instance),
            "validator": e.validator,
            "expected": e.validator_value,
            "schema_path": schema_path,
        }
        
        # Return False and a *rich error object* instead of just a string
        return False, [error_details]


def validate_csv(df, schemas, template_ids, row_mapper):
    UNMAPPED_REGIONS = set()
    
    results = []
    parsed_rows = []

    for df_index, df_row in df.iterrows():
        row_number = df_index + 2  # Excel offset
        try:
            row = row_mapper(df_row, row_number)
        except Exception as e:
            results.append({
                "row": row_number,
                "valid": False,
                "errors": [{
                    "row": row_number,
                    "property": "row_mapper",
                    "message": f"Exception during row mapping: {repr(e)}",
                    "invalid_value": None,
                    "validator": None,
                    "expected": None,
                    "schema_path": None,
                }]
            })
            continue

        final_res = []

        if isinstance(row, dict) and "componentFields" in row:
            valid = True
            for level, (schema, template_id) in enumerate(zip(schemas, template_ids)):
                try:
                    nested = next(
                        (cf["data"] for cf in row["componentFields"] if cf["templateId"] == template_id),
                        None
                    )

                    if nested is None:
                        is_valid = False
                        outcome = [{
                            "row": row_number,
                            "property": "componentFields",
                            "message": f"Missing data for template {template_id}",
                            "invalid_value": None,
                            "validator": "required",
                            "expected": template_id,
                            "schema_path": f"componentFields → {template_id}"
                        }]
                    else:
                        is_valid, outcome = validate_row(nested, schema, row_number=row_number)

                except Exception as e:
                    is_valid = False
                    outcome = [{
                        "row": row_number,
                        "property": "componentFields",
                        "message": f"Exception validating template {template_id} (level {level}): {repr(e)}",
                        "invalid_value": None,
                        "validator": None,
                        "expected": None,
                        "schema_path": f"componentFields → {template_id}"
                    }]

                results.append({
                    "row": row_number,
                    "valid": is_valid,
                    "errors": outcome if not is_valid else []
                })

                if not is_valid:
                    valid = False

            if valid:
                parsed_rows.append(row)

        else:
            # Fallback: handle as list of schema-aligned dicts
            for level, (schema, nested) in enumerate(zip(schemas, row)):
                try:
                    is_valid, outcome = validate_row(nested, schema, row_number=row_number)
                except Exception as e:
                    is_valid = False
                    outcome = [{
                        "row": row_number,
                        "property": f"nested_schema[{level}]",
                        "message": f"Exception validating nested schema at level {level}: {repr(e)}",
                        "invalid_value": None,
                        "validator": None,
                        "expected": None,
                        "schema_path": None,
                    }]

                results.append({
                    "row": row_number,
                    "valid": is_valid,
                    "errors": outcome if not is_valid else []
                })
                final_res.append(outcome if is_valid else {})

            parsed_rows.append(final_res)

    # 👇 Report unmapped regions at the end
    if UNMAPPED_REGIONS:
        print("\n⚠️ Unmapped region names detected:")
        for region in sorted(UNMAPPED_REGIONS):
            print(f"  - {region}")

    return results, parsed_rows
