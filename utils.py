import json
import pandas as pd

def printTemplateList(template_list):
    for idx, t in enumerate(template_list):
        json_schema = json.loads(t['jsonSchema'])
        print(f"{idx + 1} .")
        print(f"version : {t['version']}")
        print(json.dumps(json_schema, indent= 2))
        print ("__________________________________________")

def get_stripped(row, field):
    val = row.get(field)
    if pd.isna(val):
        return ""
    return str(val).strip()

def safe_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def safe_int(val, default=-1):
    try:
        v = int(val)
        return v
    except (ValueError, TypeError):
        return default
