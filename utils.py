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

import json
import os
from datetime import datetime
from typing import Optional, Dict, Tuple, Any

# Missing references log file
MISSING_REFS_LOG = "missing_component_references.json"

# Global store for missing references (per session)
MISSING_REFERENCES = {}

def get_component_id(
    component_type: str,
    component_name: str,
    component_id_map: Dict[Tuple[str, str], str],
    aliases: Optional[Dict[str, str]] = None,
    context: Optional[Dict[str, Any]] = None,
    required: bool = True
) -> Optional[str]:
    """
    Get component ID from type and name, with logging for missing references.
    
    Args:
        component_type: Type of component (e.g., "location", "activity")
        component_name: Name of the component to find
        component_id_map: The global component ID mapping
        aliases: Optional dictionary of name aliases/corrections
        context: Optional context info (row data, sheet name, etc.) for better logging
        required: Whether this reference is required (affects logging level)
    
    Returns:
        Component ID if found, None if not found
    """
    if not component_name or not str(component_name).strip():
        return None
    
    # Clean the name
    clean_name = component_name.strip()
    
    # Try aliases first if provided
    if aliases and clean_name in aliases:
        original_name = clean_name
        clean_name = aliases[clean_name]
        print(f"🔄 Using alias: '{original_name}' → '{clean_name}'")
    
    # Look up in component map
    lookup_key = (component_type, clean_name)
    component_id = component_id_map.get(lookup_key)
    
    if component_id:
        return component_id
    
    # Not found - log it
    if required:
        log_missing_reference(component_type, clean_name, "Component not found in cache", context, original_name=component_name if aliases else None)
        print(f"❌ WARNING: No {component_type} ID found for '{clean_name}'")
    
    return None


def log_missing_reference(
    component_type: str,
    component_name: str,
    issue: str,
    context: Optional[Dict[str, Any]] = None,
    original_name: Optional[str] = None
):
    """Log a missing component reference for client review"""
    
    # Create unique key for this missing reference
    key = f"{component_type}:{component_name}"
    
    # Build the log entry
    log_entry = {
        "component_type": component_type,
        "component_name": component_name,
        "original_name": original_name,
        "issue": issue,
        "first_seen": datetime.now().isoformat(),
        "occurrences": 1,
        "contexts": []
    }
    
    # Add context information
    if context:
        context_info = {
            "timestamp": datetime.now().isoformat(),
            "sheet_name": context.get("sheet_name"),
            "row_name": context.get("row_name"),
            "row_index": context.get("row_index"),
            "additional_info": context.get("additional_info")
        }
        log_entry["contexts"].append(context_info)
    
    # Update global missing references
    if key in MISSING_REFERENCES:
        MISSING_REFERENCES[key]["occurrences"] += 1
        MISSING_REFERENCES[key]["contexts"].extend(log_entry["contexts"])
        MISSING_REFERENCES[key]["last_seen"] = datetime.now().isoformat()
    else:
        MISSING_REFERENCES[key] = log_entry


def save_missing_references_log():
    """Save missing references into two files:
       1. By-sheet detailed report
       2. By-type unique references summary
    """
    if not MISSING_REFERENCES:
        return
    
    try:
        # -----------------------------------------------------------------
        # 1. HUMAN-READABLE REPORT BY SHEET
        # -----------------------------------------------------------------
        report_lines = [
            "MISSING COMPONENT REFERENCES REPORT (BY SHEET)",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "WHAT TO FIX:",
            "The following component names in your Excel sheets don't match any uploaded components.",
            "Please check the spelling or make sure these components exist in the system.",
            "",
            "=" * 100,
            ""
        ]
        
        # Group by sheet for easier reading
        by_sheet = {}
        for entry in MISSING_REFERENCES.values():
            for context in entry["contexts"]:
                sheet = context.get("sheet_name", "Unknown Sheet")
                if sheet not in by_sheet:
                    by_sheet[sheet] = []
                
                row_num = context.get("row_index", "?")
                if row_num is not None and str(row_num).isdigit():
                    row_num = int(row_num) + 1  # adjust for header + 0-based index
                
                field = context.get("field", context.get("additional_info", "unknown field"))
                missing_name = entry["component_name"]
                comp_type = entry["component_type"]
                
                by_sheet[sheet].append({
                    "row": row_num,
                    "field": field,
                    "missing": missing_name,
                    "type": comp_type
                })
        
        # Write each sheet's issues
        for sheet_name, issues in by_sheet.items():
            report_lines.append(f"SHEET: {sheet_name}")
            issues_sorted = sorted(
                issues,
                key=lambda x: x["row"] if isinstance(x["row"], int) else 999999
            )

            row_col_width    = max(len(str(issue["row"])) for issue in issues_sorted) + 5
            field_col_width  = max(len(str(issue["field"])) for issue in issues_sorted) + 2
            miss_col_width   = max(len(str(issue["missing"])) for issue in issues_sorted) + 2
            type_col_width   = max(len(str(issue["type"])) for issue in issues_sorted) + 8
            
            report_lines.append("-" * (row_col_width + field_col_width + miss_col_width + type_col_width + 9))
            report_lines.append(
                f"{'Row':<{row_col_width}} | {'Row Name':<{field_col_width}} | {'Missing Component':<{miss_col_width}} | {'Type':<{type_col_width}}"
            )
            report_lines.append("-" * (row_col_width + field_col_width + miss_col_width + type_col_width + 9))
            
            for issue in issues_sorted:
                row_display = str(issue["row"]) if isinstance(issue["row"], int) else "?"
                missing_quoted = f'"{issue["missing"]}"'  # wrap BEFORE padding
                report_lines.append(
                    f'{row_display:<{row_col_width}} | {issue["field"]:<{field_col_width}} | {missing_quoted:<{miss_col_width}} | {issue["type"]:<{type_col_width}}'
                )
            report_lines.append("")

        readable_file = "MISSING_COMPONENTS_REPORT.txt"
        with open(readable_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        
        # -----------------------------------------------------------------
        # 2. UNIQUE MISSING REFERENCES BY TYPE
        # -----------------------------------------------------------------
        unique_lines = [
            "MISSING COMPONENT REFERENCES REPORT (BY TYPE)",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "This report groups all unique missing component references by type.",
            "Each entry shows the component name and how many times it was missing.",
            "",
            "=" * 100,
            ""
        ]

        # Group by type
        by_type = {}
        for entry in MISSING_REFERENCES.values():
            comp_type = entry["component_type"]
            if comp_type not in by_type:
                by_type[comp_type] = []
            by_type[comp_type].append(entry)

        for comp_type, entries in by_type.items():
            unique_lines.append(f"TYPE: {comp_type}")
            unique_lines.append("-" * 80)

            # Sort alphabetically
            entries_sorted = sorted(
                entries,
                key=lambda e: (-e["occurrences"], e["component_name"].lower())
            )
            name_width = max(len(e["component_name"]) for e in entries_sorted) + 4

            unique_lines.append(f"{'Missing Component':<{name_width}} | Occurrences")
            unique_lines.append("-" * (name_width + 15))

            for entry in entries_sorted:
                missing_quoted = f'"{entry["component_name"]}"'  # wrap BEFORE padding
                unique_lines.append(
                    f'{missing_quoted:<{name_width}} | {entry["occurrences"]}'
                )
            unique_lines.append("")

        unique_file = "MISSING_COMPONENTS_BY_TYPE.txt"
        with open(unique_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(unique_lines))
        
        # -----------------------------------------------------------------
        # PRINT SUMMARY
        # -----------------------------------------------------------------
        print(f"📋 Saved detailed report: {readable_file}")
        print(f"📋 Saved unique-by-type report: {unique_file}")
        print(f"   {len(MISSING_REFERENCES)} unique missing components logged")

    except Exception as e:
        print(f"⚠️ Error saving missing references reports: {e}")



def clear_missing_references_session():
    """Clear the current session's missing references (but keep the log file)"""
    global MISSING_REFERENCES
    MISSING_REFERENCES = {}


def get_missing_references_summary():
    """Get summary of current session's missing references"""
    if not MISSING_REFERENCES:
        return "No missing references in current session."
    
    summary = {}
    for entry in MISSING_REFERENCES.values():
        comp_type = entry["component_type"]
        summary[comp_type] = summary.get(comp_type, 0) + 1
    
    total = len(MISSING_REFERENCES)
    return f"Missing references in session: {total} total ({summary})"


# Helper function for your existing mappers
def get_location_id(location_name: str, component_id_map: Dict, context: Optional[Dict] = None) -> Optional[str]:
    """Convenience function specifically for location lookups"""
    from mappings.location import LOCATION_ALIASES  # Import your existing aliases
    
    return get_component_id(
        component_type="location",
        component_name=location_name,
        component_id_map=component_id_map,
        aliases=LOCATION_ALIASES,
        context=context,
        required=True
    )

# Helper function for your existing mappers
def get_transfer_id(transfer_name: str, component_id_map: Dict, context: Optional[Dict] = None) -> Optional[str]:
    """Convenience function specifically for location lookups"""
    # from mappings.location import LOCATION_ALIASES  # Import your existing aliases
    
    return get_component_id(
        component_type="transfer",
        component_name=transfer_name,
        component_id_map=component_id_map,
        # aliases=LOCATION_ALIASES,
        context=context,
        required=True
    )



def get_activity_id(activity_name: str, component_id_map: Dict, context: Optional[Dict] = None) -> Optional[str]:
    """Convenience function specifically for activity lookups"""
    return get_component_id(
        component_type="activity",
        component_name=activity_name,
        component_id_map=component_id_map,
        context=context,
        required=True
    )


def get_accommodation_id(accom_name: str, component_id_map: Dict, context: Optional[Dict] = None) -> Optional[str]:
    """Convenience function specifically for accommodation lookups"""
    return get_component_id(
        component_type="accommodation",
        component_name=accom_name,
        component_id_map=component_id_map,
        context=context,
        required=True
    )