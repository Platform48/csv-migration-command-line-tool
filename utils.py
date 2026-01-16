import json
import math
import re
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

# def get_stripped(row, field):
#     """
#     Safely get a string value from a DataFrame row (Series or dict).
#     Returns a stripped string, never a Series.
#     """
#     # Try dict-style access first
#     val = row.get(field) if hasattr(row, "get") else None
    
#     # Fallback to Series-style access
#     if val is None and hasattr(row, "__getitem__") and field in row:
#         val = row[field]

#     # If it’s a Series, pick first non-null value
#     if isinstance(val, pd.Series):
#         val = val.dropna().astype(str).iloc[0] if not val.dropna().empty else ""

#     # Handle NaN / None
#     if pd.isna(val):
#         return ""

#     return str(val).strip()



def safe_float(val):
    try:
        v = float(val)
        if math.isnan(v): return None
        return v
    except (ValueError, TypeError):
        return None

def safe_int(val, default=-1):
    try:
        v = int(val)
        return v
    except (ValueError, TypeError):
        return default


def parse_html_list(text: str, field_name: str):
    """
    Parse an HTML bullet point list into a list of strings.
    Args:
        text: The text to parse
        field_name: The name of the field to parse
    Returns:
        A list of strings
    """
    if not text:
        return []

    if "<li" not in text.lower():
        print(f"[WARNING] Expected HTML list with <li> for '{field_name}', got: {text[:80]}.")
        return []

    items = re.findall(r"<li[^>]*>(.*?)</li>", text, flags=re.IGNORECASE | re.DOTALL)
    cleaned = []
    for item in items:
        no_tags = re.sub(r"<[^>]+>", " ", item)
        cleaned_item = " ".join(no_tags.split()).strip()
        if cleaned_item:
            cleaned.append(cleaned_item)
    return cleaned

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
                
                # FIX 1: Handle None values safely
                field = context.get("field") or context.get("additional_info") or "unknown field"
                missing_name = entry["component_name"] or "Unknown"
                comp_type = entry["component_type"] or "Unknown"
                
                by_sheet[sheet].append({
                    "row": row_num,
                    "field": str(field),  # Ensure it's a string
                    "missing": str(missing_name),  # Ensure it's a string
                    "type": str(comp_type)  # Ensure it's a string
                })
        
        # Write each sheet's issues
        for sheet_name, issues in by_sheet.items():
            report_lines.append(f"SHEET: {sheet_name}")
            issues_sorted = sorted(
                issues,
                key=lambda x: x["row"] if isinstance(x["row"], int) else 999999
            )

            # FIX 2: Handle edge case where issues_sorted might be empty
            if not issues_sorted:
                report_lines.append("No issues found.")
                report_lines.append("")
                continue

            # FIX 3: Add minimum widths to prevent zero-width columns
            row_col_width    = max(max(len(str(issue["row"])) for issue in issues_sorted), 3) + 5
            field_col_width  = max(max(len(str(issue["field"])) for issue in issues_sorted), 8) + 2
            miss_col_width   = max(max(len(str(issue["missing"])) for issue in issues_sorted), 16) + 2
            type_col_width   = max(max(len(str(issue["type"])) for issue in issues_sorted), 4) + 8
            
            report_lines.append("-" * (row_col_width + field_col_width + miss_col_width + type_col_width + 9))
            report_lines.append(
                f"{'Row':<{row_col_width}} | {'Row Name':<{field_col_width}} | {'Missing Component':<{miss_col_width}} | {'Type':<{type_col_width}}"
            )
            report_lines.append("-" * (row_col_width + field_col_width + miss_col_width + type_col_width + 9))
            
            for issue in issues_sorted:
                row_display = str(issue["row"]) if isinstance(issue["row"], int) else "?"
                # FIX 4: Handle potential Unicode/special characters
                missing_quoted = f'"{issue["missing"]}"'
                field_clean = str(issue["field"])[:50]  # Truncate very long field names
                type_clean = str(issue["type"])[:20]   # Truncate very long type names
                
                report_lines.append(
                    f'{row_display:<{row_col_width}} | {field_clean:<{field_col_width}} | {missing_quoted:<{miss_col_width}} | {type_clean:<{type_col_width}}'
                )
            report_lines.append("")

        readable_file = "MISSING_COMPONENTS_REPORT.txt"
        # FIX 5: More robust file writing with error handling
        with open(readable_file, 'w', encoding='utf-8', errors='replace') as f:
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
            comp_type = str(entry.get("component_type", "Unknown"))
            if comp_type not in by_type:
                by_type[comp_type] = []
            by_type[comp_type].append(entry)

        for comp_type, entries in by_type.items():
            unique_lines.append(f"TYPE: {comp_type}")
            unique_lines.append("-" * 80)

            # FIX 6: Handle edge case where entries might be empty
            if not entries:
                unique_lines.append("No entries found.")
                unique_lines.append("")
                continue

            # Sort alphabetically
            entries_sorted = sorted(
                entries,
                key=lambda e: (-e.get("occurrences", 0), str(e.get("component_name", "")).lower())
            )
            
            # FIX 7: Add minimum width and handle empty component names
            name_width = max(max(len(str(e.get("component_name", ""))) for e in entries_sorted), 16) + 4

            unique_lines.append(f"{'Missing Component':<{name_width}} | Occurrences")
            unique_lines.append("-" * (name_width + 15))

            for entry in entries_sorted:
                component_name = str(entry.get("component_name", "Unknown"))
                occurrences = entry.get("occurrences", 0)
                missing_quoted = f'"{component_name}"'
                unique_lines.append(
                    f'{missing_quoted:<{name_width}} | {occurrences}'
                )
            unique_lines.append("")

        unique_file = "MISSING_COMPONENTS_BY_TYPE.txt"
        # FIX 8: More robust file writing with error handling
        with open(unique_file, 'w', encoding='utf-8', errors='replace') as f:
            f.write('\n'.join(unique_lines))
        
        # -----------------------------------------------------------------
        # PRINT SUMMARY
        # -----------------------------------------------------------------
        print(f"📋 Saved detailed report: {readable_file}")
        print(f"📋 Saved unique-by-type report: {unique_file}")
        print(f"   {len(MISSING_REFERENCES)} unique missing components logged")

    except Exception as e:
        print(f"⚠️ Error saving missing references reports: {e}")
        import traceback
        traceback.print_exc()  # This will help debug the exact error



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

def get_journey_id(journey_name: str, component_id_map: Dict, context: Optional[Dict] = None) -> Optional[str]:
    
    return get_component_id(
        component_type="journey",
        component_name=journey_name,
        component_id_map=component_id_map,
        # aliases=LOCATION_ALIASES,
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