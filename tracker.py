from dataclasses import dataclass, field
from datetime import datetime
import json
from typing import List, Dict, Optional
from enum import Enum

class OperationStatus(Enum):
    SUCCESS = "success"
    CACHED = "cached"
    VALIDATION_ERROR = "validation_error"
    UPLOAD_ERROR = "upload_error"
    MAPPING_ERROR = "mapping_error"

@dataclass
class RowResult:
    sheet_name: str
    row_number: int
    component_name: str
    status: OperationStatus
    component_id: Optional[str] = None
    error_details: Optional[Dict] = None
    duration_ms: Optional[float] = None
    template_type: Optional[str] = None
    component: Optional[Dict] = None

@dataclass
class SheetSummary:
    sheet_name: str
    success_count: int = 0
    cached_count: int = 0
    validation_errors: int = 0
    upload_errors: int = 0
    mapping_errors: int = 0
    duration_seconds: float = 0
    rows: List[RowResult] = field(default_factory=list)

    @property
    def total_rows(self) -> int:
        return len(self.rows)
    
    def add_result(self, result: RowResult):
        self.rows.append(result)
        if result.status == OperationStatus.SUCCESS:
            self.success_count += 1
        elif result.status == OperationStatus.CACHED:
            self.cached_count += 1
        elif result.status == OperationStatus.VALIDATION_ERROR:
            self.validation_errors += 1
        elif result.status == OperationStatus.UPLOAD_ERROR:
            self.upload_errors += 1
        elif result.status == OperationStatus.MAPPING_ERROR:
            self.mapping_errors += 1
    
    def success_rate(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return ((self.success_count + self.cached_count) / self.total_rows) * 100

class MigrationTracker:
    def __init__(self):
        self.sheets: Dict[str, SheetSummary] = {}
        self.start_time = None
        self.end_time = None
    
    def start(self):
        self.start_time = datetime.now()
    
    def end(self):
        self.end_time = datetime.now()
    
    def add_sheet_result(self, result: RowResult):
        if result.component_name == "Untitled": return
        if result.sheet_name not in self.sheets:
            self.sheets[result.sheet_name] = SheetSummary(
                sheet_name=result.sheet_name,
                total_rows=0
            )
        self.sheets[result.sheet_name].add_result(result)
    
    def generate_report(self) -> str:
        """Generate a comprehensive text report"""
        lines = []
        lines.append("=" * 80)
        lines.append("MIGRATION REPORT")
        lines.append("=" * 80)
        
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            lines.append(f"Duration: {duration:.2f} seconds")
        
        lines.append(f"Sheets Processed: {len(self.sheets)}")
        lines.append("")
        
        # Overall statistics
        total_rows = sum(s.total_rows for s in self.sheets.values())
        total_success = sum(s.success_count for s in self.sheets.values())
        total_cached = sum(s.cached_count for s in self.sheets.values())
        total_errors = sum(s.validation_errors + s.upload_errors + s.mapping_errors 
                          for s in self.sheets.values())
        
        lines.append("OVERALL SUMMARY")
        lines.append("-" * 80)
        lines.append(f"Total Rows Processed: {total_rows}")
        lines.append(f"✅ Successful Uploads: {total_success}")
        lines.append(f"♻️  Cached (Skipped): {total_cached}")
        lines.append(f"❌ Total Errors: {total_errors}")
        overall_success_rate = ((total_success + total_cached) / total_rows * 100) if total_rows > 0 else 0
        lines.append(f"📊 Success Rate: {overall_success_rate:.1f}%")
        lines.append("")
        
        # Per-sheet breakdown
        for sheet_name, summary in self.sheets.items():
            lines.append(f"SHEET: {sheet_name}")
            lines.append("-" * 80)
            lines.append(f"Total Rows: {summary.total_rows}")
            lines.append(f"✅ Successful: {summary.success_count}")
            lines.append(f"♻️  Cached: {summary.cached_count}")
            lines.append(f"❌ Validation Errors: {summary.validation_errors}")
            lines.append(f"❌ Upload Errors: {summary.upload_errors}")
            lines.append(f"❌ Mapping Errors: {summary.mapping_errors}")
            lines.append(f"📊 Success Rate: {summary.success_rate():.1f}%")
            
            # Show error details
            error_rows = [r for r in summary.rows if r.status != OperationStatus.SUCCESS 
                         and r.status != OperationStatus.CACHED]
            if error_rows:
                lines.append(f"\n  Error Details:")
                for row in error_rows[:10]:  # Show first 10 errors
                    lines.append(f"    Row {row.row_number} ({row.component_name}): {row.status.value}")
                    if row.error_details:
                        lines.append(f"      → {row.error_details}")
                if len(error_rows) > 10:
                    lines.append(f"    ... and {len(error_rows) - 10} more errors")
            
            lines.append("")
        
        lines.append("=" * 80)
        return "\n".join(lines)
    
    def export_to_json(self, filepath: str):
        """Export detailed results to JSON for further analysis with field completeness stats"""
        data = {
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": (self.end_time - self.start_time).total_seconds() 
                                if self.start_time and self.end_time else None,
            "sheets": {},
            "overall_field_completeness": {}
        }

        # Fields to check
        fields_to_check = [
            "description.web",
            "description.quote",
            "description.final",
            "destination",
            "tripId",
            "partners",
            "regions",
            "media.images",
            "package"
        ]

        # Overall counters
        overall_counts = {f: 0 for f in fields_to_check}
        overall_total_rows = 0

        def get_nested_value(obj, path):
            """Utility to get nested value from dict using dot notation"""
            keys = path.split(".")
            val = obj
            for k in keys:
                if isinstance(val, dict):
                    val = val.get(k)
                else:
                    return None
            return val

        for sheet_name, summary in self.sheets.items():
            sheet_counts = {f: 0 for f in fields_to_check}
            sheet_total_rows = len(summary.rows)
            overall_total_rows += sheet_total_rows

            for r in summary.rows:
                comp = r.component or {}
                for f in fields_to_check:
                    val = get_nested_value(comp, f)
                    # Special handling for lists
                    if isinstance(val, list):
                        if val:
                            sheet_counts[f] += 1
                            overall_counts[f] += 1
                    # Check truthy values
                    elif val not in (None, "", {}):
                        sheet_counts[f] += 1
                        overall_counts[f] += 1

            # Per-sheet completeness percentages
            sheet_completeness = {
                f: (sheet_counts[f] / sheet_total_rows * 100 if sheet_total_rows else 0)
                for f in fields_to_check
            }

            data["sheets"][sheet_name] = {
                "total_rows": summary.total_rows,
                "success_count": summary.success_count,
                "cached_count": summary.cached_count,
                "validation_errors": summary.validation_errors,
                "upload_errors": summary.upload_errors,
                "mapping_errors": summary.mapping_errors,
                "success_rate": summary.success_rate(),
                "field_completeness": sheet_completeness,
                "rows": [
                    {
                        "row_number": r.row_number,
                        "component_name": r.component_name,
                        "status": r.status.value,
                        "component_id": r.component_id,
                        "error_details": r.error_details,
                        "duration_ms": r.duration_ms,
                        # "template_type": r.template_type,
                        # "component": r.component,
                    }
                    for r in summary.rows
                ]
            }

        # Overall completeness percentages
        overall_field_completeness = {
            f: (overall_counts[f] / overall_total_rows * 100 if overall_total_rows else 0)
            for f in fields_to_check
        }
        data["overall_field_completeness"] = overall_field_completeness

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
