from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from .ValidationIssue import ValidationIssue
from .ValidationStatus import ValidationStatus


@dataclass
class SchemaValidationResult:
    """Results of schema validation between source and target."""

    source_table_name: str
    target_table_name: str
    status: ValidationStatus
    validation_issues: List[ValidationIssue] = field(default_factory=list)
    source_col_names: List[str] = field(default_factory=list)
    target_col_names: List[str] = field(default_factory=list)
    missing_columns: List[str] = None
    extra_columns: List[str] = None
    type_mismatches: List[Dict[str, Any]] = None
    source_table_or_view: str = ""
    target_table_or_view: str = ""
