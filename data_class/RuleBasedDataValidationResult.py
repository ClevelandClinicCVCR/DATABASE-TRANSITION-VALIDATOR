from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from data_class.TableMapping import TableMapping

from .ValidationIssue import ValidationIssue
from .ValidationStatus import ValidationStatus


@dataclass
class RuleBasedDataValidationResult:
    """Results of rule_based_data_validation for a single table."""

    table_mapping: TableMapping
    total_records: int
    passed_records_count: int
    failed_records_count: int
    validation_issues: List[ValidationIssue] = field(default_factory=list)
    success_rate: float = 0.0
    overall_status: ValidationStatus = ValidationStatus.SKIP
    failed_record_samples: List[Dict[str, Any]] = field(default_factory=list)
    success_record_samples: List[Dict[str, Any]] = field(default_factory=list)
    # Or details about which rules were applied
    applied_rules: List[str] = field(default_factory=list)
