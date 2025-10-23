from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from data_class.TableMapping import TableMapping

from .ValidationIssue import ValidationIssue
from .ValidationStatus import ValidationStatus


@dataclass
class DistributionBasedDataValidationResult:
    """Results of distribution_based_data_validation for a single table."""

    table_mapping: TableMapping
    total_records: int
    validation_issues: List[ValidationIssue] = field(default_factory=list)
    overall_status: ValidationStatus = ValidationStatus.SKIP
    values_to_count: Dict[Any, int] = field(default_factory=dict)
    min_items_count: Optional[int] = None
    max_items_count: Optional[int] = None
