from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from data_class.TableMapping import TableMapping

from .ValidationIssue import ValidationIssue
from .ValidationStatus import ValidationStatus


@dataclass
class DataMatchValidationResult:
    """Results of validation for a single table."""

    table_name: str
    source_table: str
    target_table: str
    key_columns: List[str]
    unique_data_mapping_id: str
    status: ValidationStatus
    group: Optional[str] = None
    source_table_exist: bool = True
    target_table_exist: bool = True
    source_table_or_view: str = ""
    target_table_or_view: str = ""
    source_count: int = 0
    target_count: int = 0
    matching_records: int = 0
    sample_size: int = 0
    data_transformation_rules: List[str] = None
    row_count_validation_issues: List[ValidationIssue] = field(
        default_factory=list
    )
    data_match_validation_issues: List[ValidationIssue] = field(
        default_factory=list
    )
    execution_time_seconds: float = 0.0
    compare_sample_data_result: Optional["CompareSampleDataResult"] = None

    @property
    def percent_count_difference(self) -> float:
        """Calculate percent difference between source and target counts."""
        if not isinstance(self.source_count, int) or not isinstance(
            self.target_count, int
        ):
            return 100.0

        if self.source_count < 0 or self.target_count < 0:
            return 100.0

        if self.source_count == 0:
            if self.target_count == 0:
                return 0.0
            return 100.0

        return (
            (self.source_count - self.target_count)
            / float(self.source_count)
            * 100
        )

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.source_count == 0:
            return 0.0
        total_count = (
            self.sample_size if self.sample_size else self.source_count
        )
        # Convert to float to handle Decimal types from database
        return (float(self.matching_records) / float(total_count)) * 100

    @property
    def is_successful(self) -> bool:
        """Check if validation is considered successful."""
        return self.status == ValidationStatus.PASS and all(
            issue.severity == ValidationStatus.PASS
            for issue in self.data_match_validation_issues
        )

    @property
    def row_count_status(self) -> ValidationStatus:
        """Check for row count validation status."""
        if any(
            issue.severity == ValidationStatus.FAIL
            for issue in self.row_count_validation_issues
        ):
            return ValidationStatus.FAIL

        if any(
            issue.severity == ValidationStatus.WARNING
            for issue in self.row_count_validation_issues
        ):
            return ValidationStatus.WARNING

        return ValidationStatus.PASS

    @property
    def get_data_match_status(self) -> ValidationStatus:
        """Check for data match validation status."""
        if self.status == ValidationStatus.FAIL or any(
            issue.severity == ValidationStatus.FAIL
            for issue in self.data_match_validation_issues
        ):
            return ValidationStatus.FAIL

        if self.status == ValidationStatus.WARNING or any(
            issue.severity == ValidationStatus.WARNING
            for issue in self.data_match_validation_issues
        ):
            return ValidationStatus.WARNING

        return ValidationStatus.PASS
