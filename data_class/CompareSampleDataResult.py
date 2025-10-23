from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from .DataMatchValidationResult import DataMatchValidationResult
from .DataTypesCompatibleResult import DataTypesCompatibleResult
from .DistributionBasedDataValidationResult import (
    DistributionBasedDataValidationResult,
)
from .OverallValidationResult import OverallValidationResult
from .RuleBasedDataValidationResult import RuleBasedDataValidationResult
from .SchemaValidationResult import SchemaValidationResult
from .TableMapping import TableMapping
from .ValidationIssue import ValidationIssue
from .ValidationStatus import ValidationStatus


@dataclass
class CompareSampleDataResult:
    """Results of compare_sample_data for a single table."""

    table_mapping: TableMapping
    sample_size: int
    source_sample_count: int
    target_sample_count: int
    source_sample_set_count: int
    target_sample_set_count: int
    matching_set_record_count: int
    rule_based_data_validation: Optional[
        Dict[Literal["source", "target"], RuleBasedDataValidationResult]
    ] = None
    distribution_based_data_validation: Optional[
        Dict[
            Literal["source", "target"], DistributionBasedDataValidationResult
        ]
    ] = None

    data_match_validation_issues: List[ValidationIssue] = field(
        default_factory=list
    )

    matching_keys_set_sample: List[Any] = field(default_factory=list)
    source_unmatched_keys_sample: List[Any] = field(default_factory=list)
    target_unmatched_keys_sample: List[Any] = field(default_factory=list)

    @property
    def success_rate_of_2_sets(self) -> float:
        """
        Calculate success rate of comparing two sample sets.
        Success Rate = (matching_record_count / max(source_count, target_count)) * 100
        """
        denominator = max(
            self.source_sample_set_count, self.target_sample_set_count
        )
        if denominator == 0:
            return 0.0  # avoid division by zero
        return (self.matching_set_record_count / denominator) * 100

    @property
    def interpolated_success_rate_of_tables_from_success_rate_of_2_sets(
        self,
    ) -> float:
        """
        Calculate success rate of comparing two tables.
        """
        nominator = min(self.source_sample_count, self.target_sample_count)
        denominator = max(self.source_sample_count, self.target_sample_count)
        if denominator == 0:
            return 0.0  # avoid division by zero
        return (self.success_rate_of_2_sets * nominator) / denominator

    @property
    def interpolated_matching_records_of_tables_from_success_rate(self) -> int:
        """
        Calculate matching records of comparing two tables.
        """
        min_sample_count = min(
            self.source_sample_count, self.target_sample_count
        )
        if self.sample_size and self.sample_size > 0:
            min_sample_count = min(min_sample_count, self.sample_size)

        return int(self.success_rate_of_2_sets * min_sample_count / 100)
