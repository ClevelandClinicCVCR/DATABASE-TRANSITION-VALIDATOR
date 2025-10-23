from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from data_class.TableMapping import TableMapping

from .DataMatchValidationResult import DataMatchValidationResult
from .SchemaValidationResult import SchemaValidationResult
from .TableMapping import TableMapping
from .ValidationStatus import ValidationStatus


@dataclass
class OverallValidationResult:
    """Overall results of the complete validation process."""

    validation_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    schema_validation_results: List[SchemaValidationResult] = field(
        default_factory=list
    )
    data_match_validation_result: List[DataMatchValidationResult] = field(
        default_factory=list
    )
    data_match_validation_result_grouped: Dict[
        str, List[DataMatchValidationResult]
    ] = field(default_factory=dict)
    summary_stats: Dict[str, Any] = field(default_factory=dict)
    settings: Dict[str, Any] = field(default_factory=dict)

    @property
    def total_execution_time(self) -> float:
        """Total execution time in seconds."""
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

    @property
    def overall_status_table_result(self) -> ValidationStatus:
        """Determine overall validation status."""
        if not self.data_match_validation_result:
            return ValidationStatus.SKIP

        failed_tables = [
            r
            for r in self.data_match_validation_result
            if r.status == ValidationStatus.FAIL
        ]
        warning_tables = [
            r
            for r in self.data_match_validation_result
            if r.status == ValidationStatus.WARNING
        ]

        if failed_tables:
            return ValidationStatus.FAIL
        elif warning_tables:
            return ValidationStatus.WARNING
        else:
            return ValidationStatus.PASS

    @property
    def success_summary(self) -> Dict[str, Any]:
        """Generate success summary statistics."""
        total_tables = len(self.data_match_validation_result)

        # passed_tables without issues
        successful_tables = len(
            [r for r in self.data_match_validation_result if r.is_successful]
        )
        # passed_tables with some warning issues
        passed_tables = len(
            [
                r
                for r in self.data_match_validation_result
                if r.status == ValidationStatus.PASS
            ]
        )
        failed_tables = len(
            [
                r
                for r in self.data_match_validation_result
                if r.status == ValidationStatus.FAIL
            ]
        )
        warning_tables = len(
            [
                r
                for r in self.data_match_validation_result
                if r.status == ValidationStatus.WARNING
            ]
        )

        total_source_records = sum(
            r.source_count if r.source_count and r.source_count > 0 else 0
            for r in self.data_match_validation_result
        )
        total_target_records = sum(
            r.target_count if r.target_count and r.target_count > 0 else 0
            for r in self.data_match_validation_result
        )
        total_matching_records = sum(
            (
                r.matching_records
                if r.matching_records and r.matching_records > 0
                else 0
            )
            for r in self.data_match_validation_result
        )

        return {
            "total_tables": total_tables,
            "successful_tables": successful_tables,  # passed_tables without issues
            "passed_tables": passed_tables,  # passed_tables with some warning issues
            "failed_tables": failed_tables,
            "warning_tables": warning_tables,
            "success_rate_tables": (
                (successful_tables / total_tables * 100)
                if total_tables > 0
                else 0
            ),
            "total_source_records": total_source_records,
            "total_target_records": total_target_records,
            "total_matching_records": total_matching_records,
            "overall_data_success_rate": (
                (total_matching_records / total_source_records * 100)
                if total_source_records > 0
                else 0
            ),
            "execution_time_seconds": self.total_execution_time,
        }

    @property
    def overall_status_schema_result(self) -> ValidationStatus:
        """Determine overall validation status."""
        if not self.schema_validation_results:
            return ValidationStatus.SKIP

        failed_tables = [
            r
            for r in self.schema_validation_results
            if r.status == ValidationStatus.FAIL
        ]
        warning_tables = [
            r
            for r in self.schema_validation_results
            if r.status == ValidationStatus.WARNING
        ]

        if failed_tables:
            return ValidationStatus.FAIL
        elif warning_tables:
            return ValidationStatus.WARNING
        else:
            return ValidationStatus.PASS

    @property
    def overall_status(self) -> ValidationStatus:
        """Determine overall validation status considering both table and schema results."""
        table_status = self.overall_status_table_result
        schema_status = self.overall_status_schema_result

        if (
            table_status == ValidationStatus.FAIL
            or schema_status == ValidationStatus.FAIL
        ):
            return ValidationStatus.FAIL
        elif (
            table_status == ValidationStatus.WARNING
            or schema_status == ValidationStatus.WARNING
        ):
            return ValidationStatus.WARNING
        elif (
            table_status == ValidationStatus.PASS
            and schema_status == ValidationStatus.PASS
        ):
            return ValidationStatus.PASS
        else:
            return ValidationStatus.SKIP

    @property
    def overall_status_row_count_result(self) -> ValidationStatus:
        """Determine overall row count validation status."""
        if not self.data_match_validation_result:
            return ValidationStatus.SKIP

        failed_tables = [
            r
            for r in self.data_match_validation_result
            if r.row_count_status == ValidationStatus.FAIL
        ]
        warning_tables = [
            r
            for r in self.data_match_validation_result
            if r.row_count_status == ValidationStatus.WARNING
        ]

        if failed_tables:
            return ValidationStatus.FAIL
        elif warning_tables:
            return ValidationStatus.WARNING
        else:
            return ValidationStatus.PASS

    @property
    def overall_status_data_match_result(self) -> ValidationStatus:
        """Determine overall data match validation status."""
        if not self.data_match_validation_result:
            return ValidationStatus.SKIP

        failed_tables = [
            r
            for r in self.data_match_validation_result
            if r.get_data_match_status == ValidationStatus.FAIL
        ]
        warning_tables = [
            r
            for r in self.data_match_validation_result
            if r.get_data_match_status == ValidationStatus.WARNING
        ]

        if failed_tables:
            return ValidationStatus.FAIL
        elif warning_tables:
            return ValidationStatus.WARNING
        else:
            return ValidationStatus.PASS

    @property
    def overall_status_rule_based_data_validation_result(
        self,
    ) -> ValidationStatus:
        """Determine overall rule-based data validation status."""
        if not self.data_match_validation_result:
            return ValidationStatus.SKIP

        for table in self.data_match_validation_result:
            if (
                table.compare_sample_data_result
                and table.compare_sample_data_result.rule_based_data_validation
            ):
                rule_based_data_validation = (
                    table.compare_sample_data_result.rule_based_data_validation
                )
                if (
                    rule_based_data_validation.get("source")
                    and rule_based_data_validation[
                        "source"
                    ].failed_records_count
                ):
                    if any(
                        value > 0
                        for key, value in rule_based_data_validation[
                            "source"
                        ].failed_records_count.items()
                    ):
                        return ValidationStatus.FAIL

                if (
                    rule_based_data_validation.get("target")
                    and rule_based_data_validation[
                        "target"
                    ].failed_records_count
                ):
                    if any(
                        value > 0
                        for key, value in rule_based_data_validation[
                            "target"
                        ].failed_records_count.items()
                    ):
                        return ValidationStatus.FAIL

        return ValidationStatus.PASS

    def sort_results_and_add_data_match_validation_result_grouped(
        self, settings: Dict[str, Any]
    ) -> "OverallValidationResult":
        """Sorts the schema and data match results by severity order."""

        report_sorting_settings = settings.get("report_sorting_settings", {})

        for sort in report_sorting_settings.get("schema_report", []):
            if sort.get("sort_by") == "severity_status":
                reverse = sort.get("sort_order", "ascending") == "descending"
                self.schema_validation_results.sort(
                    key=lambda x: ValidationStatus.SEVERITY_ORDER.value.get(
                        str(x.status.value), 0
                    ),
                    reverse=reverse,
                )

        for sort in report_sorting_settings.get("row_count_report", []):
            if sort.get("sort_by") == "severity_status":
                reverse = sort.get("sort_order", "ascending") == "descending"
                self.data_match_validation_result.sort(
                    key=lambda x: ValidationStatus.SEVERITY_ORDER.value.get(
                        str(x.row_count_status.value), 0
                    ),
                    reverse=reverse,
                )

        for sort in report_sorting_settings.get("data_match_report", []):
            if sort.get("sort_by") == "severity_status":
                reverse = sort.get("sort_order", "ascending") == "descending"
                self.data_match_validation_result.sort(
                    key=lambda x: ValidationStatus.SEVERITY_ORDER.value.get(
                        str(x.get_data_match_status.value), 0
                    ),
                    reverse=reverse,
                )

        # put the data match validation results with the same group together
        if self.data_match_validation_result:
            grouped = defaultdict(list)
            no_group = []
            for item in self.data_match_validation_result:
                if item.group:
                    grouped[item.group].append(item)
                else:
                    no_group.append(item)

            for sort in report_sorting_settings.get(
                "detailed_data_match_report", []
            ):
                if sort.get("sort_by") == "group_name":
                    reverse = (
                        sort.get("sort_order", "ascending") == "descending"
                    )
                    # Sort the grouped dictionary by group name
                    grouped = dict(
                        sorted(
                            grouped.items(),
                            key=lambda x: x[0],
                            reverse=reverse,
                        )
                    )

            for sort in report_sorting_settings.get(
                "detailed_data_match_report", []
            ):
                if sort.get("sort_by") == "key_columns_length":
                    reverse = (
                        sort.get("sort_order", "ascending") == "descending"
                    )
                    # Within each group in the grouped dictionary, sort by key columns length
                    for group_name in grouped.keys():
                        grouped[group_name] = sorted(
                            grouped[group_name],
                            key=lambda x: len(x.key_columns),
                            reverse=reverse,
                        )

            for sort in report_sorting_settings.get(
                "detailed_data_match_report", []
            ):
                if sort.get("sort_by") == "table_view_name":
                    reverse = (
                        sort.get("sort_order", "ascending") == "descending"
                    )
                    # Within each group in the grouped dictionary, sort by table view name
                    for group_name in grouped.keys():
                        grouped[group_name] = sorted(
                            grouped[group_name],
                            key=lambda x: x.source_table,
                            reverse=reverse,
                        )

            # Flatten: items with group (sorted by group name), then items without group
            grouped_items = []
            for group_name in grouped.keys():
                grouped_items.extend(grouped[group_name])

            grouped_items.extend(no_group)

            self.data_match_validation_result = grouped_items

            self.data_match_validation_result_grouped = grouped
            self.data_match_validation_result_grouped["_no_group_"] = no_group

        return self
