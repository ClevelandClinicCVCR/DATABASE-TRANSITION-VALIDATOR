"""Main database transition validator implementation."""

import logging
import numbers
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from tkinter import N
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine.base import Engine

from build_set_from_sample_and_columns import build_set_from_sample_and_columns
from data_class.CompareSampleDataResult import CompareSampleDataResult
from data_class.DatabaseConfig import DatabaseConfig
from data_class.DataMatchValidationResult import DataMatchValidationResult
from data_class.DataTypesCompatibleResult import DataTypesCompatibleResult
from data_class.DistributionBasedDataValidationResult import (
    DistributionBasedDataValidationResult,
)
from data_class.OverallValidationResult import OverallValidationResult
from data_class.RuleBasedDataValidationResult import (
    RuleBasedDataValidationResult,
)
from data_class.SchemaValidationResult import SchemaValidationResult
from data_class.TableMapping import TableMapping
from data_class.ValidationIssue import ValidationIssue
from data_class.ValidationStatus import ValidationStatus
from load_default_validation_settings import load_default_validation_settings
from normalize_item_data_end_with_dot_0 import (
    normalize_item_data_end_with_dot_0,
)
from text_mean_none import text_mean_none


class DatabaseTransitionValidator:
    """Main class for validating database transitions from Teradata to SQL Server."""

    COLUMN_TYPE_COMPATIBLE_MAPPINGS = {
        "PASS": {
            # Lossless Conversions
            "INT": ["INTEGER", "BIGINT", "DECIMAL"],
            "INTEGER": ["INT", "BIGINT", "DECIMAL"],
            "VARCHAR": ["NVARCHAR", "TEXT"],
            "NUMERIC": ["DECIMAL", "FLOAT"],
            "DATE": ["DATETIME", "TIMESTAMP"],
            "BINARY": ["VARBINARY"],
            "VARBINARY": ["BINARY"],
            "BIT": ["BOOLEAN", "TINYINT", "BOOL", "INTEGER"],
            "SMALLINT": ["INTEGER"],
            "BOOLEAN": ["BIT", "BOOL"],
            # -----------------------
            # Minimal risk conversions
            "TIMESTAMP": ["DATE", "DATETIME"],
            "DATETIME": ["DATE", "TIMESTAMP"],
            "DECIMAL": ["NUMERIC", "FLOAT", "DOUBLE"],
            "NVARCHAR": ["VARCHAR", "TEXT"],
            "BIGINT": ["INT", "INTEGER", "DECIMAL"],
            "FLOAT": ["REAL", "DOUBLE"],
            "REAL": ["FLOAT", "DOUBLE"],
            "DOUBLE": ["FLOAT", "REAL"],
            "CHAR": ["NCHAR", "VARCHAR", "TEXT"],
            "NCHAR": ["CHAR"],
            "TEXT": ["VARCHAR", "NVARCHAR"],
        },
        "WARNING": {
            # Potentially Lossy Conversions or different data manipulation
            "DECIMAL": ["INT", "INTEGER", "BIGINT", "DECIMAL"],
            "INTEGER": ["BIT", "FLOAT", "VARCHAR"],
            "FLOAT": ["INT", "INTEGER", "BIGINT"],
            "DOUBLE": ["INT", "INTEGER", "BIGINT"],
            "NUMERIC": ["INT", "INTEGER", "BIGINT"],
            "BIGINT": ["INT", "INTEGER", "DECIMAL", "NUMERIC"],
        },
    }

    def __init__(
        self,
        source_config: DatabaseConfig,
        target_config: DatabaseConfig,
        settings: Dict[str, Any] = None,
    ):
        """Initialize the validator with source and target database configurations."""
        self.source_config = source_config
        self.target_config = target_config
        self.settings = (
            settings if settings else load_default_validation_settings()
        )
        self.logger = logging.getLogger(__name__)

        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

        self.database_inspector: dict[str, Any] = (
            self._init_database_inspector()
        )

    def set_default_number_of_set_sample_records_for_detailed_report(
        self, table_mappings: TableMapping
    ) -> TableMapping:
        """
        Set default number_of_set_sample_records_for_detailed_report for each mapping if not provided.
        """
        default_value = self.settings.get("validation_settings", {}).get(
            "number_of_set_sample_records_for_detailed_report", 5
        )
        for table_mapping in table_mappings:
            if not getattr(
                table_mapping,
                "number_of_set_sample_records_for_detailed_report",
                None,
            ):
                table_mapping.number_of_set_sample_records_for_detailed_report = (
                    default_value
                )
        return table_mappings

    def _init_database_inspector(self) -> dict[str, Any]:
        """
        Initialize database inspectors and table lists for both source and target.
        These will be reused across validation methods to avoid repeatedly creating inspectors
        and re-checking table existence during schema and data validation, thereby improving performance.
        """

        try:
            source_inspectors = (
                {
                    "inspector": inspect(self.source_config.engine),
                    "tables": [],
                    "views": [],
                }
                if self.source_config and self.source_config.engine
                else None
            )

            target_inspectors = (
                {
                    "inspector": inspect(self.target_config.engine),
                    "tables": [],
                    "views": [],
                }
                if self.target_config and self.target_config.engine
                else None
            )

            inspectors = {
                "source": source_inspectors,
                "target": target_inspectors,
            }

            if source_inspectors:
                inspectors["source"]["tables"] = inspectors["source"][
                    "inspector"
                ].get_table_names(schema=self.source_config.schema)

                inspectors["source"]["views"] = inspectors["source"][
                    "inspector"
                ].get_view_names(schema=self.source_config.schema)

            if target_inspectors:
                inspectors["target"]["tables"] = inspectors["target"][
                    "inspector"
                ].get_table_names(schema=self.target_config.schema)

                inspectors["target"]["views"] = inspectors["target"][
                    "inspector"
                ].get_view_names(schema=self.target_config.schema)

            return inspectors

        except Exception as e:
            self.logger.warning(
                f"Error initializing inspectors for {self.source_config.name} and {self.target_config.name}: {e}"
            )
            return inspectors

    def build_key_columns_cast_types_from_key_columns(
        self, table_mappings: List[TableMapping]
    ) -> List[TableMapping]:
        """
        For each TableMapping in table_mappings, build key_columns_cast_types from key_columns if not provided.
        The cast types are determined based on the source database type.
        """
        type_casting_separators = [">", "->", "=>", ":", "|"]

        # The type_casting_separators list is sorted by length in descending order, so longer separators are matched first when splitting key columns. This prevents partial matches and ensures correct parsing.
        type_casting_separators = sorted(
            type_casting_separators, key=len, reverse=True
        )

        for mapping in table_mappings:
            if mapping.key_columns and (
                not mapping.key_columns_cast_types
                or len(mapping.key_columns_cast_types)
                != len(mapping.key_columns)
            ):
                # If key_columns_cast_types is not provided or its length doesn't match key_columns, build it
                mapping.key_columns_cast_types = []
                new_key_columns = []

                for key_column in mapping.key_columns:
                    # Split key_column by any separator and extract column_name and cast_type
                    cast_type = None
                    column_name = key_column
                    for separator in type_casting_separators:
                        if separator in key_column:
                            parts = key_column.split(separator, 1)
                            column_name = parts[0].strip()
                            cast_type = (
                                parts[1].strip() if len(parts) > 1 else None
                            )
                            break
                    new_key_columns.append(column_name)
                    mapping.key_columns_cast_types.append(cast_type)

                mapping.key_columns = new_key_columns

        return table_mappings

    def validate_transition(
        self,
        table_mappings: List[TableMapping],
        max_workers: int = 4,
        sample_size: Optional[int] = None,
        enable_schema_validation: bool = True,
        enable_data_validation: bool = True,
    ) -> OverallValidationResult:
        """
        Perform complete validation of database transition.

        Args:
            table_mappings: List of table mappings to validate
            max_workers: Maximum number of parallel workers
            sample_size: Optional sample size for data validation (None = all data)
            enable_schema_validation: Whether to perform schema validation
            enable_data_validation: Whether to perform data validation

        Returns:
            OverallValidationResult with complete validation results
        """

        # for table_mappings, build key_columns_cast_types from key_columns if not provided
        table_mappings = self.build_key_columns_cast_types_from_key_columns(
            table_mappings
        )

        # Set default number_of_set_sample_records_for_detailed_report if not provided in each mapping
        table_mappings = (
            self.set_default_number_of_set_sample_records_for_detailed_report(
                table_mappings
            )
        )

        validation_id = str(uuid.uuid4())
        start_time = datetime.now()

        self.logger.info(
            f"Starting validation {validation_id} with {len(table_mappings)} tables"
        )

        result = OverallValidationResult(
            validation_id=validation_id,
            start_time=start_time,
            settings=self.settings,
        )
        result.summary_stats["sample_size"] = sample_size

        try:
            # Schema validation
            if enable_schema_validation:
                if self.source_config is None or self.target_config is None:
                    self.logger.warning(
                        "Skipping schema validation due to missing source or target database connection."
                    )
                    result.schema_validation_results = None
                else:
                    self.logger.info("Performing schema validation...")
                    schema_results = self._validate_schemas(
                        table_mappings, max_workers
                    )
                    result.schema_validation_results = schema_results

            # Data validation
            if enable_data_validation:
                self.logger.info("Performing data validation...")
                data_match_validation_result = self._validate_data(
                    table_mappings, max_workers, sample_size
                )
                result.data_match_validation_result = (
                    data_match_validation_result
                )

            result.end_time = datetime.now()
            result.summary_stats = result.success_summary

            self.logger.info(
                f"Validation {validation_id} completed in {result.total_execution_time:.2f} seconds"
            )

            return result.sort_results_and_add_data_match_validation_result_grouped(
                self.settings
            )

        except Exception as e:
            self.logger.error(f"Validation failed: {e}")
            result.end_time = datetime.now()
            raise

    def _validate_schemas(
        self, table_mappings: List[TableMapping], max_workers: int
    ) -> List[SchemaValidationResult]:
        """Validate table schemas between source and target."""
        results = []

        # Build deduplicated_mappings: keep only first occurrence of each source_table
        deduplicated_mappings: List[TableMapping] = []
        seen = set()
        for item in table_mappings:
            if item.source_table not in seen:
                deduplicated_mappings.append(item)
                seen.add(item.source_table)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit schema validation tasks for each table mapping
            schema_validation_futures = {}

            for mapping in deduplicated_mappings:
                # Submit the validation function to the executor
                schema_future = executor.submit(
                    self._validate_single_schema, mapping
                )

                # Map the future to the corresponding table mapping
                schema_validation_futures[schema_future] = mapping

            for future in as_completed(schema_validation_futures):
                mapping = schema_validation_futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    self.logger.info(
                        f"Schema validation completed for {mapping.source_table}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"Schema validation failed for {mapping.source_table}: {e}"
                    )
                    results.append(
                        SchemaValidationResult(
                            source_table_name=mapping.source_table,
                            target_table_name=mapping.target_table,
                            status=ValidationStatus.FAIL,
                            validation_issues=[
                                ValidationIssue(
                                    issue_type="schema_validation_error",
                                    description=f"Schema validation failed: {str(e)}",
                                    severity=ValidationStatus.FAIL,
                                )
                            ],
                        )
                    )

        return results

    def _validate_single_schema(
        self, mapping: TableMapping
    ) -> SchemaValidationResult:
        """Validate schema for a single table mapping."""

        validation_issues = []
        source_col_names = set()
        target_col_names = set()

        # Check if the table exists
        source_table_exist, source_table_or_view = (
            self._check_if_table_or_view_exists(
                self.source_config, mapping.source_table
            )
        )
        if not source_table_exist:
            validation_issues.append(
                ValidationIssue(
                    issue_type="no_source_table",
                    description=f"Source table '{mapping.source_table}' does not exist.",
                    severity=ValidationStatus.FAIL,
                )
            )
        else:
            source_columns = self._get_table_columns(
                self.source_config, mapping.source_table
            )
            if not source_columns:
                validation_issues.append(
                    ValidationIssue(
                        issue_type="no_columns_source_table",
                        description=f"Source table '{mapping.source_table}' has no columns or could not be inspected.",
                        severity=ValidationStatus.FAIL,
                    )
                )
            else:
                source_col_names = set(source_columns.keys())

        target_table_exist, target_table_or_view = (
            self._check_if_table_or_view_exists(
                self.target_config, mapping.target_table
            )
        )
        if not target_table_exist:
            validation_issues.append(
                ValidationIssue(
                    issue_type="no_target_table",
                    description=f"Target table '{mapping.target_table}' does not exist.",
                    severity=ValidationStatus.FAIL,
                )
            )
        else:
            target_columns = self._get_table_columns(
                self.target_config, mapping.target_table
            )
            if not target_columns:
                validation_issues.append(
                    ValidationIssue(
                        issue_type="no_column_target_table",
                        description=f"Target table '{mapping.target_table}' has no columns or could not be inspected.",
                        severity=ValidationStatus.FAIL,
                    )
                )
            else:
                target_col_names = set(target_columns.keys())

        if validation_issues:
            return SchemaValidationResult(
                source_table_name=mapping.source_table,
                target_table_name=mapping.target_table,
                source_table_or_view=source_table_or_view,
                target_table_or_view=target_table_or_view,
                status=ValidationStatus.FAIL,
                source_col_names=sorted(list(source_col_names)),
                target_col_names=sorted(list(target_col_names)),
                validation_issues=validation_issues,
            )

        # Find differences
        missing_columns = sorted(list(source_col_names - target_col_names))
        extra_columns = sorted(list(target_col_names - source_col_names))

        # Check for type mismatches in common columns
        common_columns = source_col_names & target_col_names
        type_mismatches = []
        type_mismatches_issues: list[ValidationIssue] = []

        for col in common_columns:
            source_type = str(source_columns[col]["type"])
            target_type = str(target_columns[col]["type"])

            types_compatible_check = self._are_types_compatible(
                source_type, target_type
            )
            if types_compatible_check.result != ValidationStatus.PASS:
                type_mismatches.append(
                    {
                        "column": col,
                        "source_type": source_type,
                        "target_type": target_type,
                    }
                )
                type_mismatches_issues.append(types_compatible_check.issue)

        # Determine status
        validate_schema_status = ValidationStatus.PASS

        if extra_columns:
            severity = ValidationStatus.WARNING
            validate_schema_status = (
                validate_schema_status.raise_status_level_to(severity)
            )
            validation_issues.append(
                ValidationIssue(
                    issue_type="extra_columns_in_target",
                    description=f"Target table '{mapping.target_table}' has extra {len(extra_columns)} column(s) not in the source table.",
                    severity=severity,
                )
            )
        if missing_columns:
            severity = ValidationStatus.FAIL
            validate_schema_status = (
                validate_schema_status.raise_status_level_to(severity)
            )
            validation_issues.append(
                ValidationIssue(
                    issue_type="missing_columns_in_target",
                    description=f"{len(missing_columns)} column(s) is(are) missing in target table '{mapping.target_table}'",
                    severity=severity,
                )
            )
        if type_mismatches:
            severity = ValidationStatus.WARNING
            validate_schema_status = (
                validate_schema_status.raise_status_level_to(severity)
            )

            for issue in type_mismatches_issues:
                severity = severity.raise_status_level_to(issue.severity)
                validate_schema_status = (
                    validate_schema_status.raise_status_level_to(
                        issue.severity
                    )
                )
                if issue not in validation_issues:
                    validation_issues.append(issue)

            validation_issues.append(
                ValidationIssue(
                    issue_type="type_mismatches_columns",
                    description=f"{len(type_mismatches)} column type mismatches found between source and target tables '{mapping.target_table}'",
                    severity=severity,
                )
            )

        return SchemaValidationResult(
            source_table_name=mapping.source_table,
            target_table_name=mapping.target_table,
            source_table_or_view=source_table_or_view,
            target_table_or_view=target_table_or_view,
            status=validate_schema_status,
            source_col_names=sorted(list(source_col_names)),
            target_col_names=sorted(list(target_col_names)),
            missing_columns=missing_columns,
            extra_columns=extra_columns,
            type_mismatches=type_mismatches,
            validation_issues=validation_issues,
        )

    def _get_table_columns(
        self, db_config: DatabaseConfig, table_name: str
    ) -> Dict[str, Dict[str, Any]]:
        """Get column information for a table."""
        inspector = self.database_inspector[db_config.source_or_target_type][
            "inspector"
        ]

        try:
            # Retrieve column metadata for the specified table and schema
            columns = inspector.get_columns(
                table_name, schema=db_config.schema
            )

            # Build a dictionary mapping column names to their metadata
            column_info = {}
            for col in columns:
                # Each 'col' is a dictionary with column details
                column_name = col["name"]
                column_info[column_name] = col

            return column_info
        except Exception as e:
            self.logger.warning(
                f"Could not inspect table {table_name} in {db_config.name}: {e}"
            )
            return {}

    def _are_types_compatible(
        self, source_type: str, target_type: str
    ) -> DataTypesCompatibleResult:
        """Check if two column types are compatible."""
        # Normalize type names
        source_type = source_type.upper()
        target_type = target_type.upper()

        if source_type == target_type:
            return DataTypesCompatibleResult(result=ValidationStatus.PASS)

        for (
            base_type,
            compatible_types,
        ) in self.COLUMN_TYPE_COMPATIBLE_MAPPINGS["PASS"].items():
            if base_type in source_type:
                if any(
                    target_type.startswith(comp_type)
                    for comp_type in compatible_types  # in SQL, type could be: "VARCHAR(64) COLLATE \"SQL_Latin1_General_CP1_CI_AS\"""
                ):
                    return DataTypesCompatibleResult(
                        result=ValidationStatus.PASS
                    )

        for (
            base_type,
            compatible_types,
        ) in self.COLUMN_TYPE_COMPATIBLE_MAPPINGS["WARNING"].items():
            if base_type in source_type:
                if any(
                    target_type.startswith(comp_type)
                    for comp_type in compatible_types  # in MS SQL, type could be: "VARCHAR(64) COLLATE \"SQL_Latin1_General_CP1_CI_AS\"""
                ):
                    return DataTypesCompatibleResult(
                        result=ValidationStatus.WARNING,
                        issue=ValidationIssue(
                            issue_type=f"type_compatible_{source_type}->{target_type}",
                            description=f"Column type '{source_type}' is compatible with '{target_type}' but may require attention.",
                            severity=ValidationStatus.WARNING,
                        ),
                    )

        return DataTypesCompatibleResult(
            result=ValidationStatus.FAIL,
            issue=ValidationIssue(
                issue_type=f"fail_type_compatible_{source_type}->{target_type}",
                description=f"Column type '{source_type}' is NOT compatible with '{target_type}'",
                severity=ValidationStatus.FAIL,
            ),
        )

    def _validate_data(
        self,
        table_mappings: List[TableMapping],
        max_workers: int,
        sample_size: Optional[int],
    ) -> List[DataMatchValidationResult]:
        """Validate data for all table mappings."""
        results = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit data validation tasks for each table mapping
            future_to_mapping = {}

            for mapping in table_mappings:
                # Submit the validation function to the executor
                future = executor.submit(
                    self._validate_single_table_data, mapping, sample_size
                )

                # Map the future to the corresponding table mapping
                future_to_mapping[future] = mapping

            for future in as_completed(future_to_mapping):
                mapping = future_to_mapping[future]
                try:
                    result = future.result()
                    results.append(result)
                    self.logger.info(
                        f"Data validation completed for {mapping.source_table}: {result.success_rate:.2f}% success rate"
                    )
                except Exception as e:
                    self.logger.error(
                        f"Data validation failed for {mapping.source_table}: {e}"
                    )
                    results.append(
                        DataMatchValidationResult(
                            table_name=mapping.source_table,
                            source_table=mapping.source_table,
                            target_table=mapping.target_table,
                            group=mapping.group,
                            key_columns=mapping.key_columns,
                            unique_data_mapping_id=mapping.unique_data_mapping_id,
                            status=ValidationStatus.FAIL,
                            data_match_validation_issues=[
                                ValidationIssue(
                                    issue_type="data_validation_error",
                                    description=f"Data validation failed: {str(e)}",
                                    severity=ValidationStatus.FAIL,
                                )
                            ],
                        )
                    )

        return results

    def _to_positive_integer(self, value: Any) -> int:
        """Convert a value to a positive int, returning 0 for None or invalid values."""
        try:
            return max(0, int(value))
        except (ValueError, TypeError):
            return 0

    def _validate_single_table_data(
        self, mapping: TableMapping, sample_size: Optional[int]
    ) -> DataMatchValidationResult:
        """Validate data for a single table mapping."""
        start_time = time.time()

        # Check if the table exists
        source_table_exist, source_table_or_view = (
            self._check_if_table_or_view_exists(
                self.source_config, mapping.source_table
            )
        )
        target_table_exist, target_table_or_view = (
            self._check_if_table_or_view_exists(
                self.target_config, mapping.target_table
            )
        )

        # Get row counts
        source_count = None
        target_count = None

        if self.settings.get("validation_settings", {}).get(
            "enable_row_count_validation", True
        ):
            try:
                source_count = (
                    self._get_table_count(
                        self.source_config, mapping.source_table
                    )
                    if source_table_exist
                    else None
                )
            except Exception as e:
                self.logger.warning(
                    f"Could not get row count for source table {mapping.source_table}: {e}"
                )
                source_count = None

            try:
                target_count = (
                    self._get_table_count(
                        self.target_config, mapping.target_table
                    )
                    if target_table_exist
                    else None
                )
            except Exception as e:
                self.logger.warning(
                    f"Could not get row count for target table {mapping.target_table}: {e}"
                )
                target_count = None

        row_count_validation_issues = []
        data_match_validation_issues = []

        if not source_table_exist:
            validation_issues = ValidationIssue(
                issue_type="source_table_missing",
                description=f"Could not find source table '{mapping.source_table}'.",
                severity=ValidationStatus.FAIL,
            )

            row_count_validation_issues.append(validation_issues)
            data_match_validation_issues.append(validation_issues)

        elif source_count is not None and source_count < 0:
            validation_issues = ValidationIssue(
                issue_type="source_table_count_error",
                description=f"Could not retrieve row count for source table '{mapping.source_table}'.",
                severity=ValidationStatus.FAIL,
            )

            row_count_validation_issues.append(validation_issues)
            data_match_validation_issues.append(validation_issues)

        elif source_count is not None and source_count == 0:
            validation_issues = ValidationIssue(
                issue_type="source_table_is_empty",
                description=f"Source table '{mapping.source_table}' is empty.",
                severity=ValidationStatus.WARNING,
            )

            row_count_validation_issues.append(validation_issues)
            data_match_validation_issues.append(validation_issues)

        if not target_table_exist:
            validation_issues = ValidationIssue(
                issue_type="target_table_missing",
                description=f"Could not find target table '{mapping.target_table}'.",
                severity=ValidationStatus.FAIL,
            )
            row_count_validation_issues.append(validation_issues)
            data_match_validation_issues.append(validation_issues)

        elif target_count is not None and target_count < 0:
            validation_issues = ValidationIssue(
                issue_type="target_table_count_error",
                description=f"Could not retrieve row count for target table '{mapping.target_table}'.",
                severity=ValidationStatus.FAIL,
            )
            row_count_validation_issues.append(validation_issues)
            data_match_validation_issues.append(validation_issues)

        elif target_count is not None and target_count == 0:
            validation_issues = ValidationIssue(
                issue_type="target_table_is_empty",
                description=f"Target table '{mapping.target_table}' is empty.",
                severity=ValidationStatus.WARNING,
            )
            row_count_validation_issues.append(validation_issues)
            data_match_validation_issues.append(validation_issues)

        if (source_count is None or source_count <= 0) and (
            target_count is None or target_count <= 0
        ):
            return DataMatchValidationResult(
                table_name=mapping.source_table,
                source_table=mapping.source_table,
                target_table=mapping.target_table,
                group=mapping.group,
                key_columns=mapping.key_columns,
                data_transformation_rules=mapping.data_transformation_rules,
                unique_data_mapping_id=mapping.unique_data_mapping_id,
                status=ValidationStatus.FAIL,
                source_table_exist=source_table_exist,
                target_table_exist=target_table_exist,
                source_table_or_view=source_table_or_view,
                target_table_or_view=target_table_or_view,
                source_count=self._to_positive_integer(source_count),
                target_count=self._to_positive_integer(target_count),
                sample_size=sample_size,
                row_count_validation_issues=row_count_validation_issues,
                data_match_validation_issues=data_match_validation_issues,
            )

        result = DataMatchValidationResult(
            table_name=mapping.source_table,
            source_table=mapping.source_table,
            target_table=mapping.target_table,
            group=mapping.group,
            key_columns=mapping.key_columns,
            data_transformation_rules=mapping.data_transformation_rules,
            unique_data_mapping_id=mapping.unique_data_mapping_id,
            status=ValidationStatus.PASS,
            source_table_or_view=source_table_or_view,
            target_table_or_view=target_table_or_view,
            source_count=self._to_positive_integer(source_count),
            target_count=self._to_positive_integer(target_count),
            sample_size=sample_size,
        )

        # Check if row counts match
        if (
            source_count is not None
            and target_count is not None
            and source_count != target_count
        ):
            issue_type = "row_count_mismatch"

            # Calculate percentage difference
            if source_count == 0:
                percent_diff = 100.0
            else:
                percent_diff = (
                    -(source_count - target_count) / source_count * 100.0
                )

            failure_row_count_difference_threshold = (
                self.settings["validation_settings"]
                .get("row_count_difference_threshold", {})
                .get("failure", 5.0)
            )
            warning_row_count_difference_threshold = (
                self.settings["validation_settings"]
                .get("row_count_difference_threshold", {})
                .get("success", 1.0)
            )

            if abs(percent_diff) <= warning_row_count_difference_threshold:
                severity = ValidationStatus.PASS
                issue_type = f"row_count_mismatch<={warning_row_count_difference_threshold:.1f}%"

            elif abs(percent_diff) > failure_row_count_difference_threshold:
                severity = ValidationStatus.FAIL
                issue_type = f"row_count_mismatch>{failure_row_count_difference_threshold:.1f}%"

                # if target count is more than source count, downgrade to WARNING
                if source_count < target_count:
                    severity = ValidationStatus.WARNING
                    issue_type = f"target_has_more_{percent_diff:.1f}%_data"

            else:
                severity = ValidationStatus.WARNING
                issue_type = "row_count_mismatch"

            result.status = severity

            result.row_count_validation_issues.append(
                ValidationIssue(
                    issue_type=issue_type,
                    description=f"Row count mismatch: source={source_count}, target={target_count} ({percent_diff:.2f}% difference)",
                    severity=severity,
                    source_value=source_count,
                    target_value=target_count,
                )
            )

        # Data validation
        if source_count == 0 and target_count == 0:
            result.status = result.status.raise_status_level_to(
                ValidationStatus.PASS
            )
        else:
            if mapping.key_columns is None or len(mapping.key_columns) == 0:
                result.data_match_validation_issues.append(
                    ValidationIssue(
                        issue_type="no_key_columns",
                        description=f"Skip Data validation because no key columns are defined.",
                        severity=ValidationStatus.WARNING,
                        source_value=source_count,
                        target_value=target_count,
                    )
                )
            else:
                compare_sample_data_result = self._compare_sample_data(
                    mapping, sample_size, settings=self.settings
                )
                result.compare_sample_data_result = compare_sample_data_result

                if compare_sample_data_result.data_match_validation_issues:
                    for (
                        issue
                    ) in (
                        compare_sample_data_result.data_match_validation_issues
                    ):
                        if issue not in result.data_match_validation_issues:
                            result.data_match_validation_issues.append(issue)
                            result.status = (
                                result.status.raise_status_level_to(
                                    issue.severity
                                )
                            )

                result.matching_records = (
                    compare_sample_data_result.interpolated_matching_records_of_tables_from_success_rate
                )

                success_rate = (
                    compare_sample_data_result.interpolated_success_rate_of_tables_from_success_rate_of_2_sets
                )

                # Determine status based on success rate

                success_rate_status = ValidationStatus.PASS
                if abs(success_rate) < self.settings[
                    "validation_settings"
                ].get("data_validation_threshold", {}).get("warning", 95):
                    success_rate_status = ValidationStatus.FAIL
                elif abs(success_rate) < self.settings[
                    "validation_settings"
                ].get("data_validation_threshold", {}).get("success", 99):
                    success_rate_status = ValidationStatus.WARNING

                if abs(success_rate) < 100.0:
                    result.data_match_validation_issues.append(
                        ValidationIssue(
                            issue_type=f"{success_rate:.2f}% matched data",
                            description=f"Data validation: {success_rate:.2f}% success",
                            severity=success_rate_status,
                            source_value=source_count,
                            target_value=target_count,
                        )
                    )

                result.status = result.status.raise_status_level_to(
                    success_rate_status
                )

        result.execution_time_seconds = time.time() - start_time
        return result

    def _check_if_table_or_view_exists(
        self, db_config: DatabaseConfig, table_name: str
    ) -> Tuple[bool, str | None]:
        if db_config is None:
            return None, None

        if (
            table_name
            in self.database_inspector[db_config.source_or_target_type][
                "tables"
            ]
        ):
            return True, "table"

        if (
            table_name
            in self.database_inspector[db_config.source_or_target_type][
                "views"
            ]
        ):
            return True, "view"

        return False, None

    def _get_table_count(
        self, db_config: DatabaseConfig, table_name: str
    ) -> int | None:
        """Get row count for a table."""
        try:
            with db_config.engine.connect() as conn:
                query = f"SELECT COUNT(*) FROM {db_config.schema}.{table_name}"
                result = conn.execute(text(query))
                # Convert to int to handle Decimal types from database
                return int(result.scalar())
        except Exception as e:
            self.logger.warning(
                f"Could not get count for {table_name} in {db_config.name}: {e}"
            )
            return None

    def _rule_based_data_validation(
        self, data: pd.DataFrame, table_mapping: TableMapping
    ) -> RuleBasedDataValidationResult | None:
        """
        Extract rule_based_data_validation for key columns present in table_mapping.key_columns.
        Returns a RuleBasedDataValidationResult or None if no applicable rules.
        """

        if (
            not hasattr(table_mapping, "rule_based_data_validation")
            or (not table_mapping.rule_based_data_validation)
            or data is None
        ):
            return None

        # Only keep rules for columns that are in key_columns
        key_columns_set = set(table_mapping.key_columns or [])
        filtered_rules = {
            k: v
            for k, v in table_mapping.rule_based_data_validation.items()
            if k in key_columns_set
        }

        if not filtered_rules:
            return None

        total_records = len(data)
        passed_records_count = {
            column_name: 0 for column_name in filtered_rules
        }
        failed_records_count = {
            column_name: 0 for column_name in filtered_rules
        }
        failed_set = {column_name: set() for column_name in filtered_rules}
        success_set = {column_name: set() for column_name in filtered_rules}

        for column_name in filtered_rules:

            column_data = data.get(column_name)
            if column_data is None:
                continue

            # Determine if value cannot be null based on 'nullable' rule
            nullable_rule = filtered_rules[column_name].get("nullable", None)
            if nullable_rule is not None:
                # Accept string or boolean representations
                if isinstance(nullable_rule, str):
                    value_can_be_null = nullable_rule.strip().lower() == "true"
                else:
                    value_can_be_null = bool(nullable_rule)
            else:
                value_can_be_null = False  # Default to False if not specified

            # Determine if value have to be unique based on 'unique' rule
            unique_rule = filtered_rules[column_name].get("unique", None)
            if unique_rule is not None:
                # Accept string or boolean representations
                if isinstance(unique_rule, str):
                    value_must_be_unique = (
                        unique_rule.strip().lower() == "true"
                    )
                else:
                    value_must_be_unique = bool(unique_rule)
            else:
                value_must_be_unique = (
                    False  # Default to False if not specified
                )

            # if filtered_rules[column_name] dont have pattern key, add the pattern: "^.*$" # any string and pattern_regex_description: "Accept any value"
            if "pattern" not in filtered_rules[column_name]:
                filtered_rules[column_name]["pattern"] = "^.*$"  # any string
                if (
                    "pattern_regex_description"
                    not in filtered_rules[column_name]
                ):
                    pattern_regex_description = "Accept any value"

                    if value_must_be_unique or not value_can_be_null:
                        unique_and_not_null_desc = []
                        if value_must_be_unique:
                            unique_and_not_null_desc.append(
                                "Have to be unique"
                            )
                        if not value_can_be_null:
                            unique_and_not_null_desc.append("not null")
                        pattern_regex_description += (
                            f" ({' and '.join(unique_and_not_null_desc)})"
                        )

                    filtered_rules[column_name][
                        "pattern_regex_description"
                    ] = pattern_regex_description

            for key in filtered_rules[column_name]:
                value = filtered_rules[column_name][key]
                if key.lower() == "pattern":
                    pattern = value

                    regex = re.compile(pattern)

                    # Find non-matching values
                    non_matched_set = set()
                    matched_set = set()

                    for item_data in column_data:
                        item_data_is_none = item_data is None or pd.isna(
                            item_data
                        )
                        normalized_item_data = (
                            normalize_item_data_end_with_dot_0(item_data)
                        )
                        if (
                            item_data is None
                            or item_data_is_none
                            or text_mean_none(normalized_item_data)
                        ) and value_can_be_null:
                            matched_set.add(normalized_item_data)
                        elif (value_must_be_unique) and (
                            normalized_item_data in matched_set
                        ):
                            non_matched_set.add(normalized_item_data)
                            failed_records_count[column_name] += 1
                        elif not regex.fullmatch(str(normalized_item_data)):
                            non_matched_set.add(normalized_item_data)
                            failed_records_count[column_name] += 1
                        else:
                            matched_set.add(normalized_item_data)
                    if non_matched_set:
                        failed_set[column_name] = non_matched_set
                    if matched_set:
                        success_set[column_name] = matched_set

                    passed_records_count[column_name] += (
                        len(column_data) - failed_records_count[column_name]
                    )

        failed_record_samples = {
            key: sorted(
                [
                    str(x)
                    for x in __import__("itertools").islice(
                        set_value,
                        table_mapping.number_of_set_sample_records_for_detailed_report,
                    )
                ]
            )
            for key, set_value in failed_set.items()
            if set_value
        }
        success_record_samples = {
            key: sorted(
                [
                    str(x)
                    for x in __import__("itertools").islice(
                        set_value,
                        table_mapping.number_of_set_sample_records_for_detailed_report,
                    )
                ]
            )
            for key, set_value in success_set.items()
            if set_value
        }

        max_item_length_for_html_report = (
            table_mapping.max_item_length_for_html_report
            if table_mapping.max_item_length_for_html_report is not None
            else 200
        )
        max_word_length_for_html_report = (
            table_mapping.max_word_length_for_html_report
            if table_mapping.max_word_length_for_html_report is not None
            else 30
        )
        # if any of the success_record_samples item has length > max_item_length_for_html_report characters, truncate it to first max_item_length_for_html_report characters and add "..." at the end
        for key in success_record_samples:
            success_record_samples[key] = [
                (
                    f'"{item[:max_word_length_for_html_report]}..."'
                    if (
                        max_word_length_for_html_report
                        > 0  # If max_word_length_for_html_report is set to 0, do not truncate long words
                        and len(item) > max_word_length_for_html_report
                        and not any(
                            c in item[:max_word_length_for_html_report]
                            for c in [
                                " ",
                                "\t",
                                "\n",
                                "\r",
                                "-",
                                "&shy;",
                                "<br>",
                                # Those characters that do NOT break lines by default in HTML. Only break when using CSS word-break rules.
                                # <td style="word-break: break-all;">
                                # "_",
                                # ":",
                                # ",",
                                # "=",
                                # "@",
                                # "#",
                            ]
                        )  # item[:max_word_length_for_html_report] is a long word without space
                    )
                    else (
                        f'"{item[:max_item_length_for_html_report]}..."'
                        if max_item_length_for_html_report
                        > 0  # If max_item_length_for_html_report is set to 0, do not truncate long items
                        and len(item) > max_item_length_for_html_report
                        else item
                    )
                )
                for item in success_record_samples[key]
            ]

        return RuleBasedDataValidationResult(
            table_mapping=table_mapping,
            total_records=total_records,
            passed_records_count=passed_records_count,
            failed_records_count=failed_records_count,
            failed_record_samples=failed_record_samples,
            success_record_samples=success_record_samples,
        )

    def _distribution_based_data_validation(
        self, data: pd.DataFrame, table_mapping: TableMapping
    ) -> DistributionBasedDataValidationResult | None:
        """
        Extract rule_based_data_validation for key columns present in table_mapping.key_columns.
        Returns a DistributionBasedDataValidationResult or None if no applicable rules.
        """
        if (
            not hasattr(table_mapping, "distribution_based_data_validation")
            or not table_mapping.distribution_based_data_validation
            or data is None
        ):
            return None

        # Only keep distributions for columns that are in key_columns
        key_columns_set = set(table_mapping.key_columns or [])
        filtered_distributions = {
            k: v
            for k, v in table_mapping.distribution_based_data_validation.items()
            if k in key_columns_set
        }

        if not filtered_distributions:
            return None

        total_records = len(data)
        values_to_count = {
            column_name: dict() for column_name in filtered_distributions
        }
        min_items_count = None
        max_items_count = None

        for column_name in filtered_distributions:
            column_data = data.get(column_name)
            if column_data is None:
                continue

            for key in filtered_distributions[column_name]:
                value = filtered_distributions[column_name][key]
                if key.lower() == "expected_distribution":
                    expected_distribution = value

                    # convert expected_distribution keys to lowercase for case insensitive comparison
                    expected_distribution = {
                        k.lower(): v for k, v in expected_distribution.items()
                    }

                    values_to_count[column_name] = {
                        v: {"count": 0} for v in expected_distribution.keys()
                    }

                    for item_data in column_data:
                        item_data_is_none = item_data is None or pd.isna(
                            item_data
                        )
                        item = normalize_item_data_end_with_dot_0(
                            item_data
                        ).lower()  # distribution comparison is case insensitive

                        if "null" in values_to_count[column_name] and (
                            item_data_is_none or text_mean_none(item)
                        ):
                            values_to_count[column_name]["null"]["count"] += 1
                        else:
                            if item in values_to_count[column_name]:
                                values_to_count[column_name][item][
                                    "count"
                                ] += 1
                            else:
                                for k, v in expected_distribution.items():
                                    or_list = v.get("or", [])
                                    if item in [
                                        str(or_item).strip().lower()
                                        for or_item in or_list
                                    ]:
                                        values_to_count[column_name][k][
                                            "count"
                                        ] += 1
                                        break
                elif key.lower() == "min_items_count":
                    min_items_count = int(value)
                elif key.lower() == "max_items_count":
                    max_items_count = int(value)

        for column_name, column_result in values_to_count.items():
            expected_distribution = filtered_distributions[column_name].get(
                "expected_distribution", {}
            )
            for value, stats in column_result.items():
                value_expected_distribution = expected_distribution.get(
                    value, {}
                )
                stats["value_expected_distribution"] = (
                    value_expected_distribution
                )
                stats["issue"] = []
                stats["or"] = value_expected_distribution.get(
                    "or", None
                )  # List of alternative values

                count = stats.get("count", 0)

                min_count = value_expected_distribution.get("min_count", None)
                max_count = value_expected_distribution.get("max_count", None)
                if (
                    (min_count is not None)
                    and (max_count is not None)
                    and (min_count > max_count)
                ):
                    stats["issue"].append(
                        ValidationIssue(
                            issue_type=f"min_count_{min_count}_>_max_count_{max_count}",
                            description=f"For value '{value}', the expected min_count {min_count} is greater than max_count {max_count}.",
                            severity=ValidationStatus.WARNING,
                        )
                    )

                stats["min_count"] = min_count
                stats["max_count"] = max_count
                if (min_count is not None) and (count < min_count):
                    stats["issue"].append(
                        ValidationIssue(
                            issue_type=f"distribution_below_min_count_{min_count}",
                            description=f"Value '{value}' count {count} is below expected minimum {min_count}.",
                            severity=ValidationStatus.FAIL,
                        )
                    )
                if (max_count is not None) and (count > max_count):
                    stats["issue"].append(
                        ValidationIssue(
                            issue_type=f"distribution_above_max_count_{max_count}",
                            description=f"Value '{value}' count {count} is above expected maximum {max_count}.",
                            severity=ValidationStatus.FAIL,
                        )
                    )

                percentage = (
                    (count / total_records * 100.0)
                    if total_records > 0
                    else 0.0
                )
                stats["percentage"] = percentage

                min_percent = value_expected_distribution.get(
                    "min_percent", None
                )
                max_percent = value_expected_distribution.get(
                    "max_percent", None
                )
                if (
                    (min_percent is not None)
                    and (max_percent is not None)
                    and (min_percent > max_percent)
                ):
                    stats["issue"].append(
                        ValidationIssue(
                            issue_type=f"min_percent_{min_percent}_>_max_percent_{max_percent}",
                            description=f"For value '{value}', the expected min_percent {min_percent} is greater than max_percent {max_percent}.",
                            severity=ValidationStatus.WARNING,
                        )
                    )

                stats["min_percent"] = min_percent
                stats["max_percent"] = max_percent
                if (min_percent is not None) and (percentage < min_percent):
                    stats["issue"].append(
                        ValidationIssue(
                            issue_type=f"distribution_below_min_percent_{min_percent}",
                            description=f"Value '{value}' percentage {percentage:.2f}% is below expected minimum {min_percent}%.",
                            severity=ValidationStatus.FAIL,
                        )
                    )
                if (max_percent is not None) and (percentage > max_percent):
                    stats["issue"].append(
                        ValidationIssue(
                            issue_type=f"distribution_above_max_percent_{max_percent}",
                            description=f"Value '{value}' percentage {percentage:.2f}% is above expected maximum {max_percent}%.",
                            severity=ValidationStatus.FAIL,
                        )
                    )

        return DistributionBasedDataValidationResult(
            table_mapping=table_mapping,
            total_records=total_records,
            values_to_count=values_to_count,
            min_items_count=min_items_count,
            max_items_count=max_items_count,
        )

    def _compare_sample_data(
        self,
        mapping: TableMapping,
        sample_size: Optional[int],
        settings: Optional[Dict[str, Any]] = None,
    ) -> CompareSampleDataResult:
        """Compare sample data between source and target tables."""

        source_sample = pd.DataFrame()
        target_sample = pd.DataFrame()
        source_keys_set = set()
        target_keys_set = set()
        matching_keys_set = set()

        # set sample size from individual mapping if provided instead of global sample size
        if mapping.sample_size is not None:
            sample_size = mapping.sample_size

        try:
            # Get sample data from both tables
            source_sample = self._get_sample_data(
                self.source_config,
                mapping.source_table,
                mapping.key_columns,
                mapping.key_columns_cast_types,
                sample_size,
            )

            target_sample = self._get_sample_data(
                self.target_config,
                mapping.target_table,
                mapping.key_columns,
                mapping.key_columns_cast_types,
                sample_size,
            )

            rule_based_data_validation = None
            if (
                mapping.rule_based_data_validation is not None
                and len(mapping.rule_based_data_validation) > 0
                and (
                    settings is not None
                    and settings["validation_settings"] is not None
                    and settings["validation_settings"].get(
                        "enable_rule_based_data_validation", True
                    )
                )
            ):
                rule_based_data_validation_source = (
                    self._rule_based_data_validation(source_sample, mapping)
                )
                rule_based_data_validation_target = (
                    self._rule_based_data_validation(target_sample, mapping)
                )
                rule_based_data_validation = {
                    "source": rule_based_data_validation_source,
                    "target": rule_based_data_validation_target,
                }

            distribution_based_data_validation = None
            if (
                mapping.distribution_based_data_validation is not None
                and len(mapping.distribution_based_data_validation) > 0
                and (
                    settings is not None
                    and settings["validation_settings"] is not None
                    and settings["validation_settings"].get(
                        "enable_distribution_based_data_validation", True
                    )
                )
            ):
                distribution_based_data_validation_source = (
                    self._distribution_based_data_validation(
                        source_sample, mapping
                    )
                )
                distribution_based_data_validation_target = (
                    self._distribution_based_data_validation(
                        target_sample, mapping
                    )
                )
                distribution_based_data_validation = {
                    "source": distribution_based_data_validation_source,
                    "target": distribution_based_data_validation_target,
                }

            source_keys_set = set()
            target_keys_set = set()
            if source_sample is not None and target_sample is not None:
                source_keys_set = build_set_from_sample_and_columns(
                    source_sample,
                    mapping.key_columns,
                    data_transformation_rules=mapping.data_transformation_rules,
                )
                target_keys_set = build_set_from_sample_and_columns(
                    target_sample,
                    mapping.key_columns,
                    data_transformation_rules=mapping.data_transformation_rules,
                )

            matching_keys_set = source_keys_set & target_keys_set

            source_unmatched_keys = source_keys_set - matching_keys_set
            target_unmatched_keys = target_keys_set - matching_keys_set

            return CompareSampleDataResult(
                rule_based_data_validation=rule_based_data_validation,
                distribution_based_data_validation=distribution_based_data_validation,
                table_mapping=mapping,
                sample_size=sample_size,
                source_sample_count=(
                    len(source_sample) if source_sample is not None else 0
                ),
                target_sample_count=(
                    len(target_sample) if target_sample is not None else 0
                ),
                source_sample_set_count=len(source_keys_set),
                target_sample_set_count=len(target_keys_set),
                matching_set_record_count=len(matching_keys_set),
                # uses itertools.islice to efficiently get the first N items from matching_keys_set without creating a full list. This improves performance for large sets. = list(matching_keys_set)[:number_of_sample_records],
                matching_keys_set_sample=sorted(
                    [
                        str(x)
                        for x in __import__("itertools").islice(
                            matching_keys_set,
                            mapping.number_of_set_sample_records_for_detailed_report,
                        )
                    ]
                ),
                source_unmatched_keys_sample=sorted(
                    [
                        str(x)
                        for x in __import__("itertools").islice(
                            source_unmatched_keys,
                            mapping.number_of_set_sample_records_for_detailed_report,
                        )
                    ]
                ),
                target_unmatched_keys_sample=sorted(
                    [
                        str(x)
                        for x in __import__("itertools").islice(
                            target_unmatched_keys,
                            mapping.number_of_set_sample_records_for_detailed_report,
                        )
                    ]
                ),
            )

        except Exception as e:
            error_message = f"Error comparing sample data for {mapping.source_table}: {str(e).splitlines()[0] if str(e) else 'Unknown error'}"
            self.logger.error(error_message)
            return CompareSampleDataResult(
                table_mapping=mapping,
                sample_size=sample_size,
                source_sample_count=len(source_sample),
                target_sample_count=len(target_sample),
                source_sample_set_count=len(source_keys_set),
                target_sample_set_count=len(target_keys_set),
                matching_set_record_count=len(matching_keys_set),
                data_match_validation_issues=[
                    ValidationIssue(
                        issue_type="Error_comparing_sample_data",
                        description=error_message,
                        severity=ValidationStatus.FAIL,
                        source_value=source_sample,
                        target_value=target_sample,
                    )
                ],
            )

    def build_casted_key_columns(
        self,
        key_column: str,
        cast_type: Optional[str],
        database_type: Optional[str],
    ) -> str:
        """Build SQL expression for casting key column if needed."""
        if cast_type:
            if cast_type.upper() in [
                "BOOLEAN",
                "BOOL",
                "BIT",
                "TINYINT",
                "BYTEINT",
                "SMALLINT",
                "INT",
                "INTEGER",
            ]:
                # target_cast_type = (
                #     "BYTEINT"
                #     if "Teradata".upper() in str(database_type).upper()
                #     else "BIT"  # MS_SQL_Server
                # )
                target_cast_type = (
                    "INT"  # Use INT for better compatibility across databases
                )

                return f" CAST({key_column} AS {target_cast_type}) AS {key_column} "

            return key_column  # if no type match, return as is

        return key_column  # if no cast_type provided, return as is

    def _get_sample_data(
        self,
        db_config: DatabaseConfig,
        table_name: str,
        key_columns: List[str],
        key_columns_cast_types: List[str],
        sample_size: Optional[int],
    ) -> pd.DataFrame:
        """Get sample data from a table."""

        if db_config is None:
            return None

        with db_config.engine.connect() as conn:
            # In Teradata SQL, TYPE is a reserved keyword, so it cannot be used directly as a column name without quoting/escaping it.
            quoted_key_columns = [f'"{col}"' for col in key_columns]

            casted_key_columns = []
            for i in range(len(quoted_key_columns)):
                quoted_key_column = quoted_key_columns[i]
                casted_key_column = self.build_casted_key_columns(
                    quoted_key_column,
                    key_columns_cast_types[i],
                    db_config.name,
                )

                casted_key_columns.append(casted_key_column)

            key_cols_str = ", ".join(casted_key_columns)

            # (pymssql._pymssql.OperationalError) (306, b'The text, ntext, and image data types cannot be compared or sorted, except when using IS NULL or LIKE operator.DB-Lib error message 20018, severity 16:\nGeneral SQL Server error: Check messages from the SQL Server\n')
            # order_by_key_columns = [col for col in key_columns]
            # quoted_order_by_key_columns = [
            #     f'"{col}"' for col in order_by_key_columns
            # ]
            # order_by_key_cols_str = ", ".join(quoted_order_by_key_columns)
            # order_by_clause = f"ORDER BY {order_by_key_cols_str}"
            order_by_clause = ""  # Temporarily disable ORDER BY to avoid issues with non-sortable types

            top_clause = f"TOP {sample_size}" if sample_size else ""

            query = f"""
                SELECT {top_clause} {key_cols_str}
                FROM {db_config.schema}.{table_name}
                {order_by_clause}
            """

            # print(  # For debugging purposes
            #     f"Executing sample data query on {db_config.name}.{table_name}:\n{query}"
            # )

            return pd.read_sql(query, conn)
