from dataclasses import dataclass
from typing import Dict


@dataclass
class TableMapping:
    """Mapping between source and target tables."""

    key_columns: list[str] = None
    key_columns_cast_types: list[str] = None

    source_table: str = None
    target_table: str = None
    group: str = None  # optional, for grouping similar tables
    sample_size: int = None  # number of sample records to compare
    data_transformation_rules: list[str] = None
    number_of_set_sample_records_for_detailed_report: int = None
    max_item_length_for_html_report: int = None
    max_word_length_for_html_report: int = None
    rule_based_data_validation: Dict[str, Dict] = None
    distribution_based_data_validation: Dict[str, Dict] = None

    # optional
    unique_data_mapping_id: str = (
        None  # this is a unique ID for the mapping item. If not provided, it will be generated based on source_table + target_table + key_columns
    )
    extra_key_columns_sets: list[list[str]] = None
    exclude_columns: list[str] = None
    custom_mappings: Dict[str, str] = None  # source_col -> target_col

    def __post_init__(self):
        """
        Post-initialization method for the data class.

        Ensures that at least one of `source_table` or `target_table` is provided. If only one is provided,
        the other is set to the same value. Initializes `key_columns`, `exclude_columns`, and `custom_mappings`
        to empty lists or dictionaries if they are not provided.

        Raises:
            ValueError: If neither `source_table` nor `target_table` is provided.
        """
        if not self.source_table and not self.target_table:
            raise ValueError(
                "At least one of source_table or target_table must be provided."
            )
        else:
            if not self.source_table:
                self.source_table = self.target_table
            elif not self.target_table:
                self.target_table = self.source_table

        if self.key_columns is None:
            self.key_columns = []

        # clean up key_columns by stripping whitespace and removing empty strings
        self.key_columns = [
            str(col).strip() for col in self.key_columns if str(col).strip()
        ]

        if self.exclude_columns is None:
            self.exclude_columns = []
        if self.custom_mappings is None:
            self.custom_mappings = {}

        if not self.unique_data_mapping_id:
            self.unique_data_mapping_id = (
                f"{self.source_table}|{self.target_table}|"
                + ",".join(set(self.key_columns))
                + "|"
                + ",".join(self.data_transformation_rules)
            )
