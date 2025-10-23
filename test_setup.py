"""
Simple test script to verify the database transition validator setup.
This script performs basic connectivity tests and validates a small sample.
"""

import logging
import os
import sys

# Add the src directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../.."))

try:
    from magicmodels.utils.database_transition_validator.database_setup.database_config import (
        DatabaseConfigFactory,
        TableMapping,
    )
    from magicmodels.utils.database_transition_validator.DatabaseTransitionValidator import (
        DatabaseTransitionValidator,
    )
    from magicmodels.utils.database_transition_validator.load_default_validation_settings import (
        load_default_validation_settings,
    )
    from magicmodels.utils.database_transition_validator.validation_result import (
        ValidationStatus,
    )
    from magicmodels.utils.database_transition_validator.ValidationReportGenerator import (
        ValidationReportGenerator,
    )
except ImportError as e:
    print(f"Import error: {e}")
    print(
        "Make sure you're running this from the correct directory and all dependencies are installed."
    )
    sys.exit(1)


def test_database_connections(settings=None):
    """Test database connections."""
    print("Testing database connections...")

    if settings is None:
        settings = load_default_validation_settings()

    try:
        # Test source connection
        print("  Testing source database connection...")
        source_config = DatabaseConfigFactory.create_config(
            settings, type="source"
        )
        print(f"    ✓ Source database connected: {source_config.name}")

        # Test target database connection
        print("  Testing target database connection...")
        target_config = DatabaseConfigFactory.create_config(
            settings, type="target"
        )
        print(f"    ✓ Target database connected: {target_config.name}")

        return source_config, target_config

    except Exception as e:
        print(f"    ✗ Connection failed: {e}")
        return None, None


def test_small_sample():
    """Test validation with a simple table mapping."""

    # Get database configurations
    settings = load_default_validation_settings()

    source_config, target_config = test_database_connections(settings)
    if not source_config or not target_config:
        print(
            "Cannot proceed with validation test - database connections failed"
        )
        return False

    try:
        # Create a simple table mapping for testing
        test_mappings = [
            # TableMapping(
            #     source_table="PATIENT",
            #     key_columns=[
            #         "PAT_ID",
            #         "FIRST_NAME",
            #         "LAST_NAME",
            #         "DATE_OF_BIRTH",
            #     ],
            # ),
            # TableMapping(
            #     source_table="PATIENT_V",
            #     key_columns=[
            #         "PAT_ID",
            #         "PAT_EPI_ID",
            #         "MRN",
            #         "FIRST_NAME",
            #         "LAST_NAME",
            #         "DATE_OF_BIRTH",
            #         "VITAL_STATUS",
            #         "SEX_ASSIGNED_AT_BIRTH",
            #         "RACE",
            #         "ETHNICITY",
            #         "ECOG_PERFORMANCE_STATUS",
            #     ],
            # ),
            # TableMapping(
            #     source_table="NGS_SNV_TYPE",
            #     key_columns=[
            #         "NAME",
            #     ],
            # ),
            # TableMapping(
            #     source_table="PATIENT_ECOG_PERFORMANCE_STATUS",
            #     key_columns=[],
            # ),
            # TableMapping(
            #     source_table="TRIAL",
            #     key_columns=["NCT_ID", "PRMC_ID", "TRIAL_TITLE"],
            # ),
            # TableMapping(
            #     source_table="CANCER_TYPE",
            #     key_columns=[
            #         "Case_ID",
            #         # "Cancer_Type",
            #         # "Cancer_Type_Detailed",
            #         # "Tissue_Type",
            #         # "Sample_Type",
            #         "PAT_ID",
            #     ],
            # ),
            # TableMapping(
            #     source_table="RNASEQ_TPM_CCF_BTC_GBM_MULTISITE_FFTS",
            #     key_columns=[],
            # ),
            # TableMapping(
            #     source_table="MUTATION_ANNOTATION_CARIS_720_SILENT",
            #     key_columns=[],
            # ),
            # TableMapping(
            #     source_table="PATIENT_CANCER_DIAGNOSIS",
            #     key_columns=["CONFIDENCE_PERCENTAGE"],
            # ),
            # TableMapping(
            #     source_table="NGS_CNV_V",
            #     key_columns=[
            #         "ACCESSION_NUMBER",
            #         "MRN",
            #         "PATIENT_FIRST_NAME",
            #         "PATIENT_LAST_NAME",
            #         "HUGO_SYMBOL",
            #         "BIOMARKER",
            #         "ASSAY_RESULT",
            #         "NUMERIC_VALUE",
            #     ],
            # ),
            # TableMapping(
            #     source_table="NGS_SNV_V",
            #     key_columns=[
            #         "ACCESSION_NUMBER",
            #         "MRN",
            #         "PATIENT_FIRST_NAME",
            #         "PATIENT_LAST_NAME",
            #         "HUGO_SYMBOL",
            #         "BIOMARKER",
            #         "CHROMOSOME",
            #         "START_POSITION",
            #         "END_POSITION",
            #         "TYPE",
            #         "HGVSC",
            #         "HGVSP_SHORT",
            #         "T_REF_COUNT",
            #         "T_ALT_COUNT",
            #         "VARIANT_ALLELE_FREQUENCY",
            #         "IS_VARIANT_OF_UNKNOWN_SIGNIFICANCE",
            #         "IS_WILD_TYPE",
            #     ],
            # ),
            # TableMapping(
            #     source_table="NGS_SNV_CLASSIFICATION_TRANSLATION_V",
            #     key_columns=[
            #         "CLASSIFICATION_GROUP_NAME",
            #         "SOURCE_CLASSIFICATION_STRING",
            #         "MAGIC_CLASSIFICATION",
            #     ],
            # ),
            # TableMapping(
            #     source_table="NGS_HRR_V",
            #     key_columns=[
            #         "ACCESSION_NUMBER",
            #         "MRN",
            #         "PATIENT_FIRST_NAME",
            #         "PATIENT_LAST_NAME",
            #         "ASSAY_RESULT",
            #         "NUMERIC_VALUE",
            #     ],
            # ),
            # TableMapping(
            #     source_table="SPECIMEN_V",
            #     key_columns=[
            #         "PAT_ID",
            #         "PAT_EPI_ID",
            #         "MRN",
            #         "FIRST_NAME",
            #         "LAST_NAME",
            #         "DATE_OF_BIRTH",
            #         "SPEC_NUMBER_LN1",
            #         "PART",
            #     ],
            # ),
            TableMapping(
                source_table="NGS_TEST_RESULT",
                key_columns=["ACCESSION_NUMBER", "IS_QNS -> boolean"],
            )
        ]

        print("  Creating validator...")
        validator = DatabaseTransitionValidator(source_config, target_config)

        sample_size = 0
        print(f"  Running validation (sample size: {sample_size})...")
        result = validator.validate_transition(
            table_mappings=test_mappings,
            max_workers=1,
            sample_size=sample_size,
            enable_schema_validation=True,
            enable_data_validation=True,
        )

        print(
            f"  ✓ Validation completed: {result.overall_status_table_result.value}"
        )
        print(f"    Execution time: {result.total_execution_time:.2f} seconds")

        if result.data_match_validation_result:
            table_result = result.data_match_validation_result[0]
            print(f"    Table: {table_result.table_name}")
            print(f"    Source count: {table_result.source_count}")
            print(f"    Target count: {table_result.target_count}")
            print(f"    Success rate: {table_result.success_rate:.2f}%")

        # Generate reports
        print("\nGenerating validation reports...")
        report_generator = ValidationReportGenerator(settings)
        report_generator.generate_all_reports(result)

        return True

    except Exception as e:
        print(f"  ✗ Validation test failed: {e}")
        return False


def main():
    print("=" * 50)

    # Setup logging
    logging.basicConfig(
        level=logging.WARNING
    )  # Suppress info logs for cleaner output

    # Simple validation
    test_small_sample()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
