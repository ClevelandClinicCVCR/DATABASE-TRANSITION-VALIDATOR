"""
Main validation script for database transition from Teradata to SQL Server.

This script validates that data has been successfully migrated from Teradata
to SQL Server by comparing table schemas, row counts, and sample data.

Usage:
    python main_validation.py [--config config.yml] [--sample-size 1000] [--parallel-workers 4]
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

from data_class.TableMapping import TableMapping
from data_class.ValidationStatus import ValidationStatus
from database_setup.DatabaseConfigFactory import DatabaseConfigFactory
from DatabaseTransitionValidator import DatabaseTransitionValidator
from load_default_validation_settings import load_default_validation_settings
from ValidationReportGenerator import ValidationReportGenerator

VALIDATION_CONFIG_DEFAULT_FILENAME = "validation_config_default.yml"


def load_custom_table_mappings_and_setting(
    config_file: str,
) -> Tuple[List[TableMapping], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """
    Load table mappings from a configuration file.
    """
    try:
        # Try to load config_file from current directory, root directory, and the directory of the main running file
        # Define possible locations to search for the config file:
        search_paths = [
            Path(config_file),  # As given (absolute or relative path)
            Path(__file__).parent
            / "validation_configs"
            / config_file,  # In the validation_configs folder
            Path(__file__).parent
            / config_file,  # In the directory of this script (__file__)
            Path.cwd() / config_file,  # In the current working directory
            (
                Path(sys.argv[0]).parent / config_file
                if hasattr(sys, "argv") and sys.argv[0]
                else None
            ),  # In the directory of the main running file (sys.argv[0])
            Path("/") / config_file,  # In the root directory
        ]
        config_path = None
        for path in search_paths:
            if path and path.exists():
                config_path = path
                break
        if not config_path:
            raise FileNotFoundError(
                f"Config file '{config_file}' not found in search paths: {search_paths}"
            )

        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
            # print(f"Loaded configuration from {config_path}")

        mappings = []
        table_mappings = config.get("table_mappings", [])
        for mapping_config in table_mappings:
            mapping = TableMapping(
                source_table=mapping_config.get("source_table", ""),
                target_table=mapping_config.get("target_table", ""),
                group=mapping_config.get("group", ""),
                data_transformation_rules=mapping_config.get(
                    "data_transformation_rules", []
                ),
                number_of_set_sample_records_for_detailed_report=mapping_config.get(
                    "number_of_set_sample_records_for_detailed_report", None
                ),
                max_word_length_for_html_report=mapping_config.get(
                    "max_word_length_for_html_report", None
                ),
                max_item_length_for_html_report=mapping_config.get(
                    "max_item_length_for_html_report", None
                ),
                sample_size=mapping_config.get("sample_size", None),
                key_columns=mapping_config.get("key_columns", []),
                rule_based_data_validation=mapping_config.get(
                    "rule_based_data_validation", {}
                ),
                distribution_based_data_validation=mapping_config.get(
                    "distribution_based_data_validation", {}
                ),
                exclude_columns=mapping_config.get("exclude_columns", []),
                custom_mappings=mapping_config.get("custom_mappings", {}),
            )
            mappings.append(mapping)

        validation_settings = config.get("validation_settings", {})
        database_setting = config.get("database_setting", {})
        report_sorting_settings = config.get("report_sorting_settings", {})
        data_transformation_rules = config.get("data_transformation_rules", [])

        return (
            mappings,
            validation_settings,
            database_setting,
            report_sorting_settings,
            data_transformation_rules,
        )

    except Exception as e:
        logging.error(f"Failed to load configuration from {config_file}: {e}")
        sys.exit(1)


def setup_logging(settings, verbose: bool = False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    output_dir = "database_validation_reports"
    # Try to get output_dir from validation_settings if available
    try:
        output_dir = settings.get("validation_settings", {}).get(
            "output_dir", output_dir
        )
    except Exception:
        pass

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    log_file_path = Path(output_dir) / "validation.log"
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file_path),
        ],
    )


def parse_args_and_setup_logging(settings):
    parser = argparse.ArgumentParser(
        description="Validate database transition from Teradata to SQL Server"
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Path to configuration file with custom table mappings",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        # default=None,
        help="Sample size for data validation (default: all data)",
    )
    parser.add_argument(
        "--parallel-workers",
        type=int,
        default=2,
        help="Number of parallel workers for validation (default: 2)",
    )
    parser.add_argument(
        "--skip-schema-validation",
        action="store_true",
        help="Skip schema validation",
    )
    parser.add_argument(
        "--skip-data-validation",
        action="store_true",
        help="Skip data validation",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="database_validation_reports",
        help="Output directory for reports (default: database_validation_reports)",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(settings, args.verbose)
    logger = logging.getLogger(__name__)

    return args, logger


def load_table_mappings_and_update_settings(args, logger, settings):
    # Load table mappings
    if args.config:
        logger.info(f"Loading table mappings from {args.config}")
    else:
        logger.info("Using default table mappings")
        sys.argv += [
            "--config",
            VALIDATION_CONFIG_DEFAULT_FILENAME,
        ]

    (
        custom_mappings,
        custom_validation_settings,
        custom_database_setting,
        custom_report_sorting_settings,
        custom_data_transformation_rules,
    ) = load_custom_table_mappings_and_setting(args.config)
    if custom_mappings:
        table_mappings = custom_mappings
    else:
        logger.warning("No table mappings found in config")

    if custom_validation_settings:
        settings["validation_settings"] = custom_validation_settings

    if custom_database_setting:
        settings["database_setting"] = custom_database_setting

    if custom_report_sorting_settings:
        settings["report_sorting_settings"] = custom_report_sorting_settings

    if custom_data_transformation_rules:
        settings["data_transformation_rules"] = (
            custom_data_transformation_rules
        )

    # Overwrite the validation settings file by args if provided
    if args.parallel_workers is not None:
        settings["validation_settings"][
            "parallel_workers"
        ] = args.parallel_workers
    if args.sample_size is not None:
        settings["validation_settings"]["sample_size"] = args.sample_size
    if args.skip_schema_validation is not None:
        settings["validation_settings"][
            "skip_schema_validation"
        ] = args.skip_schema_validation
    if args.skip_data_validation is not None:
        settings["validation_settings"][
            "skip_data_validation"
        ] = args.skip_data_validation
    if args.output_dir is not None:
        settings["validation_settings"]["output_dir"] = args.output_dir

    logger.info(f"Loaded {len(table_mappings)} table mappings")

    return table_mappings, settings


def main():
    """Main validation execution function."""

    settings = load_default_validation_settings(
        config_file=VALIDATION_CONFIG_DEFAULT_FILENAME
    )

    args, logger = parse_args_and_setup_logging(settings)

    try:
        # Load table mappings and update settings
        table_mappings, settings = load_table_mappings_and_update_settings(
            args, logger, settings
        )

        # Create database configurations
        try:
            logger.info("Connecting to databases...")
            source_config = DatabaseConfigFactory.create_config(
                settings, type="source"
            )
            target_config = DatabaseConfigFactory.create_config(
                settings, type="target"
            )

            if source_config:
                logger.info(
                    f"Source: {source_config.name} ({source_config.schema})"
                )

            if target_config:
                logger.info(
                    f"Target: {target_config.name} ({target_config.schema})"
                )

            if source_config is None and target_config is None:
                logger.error(
                    "Both source and target database connections failed."
                )
                return

        except (
            Exception
        ) as e:  # sqlalchemy.exc.OperationalError: (teradatasql.OperationalError) [Teradata SQL Driver] Hostname lookup failed
            message = f"Database connection failed: {str(e).splitlines()[0] if str(e) else 'Unknown error'}"
            logger.error(message)
            print(
                message
                + "\n----------------------\nPlease check your VPN and database connection settings."
                # + f"\nSource: {settings['database_setting']['source_database']['type']} ({settings['database_setting']['source_database']['schema']})"
                # + f"\nTarget: {settings['database_setting']['target_database']['type']} ({settings['database_setting']['target_database']['schema']})"
                + f"\n"
            )
            sys.exit(1)

        # Create validator
        validator = DatabaseTransitionValidator(
            source_config, target_config, settings
        )

        # Run validation
        logger.info("Starting validation process...")

        max_workers = 2  # default
        if (
            settings["validation_settings"]
            and int(settings["validation_settings"]["max_workers"]) > 0
        ):
            max_workers = int(settings["validation_settings"]["max_workers"])
        logger.info(f"Using {max_workers} parallel workers for validation")

        result = validator.validate_transition(
            table_mappings=table_mappings,
            max_workers=max_workers,
            sample_size=settings["validation_settings"].get(
                "sample_size", None
            ),
            enable_schema_validation=settings["validation_settings"].get(
                "enable_schema_validation", True
            ),
            enable_data_validation=settings["validation_settings"].get(
                "enable_data_validation", True
            ),
        )

        # Generate reports
        logger.info("Generating validation reports...")
        report_generator = ValidationReportGenerator(settings)
        report_files = report_generator.generate_all_reports(result)

        # Set exit code based on validation result
        if result.overall_status_table_result == ValidationStatus.FAIL:
            logger.error("Validation failed - exiting with error code")
            sys.exit(1)
        elif result.overall_status_table_result == ValidationStatus.WARNING:
            logger.warning("Validation completed with warnings")
            sys.exit(0)
        else:
            logger.info("Validation passed successfully")
            sys.exit(0)

    except KeyboardInterrupt:
        logger.info("Validation interrupted by user")
        sys.exit(1)
    except Exception as e:
        short_msg = str(e).splitlines()[0] if str(e) else "Unknown error"
        logger.error(f"Validation failed: {short_msg}")
        print(f"Validation failed: {short_msg}")
        sys.exit(1)


if __name__ == "__main__":
    sys.argv += [
        "--config",
        "DL_MAGIC_PROD-MAGIC_CORE tables and views8 distribution-based validation.yml",
        # DL_MAGIC_PROD-MAGIC_CORE-test tables.yml
        # DL_MAGIC_PROD-MAGIC_CORE tables and views8 distribution-based validation.yml
        # DL_MAGIC_PROD-MAGIC_CORE tables and views7 rule-based validation.yml
        # DL_MAGIC_PROD-MAGIC_CORE tables and views6 timestamp_to_date_only.yml
        # DL_MAGIC_PROD-MAGIC_CORE tables and views5 normalize null nan.yml
        # DL_MAGIC_PROD-MAGIC_CORE tables and views4 cast boolean to int.yml
        # DL_MAGIC_PROD-MAGIC_CORE tables and views3 group to show multiple key_column set.yml
        # DL_MAGIC_PROD-MAGIC_CORE tables and views2 remove perfectly matched tables.yml
        # DL_MAGIC_PROD-MAGIC_CORE tables and views1 remove the missing tables.yml
        # DL_MAGIC_PROD-MAGIC_CORE tables and views.yml
        # DL_MAGIC_PROD-MAGIC_CORE views.yml
        # DL_MAGIC_PROD-MAGIC_CORE tables.yml
        # --------------------------------------
        # dbo DEV-PYTHIA_CORE.yml
        # dbo DEV-PYTHIA_CORE-test tables.yml
        # --------------------------------------
        # PYTHIA_CORE-dbo PROD 3 apply data transformation rule normalize null and nan.yml
        # PYTHIA_CORE-dbo PROD 2 add more key_column sets.yml
        # PYTHIA_CORE-dbo PROD.yml
        # PYTHIA_CORE-dbo PROD test table.yml
        # --------------------------------------
        # dbo PROD-PYTHIA_CORE.yml
        # dbo PROD-PYTHIA_CORE-test tables.yml
    ]
    main()
