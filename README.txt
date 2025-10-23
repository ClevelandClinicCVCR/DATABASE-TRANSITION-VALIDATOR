Database Transition Validator - User Guide
==========================================
This tool validates data migration from database to database and schema to schema (currently support Teradata and MS_SQL_Server) by comparing table schemas, row counts, sample data, rule based data validation, and distribution based validation across databases.


AUTHOR
-----------------
Phong Nguyen <nguyenp7@ccf.org>


CREATE VIRTUAL ENVIRONMENT and VS CODE SETTINGS
-----------------
Creating the virtual environment in the workspace root
```
python3 -m venv .venv
```

Activating the virtual environment and upgrading core packaging tools
```
source .venv/bin/activate && python -m pip install --upgrade pip setuptools wheel
```

Installing dependencies from the new consolidated requirements file
```
source .venv/bin/activate && pip install -r requirements.txt
```

VS Code Settings:

create .vscode/settings.json file:
   {
      "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
      "python.terminal.activateEnvironment": true,
      "python.analysis.autoImportCompletions": true,
      "python.linting.enabled": true,
      "python.testing.pytestEnabled": false,
      "python.testing.unittestEnabled": false
   }

create .vscode/launch.json file:
   {
   "version": "0.2.0",
   "configurations": [
      {
         "name": "Python: main_validation (module, venv)",
         "type": "debugpy",
         "request": "launch",
         "module": "database_transition_validator.main_validation",
         "python": "${workspaceFolder}/.venv/bin/python",
         "console": "integratedTerminal",
         "cwd": "${workspaceFolder}/..",
         "env": {
         "PYTHONUNBUFFERED": "1"
         }
      }
   ]
   }

Make sure VS Code use venv interpreter and not injecting the global python path. On a Mac, press Command Shift P → Python: Select Interpreter


ENVIRONMENT and DATABASE SEVER USER SETUP
-----------------
1. Rename file "database_credentials.yml" to "database_credential_local.yml" and set up the file:
```yaml
   # TERADATA Server user setup
   TERADATA:
   - NAME: "Teradata"
      HOST: ""
      PATH_TO_KEYS: ""
      USERNAME: ""
      PASSWORD: ""  # should be left blank on localhost then it will read from the Teradata Credential Files in PATH_TO_KEYS

   # MS-SQL Server user setup
   MS_SQL_SERVER:
   - NAME: "MS_SQL_Server_DEV"
      HOST: ""
      PORT: 51464
      DATABASE: ""
      USERNAME: ""
      PASSWORD: ""

   - NAME: "MS_SQL_Server_PROD"
      HOST: ""
      PORT: 51464
      DATABASE: ""
      USERNAME: ""
      PASSWORD: ""
```
You can configure multiple TERADATA and MS_SQL_SERVER servers, and assign each a name so they can be referenced in the custom table mappings YAML configuration file either by name or by index.

For example, you might name your servers MS_SQL_Server_DEV and MS_SQL_Server_PROD. These can then be referred to by their index (starting from 0 for each database type) or by their assigned name.

If the index is set to null or -1, the server will be identified by its name (e.g., name: "MS_SQL_Server_DEV").
If both index and name are provided, the index takes precedence.
If neither index nor name is provided, the first entry (index 0) will be used by default.

2. Install Database Transition Validator Requirements — to be installed on top of the existing Magic-Models dependencies.
```
pip install -r requirements_database_transition_validator.txt
```
this will install pandas for data manipulation and analysis, jinja2 for template engine for HTML report generation, and teradatasql for teradata database connector.

Alternatively, these dependencies are included in the [database_transition_validator] optional group within pyproject.toml, allowing you to install them using:
```
pip install .[database_transition_validator]
```


QUICK START
-----------
Move to the directory: magicmodels/utils/database_transition_validator/

1. Run validation with default settings: Compare DL_MAGIC_PROD vs MAGIC_CORE using a few example tables (PATIENT, TRIAL, TREATMENT, CANCER_DIAGNOSIS) to demo the report format:
```bash
python main_validation.py
```

2. Run validation with a custom configuration
Validate all tables in MAGIC_CORE against DL_MAGIC_PROD using a specific config file:
```bash
python main_validation.py --config validation_config-DL_MAGIC_PROD-MAGIC_CORE.yml
```

3. Run validation with parallel processing
Improve performance by using multiple workers:
```bash
python main_validation.py --config validation_config-DL_MAGIC_PROD-MAGIC_CORE.yml --parallel-workers 8
```


COMMAND LINE ARGUMENTS
----------------------

Optional Arguments:
  --config CONFIG_FILE
      Path to YAML configuration file with custom table mappings and settings
      Example: --config validation_config.yml

  --sample-size SIZE
      Number of records to sample for data validation (default: all data)
      Use smaller values for faster validation of large tables
      Example: --sample-size 10000

  --parallel-workers COUNT
      Number of parallel workers for validation (default: 4)
      Increase for better performance on systems with more CPU cores
      Example: --parallel-workers 8

  --skip-schema-validation
      Skip schema structure validation
      Use when you only want to validate data, not table structure

  --skip-data-validation
      Skip data comparison validation
      Use when you only want to validate schema structure

  --output-dir DIRECTORY
      Output directory for validation reports (default: validation_reports)
      Example: --output-dir /path/to/reports

  --verbose
      Enable verbose logging for detailed debugging information

  --help
      Show help message with all available options



CONFIGURATION FILE
------------------

Create a YAML configuration file to customize table mappings and validation settings.
In the table_mappings, ensures that at least one of `source_table` or `target_table` is provided. If only one is provided, the other is set to the same value. The `key_columns` will be an empty lists if they are not provided.

Example validation_config.yml:
```yaml
table_mappings:
  - source_table: "TEST_TABLE"

  - source_table: "PATIENT"
    key_columns: ["PAT_ID", "PAT_MRN_ID", "FIRST_NAME", "LAST_NAME", "DATE_OF_BIRTH"]

  - source_table: "TRIAL"
    target_table: "TRIAL"
    group: "TRIAL"
    key_columns: ["NCT_ID", "PRMC_ID", "TRIAL_TITLE", "IS_CCF : int"]
    data_transformation_rules: ["normalize_null_nan", "timestamp_to_date_only", "round_float_to_decimal:2"]
    sample_size: 1_000 # only validate a sample of 1,000 rows for performance.
    max_item_length_for_html_report: 100
    max_word_length_for_html_report: 30
    number_of_set_sample_records_for_detailed_report: 10
    rule_based_data_validation:
      NCT_ID:
        pattern: '^NCT\d{8}$'
        pattern_regex_description: "Starts with NCT. Followed by exactly 8 digits"
        nullable: true # can be null
        unique: true # must be unique

validation_settings:
  output_dir: "database_validation_reports"

  # Maximum number of parallel workers
  max_workers: 4

  data_validation_threshold:
    success: 99.0  # Threshold for success rate (percentage)
    warning: 95.0  # Threshold for warning (percentage)

  row_count_difference_threshold:
    success: 1.0   # Threshold for success rate (percentage)
    warning: 5.0   # Threshold for warning (percentage)

database_setting:
  source_database:
    type: "Teradata" # Teradata, MS_SQL_Server
    index: 0 # Index (start from 0) of the database type in database_credentials_local.yml. Null or -1 means don't use index value, use name instead.
    name: "Teradata" # If index is null or -1, use name to identify the database credentials in database_credentials_local.yml
    # if both index and name are provided, index will be used.
    # if none of index and name is provided, the first entry (index 0) will be used.
    schema: "DL_MAGIC_PROD" # DL_MAGIC_DEV, DL_MAGIC_PROD (for Teradata). MAGIC_CORE, PYTHIA_CORE, dbo (for MS_SQL_Server)

  target_database:
    type: "MS_SQL_Server" # Teradata, MS_SQL_Server
    index: 0
    name: "MS_SQL_Server_DEV"
    schema: "MAGIC_CORE" # MAGIC_CORE, PYTHIA_CORE, dbo (for MS_SQL_Server). DL_MAGIC_DEV, DL_MAGIC_PROD (for Teradata)
```


GROUPING TABLES OR VIEWS
------------------------
You can use the `group` field in your table mappings to organize tables or views into logical groups. All tables or views with the same group name will be grouped together in the validation report and output files. This is useful for organizing related tables, such as all trial-related tables or all NGS-related tables.

Example:
```yaml
  - source_table: "TRIAL_TYPE_ASSIGNMENT"
    group: "TRIAL Group"

```

All tables with the same `group` value will be displayed together in the HTML report and other outputs. If no group is specified, the table will be placed in the default group.

CASTING KEY COLUMN TYPES
------------------------
You can cast key columns to a different type to match columns with different types in different databases. Currently, only casting boolean columns to integer is supported. To cast a column, use the syntax `COLUMN_NAME -> int` or `COLUMN_NAME : int` in the `key_columns` list.

Example:
```yaml
  - source_table: "NGS_CNV"
    key_columns: ["NUMERIC_VALUE", "REFSEQ", "IS_VARIANT_OF_UNKNOWN_SIGNIFICANCE -> int", "IS_WILD_TYPE : int"]
```

This will cast the columns `IS_VARIANT_OF_UNKNOWN_SIGNIFICANCE` and `IS_WILD_TYPE` to integer type during validation, allowing you to match boolean columns in one database to integer columns in another.


DATA_TRANSFORMATION_RULES
------------------------
You can add the optional `data_transformation_rules` field to any table mapping in your YAML configuration to standardize and clean up data before validation. This helps ensure that minor differences in formatting, null handling, or numeric precision do not cause false mismatches during validation.
Add a `data_transformation_rules` list to your table mapping. Each entry specifies a transformation to apply to all relevant columns before comparing source and target data. You can combine multiple rules as needed.

Example:
```yaml
   -  source_table: "TRIAL"
      key_columns: ["NCT_ID", "PRMC_ID", "TRIAL_TITLE", "IS_CCF : int"]
      data_transformation_rules: ["normalize_null_nan", "timestamp_to_date_only", "round_float_to_decimal:2"]
```

Available Data Transformation Rules:

- `normalize_null_nan`: Converts all recognized null-like values (e.g., "null", "None", "nan", "N/A", "", etc.) to a standard null value. This ensures that different representations of missing data are treated as equivalent.

- `timestamp_to_date_only`: If a column contains datetime or timestamp values, this rule strips the time portion, keeping only the date (YYYY-MM-DD). Useful when the time component is not relevant for comparison.

- `round_float_to_decimal:N`: Rounds all float values to N decimal places. For example, `round_float_to_decimal:2` will round 1.23456 to 1.23. This is helpful when databases store numbers with different precision.

How rules are applied:
- Each rule is applied to all columns unless a rule is designed to target specific types (e.g., only numeric columns for rounding).
- You can combine as many rules as needed for your use case.


SHOW MORE NUMBER OF SET SAMPLE RECORDS FOR DETAILED REPORT
------------------------
You can use the `number_of_set_sample_records_for_detailed_report` field in your table mapping to control how many sample records from the matching/unmatched key sets are shown in the detailed HTML reports. This is useful for debugging, auditing, or sharing specific examples of matched/unmatched records between source and target tables.

If not specified, the default value is 5. You can increase this number to show more sample records for each table, or decrease it for a more concise report.

This setting is per-table, so you can customize it for each mapping as needed.

Example:
```yaml
   - source_table: "NGS_CNV"
      key_columns: ["NUMERIC_VALUE", "REFSEQ"]
      number_of_set_sample_records_for_detailed_report: 5
```


OTHER OPTIONAL VALUES
-------------
You can further customize the validation and reporting for each table mapping using these optional parameters:

**sample_size**
   - Controls how many rows are sampled for validation in this specific table mapping.
   - Example: `sample_size: 1_000` will only validate a sample of 1,000 rows for performance.
   - If not specified, the global sample size setting (from command line or config) is used for all tables.
   - Use a smaller sample size for very large tables to speed up validation, or omit for full-table validation.

**max_item_length_for_html_report**
   - Sets the maximum number of characters to display for any value in the HTML report before truncating with ellipsis (`...`).
   - Example: `max_item_length_for_html_report: 100` will show up to 100 characters per value; longer values are truncated.
   - Default is 100 if not specified.
   - Useful for keeping reports readable when columns contain long text or codes.

**max_word_length_for_html_report**
   - Sets the maximum number of characters to display for any single word (no spaces/tabs/newlines) in the HTML report before truncating with ellipsis.
   - Example: `max_word_length_for_html_report: 30` will show up to 30 characters per word; longer words are truncated.
   - Default is 30 if not specified.
   - Helps prevent long codes or identifiers from making the report hard to read.

**number_of_set_sample_records_for_detailed_report**
   - Controls how many sample records from matched/unmatched key sets are shown in the detailed HTML report for this table.
   - Example: `number_of_set_sample_records_for_detailed_report: 10` will show up to 10 sample records per set.
   - Default is 5 if not specified.
   - Increase for more detailed debugging/auditing, decrease for a more concise report.

**Usage Example:**
```yaml
   - source_table: "TRIAL"
      sample_size: 1000
      max_item_length_for_html_report: 100
      max_word_length_for_html_report: 30
      number_of_set_sample_records_for_detailed_report: 10
```


OTHER USAGE EXAMPLES
--------------

1. Validation using default tables (PATIENT, TRIAL, TREATMENT, CANCER_DIAGNOSIS): Table mappings and configuration are loaded from "validation_config_default.yml". You can modify this file to add more tables or settings, but it is recommended to create a new configuration file and pass it via the --config argument, as shown in usage example 2.
   python main_validation.py

2. Custom configuration with specific tables:
   python main_validation.py --config my_config.yml
   python main_validation.py --config validation_config-DL_MAGIC_PROD-MAGIC_CORE.yml

3. Schema-only validation:
   python main_validation.py --config my_config.yml --skip-data-validation

4. Data-only validation with custom output:
   python main_validation.py --config my_config.yml --skip-schema-validation --output-dir data_validation_reports

5. High-performance validation for production:
   python main_validation.py --config production.yml --parallel-workers 16 --verbose



DEVELOPPER USAGE NOTES
--------------

1. main_validation.py file. For easier debugging, you can add a default config argument in the main run:
```
if __name__ == "__main__":
    sys.argv += [
        "--config",
        "validation_config-DL_MAGIC_PROD-MAGIC_CORE.yml",
    ]
    main()
```

2. test_setup.py file. To quickly test a specific table, add it to test_mappings and run test_setup.py:
```
test_mappings = [
    TableMapping(
        source_table="PATIENT",
        key_columns=[
            "PAT_ID",
            "FIRST_NAME",
            "LAST_NAME",
            "DATE_OF_BIRTH",
        ],
    ),
]

```


UNDERSTANDING THE OUTPUT
------------------------

The validator generates multiple report formats:

1. Console Output:
   - Real-time progress and summary statistics
   - Overall validation status (PASS/FAIL/WARNING)
   - Success rates and execution time

2. JSON Report (validation_report_TIMESTAMP.json):
   - Complete validation results in machine-readable format
   - Detailed error information and issue descriptions
   - Suitable for integration with other tools

3. CSV Report (validation_report_TIMESTAMP.csv):
   - Table-level results in spreadsheet format
   - Easy to analyze in Excel or other tools

4. HTML Report (validation_report_TIMESTAMP.html) (the best):
   - Interactive dashboard-style report
   - Visual progress bars and color-coded status
   - Best for sharing results with stakeholders

5. Summary Report (validation_summary_TIMESTAMP.txt):
   - Concise text summary
   - Failed and warning tables listed
   - Quick overview of validation results


VALIDATION PROCESS
------------------

The validator performs these checks:

1. Schema Validation:
   - Table existence in both databases
   - Column name matching
   - Data type compatibility
   - Missing/extra columns identification

2. Data Validation:
   - Row count comparison
   - Sample data matching based on key columns
   - Success rate calculation
   - Issue identification and categorization

3. Report Generation:
   - Multiple output formats
   - Detailed issue descriptions
   - Performance metrics and timing


PERFORMANCE TIPS
----------------

1. Use --sample-size for large tables:
   - Start with small samples (100-1000) for testing
   - Increase gradually based on accuracy needs
   - Full validation for critical tables only

2. Optimize parallel workers:
   - Start with --parallel-workers 4
   - Increase up to number of CPU cores
   - Monitor system resources during validation

3. Skip unnecessary validations:
   - Use --skip-schema-validation if schema is known to be correct
   - Use --skip-data-validation for schema-only checks


Last Updated: Oct. 2025
