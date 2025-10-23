import sys
from pathlib import Path
from typing import Any, Dict

import yaml


def load_default_validation_settings(
    config_file: str = "validation_config_default.yml",
) -> Dict[str, Any]:
    """
    Expected format (YAML):
    ```yaml
        database_setting:
        table_mappings:
        validation_settings:
        report_sorting_settings:
        data_transformation_rules:
    ```
    """
    try:
        config_path = Path(__file__).parent / config_file
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        return config

    except Exception as e:
        print(f"Failed to load configuration from {config_file}: {e}")
        sys.exit(1)
