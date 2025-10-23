"""Validation result classes for database transition validation."""

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from data_class.TableMapping import TableMapping


class ValidationStatus(Enum):
    """Status of validation check."""

    PASS = "PASS"
    FAIL = "FAIL"
    WARNING = "WARNING"
    SKIP = "SKIP"

    SEVERITY_ORDER = {
        "SKIP": 0,  # SKIP is considered lowest severity
        "PASS": 1,
        "WARNING": 2,
        "FAIL": 3,
    }

    @property
    def print_value(self) -> str:
        """Print the value of the ValidationStatus."""
        return self.value[:4]

    def raise_status_level_to(
        self, new_status: "ValidationStatus"
    ) -> "ValidationStatus":
        """Return the more severe status between self and new_status."""
        new_status_value = self.SEVERITY_ORDER.value[new_status.value]
        current_status_value = self.SEVERITY_ORDER.value[self.value]

        if new_status_value > current_status_value:
            return new_status

        return self
