"""Validation result classes for database transition validation."""

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from data_class.TableMapping import TableMapping

from .ValidationIssue import ValidationIssue
from .ValidationStatus import ValidationStatus


@dataclass
class DataTypesCompatibleResult:
    """Results of type compatibility check when comparing different database types."""

    result: ValidationStatus  # "PASS", "WARNING", "FAIL"
    issue: Optional[ValidationIssue] = None
