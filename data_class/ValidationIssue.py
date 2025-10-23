"""Validation result classes for database transition validation."""

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from data_class.TableMapping import TableMapping

from .ValidationStatus import ValidationStatus


@dataclass
class ValidationIssue:
    """Represents a specific validation issue."""

    issue_type: str
    description: str
    severity: ValidationStatus
    source_value: Any = None
    target_value: Any = None
    additional_info: Dict[str, Any] = field(default_factory=dict)
