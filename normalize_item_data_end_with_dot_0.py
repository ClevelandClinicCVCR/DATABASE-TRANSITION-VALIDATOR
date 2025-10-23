import numbers

import pandas as pd


def normalize_item_data_end_with_dot_0(item_data):
    """
    Normalize item_data for pattern matching and counting:
    - If item_data is a number and ends with .0, convert to integer string (e.g., 123.0 -> "123")
    - Otherwise, convert to string
    """

    item_data_is_none = item_data is None or pd.isna(item_data)
    item_data_is_number = isinstance(item_data, numbers.Number) or type(
        item_data
    ) in {float, int, complex}
    item_data_str = str(item_data)
    if (
        item_data_is_number
        and item_data_str.endswith(".0")
        and len(item_data_str) > 2
    ):
        return str(item_data_str[:-2])
    return item_data_str
