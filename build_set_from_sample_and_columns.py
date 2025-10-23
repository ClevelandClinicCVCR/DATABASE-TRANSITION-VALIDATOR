import numpy as np
import pandas as pd


def normalize_null_nan(val):
    if val is None or pd.isnull(val):
        return "null"
    if (isinstance(val, float) or isinstance(val, int)) and pd.isna(val):
        return "nan"
    return val


def datetime_timestamp_to_date(val):
    # Convert pandas/numpy/py datetime or timestamp to date string (YYYY-MM-DD), else return as is
    if pd.isnull(val):
        return val

    # pandas.Timestamp, datetime.datetime, numpy.datetime64
    if hasattr(val, "date"):
        return str(val.date())

    # Try to convert numpy.datetime64
    try:
        if isinstance(val, np.datetime64):
            return str(pd.to_datetime(val).date())
    except Exception:
        pass
    return val


def apply_all(val, rules, round_float_n=None):
    v = val

    if "timestamp_to_date_only" in rules:
        v = datetime_timestamp_to_date(v)

    if round_float_n is not None and isinstance(v, float):
        v = round(v, round_float_n)

    if "normalize_null_nan" in rules:
        v = normalize_null_nan(v)

    return v


def build_set_from_sample_and_columns(
    df, key_columns, data_transformation_rules=None
):
    """
    Builds a set of tuples from the specified key columns in the DataFrame.

    None and NaN values are normalized to the strings "null" and "nan", ensuring consistent handling of missing or invalid values. This normalization is important for accurate comparison of key columns between source and target datasets, as it prevents mismatches caused by differing null or NaN representations.

    """

    if data_transformation_rules:
        rules = [r.lower().strip() for r in data_transformation_rules]

        # Check for round_float_to_decimal:n rule
        round_rule = next(
            (r for r in rules if r.startswith("round_float_to_decimal")), None
        )
        round_float_n = None
        if round_rule:
            try:
                round_float_n = int(round_rule.split(":", 1)[1])
            except Exception:
                round_float_n = (
                    2  # default to 2 decimal places if parsing fails
                )

        if (
            any(
                r in rules
                for r in [
                    "normalize_null_nan",
                    "timestamp_to_date_only",
                ]
            )
            or round_float_n is not None
        ):
            return set(
                tuple(
                    apply_all(val, rules=rules, round_float_n=round_float_n)
                    for val in row
                )
                for row in df[key_columns].values
            )

    return build_set_from_sample_and_columns_original(df, key_columns)


def build_set_from_sample_and_columns_original(df, key_columns):
    return set(tuple(row) for row in df[key_columns].values)
