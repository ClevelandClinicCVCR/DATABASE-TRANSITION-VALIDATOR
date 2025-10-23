def text_mean_none(text: str) -> bool:
    return str(text).strip().lower() in {
        "none",
        "nan",
        "null",
        "n/a",
        "na",
        "",
        "undefined",
    }
