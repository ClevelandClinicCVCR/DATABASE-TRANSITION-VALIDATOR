def build_database_description(db_settings: dict) -> str:
    """Build a human-readable description for a database config."""

    if db_settings is None:
        return "{No Database}"

    desc = ""
    if db_settings.get("index"):
        desc += f"{db_settings.get('type', '')} [{db_settings['index']}] "
    elif db_settings.get("name"):
        desc += f"{db_settings['name']} "
    else:
        desc += f"{db_settings.get('type', '')} "

    if db_settings.get("schema"):
        desc += f"({db_settings['schema']})"

    return desc
