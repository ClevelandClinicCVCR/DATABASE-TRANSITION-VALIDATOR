import os
from pathlib import Path
from urllib.parse import quote

from sqlalchemy.engine.base import Engine
from sqlmodel import create_engine

from database_setup.config import get_teradata_config_values


def get_teradata_datalab() -> str:
    """Return the appropriate datalab name for the current environment.

    Utilized by almost every Teradata model.
    """
    magic_environment = os.environ.get("MAGIC_ENVIRONMENT", "DEV")
    if magic_environment.upper() == "PROD":
        return "DL_MAGIC_PROD"
    else:
        return "DL_MAGIC_DEV"


def get_teradata_db_engine(
    index: int, name: str, log_queries: bool = False
) -> Engine:
    """Create and test an engine for connections to Teradata.

    NOTE: use of `tmode=ANSI` in the connection strings forces columns in newly created
    tables to be CASESPECIFIC by default.  Without that option all columns would be
    created as NOT CASESPECIFIC."""

    (
        teradata_host,
        path_to_teradata_keys,
        teradata_username,
        teradata_password,
    ) = get_teradata_config_values(index=index, name=name)

    if teradata_password:
        # URL-encode password to handle special characters (e.g., '@', ':', '#').
        encoded_password = quote(teradata_password, safe="")
        engine = create_engine(
            f"teradatasql://{teradata_host}/?user={teradata_username}&password={encoded_password}&tmode=ANSI",
            future=True,
            echo=log_queries,
        )
    else:
        passkey_filename = Path(path_to_teradata_keys) / "PassKey.properties"
        encpass_filename = Path(path_to_teradata_keys) / "EncPass.properties"

        password_from_key_files = "ENCRYPTED_PASSWORD(file:{},file:{})".format(
            passkey_filename, encpass_filename
        )
        engine = create_engine(
            f"teradatasql://{teradata_host}/?user={teradata_username}&password={password_from_key_files}&logmech=LDAP&tmode=ANSI",
            future=True,
            echo=log_queries,
        )
    assert engine.connect()
    return engine
