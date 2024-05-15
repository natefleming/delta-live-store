import importlib.resources as pkg_resources
import json
import re
from typing import Any, Dict, Tuple

from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession


def create_session() -> SparkSession:
    """
    Creates a SparkSession for executing Spark operations.

    Returns:
        SparkSession: The created SparkSession object.
    """
    session: SparkSession
    try:

        session = DatabricksSession.builder.getOrCreate()
    except ImportError:
        session = SparkSession.builder.getOrCreate()

    session.conf.set("spark.sql.legacy.allowHashOnMapType", "true")
    return session


def parse_db_schema_table(input_string: str) -> Tuple[str, str, str]:
    """
    Parses a database schema table string into its components.

    Args:
        input_string (str): The input string in the format "database.schema.table".

    Returns:
        Tuple[str, str, str]: A tuple containing the database, schema, and table components.

    Raises:
        ValueError: If the input string does not match the expected format.
    """
    pattern = re.compile(r"^(?:(\w+)\.)?(?:(\w+)\.)?(\w+)$")

    match = pattern.match(input_string)
    if match:
        # Extract groups: database, schema, table
        database, schema, table = match.groups()
        return database, schema, table
    else:
        raise ValueError("Input string does not match the expected format")


def load_config_schema() -> Dict[str, Any]:
    """
    Loads the configuration schema from the resources.

    Returns:
        str: The configuration schema as a string.
    """
    return json.loads(pkg_resources.read_text("dbrx.dls", "config_schema.json"))
