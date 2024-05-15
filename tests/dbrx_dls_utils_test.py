from typing import Any, Dict

from dbrx.dls.utils import (create_session, load_config_schema,
                            parse_db_schema_table)


def test_load_config_schema() -> None:
    schema: Dict[str, Any] = load_config_schema()
    assert schema is not None
    assert isinstance(schema, dict)
    assert len(schema) > 0


def test_create_session() -> None:
    session = create_session()
    assert session is not None


def test_parse_db_schema_table() -> None:
    database, schema, table = parse_db_schema_table("database.schema.table")
    assert database == "database"
    assert schema == "schema"
    assert table == "table"

    database, schema, table = parse_db_schema_table("database.table")
    assert database == "database"
    assert schema is None
    assert table == "table"

    database, schema, table = parse_db_schema_table("table")
    assert database is None
    assert schema is None
    assert table == "table"

    try:
        database, schema, table = parse_db_schema_table("database.schema.table.extra")
    except ValueError:
        assert True
    else:
        assert False
