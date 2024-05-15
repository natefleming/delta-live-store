import os
from typing import List, Tuple

import pytest
from pyspark.sql import DataFrame

from dbrx.dls.filters import by_all, by_entity_id, by_source
from dbrx.dls.store import DeltaLiveEntity, DeltaLiveEntityList, DeltaLiveStore
from dbrx.dls.types import DeltaLiveEntityExpectations


@pytest.fixture
def new_delta_live_entity_expectations() -> DeltaLiveEntityExpectations:
    expectations: Dict[str, Dict[str, str]] = {
        "expect_all": {"constraint_1": "1==1"},
        "expect_all_or_drop": {"constraint_2": "1==1"},
        "expect_all_or_fail": {"constraint_3": "1==1"},
    }
    return expectations


@pytest.fixture
def new_delta_live_entity(
    new_delta_live_entity_expectations: DeltaLiveEntityExpectations,
) -> DeltaLiveEntity:
    return DeltaLiveEntity(
        entity_id="1",
        source="path/to/data1",
        destination="table1",
        source_format="parquet",
        read_options={"key": "value"},
        expectations=new_delta_live_entity_expectations,
    )


@pytest.fixture
def new_delta_live_entities() -> List[DeltaLiveEntity]:
    return [
        DeltaLiveEntity(
            entity_id="1",
            source="path/to/data1",
            destination="table1",
            source_format="parquet",
            read_options={"key": "value"},
        ),
        DeltaLiveEntity(
            entity_id="2",
            source="table1",
            destination="table2",
            source_format="parquet",
            read_options={"key": "value"},
        ),
        DeltaLiveEntity(
            entity_id="3",
            source="table2",
            destination="table3",
            source_format="parquet",
            read_options={"key": "value"},
        ),
    ]


@pytest.fixture
def update_control_entries() -> List[DeltaLiveEntity]:
    return [
        DeltaLiveEntity(
            entity_id="1",
            source="path/to/data1",
            destination="table1",
            source_format="csv",
            read_options={"key": "value"},
        ),
        DeltaLiveEntity(
            entity_id="2",
            source="path/to/data2",
            destination="table1",
            source_format="csv",
            read_options={"key": "value"},
        ),
        DeltaLiveEntity(
            entity_id="4",
            source="path/to/data4",
            destination="table1",
            source_format="csv",
            read_options={"key": "value"},
        ),
    ]


@pytest.mark.skipif(
    not os.environ.get("DELTA_LIVE_STORE_TEST_TABLE_NAME"),
    reason="DELTA_LIVE_STORE_TEST_TABLE_NAME is not set",
)
def test_control_init(delta_live_store: DeltaLiveStore) -> None:
    assert delta_live_store.is_initialized() == True
    delta_live_store.destroy()
    assert delta_live_store.is_initialized() == False


def test_control_entity_list_to_df(
    new_delta_live_entities: List[DeltaLiveEntity],
) -> None:
    control_entity_list = DeltaLiveEntityList(new_delta_live_entities)
    df: DataFrame = control_entity_list.to_df()
    for i, row in enumerate(df.collect()):
        for key, value in row.asDict().items():
            assert value == new_delta_live_entities[i].__dict__[key]
    assert df.count() == len(new_delta_live_entities)


@pytest.mark.skipif(
    not os.environ.get("DELTA_LIVE_STORE_TEST_TABLE_NAME"),
    reason="DELTA_LIVE_STORE_TEST_TABLE_NAME is not set",
)
def test_control_upsert(
    delta_live_store: DeltaLiveStore,
    new_delta_live_entity: DeltaLiveEntity,
    new_delta_live_entity_expectations: DeltaLiveEntityExpectations,
) -> None:
    delta_live_store.upsert(new_delta_live_entity)
    entities: DeltaLiveEntityList = delta_live_store.find_entities(by_all())
    assert len(entities.to_list()) == 1
    assert entities.first().expectations == new_delta_live_entity_expectations


@pytest.mark.skipif(
    not os.environ.get("DELTA_LIVE_STORE_TEST_TABLE_NAME"),
    reason="DELTA_LIVE_STORE_TEST_TABLE_NAME is not set",
)
def test_control_upsert_many(
    delta_live_store: DeltaLiveStore, new_delta_live_entities: List[DeltaLiveEntity]
) -> None:
    delta_live_store.upsert(*new_delta_live_entities)
    assert len(delta_live_store.find_entities(by_all()).to_list()) == 3


@pytest.mark.skipif(
    not os.environ.get("DELTA_LIVE_STORE_TEST_TABLE_NAME"),
    reason="DELTA_LIVE_STORE_TEST_TABLE_NAME is not set",
)
def test_control_upsert_update(
    delta_live_store: DeltaLiveStore,
    new_delta_live_entities: List[DeltaLiveEntity],
    update_control_entries: List[DeltaLiveEntity],
) -> None:
    delta_live_store.upsert(*new_delta_live_entities)
    delta_live_store.upsert(*update_control_entries)
    assert len(delta_live_store.find_history(by_all()).to_list()) == 6
    assert len(delta_live_store.find_history(by_entity_id("1")).to_list()) == 2
    assert len(delta_live_store.find_history(by_entity_id("2")).to_list()) == 2
    assert len(delta_live_store.find_history(by_entity_id("3")).to_list()) == 1
    assert len(delta_live_store.find_history(by_entity_id("4")).to_list()) == 1

    entity_1_versions: List[DeltaLiveEntity] = delta_live_store.find_history(
        by_entity_id("1")
    ).to_list()
    new: DeltaLiveEntity = entity_1_versions[0]
    old: DeltaLiveEntity = entity_1_versions[1]
    assert new.created_ts == old.expired_ts
    assert old.is_latest == False
    assert new.is_latest == True


@pytest.mark.skipif(
    not os.environ.get("DELTA_LIVE_STORE_TEST_TABLE_NAME"),
    reason="DELTA_LIVE_STORE_TEST_TABLE_NAME is not set",
)
def test_find_by_source_path(
    delta_live_store: DeltaLiveStore, new_delta_live_entities: List[DeltaLiveEntity]
) -> None:
    source_path: str = "path/to/data1"
    delta_live_store.upsert(*new_delta_live_entities)
    found: Optional[DeltaLiveEntity] = delta_live_store.find_entity(
        by_source(source_path)
    )
    assert found is not None
    assert found.source == source_path


@pytest.mark.skipif(
    not os.environ.get("DELTA_LIVE_STORE_TEST_TABLE_NAME"),
    reason="DELTA_LIVE_STORE_TEST_TABLE_NAME is not set",
)
def test_find_by_entity_id(
    delta_live_store: DeltaLiveStore, new_delta_live_entities: List[DeltaLiveEntity]
) -> None:
    entity_id: str = "2"
    delta_live_store.upsert(*new_delta_live_entities)
    found: Optional[DeltaLiveEntity] = delta_live_store.find_entity(
        by_entity_id(entity_id)
    )
    assert found is not None
    assert found.entity_id == entity_id


@pytest.mark.skipif(
    not os.environ.get("DELTA_LIVE_STORE_TEST_TABLE_NAME"),
    reason="DELTA_LIVE_STORE_TEST_TABLE_NAME is not set",
)
def test_disable_by_entity_id(
    delta_live_store: DeltaLiveStore, new_delta_live_entities: List[DeltaLiveEntity]
) -> None:
    entity_id: str = "2"
    delta_live_store.upsert(*new_delta_live_entities)
    enabled_entity: DeltaLiveEntity = delta_live_store.find_entity(
        by_entity_id(entity_id)
    ).copy()
    assert enabled_entity.is_enabled
    delta_live_store.disable(by_entity_id(entity_id))
    disabled_entity: DeltaLiveEntity = delta_live_store.find_entity(
        by_entity_id(entity_id)
    ).copy()
    assert not disabled_entity.is_enabled

    assert enabled_entity.id != disabled_entity.id


@pytest.mark.skipif(
    not os.environ.get("DELTA_LIVE_STORE_TEST_TABLE_NAME"),
    reason="DELTA_LIVE_STORE_TEST_TABLE_NAME is not set",
)
def test_enable_by_entity_id(
    delta_live_store: DeltaLiveStore, new_delta_live_entities: List[DeltaLiveEntity]
) -> None:
    entity_id: str = "2"
    delta_live_store.upsert(*new_delta_live_entities)
    delta_live_store.disable(by_entity_id(entity_id))
    disabled_entity: DeltaLiveEntity = delta_live_store.find_entity(
        by_entity_id(entity_id)
    )
    assert not disabled_entity.is_enabled
    delta_live_store.enable(by_entity_id(entity_id))
    enabled_entity: DeltaLiveEntity = delta_live_store.find_entity(
        by_entity_id(entity_id)
    )
    assert enabled_entity.is_enabled
    assert enabled_entity.id != disabled_entity.id


@pytest.mark.skipif(
    not os.environ.get("DELTA_LIVE_STORE_TEST_TABLE_NAME"),
    reason="DELTA_LIVE_STORE_TEST_TABLE_NAME is not set",
)
def test_update_read_options(
    delta_live_store: DeltaLiveStore, new_delta_live_entities: List[DeltaLiveEntity]
) -> None:
    entity_id: str = "2"
    delta_live_store.upsert(*new_delta_live_entities)
    entity: DeltaLiveEntity = delta_live_store.find_entity(by_entity_id(entity_id))
    updated_read_options = {"new_key": "new_value"}
    entity.read_options |= updated_read_options
    delta_live_store.upsert(entity)
    updated_entity: DeltaLiveEntity = delta_live_store.find_entity(
        by_entity_id(entity_id)
    )
    assert len(updated_entity.read_options) == 2


@pytest.mark.skipif(
    not os.environ.get("DELTA_LIVE_STORE_TEST_TABLE_NAME"),
    reason="DELTA_LIVE_STORE_TEST_TABLE_NAME is not set",
)
def test_update_missing_options(
    delta_live_store: DeltaLiveStore, new_delta_live_entities: List[DeltaLiveEntity]
) -> None:
    entity_id: str = "2"
    delta_live_store.upsert(*new_delta_live_entities)
    entity: DeltaLiveEntity = delta_live_store.find_entity(by_entity_id(entity_id))
    updated_write_options = {"new_key": "new_value"}
    entity.table_properties |= updated_write_options
    delta_live_store.upsert(entity)
    updated_entity: DeltaLiveEntity = delta_live_store.find_entity(
        by_entity_id(entity_id)
    )
    assert len(updated_entity.table_properties) == 1


@pytest.mark.skipif(
    not os.environ.get("DELTA_LIVE_STORE_TEST_TABLE_NAME"),
    reason="DELTA_LIVE_STORE_TEST_TABLE_NAME is not set",
)
def test_disable_all_dependant_entities(
    delta_live_store: DeltaLiveStore, new_delta_live_entities: List[DeltaLiveEntity]
) -> None:
    target_entity_id: str = "1"
    delta_live_store.upsert(*new_delta_live_entities)
    delta_live_store.disable(by_entity_id(target_entity_id))

    entities: DeltaLiveEntityList = delta_live_store.find_entities(
        by_entity_id("1") | by_entity_id("2") | by_entity_id("3")
    )
    assert all([not entity.is_enabled for entity in entities.to_list()])


@pytest.mark.skipif(
    not os.environ.get("DELTA_LIVE_STORE_TEST_TABLE_NAME"),
    reason="DELTA_LIVE_STORE_TEST_TABLE_NAME is not set",
)
def test_disable_some_dependant_entities(
    delta_live_store: DeltaLiveStore, new_delta_live_entities: List[DeltaLiveEntity]
) -> None:
    target_entity_id: str = "2"
    delta_live_store.upsert(*new_delta_live_entities)
    delta_live_store.disable(by_entity_id(target_entity_id))

    entities: DeltaLiveEntityList = delta_live_store.find_entities(
        by_entity_id("1") | by_entity_id("2") | by_entity_id("3")
    )
    assert entities.filter(by_entity_id("1")).first().is_enabled
    assert not entities.filter(by_entity_id("2")).first().is_enabled
    assert not entities.filter(by_entity_id("3")).first().is_enabled


@pytest.mark.skipif(
    not os.environ.get("DELTA_LIVE_STORE_TEST_TABLE_NAME"),
    reason="DELTA_LIVE_STORE_TEST_TABLE_NAME is not set",
)
def test_enable_some_dependant_entities(
    delta_live_store: DeltaLiveStore, new_delta_live_entities: List[DeltaLiveEntity]
) -> None:
    target_entity_id: str = "2"
    delta_live_store.upsert(*new_delta_live_entities)
    delta_live_store.disable(by_all())
    delta_live_store.enable(by_entity_id(target_entity_id))
    entities: DeltaLiveEntityList = delta_live_store.find_entities(
        by_entity_id("1") | by_entity_id("2") | by_entity_id("3")
    )
    assert entities.filter(by_entity_id("1")).first().is_enabled
    assert entities.filter(by_entity_id("2")).first().is_enabled
    assert not entities.filter(by_entity_id("3")).first().is_enabled
