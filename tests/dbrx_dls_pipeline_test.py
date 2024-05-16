from datetime import datetime

import pytest

from dbrx.dls.models import (
    DeltaLiveEntity,
    DeltaLiveEntityList,
    ApplyChanges,
    Expectations,
)
from dbrx.dls.pipeline import (
    QUARANTINE_COL,
    DeltaLiveStorePipeline,
    can_quarantine,
    quarantine_rules,
    has_scd,
)


def test_has_scd() -> None:
    entity = DeltaLiveEntity(
        entity_id="1",
        group="group1",
        source="source1",
        tags={"tag1": "value1", "tag2": "value2"},
        is_enabled=True,
        destination="destination1",
        expectations=Expectations(
            expect_all={"valid_id": "ID is not null"},
        ),
    )
    assert has_scd(entity) == False

    entity = DeltaLiveEntity(
        entity_id="1",
        group="group1",
        source="source1",
        tags={"tag1": "value1", "tag2": "value2"},
        is_enabled=True,
        destination="destination1",
        expectations=Expectations(
            expect_all={"valid_id": "ID is not null"},
        ),
        primary_keys=["id"],
        apply_changes={"sequence_by": "id", "column_list": ["id"]},
    )
    assert has_scd(entity) == True


def test_quarantine_rules() -> None:
    entity = DeltaLiveEntity(
        entity_id="1",
        group="group1",
        source="source1",
        tags={"tag1": "value1", "tag2": "value2"},
        is_enabled=True,
        destination="destination1",
        is_quarantined=True,
        expectations=Expectations(
            expect_all={"valid_id": "ID is not null"},
        ),
    )
    assert quarantine_rules(entity) != "1=0"

    entity = DeltaLiveEntity(
        entity_id="1",
        group="group1",
        source="source1",
        tags={"tag1": "value1", "tag2": "value2"},
        is_enabled=True,
        destination="destination1",
        is_quarantined=True,
        expectations=Expectations(
            expect_all_or_drop={"valid_id": "ID is not null"},
        ),
    )
    assert quarantine_rules(entity) == "1=0"

    entity = DeltaLiveEntity(
        entity_id="1",
        group="group1",
        source="source1",
        tags={"tag1": "value1", "tag2": "value2"},
        is_enabled=True,
        destination="destination1",
        is_quarantined=False,
        expectations=Expectations(
            expect_all={"valid_id": "ID is not null"},
        ),
    )
    assert quarantine_rules(entity) == "1=0"


def test_can_quarantine() -> None:
    entity = DeltaLiveEntity(
        entity_id="1",
        group="group1",
        source="source1",
        tags={"tag1": "value1", "tag2": "value2"},
        is_enabled=True,
        destination="destination1",
        is_quarantined=True,
        expectations=Expectations(
            expect_all={"valid_id": "ID is not null"},
        ),
    )
    assert can_quarantine(entity) == True

    entity = DeltaLiveEntity(
        entity_id="1",
        group="group1",
        source="source1",
        tags={"tag1": "value1", "tag2": "value2"},
        is_enabled=True,
        destination="destination1",
        is_quarantined=True,
        expectations=Expectations(
            expect_all_or_fail={"valid_id": "ID is not null"},
        ),
    )
    assert can_quarantine(entity) == False

    entity = DeltaLiveEntity(
        entity_id="1",
        group="group1",
        source="source1",
        tags={"tag1": "value1", "tag2": "value2"},
        is_enabled=True,
        destination="destination1",
        is_quarantined=False,
        expectations=Expectations(
            expect_all={"valid_id": "ID is not null"},
        ),
    )
    assert can_quarantine(entity) == False
