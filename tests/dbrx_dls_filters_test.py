from datetime import datetime
from typing import List

import pytest

from dbrx.dls.filters import (AndPredicate, OrPredicate, Predicate, by_all,
                              by_created_ts_between, by_destination,
                              by_destination_type, by_entity_id, by_group,
                              by_has_tag, by_id, by_is_enabled, by_is_latest,
                              by_reference_ts, by_source)
from dbrx.dls.store import DeltaLiveEntity


@pytest.fixture
def test_entity() -> DeltaLiveEntity:
    return DeltaLiveEntity(
        entity_id="1",
        group="group1",
        source="source1",
        tags={"tag1": "value1", "tag2": "value2"},
        id="123",
        is_enabled=True,
        is_latest=True,
        destination="destination1",
        created_ts=datetime.strptime("2022-01-01", "%Y-%m-%d"),
        destination_type="type1",
    )


def test_by_all(test_entity: DeltaLiveEntity):
    by_all_predicate = by_all()
    assert by_all_predicate(test_entity) == True


def test_by_entity_id(test_entity: DeltaLiveEntity):
    entity_id = "1"
    by_entity_id_predicate = by_entity_id(entity_id)
    assert by_entity_id_predicate(test_entity) == True


def test_by_group(test_entity: DeltaLiveEntity):
    group = "group1"
    by_group_predicate = by_group(group)
    assert by_group_predicate(test_entity) == True


def test_by_source(test_entity: DeltaLiveEntity):
    source = "source1"
    by_source_predicate = by_source(source)
    assert by_source_predicate(test_entity) == True


def test_by_tag(test_entity: DeltaLiveEntity):
    tag = "tag1"
    by_tag_predicate = by_has_tag(tag)
    assert by_tag_predicate(test_entity) == True


def test_by_id(test_entity: DeltaLiveEntity):
    entity_id = "123"
    by_id_predicate = by_id(entity_id)
    assert by_id_predicate(test_entity) == True


def test_by_is_enabled(test_entity: DeltaLiveEntity):
    is_enabled = True
    by_is_enabled_predicate = by_is_enabled(is_enabled)
    assert by_is_enabled_predicate(test_entity) == True


def test_by_is_latest(test_entity: DeltaLiveEntity):
    is_latest = True
    by_is_latest_predicate = by_is_latest(is_latest)
    assert by_is_latest_predicate(test_entity) == True


def test_by_destination(test_entity: DeltaLiveEntity):
    destination = "destination1"
    by_destination_predicate = by_destination(destination)
    assert by_destination_predicate(test_entity) == True


def test_by_created_ts_between(test_entity: DeltaLiveEntity):
    start_ts = datetime.strptime("2022-01-01", "%Y-%m-%d")
    end_ts = datetime.strptime("2022-01-31", "%Y-%m-%d")
    by_created_ts_between_predicate = by_created_ts_between(start_ts, end_ts)
    assert by_created_ts_between_predicate(test_entity) == True


def test_by_destination_type(test_entity: DeltaLiveEntity):
    destination_type = "type1"
    by_destination_type_predicate = by_destination_type(destination_type)
    assert by_destination_type_predicate(test_entity) == True


def test_by_reference_ts(test_entity: DeltaLiveEntity):
    reference_ts = datetime.strptime("2022-01-01", "%Y-%m-%d")
    test_entity.expired_ts = datetime.strptime("2022-01-31", "%Y-%m-%d")
    by_reference_ts_predicate = by_reference_ts(reference_ts)
    assert by_reference_ts_predicate(test_entity) == True


def test_and_predicate_contains(test_entity: DeltaLiveEntity):
    predicate1 = by_entity_id("1")
    predicate2 = by_group("group1")
    predicate3 = by_has_tag("group1")
    and_predicate = AndPredicate(predicate1, predicate2)
    assert and_predicate.contains(predicate1) == True
    assert and_predicate.contains(predicate2) == True
    assert and_predicate.contains(predicate3) == False


def test_or_predicate_contains(test_entity: DeltaLiveEntity):
    predicate1 = by_entity_id("1")
    predicate2 = by_group("group1")
    predicate3 = by_has_tag("group1")
    and_predicate = OrPredicate(predicate1, predicate2)
    assert and_predicate.contains(predicate1) == True
    assert and_predicate.contains(predicate2) == True
    assert and_predicate.contains(predicate3) == False


def test_entity_id_predicate_contains(test_entity: DeltaLiveEntity):
    predicate1 = by_entity_id("1")
    predicate2 = by_entity_id("1")
    predicate3 = by_entity_id("2")
    assert predicate1.contains(predicate1) == True
    assert predicate1.contains(predicate2) == True
    assert predicate1.contains(predicate3) == False
