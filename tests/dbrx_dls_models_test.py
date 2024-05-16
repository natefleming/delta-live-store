from typing import List

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.testing.utils import assertDataFrameEqual

from dbrx.dls.models import (
    DeltaLiveEntity,
    ApplyChanges,
    Expectations,
    DeltaLiveEntityList,
)


def test_delta_live_entity_list_to_df(spark: SparkSession) -> None:
    """
    Tests the DeltaLiveEntityList.to_df method.
    """

    entities: List[DeltaLiveEntity] = [
        DeltaLiveEntity(
            entity_id="entity1",
            source="source1",
            destination="table",
            source_format="csv",
            read_options={"option1": "value1"},
            table_properties={"property1": "value1"},
            tags={"tag1": "value1"},
            spark_conf={"conf1": "value1"},
            expectations=Expectations(
                expect_all={"column1": "value1"},
                expect_all_or_fail={"column2": "value2"},
            ),
            is_quarantined=False,
            primary_keys=["column1"],
            apply_changes=ApplyChanges(
                sequence_by="foo1",
                column_list=["column1"],
            ),
        ),
        DeltaLiveEntity(
            entity_id="entity2",
            source="source2",
            destination="view",
            source_format="json",
            read_options={"option2": "value2"},
            table_properties={"property2": "value2"},
            tags={"tag2": "value2"},
            spark_conf={"conf2": "value2"},
            expectations=Expectations(
                expect_all={"column3": "value3"},
                expect_all_or_fail={"column4": "value4"},
            ),
            is_quarantined=True,
            primary_keys=["column2"],
            apply_changes=ApplyChanges(
                sequence_by="foo2",
                column_list=["column2"],
            ),
        ),
    ]
    actual_df: DeltaLiveEntityList = DeltaLiveEntityList(entities=entities).to_df()

    rows: List[Row] = actual_df.collect()

    assert len(rows) == 2
    assert rows[0].entity_id == "entity1"
    assert rows[0].source == "source1"
    assert rows[0].destination == "table"
    assert rows[0].source_format == "csv"
    assert rows[0].read_options == {"option1": "value1"}
    assert rows[0].table_properties == {"property1": "value1"}
    assert rows[0].tags == {"tag1": "value1"}
    assert rows[0].spark_conf == {"conf1": "value1"}
    assert rows[0].expectations.expect_all == {"column1": "value1"}
    assert rows[0].expectations.expect_all_or_fail == {"column2": "value2"}
    assert rows[0].is_quarantined == False
    assert rows[0].apply_changes.sequence_by == "foo1"
    assert rows[0].apply_changes.column_list == ["column1"]
    assert rows[0].primary_keys == ["column1"]
