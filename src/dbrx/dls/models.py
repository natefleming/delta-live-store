from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Union, Optional, Dict

import pandas as pd
import pyspark.sql.types as T
from pyspark.sql import DataFrame, Row, SparkSession

from dbrx.dls.types import (
    DestinationType,
    ReadOptions,
    SourceFormat,
    SparkConf,
    TableProperties,
    Tags,
)
from dbrx.dls.utils import create_session


@dataclass
class ApplyChanges:

    sequence_by: str
    where: Optional[str] = None
    ignore_null_updates: Optional[bool] = None
    apply_as_deletes: Optional[str] = None
    apply_as_truncates: Optional[str] = None
    column_list: Optional[List[str]] = None
    except_column_list: Optional[List[str]] = None
    stored_as_scd_type: int = 1
    track_history_column_list: Optional[List[str]] = None
    track_history_except_column_list: Optional[List[str]] = None
    flow_name: Optional[str] = None
    ignore_null_updates_column_list: Optional[List[str]] = None
    ignore_null_updates_except_column_list: Optional[List[str]] = None

    @classmethod
    def spark_schema(cls) -> T.StructType:
        """
        Returns the Spark schema for the Delta Live entity expectations.

        Returns:
            T.StructType: The Spark schema for the Delta Live entity expectations.
        """
        schema: T.StructType = T.StructType(
            [
                T.StructField("sequence_by", T.StringType(), True),
                T.StructField("where", T.StringType(), True),
                T.StructField("ignore_null_updates", T.BooleanType(), True),
                T.StructField("apply_as_deletes", T.StringType(), True),
                T.StructField("apply_as_truncates", T.StringType(), True),
                T.StructField("column_list", T.ArrayType(T.StringType()), True),
                T.StructField("except_column_list", T.ArrayType(T.StringType()), True),
                T.StructField("stored_as_scd_type", T.IntegerType(), True),
                T.StructField(
                    "track_history_column_list", T.ArrayType(T.StringType()), True
                ),
                T.StructField(
                    "track_history_except_column_list",
                    T.ArrayType(T.StringType()),
                    True,
                ),
                T.StructField("flow_name", T.StringType(), True),
                T.StructField(
                    "ignore_null_updates_column_list", T.ArrayType(T.StringType()), True
                ),
                T.StructField(
                    "ignore_null_updates_except_column_list",
                    T.ArrayType(T.StringType()),
                    True,
                ),
            ]
        )
        return schema

    def copy(self) -> ApplyChanges:
        """
        Creates a copy of the Delta Live entity apply changes.

        Returns:
            ApplyChanges: The copy of the Delta Live entity apply changes.
        """
        return ApplyChanges(**self.to_dict())

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the Delta Live entity apply changes to a dictionary.

        Returns:
            Dict[str, any]: The dictionary representation of the Delta Live entity apply changes.
        """
        return asdict(self)


@dataclass
class Expectations:
    """
    Represents the expectations for a Delta Live entity.

    Attributes:
        expect_all (Dict[str, str], optional): The expect all expectations. Defaults to an empty dictionary.
        expect_all_or_drop (Dict[str, str], optional): The expect all or drop expectations. Defaults to an empty dictionary.
        expect_all_or_fail (Dict[str, str], optional): The expect all or fail expectations. Defaults to an empty dictionary.
    """

    expect_all: Dict[str, str] = field(default_factory=dict)
    expect_all_or_drop: Dict[str, str] = field(default_factory=dict)
    expect_all_or_fail: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def spark_schema(cls) -> T.StructType:
        """
        Returns the Spark schema for the Delta Live entity expectations.

        Returns:
            T.StructType: The Spark schema for the Delta Live entity expectations.
        """
        schema: T.StructType = T.StructType(
            [
                T.StructField(
                    "expect_all", T.MapType(T.StringType(), T.StringType()), True
                ),
                T.StructField(
                    "expect_all_or_drop",
                    T.MapType(T.StringType(), T.StringType()),
                    True,
                ),
                T.StructField(
                    "expect_all_or_fail",
                    T.MapType(T.StringType(), T.StringType()),
                    True,
                ),
            ]
        )
        return schema

    def copy(self) -> Expectations:
        """
        Creates a copy of the Delta Live entity expectations.

        Returns:
            DeltaLiveEntityExpectations: The copy of the Delta Live entity expectations.
        """
        return Expectations(**self.to_dict())

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the Delta Live entity expectations to a dictionary.

        Returns:
            Dict[str, any]: The dictionary representation of the Delta Live entity expectations.
        """
        return asdict(self)


@dataclass
class DeltaLiveEntity:
    """
    Represents a Delta Live entity.

    Attributes:
        entity_id (str): The ID of the entity.
        source (str): The source of the entity.
        destination (str): The destination of the entity.
        destination_type (DestinationType, optional): The type of the destination. Defaults to "table".
        source_format (SourceFormat, optional): The format of the source. Defaults to "cloudFiles".
        is_streaming (bool, optional): Indicates if the entity is streaming. Defaults to True.
        primary_keys (List[str], optional): The primary keys. Defaults to an empty list.
        source_schema (str, optional): The schema of the source. Defaults to None.
        select_expr (List[str], optional): The list of select expressions. Defaults to an empty list.
        read_options (ReadOptions, optional): The read options for the entity. Defaults to an empty dictionary.
        table_properties (TableProperties, optional): The properties of the table. Defaults to an empty dictionary.
        tags (Tags, optional): The tags associated with the entity. Defaults to an empty dictionary.
        spark_conf (SparkConf, optional): The Spark configuration for the entity. Defaults to an empty dictionary.
        partition_cols (List[str], optional): The partition columns of the entity. Defaults to an empty list.
        group (str, optional): The group of the entity. Defaults to None.
        comment (str, optional): The comment for the entity. Defaults to None.
        id (str, optional): The ID of the entity. Defaults to None.
        created_ts (datetime, optional): The timestamp when the entity was created. Defaults to None.
        expired_ts (datetime, optional): The timestamp when the entity expired. Defaults to None.
        created_by (str, optional): The user who created the entity. Defaults to None.
        is_enabled (bool, optional): Indicates if the entity is enabled. Defaults to True.
        is_latest (bool, optional): Indicates if the entity is the latest version. Defaults to None.
        hash (bool, optional): The hash value of the entity. Defaults to None.
        expectations (DeltaLiveEntityExpectations, optional): The expectations for the entity. Valid keys are: expect_all, expect_all_or_drop, expect_all_or_fail. Defaults to an empty dictionary.
        is_quarantined (bool, optional): Indicates if the entity is will quarantine invalid records. Defaults to False.
        apply_changes (ApplyChanges, optional): The apply CDC changes for the entity. Defaults to an empty dictionary.
    """

    entity_id: str
    source: str
    destination: str
    destination_type: DestinationType = field(default="table")
    source_format: SourceFormat = field(default="cloudFiles")
    is_streaming: bool = True
    primary_keys: List[str] = field(default_factory=list)
    source_schema: Optional[str] = None
    select_expr: List[str] = field(default_factory=list)
    read_options: ReadOptions = field(default_factory=dict)
    table_properties: TableProperties = field(default_factory=dict)
    tags: Tags = field(default_factory=dict)
    spark_conf: SparkConf = field(default_factory=dict)
    partition_cols: List[str] = field(default_factory=list)
    group: Optional[str] = None
    comment: Optional[str] = None
    id: Optional[str] = None
    created_ts: Optional[datetime] = None
    expired_ts: Optional[datetime] = None
    created_by: Optional[str] = None
    modified_by: Optional[str] = None
    is_enabled: bool = True
    is_latest: Optional[bool] = None
    hash: Optional[bool] = None
    expectations: Union[Expectations, Row, Dict[str, Dict[str, Any]], None] = None
    apply_changes: Union[ApplyChanges, Row, Dict[str, Any], None] = None
    is_quarantined: bool = False

    def __post_init__(self):
        """
        Post-initialization method.
        Performs additional initialization logic after the object is created.
        """
        if self.source_format in ["cloudFiles", "kafka"]:
            self.is_streaming = True
        if self.source_format in ["parquet", "csv", "json", "avro", "orc"]:
            self.is_streaming = False

        self.group = None if self.group == "" else self.group
        self.tags = {} if self.tags is None else self.tags
        self.spark_conf = {} if self.spark_conf is None else self.spark_conf
        self.partition_cols = [] if self.partition_cols is None else self.partition_cols
        self.table_properties = (
            {} if self.table_properties is None else self.table_properties
        )
        self.read_options = {} if self.read_options is None else self.read_options

        self.expectations = (
            Expectations() if self.expectations is None else self.expectations
        )

        if self.expectations is not None and not isinstance(
            self.expectations, Expectations
        ):
            if isinstance(self.expectations, Row):
                self.expectations = Expectations(**self.expectations.asDict())
            elif isinstance(self.expectations, dict):
                self.expectations = Expectations(**self.expectations)
            else:
                raise ValueError(
                    f"Invalid type for expectations. Must be a Row or a dictionary. Found: {type(self.expectations)}"
                )

        if self.apply_changes is not None and not isinstance(
            self.apply_changes, ApplyChanges
        ):
            if isinstance(self.apply_changes, Row):
                self.apply_changes = ApplyChanges(**self.apply_changes.asDict())
            elif isinstance(self.apply_changes, dict):
                self.apply_changes = ApplyChanges(**self.apply_changes)
            else:
                raise ValueError(
                    f"Invalid type for apply_changes. Must be a Row or a dictionary. Found: {type(self.apply_changes)}"
                )

    @classmethod
    def spark_schema(cls) -> T.StructType:
        """
        Returns the Spark schema for the Delta Live entity.

        Returns:
            T.StructType: The Spark schema for the Delta Live entity.
        """
        schema: T.StructType = T.StructType(
            [
                T.StructField("entity_id", T.StringType(), True),
                T.StructField("source", T.StringType(), True),
                T.StructField("destination", T.StringType(), True),
                T.StructField("destination_type", T.StringType(), True),
                T.StructField("source_format", T.StringType(), True),
                T.StructField("is_streaming", T.BooleanType(), True),
                T.StructField("primary_keys", T.ArrayType(T.StringType()), True),
                T.StructField("source_schema", T.StringType(), True),
                T.StructField("select_expr", T.ArrayType(T.StringType()), True),
                T.StructField(
                    "read_options", T.MapType(T.StringType(), T.StringType()), True
                ),
                T.StructField(
                    "table_properties", T.MapType(T.StringType(), T.StringType()), True
                ),
                T.StructField("tags", T.MapType(T.StringType(), T.StringType()), True),
                T.StructField(
                    "spark_conf", T.MapType(T.StringType(), T.StringType()), True
                ),
                T.StructField("partition_cols", T.ArrayType(T.StringType()), True),
                T.StructField("group", T.StringType(), True),
                T.StructField("comment", T.StringType(), True),
                T.StructField("id", T.StringType(), True),
                T.StructField("created_ts", T.TimestampType(), True),
                T.StructField("expired_ts", T.TimestampType(), True),
                T.StructField("created_by", T.StringType(), True),
                T.StructField("modified_by", T.StringType(), True),
                T.StructField("is_enabled", T.BooleanType(), True),
                T.StructField("is_latest", T.BooleanType(), True),
                T.StructField("hash", T.IntegerType(), True),
                T.StructField(
                    "expectations",
                    Expectations.spark_schema(),
                    True,
                ),
                T.StructField(
                    "apply_changes",
                    ApplyChanges.spark_schema(),
                    True,
                ),
                T.StructField("is_quarantined", T.BooleanType(), True),
            ]
        )
        return schema

    def copy(self) -> DeltaLiveEntity:
        """
        Creates a copy of the Delta Live entity.

        Returns:
            DeltaLiveEntity: The copy of the Delta Live entity.
        """
        return DeltaLiveEntity(**self.to_dict())

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the Delta Live entity to a dictionary.

        Returns:
            Dict[str, any]: The dictionary representation of the Delta Live entity.
        """
        return asdict(self)


class DeltaLiveEntityList:
    """
    Represents a list of Delta Live entities.

    Attributes:
        entities (List[DeltaLiveEntity]): The list of Delta Live entities.
        spark (Optional[SparkSession]): The Spark session. Defaults to None.
    """

    def __init__(
        self, entities: List[DeltaLiveEntity] = [], spark: Optional[SparkSession] = None
    ):
        """
        Initializes a DeltaLiveEntityList object.

        Args:
            entities (List[DeltaLiveEntity]): The list of Delta Live entities.
            spark (Optional[SparkSession], optional): The Spark session. Defaults to None.
        """
        self.entities: List[DeltaLiveEntity] = entities
        self.spark = spark if spark is not None else create_session()

    def filter(
        self, predicate: Callable[[DeltaLiveEntity], bool]
    ) -> DeltaLiveEntityList:
        """
        Filters the list of entities based on the given predicate.

        Args:
            predicate (Callable[[DeltaLiveEntity], bool]): The predicate function.

        Returns:
            DeltaLiveEntityList: The filtered list of entities.
        """
        filtered_entries: List[DeltaLiveEntity] = [
            e for e in self.entities if predicate(e)
        ]
        return DeltaLiveEntityList(entities=filtered_entries, spark=self.spark)

    def first(self) -> Optional[DeltaLiveEntity]:
        """
        Returns the first entity in the list.

        Returns:
            Optional[DeltaLiveEntity]: The first entity in the list, or None if the list is empty.
        """
        return next(iter(self.entities), None)

    def to_df(self) -> DataFrame:
        """
        Converts the list of entities to a Spark DataFrame.

        Returns:
            DataFrame: The Spark DataFrame representing the list of entities.
        """
        rows: List[Row] = [Row(**asdict(entity)) for entity in self.entities]
        schema: T.StructType = DeltaLiveEntity.spark_schema()
        return self.spark.createDataFrame(rows, schema=schema)

    def to_pdf(self) -> pd.DataFrame:
        """
        Converts the list of entities to a pandas DataFrame.

        Returns:
            pd.DataFrame: The pandas DataFrame representing the list of entities.
        """
        return self.to_df().toPandas()

    def to_list(self) -> List[DeltaLiveEntity]:
        """
        Converts the DeltaLiveEntityList to a list.

        Returns:
            List[DeltaLiveEntity]: The list of Delta Live entities.
        """
        return self.entities

    def to_json(self) -> List[Dict[str, Any]]:
        """
        Converts the DeltaLiveEntityList to a list of dictionaries.

        Returns:
            List[Dict[str, Any]]: The list of dictionaries representing the Delta Live entities.
        """
        return [entity.to_dict() for entity in self.entities]

    @classmethod
    def from_df(cls, df: DataFrame) -> DeltaLiveEntityList:
        rows: List[Row] = df.collect()
        entities: List[DeltaLiveEntity] = [
            DeltaLiveEntity(**row.asDict()) for row in rows
        ]
        return DeltaLiveEntityList(entities=entities, spark=df.sparkSession)

    def __add__(self, other: DeltaLiveEntityList) -> DeltaLiveEntityList:
        return DeltaLiveEntityList(
            entities=[*self.entities, *other.entities], spark=self.spark
        )

    def __iadd__(self, other: DeltaLiveEntityList) -> DeltaLiveEntityList:
        self.entities.extend(other.entities)
        return self

    def __iter__(self):
        return iter(self.entities)

    def __len__(self):
        return len(self.entities)

    def __getitem__(self, key):
        return self.entities[key]

    def __repr__(self):
        return f"DeltaLiveEntityList({self.entities})"
