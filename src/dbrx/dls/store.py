from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
import pyspark.sql.types as T
from pyspark.sql import DataFrame, Row, SparkSession

from .logging import Logger, create_logger
from .types import (DeltaLiveEntityExpectations, DestinationType, ReadOptions,
                    SourceFormat, SparkConf, TableProperties, Tags)
from .utils import create_session, parse_db_schema_table

logger: Logger = create_logger(__name__)


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
    expectations: DeltaLiveEntityExpectations = field(default_factory=dict)
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

        for key, value in self.expectations.items():
            if key not in ["expect_all", "expect_all_or_drop", "expect_all_or_fail"]:
                raise ValueError(
                    f"Invalid expectation key: {key}. Valid keys are: expect_all, expect_all_or_drop, expect_all_or_fail"
                )
            if not isinstance(value, dict):
                raise ValueError(f"Invalid expectation value: {value}")
            for k, v in value.items():
                if not isinstance(k, str) or not isinstance(v, str):
                    raise ValueError(f"Invalid expectation value: {value}")

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
                    T.MapType(
                        T.StringType(), T.MapType(T.StringType(), T.StringType())
                    ),
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


PRIMARY_KEYS: List[str] = [
    "entity_id",
]

PROVIDED_COLUMNS: List[str] = [
    "comment",
    "destination",
    "destination_type",
    "is_enabled",
    "entity_id",
    "expectations",
    "group",
    "is_quarantined",
    "is_streaming",
    "partition_cols",
    "primary_keys",
    "read_options",
    "select_expr",
    "source_format",
    "source",
    "source_schema",
    "spark_conf",
    "table_properties",
    "tags",
]

GENERATED_COLUMNS: List[str] = [
    "created_by",
    "created_ts",
    "modified_by",
    "expired_ts",
    "hash",
    "id",
    "is_latest",
]


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


class DeltaLiveStore:
    """
    Represents a Delta Live store.

    Attributes:
        table (str): The table name.
        spark (Optional[SparkSession]): The Spark session. Defaults to None.
    """

    table: str
    spark: Optional[SparkSession]

    # TODO: Reconsider/Refactor caching strategy.
    _is_latest_dirty: bool
    _is_history_dirty: bool
    _cached_latest_entities: DeltaLiveEntityList
    _cached_history_entities: DeltaLiveEntityList

    def __init__(self, table: str, spark: Optional[SparkSession] = None):
        """
        Initializes a DeltaLiveStore object.

        Args:
            table (str): The table name.
            spark (Optional[SparkSession], optional): The Spark session. Defaults to None.
        """
        self.table: str = table
        self.spark = spark if spark is not None else create_session()
        self._clear_cache()

    def initialize(self) -> None:
        """
        Ensures that the table exists in the store.
        If the table does not exist, it will be created.
        """
        logger.info(f"Ensuring table exists: {self.table}")
        catalog: str
        database: str
        table: str
        catalog, database, table = parse_db_schema_table(self.table)
        if catalog:
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
            self.spark.sql(f"USE CATALOG {catalog}")
        if database:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
            self.spark.sql(f"USE DATABASE {database}")

        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id STRING,
                entity_id STRING,
                destination_type STRING,
                group STRING,
                source STRING,
                source_format STRING,
                source_schema STRING,
                select_expr ARRAY<STRING>,
                is_streaming BOOLEAN,
                primary_keys ARRAY<STRING>,
                read_options MAP<STRING, STRING>,
                table_properties MAP<STRING, STRING>,
                tags MAP<STRING, STRING>,
                expectations MAP<STRING, MAP<STRING, STRING>>,
                is_quarantined BOOLEAN,
                spark_conf MAP<STRING, STRING>,
                partition_cols ARRAY<STRING>,
                destination STRING,
                comment STRING,
                created_ts TIMESTAMP,
                expired_ts TIMESTAMP,
                created_by STRING,
                modified_by STRING,
                is_enabled BOOLEAN,
                is_latest BOOLEAN,
                hash INTEGER
            )
        """
        )

    def destroy(self) -> None:
        """
        Drops the table from the store.
        """
        logger.info(f"Dropping table: {self.table}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.table}")
        self._clear_cache()

    def is_initialized(self) -> bool:
        """
        Checks if the table exists in the store.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        logger.info(f"Checking if table exists: {self.table}")
        return bool(self.spark.catalog.tableExists(self.table))

    def find_history(
        self, predicate: Optional[Callable[[DeltaLiveEntity], bool]]
    ) -> DeltaLiveEntityList:
        return self.history_entities.filter(predicate)

    def find_entities(
        self, predicate: Optional[Callable[[DeltaLiveEntity], bool]] = None
    ) -> DeltaLiveEntityList:
        """
        Finds the entities in the store based on the given predicate.

        Args:
            predicate (Optional[Callable[[DeltaLiveEntity], bool]], optional): The predicate function. Defaults to None.
            include_history (bool, optional): Indicates whether to include historical versions of the entities. Defaults to False.

        Returns:
            DeltaLiveEntityList: The list of entities matching the predicate.
        """
        if predicate is None:
            from .filters import by_is_enabled, by_is_latest

            predicate = by_is_latest() & by_is_enabled()

        logger.info(f"Finding entities matching predicate: {predicate}")
        return self.latest_entities.filter(predicate)

    def find_entity(
        self, predicate: Callable[[DeltaLiveEntity], bool]
    ) -> Optional[DeltaLiveEntity]:
        """
        Finds a single entity in the store based on the given predicate.

        Args:
            predicate (Callable[[DeltaLiveEntity], bool]): The predicate function.

        Returns:
            Optional[DeltaLiveEntity]: The entity matching the predicate, or None if no entity is found.
        """
        logger.info(f"Finding entity matching predicate: {predicate}")
        return self.find_entities(predicate).first()

    def entity_exists(self, predicate: Callable[[DeltaLiveEntity], bool]) -> bool:
        """
        Checks if an entity exists in the store based on the given predicate.

        Args:
            predicate (Callable[[DeltaLiveEntity], bool]): The predicate function.

        Returns:
            bool: True if the entity exists, False otherwise.
        """
        logger.info(f"Checking if entity exists matching predicate: {predicate}")
        return bool(self.find_entity(predicate))

    def enable(self, predicate: Callable[[DeltaLiveEntity], bool]) -> None:
        """
        Enables entities in the store based on the given predicate.

        Args:
            predicate (Callable[[DeltaLiveEntity], bool]): The predicate function.
        """
        logger.info(f"Enabling entities matching predicate: {predicate}")
        self._update_enabled(predicate, True)

    def disable(self, predicate: Callable[[DeltaLiveEntity], bool]) -> None:
        """
        Disables entities in the store based on the given predicate.

        Args:
            predicate (Callable[[DeltaLiveEntity], bool]): The predicate function.
        """
        logger.debug(f"Disabling entities matching predicate: {predicate}")
        self._update_enabled(predicate, False)

    def upsert(self, entity: DeltaLiveEntity, *additional: DeltaLiveEntity) -> None:
        """
        Upserts the given entities into the store.

        Args:
            entity (DeltaLiveEntity): The first entity to upsert.
            additional (DeltaLiveEntity): Additional entities to upsert.
        """
        new_entries: DeltaLiveEntityList = DeltaLiveEntityList(
            [entity, *additional], spark=self.spark
        )
        logger.info(f"Upserting [{len(new_entries)}] entities...")
        new_entries_df: DataFrame = new_entries.to_df()

        new_entries_df = new_entries_df.selectExpr(
            "CURRENT_USER() AS created_by",
            "CURRENT_TIMESTAMP() AS created_ts",
            "NULL::TIMESTAMP AS expired_ts",
            f"HASH({','.join(PROVIDED_COLUMNS)}) AS hash",
            "UUID() AS id",
            "TRUE AS is_latest",
            f"* EXCEPT({','.join(GENERATED_COLUMNS)})",
        )
        new_entries_df.createOrReplaceTempView("new_entries_df")

        merge_key_col: str = "entity_id"

        sql: str = f"""
            MERGE INTO {self.table} AS target
            USING(
                SELECT 
                    new_entries_df.{merge_key_col} AS _merge_key
                    ,new_entries_df.*
                FROM new_entries_df
                UNION ALL
                SELECT 
                    NULL::STRING AS _merge_key
                    ,new_entries_df.*
                FROM new_entries_df
                JOIN {self.table} 
                ON new_entries_df.{merge_key_col} = {self.table}.{merge_key_col}
                WHERE {self.table}.is_latest = TRUE AND new_entries_df.hash <> {self.table}.hash
            ) AS source
            ON target.{merge_key_col} = source._merge_key
            WHEN MATCHED AND target.is_latest = TRUE AND target.hash <> source.hash 
            THEN UPDATE SET 
                is_latest = FALSE
                ,expired_ts = source.created_ts
                ,modified_by = CURRENT_USER()
            WHEN NOT MATCHED 
            THEN INSERT (
                {','.join(new_entries_df.columns)}
            ) VALUES (
                {','.join([f'source.{c}' for c in new_entries_df.columns])}
            )
        """

        logger.debug(f"Executing SQL: {sql}")

        self.spark.sql(sql)

        self._is_latest_dirty = True
        self._is_history_dirty = True

    @property
    def latest_entities(self) -> DeltaLiveEntityList:
        if self._is_latest_dirty:
            self._cached_latest_entities = self._refresh(is_latest=True)
            self._is_latest_dirty = False
        return self._cached_latest_entities

    @property
    def history_entities(self) -> DeltaLiveEntityList:
        if self._is_history_dirty:
            self._cached_history_entities = self._refresh(is_latest=False)
            self._is_history_dirty = False
        return self._cached_history_entities

    def _refresh(self, is_latest: bool = True) -> DeltaLiveEntityList:
        """
        Refreshes the entities in the store.

        Args:
            is_latest (bool, optional): Flag to indicate whether to filter only the latest entities.
                Defaults to True.

        Returns:
            DeltaLiveEntityList: A list of refreshed DeltaLiveEntity objects.
        """

        logger.debug(f"Refreshing entities(is_latest={is_latest})...")

        df: DataFrame = self.spark.table(self.table)
        if is_latest:
            df = df.filter("is_latest = TRUE")
        df = df.orderBy("created_ts", ascending=False)

        refreshed_entities: DeltaLiveEntityList = DeltaLiveEntityList.from_df(df)
        return refreshed_entities

    def _find_dependant_entities(
        self,
        entity: DeltaLiveEntity,
        predicate_factory: Callable[[DeltaLiveEntity], "Predicate"],
        dependant_entities: List[DeltaLiveEntity],
    ) -> None:
        """
        Recursively finds all dependent entities for a given entity based on a predicate.

        Args:
            entity (DeltaLiveEntity): The entity for which to find dependant entities.
            predicate_factory (Callable[[DeltaLiveEntity], "Predicate"]): A factory function that creates a predicate for filtering entities.
            dependant_entities (List[DeltaLiveEntity]): A list to store the found dependant entities.

        Returns:
            None
        """
        from .filters import Predicate

        predicate: Predicate = predicate_factory(entity)
        logger.debug(f"predicate: {predicate}")
        entities: DeltaLiveEntityList = self.find_entities(predicate)
        for entity in entities:
            if any(e.entity_id == entity.entity_id for e in dependant_entities):
                continue
            dependant_entities.append(entity)
            self._find_dependant_entities(entity, predicate_factory, dependant_entities)

    def _update_enabled(
        self, predicate: Callable[[DeltaLiveEntity], bool], enabled: bool
    ) -> None:
        """
        Updates the enabled status of entities and all of their dependencies in the store based on the given predicate.

        Args:
            predicate (Callable[[DeltaLiveEntity], bool]): The predicate function.
            enabled (bool): The enabled status to set for the entities.
        """
        from .filters import (Predicate, by_destination, by_is_enabled,
                              by_source)

        predicate_factory: Callable[[DeltaLiveEntity], Predicate]
        if enabled:
            predicate_factory = lambda entity: by_destination(
                entity.source
            ) & by_is_enabled(not enabled)
        else:
            predicate_factory = lambda entity: by_source(
                entity.destination
            ) & by_is_enabled(not enabled)

        entities: List[DeltaLiveEntity] = []
        parent_entries: List[DeltaLiveEntity] = self.find_entities(predicate).to_list()
        for parent in parent_entries:
            if parent in entities:
                continue
            entities.append(parent)
            dependant_entities: List[DeltaLiveEntity] = []
            self._find_dependant_entities(parent, predicate_factory, dependant_entities)
            entities.extend(dependant_entities)

        for entity in entities:
            entity.is_enabled = enabled

        if len(entities) > 0:
            self.upsert(*entities)

    def _clear_cache(self) -> None:
        self._is_latest_dirty = True
        self._is_history_dirty = True
        self._cached_latest_entities = DeltaLiveEntityList(
            entities=[], spark=self.spark
        )
        self._cached_history_entities = DeltaLiveEntityList(
            entities=[], spark=self.spark
        )
