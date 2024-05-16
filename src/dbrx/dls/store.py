from __future__ import annotations

from typing import Callable, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession

from dbrx.dls.models import DeltaLiveEntity, DeltaLiveEntityList

from .logging import Logger, create_logger
from .types import (  # DeltaLiveEntityExpectations,; ApplyChanges,
    DestinationType,
    SourceFormat,
)
from .utils import create_session, parse_db_schema_table

logger: Logger = create_logger(__name__)


PRIMARY_KEYS: List[str] = [
    "entity_id",
]

PROVIDED_COLUMNS: List[str] = [
    "apply_changes",
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
                expectations STRUCT<
                    expect_all: MAP<STRING, STRING>,
                    expect_all_or_drop: MAP<STRING, STRING>,
                    expect_all_or_fail: MAP<STRING, STRING>>,
                apply_changes STRUCT<
                    sequence_by: STRING,
                    where: STRING,
                    ignore_null_updates: BOOLEAN,
                    apply_as_deletes: STRING,
                    apply_as_truncates: STRING,
                    column_list: ARRAY<STRING>,
                    except_column_list: ARRAY<STRING>,
                    stored_as_scd_type: INTEGER,
                    track_history_column_list: ARRAY<STRING>,
                    track_history_except_column_list: ARRAY<STRING>,
                    flow_name: STRING,
                    ignore_null_updates_column_list: ARRAY<STRING>,
                    ignore_null_updates_except_column_list: ARRAY<STRING>>,
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

    def sync(self) -> None:
        """
        Synchronizes the store by marking both history and latest as dirty.
        """
        self._is_history_dirty = True
        self._is_latest_dirty = True
        
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
        from .filters import Predicate, by_destination, by_is_enabled, by_source

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
