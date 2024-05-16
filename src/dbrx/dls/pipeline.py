from __future__ import annotations

from typing import Any, Dict, List, Optional

import dlt
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from .factory import DataFrameFactory
from .filters import Predicate, by_is_enabled, by_is_latest
from .logging import Logger, create_logger
from .models import (
    DeltaLiveEntity,
    ApplyChanges,
    Expectations,
    DeltaLiveEntityList,
)
from .store import DeltaLiveStore

# from .types import DeltaLiveEntityExpectations
from .utils import create_session

logger: Logger = create_logger(__name__)

QUARANTINE_COL: str = "is_quarantined"


def can_quarantine(entity: DeltaLiveEntity) -> bool:
    expect_all: Dict[str, str] = entity.expectations.expect_all
    quarantine: bool = entity.is_quarantined and bool(expect_all)
    logger.debug(f"Can quarantine: {quarantine}")
    return quarantine


def quarantine_rules(entity: DeltaLiveEntity) -> str:
    expect_all: Dict[str, str] = entity.expectations.expect_all
    rules: str = (
        "NOT({0})".format(" AND ".join(expect_all.values()))
        if can_quarantine(entity)
        else "1=0"
    )
    logger.debug(f"Quarantine rules: {rules}")
    return rules


def has_scd(entity: DeltaLiveEntity) -> bool:
    scd: bool = bool(entity.primary_keys) and bool(entity.apply_changes)
    return scd


class DeltaLiveStorePipeline:
    """
    Represents a pipeline for generating tables or views for Delta Live Store entities.
    """

    def __init__(
        self, store: DeltaLiveStore, spark: Optional[SparkSession] = None
    ) -> None:
        """
        Initializes a new instance of the DeltaLiveStorePipeline class.

        Args:
            store: The DeltaLiveStore instance representing the store to generate tables/views for.
            spark: Optional SparkSession instance. If not provided, a new session will be created.
        """
        self.store: DeltaLiveStore = store
        self.filter: Predicate = by_is_enabled() & by_is_latest()

        self.spark: SparkSession
        if spark is None:
            self.spark = store.spark if store.spark is not None else create_session()
        else:
            self.spark = spark

    def with_filter(self, filter: Predicate) -> DeltaLiveStorePipeline:
        """
        Sets the filter predicate for the pipeline.

        Args:
            filter: The filter predicate to apply.

        Returns:
            The updated DeltaLiveStorePipeline instance.
        """
        self.filter = filter
        return self

    def run(self) -> None:
        """
        Runs the pipeline, generating tables/views for the entities in the store.
        """
        logger.info(f"Running pipeline for store: {self.store.table}")
        entities: DeltaLiveEntityList = self.store.find_entities(self.filter)
        for entity in entities:
            self.generate(entity)

    def generate(self, entity: DeltaLiveEntity) -> None:
        """
        Generates a table or view for the specified entity.

        Args:
            entity: The DeltaLiveEntity instance representing the entity to generate for.
        """
        match entity.destination_type:
            case "table":
                self.generate_table(entity)
            case "view":
                self.generate_table(entity)
            case _:
                raise ValueError(
                    f"Unsupported destination type: {entity.destination_type}"
                )

    def generate_table(self, entity: DeltaLiveEntity) -> None:
        """
        Generates a table for the specified entity.

        Args:
            entity: The DeltaLiveEntity instance representing the entity to generate a table for.
        """
        logger.info(f"Generating table for entity: {entity.entity_id}")

        partition_cols: List[str] = entity.partition_cols
        name: str = entity.destination
        quarantine_name: str = f"{name}_quarantine"
        invalid_name: str = f"{name}_invalid"

        if can_quarantine(entity):

            self._create_quarantine_tables(
                valid_name=name,
                invalid_name=invalid_name,
                quarantine_name=quarantine_name,
                entity=entity,
            )

            name = quarantine_name
            partition_cols = [QUARANTINE_COL] + partition_cols

        if has_scd(entity):
            self._create_scd_table(name, partition_cols, entity)
        else:
            self._create_table(name, partition_cols, entity)

    def generate_view(self, entity: DeltaLiveEntity) -> None:
        """
        Generates a view for the specified entity.

        Args:
            entity: The DeltaLiveEntity instance representing the entity to generate a view for.
        """
        logger.info(f"Generating view for entity: {entity.entity_id}")

        entity_expectations: Expectations = entity.expectations

        @dlt.view(
            name=entity.destination,
            comment=entity.comment,
            spark_conf=entity.spark_conf,
        )
        @dlt.expect_all(expectations=entity_expectations.expect_all)
        @dlt.expect_all_or_drop(expectations=entity_expectations.expect_all_or_drop)
        @dlt.expect_all_or_fail(expectations=entity_expectations.expect_all_or_fail)
        def _():
            df: DataFrame = DataFrameFactory(entity, spark=self.spark).create()
            return df

    def _create_quarantine_tables(
        self,
        valid_name: str,
        invalid_name: str,
        quarantine_name: str,
        entity: DeltaLiveEntity,
    ):
        @dlt.table(name=valid_name, partition_cols=entity.partition_cols)
        def valid_data():
            df: DataFrame = (
                dlt.readStream(quarantine_name)
                if entity.is_streaming and not has_scd(entity)
                else dlt.read(quarantine_name)
            )
            return df.filter(f"{QUARANTINE_COL}=false").drop(
                QUARANTINE_COL, "_rescued_data"
            )

        @dlt.table(name=invalid_name, partition_cols=entity.partition_cols)
        def invalid_data():
            df: DataFrame = (
                dlt.readStream(quarantine_name)
                if entity.is_streaming and not has_scd(entity)
                else dlt.read(quarantine_name)
            )
            return df.filter(f"{QUARANTINE_COL}=true").drop(QUARANTINE_COL)

    def _create_table(
        self, name: str, partition_cols: List[str], entity: DeltaLiveEntity
    ):
        logger.debug(f"Creating table: {name}")
        entity_expectations: Expectations = entity.expectations
        is_temporary: bool = entity.is_quarantined

        @dlt.table(
            name=name,
            schema=entity.source_schema,
            comment=entity.comment,
            partition_cols=partition_cols,
            table_properties=entity.table_properties,
            spark_conf=entity.spark_conf,
            temporary=is_temporary,
        )
        @dlt.expect_all(expectations=entity_expectations.expect_all)
        @dlt.expect_all_or_drop(expectations=entity_expectations.expect_all_or_drop)
        @dlt.expect_all_or_fail(expectations=entity_expectations.expect_all_or_fail)
        def target_table():
            factory: DataFrameFactory = DataFrameFactory(
                entity, store=self.store, spark=self.spark
            )
            df: DataFrame = factory.create()
            if can_quarantine(entity):
                rules: str = quarantine_rules(entity)
                df = df.withColumn(QUARANTINE_COL, F.expr(rules))
            return df

    def _create_scd_table(
        self, name: str, partition_cols: List[str], entity: DeltaLiveEntity
    ):
        logger.debug(f"Creating SCD table: {name}")
        entity_expectations: Expectations = entity.expectations

        dlt.create_streaming_table(
            name=name,
            schema=entity.source_schema,
            comment=entity.comment,
            partition_cols=partition_cols,
            table_properties=entity.table_properties,
            spark_conf=entity.spark_conf,
            expect_all=entity_expectations.expect_all,
            expect_all_or_drop=entity_expectations.expect_all_or_drop,
            expect_all_or_fail=entity_expectations.expect_all_or_fail,
        )
        dlt.apply_changes(
            target=name,
            source=entity.source,
            keys=entity.primary_keys,
            sequence_by=entity.apply_changes.sequence_by,
            where=entity.apply_changes.where,
            ignore_null_updates=entity.apply_changes.ignore_null_updates,
            apply_as_deletes=entity.apply_changes.apply_as_deletes,
            apply_as_truncates=entity.apply_changes.apply_as_truncates,
            column_list=entity.apply_changes.column_list,
            except_column_list=entity.apply_changes.except_column_list,
            stored_as_scd_type=entity.apply_changes.stored_as_scd_type,
            track_history_column_list=entity.apply_changes.track_history_column_list,
            track_history_except_column_list=entity.apply_changes.track_history_except_column_list,
            flow_name=entity.apply_changes.flow_name,
            ignore_null_updates_column_list=entity.apply_changes.ignore_null_updates_column_list,
            ignore_null_updates_except_column_list=entity.apply_changes.ignore_null_updates_except_column_list,
        )
