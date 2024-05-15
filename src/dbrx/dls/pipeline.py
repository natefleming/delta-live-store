from __future__ import annotations

from typing import Any, Dict, List, Optional

import dlt
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from .factory import DataFrameFactory
from .filters import Predicate, by_is_enabled, by_is_latest
from .logging import Logger, create_logger
from .store import DeltaLiveEntity, DeltaLiveEntityList, DeltaLiveStore
from .types import DeltaLiveEntityExpectations
from .utils import create_session

logger: Logger = create_logger(__name__)

QUARANTINE_COL: str = "is_quarantined"


def can_quarantine(entity: DeltaLiveEntity) -> bool:
    expect_all: Dict[str, str] = entity.expectations.get("expect_all", {})
    quarantine: bool = entity.is_quarantined and bool(expect_all)
    logger.debug(f"Can quarantine: {quarantine}")
    return quarantine


def quarantine_rules(entity: DeltaLiveEntity) -> str:
    expect_all: Dict[str, str] = entity.expectations.get("expect_all", {})
    rules: str = (
        "NOT({0})".format(" AND ".join(expect_all.values()))
        if can_quarantine(entity)
        else "1=0"
    )
    logger.debug(f"Quarantine rules: {rules}")
    return rules


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

        entity_expectations: DeltaLiveEntityExpectations = entity.expectations

        partition_cols: List[str] = entity.partition_cols
        name: str = entity.destination
        quarantine_name: str = f"{name}_quarantine"
        invalid_name: str = f"{name}_invalid"

        if can_quarantine(entity):

            partition_cols = [QUARANTINE_COL] + partition_cols

            @dlt.table(name=name, partition_cols=entity.partition_cols)
            def valid_data():
                df: DataFrame = (
                    dlt.readStream(name) if entity.is_streaming else dlt.read(name)
                )
                return df.filter(f"{QUARANTINE_COL}=false").drop(
                    QUARANTINE_COL, "_rescued_data"
                )

            @dlt.table(name=invalid_name, partition_cols=entity.partition_cols)
            def invalid_data():
                df: DataFrame = (
                    dlt.readStream(name) if entity.is_streaming else dlt.read(name)
                )
                return df.filter(f"{QUARANTINE_COL}=true").drop(QUARANTINE_COL)

            name = quarantine_name

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
        @dlt.expect_all(expectations=entity_expectations.get("expect_all", {}))
        @dlt.expect_all_or_drop(
            expectations=entity_expectations.get("expect_all_or_drop", {})
        )
        @dlt.expect_all_or_fail(
            expectations=entity_expectations.get("expect_all_or_fail", {})
        )
        def target_table():
            factory: DataFrameFactory = DataFrameFactory(
                entity, store=self.store, spark=self.spark
            )
            df: DataFrame = factory.create()
            if can_quarantine(entity):
                rules: str = quarantine_rules(entity)
                df = df.withColumn(QUARANTINE_COL, F.expr(rules))
            return df

    def generate_view(self, entity: DeltaLiveEntity) -> None:
        """
        Generates a view for the specified entity.

        Args:
            entity: The DeltaLiveEntity instance representing the entity to generate a view for.
        """
        logger.info(f"Generating view for entity: {entity.entity_id}")

        entity_expectations: DeltaLiveEntityExpectations = entity.expectations

        @dlt.view(
            name=entity.destination,
            comment=entity.comment,
            spark_conf=entity.spark_conf,
        )
        @dlt.expect_all(expectations=entity_expectations.get("expect_all", {}))
        @dlt.expect_all_or_drop(
            expectations=entity_expectations.get("expect_all_or_drop", {})
        )
        @dlt.expect_all_or_fail(
            expectations=entity_expectations.get("expect_all_or_fail", {})
        )
        def _():
            df: DataFrame = DataFrameFactory(entity, spark=self.spark).create()
            return df
