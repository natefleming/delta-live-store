from typing import Optional

import dlt
from pyspark.sql import DataFrame, SparkSession

from .filters import Predicate, by_destination, by_is_enabled, by_is_latest
from .logging import Logger, create_logger
from .models import DeltaLiveEntity
from .store import DeltaLiveStore
from .utils import create_session

logger: Logger = create_logger(__name__)


class DataFrameFactory:
    """
    A factory class for creating DataFrames based on DeltaLiveEntity.

    Args:
        entity (DeltaLiveEntity): The DeltaLiveEntity for which the DataFrame is created.
        store (DeltaLiveStore): The DeltaLiveStore instance used for checking pipeline dependencies.
        spark (Optional[SparkSession]): The SparkSession instance to use. If not provided, a new session will be created.

    Attributes:
        entity (DeltaLiveEntity): The DeltaLiveEntity for which the DataFrame is created.
        store (DeltaLiveStore): The DeltaLiveStore instance used for checking pipeline dependencies.
        spark (SparkSession): The SparkSession instance used for creating DataFrames.
    """

    def __init__(
        self,
        entity: DeltaLiveEntity,
        store: DeltaLiveStore,
        spark: Optional[SparkSession] = None,
    ):
        self.entity = entity
        self.store = store
        self.spark = spark if spark is not None else create_session()

    def create(self) -> DataFrame:
        """
        Creates a DataFrame based on the type of DeltaLiveEntity.

        Returns:
            DataFrame: The created DataFrame.
        """
        logger.info(f"Creating DataFrame for entity: {self.entity.entity_id}")
        df: DataFrame = None
        if self.entity.is_streaming:
            df = self.create_streaming()
        else:
            df = self.create_static()

        if self.entity.select_expr:
            logger.debug(f"Applying select expression: {self.entity.select_expr}")
            df = df.selectExpr(*self.entity.select_expr)

        return df

    def create_streaming(self) -> DataFrame:
        """
        Creates a streaming DataFrame based on the DeltaLiveEntity.

        Returns:
            DataFrame: The created streaming DataFrame.
        """
        logger.info(f"Creating streaming DataFrame for entity: {self.entity.entity_id}")
        df: DataFrame = None
        if self._has_pipeline_dependency(self.entity):
            df = dlt.readStream(self.entity.source)
        else:
            df = (
                self.spark.readStream.format(self.entity.source_format)
                .options(**self.entity.read_options)
                .load(self.entity.source)
            )
        return df

    def create_static(self) -> DataFrame:
        """
        Creates a static DataFrame based on the DeltaLiveEntity.

        Returns:
            DataFrame: The created static DataFrame.
        """
        logger.info(f"Creating static DataFrame for entity: {self.entity.entity_id}")
        df: DataFrame = None
        if self._has_pipeline_dependency(self.entity):
            df = dlt.read(self.entity.source)
        else:
            df = (
                self.spark.read.format(self.entity.source_format)
                .options(**self.entity.read_options)
                .load(self.entity.source)
            )
        return df

    def _has_pipeline_dependency(self, entity: DeltaLiveEntity) -> bool:
        """
        Checks if the DeltaLiveEntity has a pipeline dependency.

        Args:
            entity (DeltaLiveEntity): The DeltaLiveEntity to check.

        Returns:
            bool: True if the entity has a pipeline dependency, False otherwise.
        """
        has_pipeline_dependency: bool = entity.source_format == "dlt"
        if not has_pipeline_dependency:
            predicate: Predicate = (
                by_is_enabled() & by_is_latest() & by_destination(entity.source)
            )
            has_pipeline_dependency: bool = self.store.entity_exists(predicate)
        return has_pipeline_dependency
