import functools
from typing import Callable

import dlt
from pyspark.sql import DataFrame

from .logging import Logger, create_logger
from .models import DeltaLiveEntity
from .types import DeltaLiveEntityExpectations

logger: Logger = create_logger(__name__)


class table:
    """
    Decorator class for creating Delta Lake tables.
    """

    def __init__(self, entity: DeltaLiveEntity):
        self.entity = entity

    def __call__(
        self, function
    ) -> Callable[[Callable[[], DataFrame]], Callable[[], DataFrame]]:
        """
        Decorator method that wraps the decorated function with the dlt.table decorator.

        Args:
            function: The function to be decorated.

        Returns:
            The decorated function.
        """

        entity: DeltaLiveEntity = self.entity
        entity_expectations: DeltaLiveEntityExpectations = entity.expectations

        @functools.wraps(function)
        @dlt.table(
            name=entity.destination,
            schema=entity.source_schema,
            comment=entity.comment,
            partition_cols=entity.partition_cols,
            table_properties=entity.table_properties,
            spark_conf=entity.spark_conf,
        )
        @dlt.expect_all(expectations=entity_expectations.get("expect_all", {}))
        @dlt.expect_all_or_drop(
            expectations=entity_expectations.get("expect_all_or_drop", {})
        )
        @dlt.expect_all_or_fail(
            expectations=entity_expectations.get("expect_all_or_fail", {})
        )
        def wrapper(*args, **kwargs):
            return function(*args, **kwargs)

        return wrapper


class view:
    """
    Decorator class for creating Delta Lake views.
    """

    def __init__(self, entity: DeltaLiveEntity):
        self.entity = entity

    def __call__(
        self, function
    ) -> Callable[[Callable[[], DataFrame]], Callable[[], DataFrame]]:
        """
        Decorator method that wraps the decorated function with the dlt.view decorator.

        Args:
            function: The function to be decorated.

        Returns:
            The decorated function.
        """

        entity: DeltaLiveEntity = self.entity
        entity_expectations: DeltaLiveEntityExpectations = entity.expectations

        @functools.wraps(function)
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
        def wrapper(*args, **kwargs):
            return function(*args, **kwargs)

        return wrapper
