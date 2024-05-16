import os
from typing import Generator

import pytest
from pyspark.sql import SparkSession

from dbrx.dls.store import DeltaLiveStore
from dbrx.dls.utils import create_session
from databricks.sdk import WorkspaceClient


@pytest.fixture()
def config_file() -> str:
    return os.path.dirname(os.path.abspath(__file__)) + "/sample.yml"


@pytest.fixture
def table_name() -> str:
    name: str = os.environ.get("DELTA_LIVE_STORE_TEST_TABLE_NAME")
    if name is None:
        raise ValueError("DELTA_LIVE_STORE_TEST_TABLE_NAME is not set")
    return name


@pytest.fixture
def volume_path() -> str:
    name: str = os.environ.get("DELTA_LIVE_STORE_TEST_VOLUME")
    if name is None:
        raise ValueError("DELTA_LIVE_STORE_TEST_VOLUME is not set")
    return name


@pytest.fixture(scope="function")
def delta_live_store(table_name: str) -> Generator[DeltaLiveStore, None, None]:
    control = DeltaLiveStore(table_name)
    control.initialize()
    yield control
    control.destroy()
    control = None


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    spark: SparkSession = create_session()
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def workspace_client() -> WorkspaceClient:
    return WorkspaceClient()
