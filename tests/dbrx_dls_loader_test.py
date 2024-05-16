import os
from typing import Generator

import pytest

from dbrx.dls.filters import by_is_latest
from dbrx.dls.loaders import DeltaLiveStoreLoader, FileServiceLoader, YamlLoader
from dbrx.dls.models import DeltaLiveEntity, DeltaLiveEntityList
from dbrx.dls.store import DeltaLiveStore
from databricks.sdk import WorkspaceClient


@pytest.mark.skipif(
    not os.environ.get("DELTA_LIVE_STORE_TEST_VOLUME"),
    reason="DELTA_LIVE_STORE_TEST_VOLUME is not set",
)
@pytest.mark.skipif(
    not os.environ.get("DELTA_LIVE_STORE_TEST_TABLE_NAME"),
    reason="DELTA_LIVE_STORE_TEST_TABLE_NAME is not set",
)
def test_file_system_loader(
    delta_live_store: DeltaLiveStore,
    volume_path: str,
    workspace_client: WorkspaceClient,
) -> None:
    actual_datset_count: int = len(
        list(workspace_client.files.list_directory_contents(volume_path))
    )
    loader: DeltaLiveStoreLoader = FileServiceLoader(volume_path)
    loader.load(delta_live_store)
    assert len(delta_live_store.find_entities().to_list()) == actual_datset_count
    assert True


@pytest.mark.skipif(
    not os.environ.get("DELTA_LIVE_STORE_TEST_TABLE_NAME"),
    reason="DELTA_LIVE_STORE_TEST_TABLE_NAME is not set",
)
def test_yaml_loader(delta_live_store: DeltaLiveStore, config_file: str) -> None:
    loader: DeltaLiveStoreLoader = YamlLoader(config_file)
    loader.load(delta_live_store)
    assert len(delta_live_store.find_entities().to_list()) == 2
    assert True


def test_yaml_loader_store(delta_live_store: DeltaLiveStore, config_file: str) -> None:
    path: str = "/tmp/test_yaml_loader_store.yaml"
    loader: DeltaLiveStoreLoader = YamlLoader(config_file)
    loader.load(delta_live_store)
    assert len(delta_live_store.find_entities().to_list()) == 2
    entity: DeltaLiveEntity = DeltaLiveEntity(
        entity_id="new",
        group="group3",
        source="source3",
        tags={"tag3": "value3", "tag4": "value4"},
        id="456",
        destination="destination3",
    )
    delta_live_store.upsert(entity)
    assert len(delta_live_store.find_entities().to_list()) == 3
    loader.store(delta_live_store, path=path)

    delta_live_store.destroy()
    delta_live_store.initialize()

    loader: DeltaLiveStoreLoader = YamlLoader(path)
    loader.load(delta_live_store)

    assert len(delta_live_store.find_entities().to_list()) == 3
