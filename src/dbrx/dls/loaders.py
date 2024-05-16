import json
import os
from abc import ABC, abstractmethod
from functools import reduce
from operator import or_
from typing import Any, Callable, Dict, Generator, List, Optional

import yaml
from databricks.sdk import WorkspaceClient
from jsonschema import ValidationError, validate
from pyspark.sql import DataFrame

from .filters import Predicate, by_entity_id, by_group, by_is_enabled, by_is_latest
from .logging import Logger, create_logger
from .models import DeltaLiveEntity, DeltaLiveEntityList
from .store import GENERATED_COLUMNS, DeltaLiveStore
from .utils import load_config_schema, remove_null_values

logger: Logger = create_logger(__name__)


class DeltaLiveStoreLoader(ABC):
    """
    Abstract base class for Delta Live Store loaders.
    """

    @abstractmethod
    def load(self, store: DeltaLiveStore) -> None:
        """
        Load entities into the Delta Live Store.

        Args:
            store (DeltaLiveStore): The Delta Live Store to load entities into.
        """
        pass

    def validate(self, store: DeltaLiveStore) -> None:
        """
        Validate the entities in the Delta Live Store.
        """
        pass


DEFAULT_SOURCE_FORMAT: str = "cloudFiles"
DEFAULT_READ_OPTIONS: Dict[str, str] = {"cloudFiles.format": "csv", "header": "true"}


def medallion_from_directory_entry(
    *,
    bronze_prefix: str = "",
    bronze_suffix: str = "_bronze",
    silver_prefix: str = "",
    silver_suffix: str = "_silver",
    source_format: str = DEFAULT_SOURCE_FORMAT,
    read_options: Dict[str, str] = DEFAULT_READ_OPTIONS,
    group: Optional[str] = None,
    tags: Dict[str, str] = {},
    expectations: Dict[str, Any] = {},
    is_quarantined: bool = False,
) -> List[DeltaLiveEntity]:
    """
    Creates DeltaLiveEntity objects for bronze and silver destinations based on a directory entry.

    Args:
        bronze_prefix (str, optional): Prefix for the bronze destination. Defaults to "".
        bronze_suffix (str, optional): Suffix for the bronze destination. Defaults to "_bronze".
        silver_prefix (str, optional): Prefix for the silver destination. Defaults to "".
        silver_suffix (str, optional): Suffix for the silver destination. Defaults to "_silver".
        source_format (str, optional): Source format for the DeltaLiveEntity objects. Defaults to DEFAULT_SOURCE_FORMAT.
        read_options (Dict[str, str], optional): Read options for the DeltaLiveEntity objects. Defaults to DEFAULT_READ_OPTIONS.
        group (Optional[str], optional): Group for the DeltaLiveEntity objects. Defaults to None.
        tags (Dict[str, str], optional): Tags for the DeltaLiveEntity objects. Defaults to {}.
        expectations (Dict[str, Any], optional): Expectations for the DeltaLiveEntity objects. Defaults to {}.
        is_quarantined (bool, optional): Whether the DeltaLiveEntity objects are quarantined. Defaults to False.

    Returns:
        List[DeltaLiveEntity]: List of DeltaLiveEntity objects for bronze and silver destinations.
    """

    def _(directory_entry: "DirectoryEntry") -> List[DeltaLiveEntity]:
        bronze_destination: str = (
            f"{bronze_prefix}{directory_entry.name}{bronze_suffix}"
        )
        bronze_entity: DeltaLiveEntity = DeltaLiveEntity(
            entity_id=hash(f"{bronze_destination}|{directory_entry.path}"),
            source_format=source_format,
            group=group,
            tags=tags,
            source=directory_entry.path,
            destination=bronze_destination,
            read_options=read_options,
        )

        silver_destination: str = (
            f"{silver_prefix}{directory_entry.name}{silver_suffix}"
        )
        silver_entity: DeltaLiveEntity = DeltaLiveEntity(
            entity_id=hash(f"{silver_destination}|{directory_entry.path}"),
            group=group,
            tags=tags,
            source=bronze_destination,
            source_format="dlt",
            destination=silver_destination,
            read_options=read_options,
            expectations=expectations,
            is_quarantined=is_quarantined,
        )

        return [bronze_entity, silver_entity]

    return _


def entity_from_directory_entry(
    *,
    prefix: str = "",
    suffix: str = "_bronze",
    source_format: str = DEFAULT_SOURCE_FORMAT,
    read_options: Dict[str, str] = DEFAULT_READ_OPTIONS,
    group: Optional[str] = None,
    tags: Dict[str, str] = {},
    expectations: Dict[str, Any] = {},
    is_quarantined: bool = False,
) -> List[DeltaLiveEntity]:
    """
    Create a DeltaLiveEntity from a DirectoryEntry.

    Args:
        prefix (str, optional): Prefix to add to the destination path. Defaults to "".
        suffix (str, optional): Suffix to add to the destination path. Defaults to "_bronze".
        source_format (str, optional): Source format of the entity. Defaults to DEFAULT_SOURCE_FORMAT.
        read_options (Dict[str, str], optional): Read options for the entity. Defaults to DEFAULT_READ_OPTIONS.
        group (Optional[str], optional): Group of the entity. Defaults to None.
        tags (Dict[str, str], optional): Tags for the entity. Defaults to {}.
        expectations (Dict[str, Any], optional): Expectations for the entity. Defaults to {}.
        is_quarantined (bool, optional): Whether the entity is quarantined. Defaults to False.

    Returns:
        List[DeltaLiveEntity]: List of DeltaLiveEntity objects.
    """

    def _(directory_entry: "DirectoryEntry") -> List[DeltaLiveEntity]:
        destination: str = f"{prefix}{directory_entry.name}{suffix}"
        entity: DeltaLiveEntity = DeltaLiveEntity(
            entity_id=hash(f"{destination}|{directory_entry.path}"),
            group=group,
            tags=tags,
            source=directory_entry.path,
            destination=destination,
            source_format=source_format,
            read_options=read_options,
            expectations=expectations,
            is_quarantined=is_quarantined,
        )
        return [entity]

    return _


class FileServiceLoader(DeltaLiveStoreLoader):
    """
    Delta Live Store loader for file services.
    """

    def __init__(
        self,
        path: str,
        *,
        host: Optional[str] = None,
        token: Optional[str] = None,
        factory: Callable[
            ["DirectoryEntry"], List[DeltaLiveEntity]
        ] = entity_from_directory_entry(),
    ) -> None:
        """
        Initialize the FileServiceLoader.

        Args:
            path (str): The path to the directory containing the entities.
            host (str, optional): The host of the workspace. Defaults to None.
            token (str, optional): The token for authentication. Defaults to None.
            factory (Callable[[DirectoryEntry], List[DeltaLiveEntity]], optional): The factory function to create DeltaLiveEntity objects. Defaults to entity_from_directory_entry().
        """
        self.path = path
        self.factory = factory
        self.workspace_client: WorkspaceClient = self._get_workspace_client(host, token)

    def load(self, store: DeltaLiveStore) -> None:
        """
        Load entities from the file service into the Delta Live Store.

        Args:
            store (DeltaLiveStore): The Delta Live Store to load entities into.
        """
        logger.info(f"Loading entities from {self.path}")
        entities: List[DeltaLiveEntity] = []
        directory_entries: Generator["DirectoryEntry", None, None] = (
            self.workspace_client.files.list_directory_contents(self.path)
        )

        for directory_entry in directory_entries:
            next_entities: List[DeltaLiveEntity] = self.factory(directory_entry)
            entities += next_entities

        store.upsert(*entities)

    def validate(self, store: DeltaLiveStore) -> None: ...

    def _get_workspace_client(self, host: str, token: str) -> WorkspaceClient:
        """
        Get the WorkspaceClient for the given host and token.

        Args:
            host (str): The host of the workspace.
            token (str): The token for authentication.

        Returns:
            WorkspaceClient: The WorkspaceClient instance.
        """
        host = host if host is not None else os.getenv("DATABRICKS_HOST")
        token = token if token is not None else os.getenv("DATABRICKS_TOKEN")
        try:
            return WorkspaceClient(host=host, token=token)
        except Exception as e:
            message: str = (
                f"""
                Unable to create WorkspaceClient. 
                This could be due to invalid authentication credentials. 
                Please provide a valid host and token. 
                These can be provided programatically or by setting the DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.
                A temporary token can be retrieved from a notebook using: dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
                """
            )
            logger.error(message)
            raise ValueError(message) from e


class YamlLoader(DeltaLiveStoreLoader):
    """
    A class for loading and storing entities from/to a YAML file in a Delta Live Store.
    """

    def __init__(self, path: str) -> None:
        """
        Initializes a new instance of the YamlLoader class.

        Args:
            path (str): The path to the YAML file.
        """
        self.path = path

    def load(self, store: DeltaLiveStore) -> None:
        """
        Loads entities from the YAML file and upserts them into the Delta Live Store.

        Args:
            store (DeltaLiveStore): The Delta Live Store to upsert the entities into.
        """
        logger.info(f"Loading entities from {self.path}")
        entities: List[DeltaLiveEntity] = []

        try:
            schema: Dict[str, Any] = load_config_schema()
            with open(self.path, "r") as file:
                data = yaml.safe_load(file)
                validate(instance=data, schema=schema)
        except yaml.YAMLError as ye:
            logger.error(f"Error loading YAML file: {self.path}")
            raise ye
        except ValidationError as ve:
            logger.error(f"Error validating YAML file: {self.path}")
            raise ve
        except Exception as e:
            logger.error(f"Error loading YAML file: {self.path}")
            raise e

        for entity in data["delta_live_store"]:
            entities.append(DeltaLiveEntity(**entity))

        entity_ids_by_group: Dict[str, List[int]] = {}
        for entity in entities:
            entity_ids_by_group.setdefault(entity.group, []).append(entity.entity_id)

        for group, entity_ids in entity_ids_by_group.items():
            predicates: List[Predicate] = [
                by_entity_id(entity_id) for entity_id in entity_ids
            ]
            predicate: Predicate = reduce(or_, predicates)
            store.disable(
                by_is_latest() & by_is_enabled() & by_group(group) & ~predicate
            )

        store.upsert(*entities)

    def validate(self, store: DeltaLiveStore) -> None:
        logger.debug("Validating entities in the Delta Live Store...")
        store.disable(by_is_latest() & ~by_is_enabled())

    def store(self, store: DeltaLiveStore, path: str = None) -> None:
        """
        Stores entities from the Delta Live Store into the YAML file.

        Args:
            store (DeltaLiveStore): The Delta Live Store to retrieve the entities from.
        """
        path = path if path is not None else self.path
        logger.info(f"Storing entities to {path}")
        entities: List[Dict[str, Any]] = store.find_entities(by_is_latest()).to_json()
        entity: Dict[str, Any]
        for entity in entities:
            for column in GENERATED_COLUMNS:
                entity.pop(column, None)
            remove_null_values(entity)
        data: Dict[str, Any] = {"delta_live_store": entities}

        logger.debug(f"Entities: {json.dumps(data, indent=2)}")

        try:
            schema: Dict[str, Any] = load_config_schema()
            validate(instance=data, schema=schema)
            with open(path, "w") as file:
                yaml.dump(data, file)
        except yaml.YAMLError as ye:
            logger.error(f"Error storing YAML file: {path}")
            raise ye
        except ValidationError as ve:
            logger.error(f"Error validating YAML file: {path}")
            raise ve
        except Exception as e:
            logger.error(f"Error storing YAML file: {path}")
            raise e
