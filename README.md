# Delta Live Store

This Python project is an advanced accelerator for generating Delta Live Tables, designed to provide a metadata-driven ETL framework that is highly extensible. It leverages several cutting-edge technologies and libraries to facilitate efficient data transformation and management within a robust data pipeline architecture.

## Project Overview

The project is structured around the creation and management of Delta Live Tables, which are used to handle streaming and batch data processes efficiently. It incorporates a comprehensive set of features that allow for dynamic metadata management, pipeline modifications, and deployment using Databricks environments.

## Key Features

- **Metadata Loading**: Ability to load metadata from existing directory structures and lambda functions, allowing for flexible data schema definitions and transformations.
- **Pipeline Table Management**: Supports enabling and disabling pipeline tables dynamically, providing control over data processing workflows.
- **Extensible Framework**: Includes integration points for custom modifications, enabling users to tailor the framework according to specific requirements.
- **Deployment**: Utilizes Databricks Asset Bundles for deploying configurations and dependencies, ensuring consistent setups across different environments.
- **Historical Data Management**: Implements Slowly Changing Dimension (SCD) type 2 for metadata history, facilitating complex historical data queries.
- **Data Quality Management**: Automatic support for quarantine tables helps manage data quality by isolating records that do not meet predefined expectations.
- **Data Transformation**: Provides capabilities to perform data transformations, essential for processing and preparing data for analytics.
- **Table Management API**: Supports the ability to programmatically manage and query the table through a rich API.

## Technologies and Libraries

- **Delta Live Tables**: Used for managing and automating data pipelines directly within Databricks.
- **Delta Lake**: Provides a layer over Apache Spark to ensure reliability, security, and performance for data lakes.
- **Databricks Asset Bundles**: Facilitates the deployment of notebooks, libraries, and configurations in Databricks.
- **Databricks-Connect**: Allows for connecting and executing Spark code from local development environments to remote Databricks clusters.

## Current Limitations

- At the moment *expectations* and *quarantine* tables are not compatible with *apply_changes* (SCD1/SCD2). 

## Setup and Installation

### Development Setup

1. Create a virtual environment:
   ```bash
   python -m venv .venv
   ```
2. Activate the virtual environment and install development dependencies:
   ```bash
   source .venv/bin/activate  # On Unix/macOS
   .venv\Scripts\activate  # On Windows
   pip install -r requirements-dev.txt
   ```

### Deployment

Deployment is handled through the Databricks CLI, configured to manage and deploy asset bundles:

```bash
databricks -p default bundle --var "delta_live_store_table=main.etl.control_table" --var "target_catalog=main" --var "target_schema=etl" --var "source_volume=/Volumes/main/etl/data" --var "host=https://<databricks-host>" deploy
```

An example Databricks Asset Bundle deployment descriptor is provided in databricks.yml and resources/*.yml  
Modify/Duplicate these configuration files to change the deployment stragegy.  

The following parameterized notebooks have been provided for reuse within DAB pipelines and jobs.

- [delta_live_store_pipeline](src/delta_live_store_pipeline.ipynb)
- [load_from_list](src/load_from_list.ipynb)
- [load_from_volume](src/load_from_volume.ipynb)
- [load_from_yaml](src/load_from_yaml.ipynb)

This command sets up the necessary variables and points to the specific Databricks job and host for deployment, ensuring that all components are correctly configured and deployed to the specified Databricks environment.

## Conclusion

This Python project represents a sophisticated tool for managing data transformations and pipeline operations within a cloud-based data platform like Databricks. It leverages modern data management techniques and tools to provide a scalable, reliable, and efficient data processing framework, suitable for handling large-scale data operations in real-time and batch processing modes.

## Examples

### Load MetaData for a DLT Pipeline from a provided base directory.

In this example the data exists in a Volume  
```
%fs ls /Volumes/main/etl/data

dbfs:/Volumes/main/etl/data/dataset1/
dbfs:/Volumes/main/etl/data/dataset2/
dbfs:/Volumes/main/etl/data/dataset3/
...
```

```python
from dbrx.dls.store import DeltaLiveStore
from dbrx.dls.models import DeltaLiveEntity
from dbrx.dls.loaders import (
   DeltaLiveStoreLoader, 
   FileServiceLoader, 
   entity_from_directory_entry, 
   medallion_from_directory_entry
)

host: str = "https://<databrick-host>.azuredatabricks.net"
token: str = "<access-token>" # dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
delta_live_store_table: str = "<catalog>.<table>.<name>" # ie. main.elt.control_table
volume: str = "<path>" # ie. /Volumes/main/etl/data

store: DeltaLiveStore = DeltaLiveStore(delta_live_store_table)

# Ensure the underlying table has been provisioned
store.initialize()

# The factories accept a number of default parameters which allow table customization

# Create a source->bronze pipeline
bronze_factory: Callable[["DirectoryEntry"], List[DeltaLiveEntity]] = entity_from_directory_entry()

# OR
# Create a source->bronze->silver pipeline
medallion_factory: Callable[["DirectoryEntry"], List[DeltaLiveEntity]] = medallion_from_directory_entry()

loader: DeltaLiveStoreLoader = FileServiceLoader(volume, host=host, token=token, factory=medallion_factory)

loader.load(store)
```

### Load MetaData for a DLT Pipeline from a provided YAML config file.

```python
from dbrx.dls.store import DeltaLiveStore
from dbrx.dls.loaders import DeltaLiveStoreLoader, YamlLoader

delta_live_store_table: str = "<catalog>.<table>.<name>" # ie. main.etl.control_table
config_path: str = "/path/to/config/file.yml"

store: DeltaLiveStore = DeltaLiveStore(delta_live_store_table)

# Ensure the underlying table has been provisioned
store.initialize()

loader: DeltaLiveStoreLoader = YamlLoader(config_path)

loader.load(store)
```

### Create A DLT Pipeline from previously loaded MetaData.

```python
from dbrx.dls.store import DeltaLiveStore
from dbrx.dls.filters import by_is_enabled, by_is_latest, by_group
from dbrx.dls.pipeline import DeltaLiveStorePipeline

delta_live_store_table: str = "<catalog>.<table>.<name>" # ie. main.etl.control_table

store: DeltaLiveStore = DeltaLiveStore(table=delta_live_store_table, spark=spark)
pipeline: DeltaLiveStorePipeline = (
    DeltaLiveStorePipeline(store)
        .with_filter(by_is_enabled() & by_is_latest() & by_group(delta_live_store_entity_group))
)

pipeline.run()

```

### Query Metadata based on filters

#### Available Filters
* by_all 
* by_id 
* by_destination_type
* by_entity_id 
* by_group 
* by_has_tag 
* by_has_tag_value 
* by_source 
* by_destination 
* by_is_enabled 
* by_is_latest 
* by_created_ts_between 
* by_created_by 
* by_modified_by 
* by_reference_ts 
* by_is_streaming 
* by_predicate

```python
from dbrx.dls.store import DeltaLiveStore
from dbrx.dls.filters import Predicate, by_is_enabled, by_is_latest, by_group
from dbrx.dls.pipeline import DeltaLiveStorePipeline

delta_live_store_table: str = "<catalog>.<table>.<name>" # ie. main.etl.control_table

store: DeltaLiveStore = DeltaLiveStore(table=delta_live_store_table, spark=spark)

# Filters can be chained together using '|' and '&' operators 
predicate: Predicate = by_is_latest() & by_is_enabled() & by_group("my_group")

# DeltaLiveStore.find_entities(...) only returns the current versions of metadata. That is, it applies by_is_latest() implicitly
entities: DeltaLiveEntityList = store.find_entities(predicate)

# DeltaLiveStore.find_history(...) will return all version of metadata
entities: DeltaLiveEntityList = store.find_history(predicate)

display(entities.to_df())

```

### Enabling/Disabling tables

DeltaLiveStore allows entities to be enabled and disabled. This action will be applied to all dependant tables as well.


```python
from dbrx.dls.store import DeltaLiveStore
from dbrx.dls.filters import Predicate, by_entitiy_id
from dbrx.dls.pipeline import DeltaLiveStorePipeline

delta_live_store_table: str = "<catalog>.<table>.<name>" # ie. main.etl.control_table

store: DeltaLiveStore = DeltaLiveStore(table=delta_live_store_table, spark=spark)

store.disable(by_entitiy_id("1"))

store.enable(by_entitiy_id("1"))

```

### Programmatically creating/updating entities

DeltaLiveStore allows entities to be enabled and disabled. This action will be applied to all dependant tables as well.


```python
from dbrx.dls.store import DeltaLiveStore
from dbrx.dls.models import DeltaLiveEntity
from dbrx.dls.filters import Predicate, by_entitiy_id
from dbrx.dls.pipeline import DeltaLiveStorePipeline

delta_live_store_table: str = "<catalog>.<table>.<name>" # ie. main.etl.control_table

store: DeltaLiveStore = DeltaLiveStore(table=delta_live_store_table, spark=spark)

def new_delta_live_entity() -> DeltaLiveEntity:
    return DeltaLiveEntity(
        entity_id="1",
        source="/Volumes/main/etl/data/dataset1,
        destination="dataset1_bronze",
        source_format="cloudFiles",
        read_options={
            "cloudFiles.format": "csv",
            "header": "true"
        },
    )

entity: DeltaLiveEntity = new_delta_live_entity()

store.upsert(entity)

# Change the source from autoloader (cloudFiles) to a static CSV
entity.source_format = "csv"
entity.read_options = {"header": "true", "inferSchema", "true"}

store.upsert(entity)

```

### Example YAML Configuration

A json schema has been provided. Schema can be enforced in your IDE (ie VSCode) by copying the comment line beginning with #yaml-language-server and updating the path to correct location

```yaml
# yaml-language-server: $schema=../src/dbrx/dls/config_schema.json

delta_live_store:
  - entity_id: "1"                                # required
    source: /Volumes/main/elt/data/dataset1       # required
    destination: dataset1_bronze                  # required
    destination_type: table                       # optional, default=table, [table|view]
    source_format: cloudFiles                     # optional, default=cloudFiles, [cloudFiles|kafka|csv|json|parquet|avro|orc|delta|dlt]
    is_streaming: true                            # optional, default=true
    is_enabled: true                              # optional, default=true
    source_schema: ~                              # optional, default=None, ex: "id int, name string"
    primary_keys:                                 # optional, default=[]
      - id
    select_expr: []                               # optional, default=[]
    read_options:                                 # optional, default={}, NOTE: required for cloudFiles
      cloudFiles.format: csv
      header: "true"
    table_properties: {}                          # optional, default={}
    tags: {}                                      # optional, default={}
    spark_conf: {}                                # optional, default={}
    partition_cols: []                            # optional, default=[]
    group: main.etl                               # optional, default=None
    comment: This is a an elt table.              # optional, default=None
    expectations:                                 # optional, default={}
      expect_all: {}
      expect_all_or_drop: {}
      expect_all_or_fail: {}
    is_quarantined: false                        # optional, default=false
  - entity_id: "2"
    source: dataset1_bronze
    destination: dataset1_silver
    destination_type: table
    primary_keys:
      - id
    source_format: dlt
    is_streaming: true
    group: main.etl
    is_quarantined: True
    expectations: 
      expect_all: 
        temp_gt_50: "temp > 50"
    apply_changes:
      sequence_by: "my_seq_num"
      stored_as_scd_type: 2
```


The 'delta_live_store' project was generated by using the default-python template.

## Getting started

1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

3. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project.
    For example, the default template would deploy a job called
    `[dev yourname] delta_live_store_job` to your workspace.
    You can find that job by opening your workpace and clicking on **Workflows**.

4. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```

   Note that the default job from the template has a schedule that runs every day
   (defined in resources/delta_live_store_job.yml). The schedule
   is paused when deploying in development mode (see
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).

5. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```

6. Optionally, install developer tools such as the Databricks extension for Visual Studio Code from
   https://docs.databricks.com/dev-tools/vscode-ext.html. Or read the "getting started" documentation for
   **Databricks Connect** for instructions on running the included Python code from a different IDE.

7. For documentation on the Databricks asset bundles format used
   for this project, and for CI/CD configuration, see
   https://docs.databricks.com/dev-tools/bundles/index.html.
