from typing import Dict, Literal

DestinationType = Literal[
    "view",
    "table",
]

# fmt:off
SourceFormat = Literal[
    "cloudFiles", 
    "kafka", 
    "csv", 
    "json", 
    "parquet", 
    "avro", 
    "orc", 
    "delta",
    "dlt"
]
# fmt:on

ReadOptions = Dict[str, str]

TableProperties = Dict[str, str]

Tags = Dict[str, str]

SparkConf = Dict[str, str]

DeltaLiveEntityExpectations = Dict[str, Dict[str, str]]
