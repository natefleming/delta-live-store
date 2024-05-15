#!/usr/bin/env python
 
import argparse
import os
import sys
from argparse import ArgumentParser, Namespace
from typing import Dict, List, Sequence
import logging
import subprocess

logger = logging.getLogger(__name__)

def run_command(command: str) -> None:
    logger.info("run_command", command=command)
    print(command)
    subprocess.run(command, shell=True)
        
def parse_args(args: Sequence[str]) -> Namespace:
    parser: ArgumentParser = ArgumentParser()
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Increase output verbosity"
    )
    parser.add_argument("--delta-live-store-table", help="The Delta Live Store table. [catalog].[schema].[table]", required=True)
    parser.add_argument("--catalog", help="The catalog for the Delta Live Table", required=True)
    parser.add_argument("--schema", help="The schema for the Delta Live Table", required=True)
    parser.add_argument("--volume", help="The source volume base directory", default=True)
    parser.add_argument("--host", help="The Databricks host", required=True)
    parser.add_argument("-p", "--profile", help="The Databricks profile", default="default", required=False)
    
    options: Namespace = parser.parse_args(args)
    logger.info("options", options=options)
    return options

def main(args: List[str] = sys.argv[1:]) -> None:
    logger.info("main", args=args)
    options: Namespace = parse_args(args)
    print(options)
    
    # Construct the Databricks command
    databricks_command: str = f"""
        databricks 
            -p {options.profile} bundle 
            --var \"delta_live_store_table={options.delta_live_store_table}\" 
            --var \"target_catalog={options.catalog}\" 
            --var \"target_schema={options.schema}\" 
            --var \"source_volume={options.volume}\" 
            --var \"host={options.host}\" 
            deploy
    """

    # Execute the Databricks command
    run_command(databricks_command)
    
if __name__ == "__main__":
    main()
