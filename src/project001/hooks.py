"""Project hooks."""
from typing import Any, Dict, Iterable, Optional

from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.pipeline.node import Node
from kedro.pipeline import Pipeline
from kedro.config import OmegaConfigLoader
from pathlib import Path

from project001.config.logging_config import get_logging_config

class ProjectHooks:
    """
    Project hooks forked from Kedro.
    """

    @hook_impl
    def before_pipeline_run(self, pipeline: Pipeline, run_params: Dict[str, Any]) -> None:
        """Hook to set up logging before pipeline is run.
        
        Args:
            pipeline: The pipeline that will be run.
            run_params: Parameters passed to the pipeline run.
        """
        pipeline_name = run_params.get("pipeline_name", "__default__")
        
        if pipeline_name == "__default__":
            print("\n--- Starting all pipelines ---")
        else:
            print(f"\n--- Starting pipeline {pipeline_name} ---")
            
        # Configure logging with the pipeline name
        get_logging_config(pipeline_name)
    
    @hook_impl
    def before_node_run(self, node: Node, catalog: DataCatalog, inputs: Dict[str, Any], is_async: bool) -> None:
        """Hook to be invoked before a node runs.
        
        Args:
            node: The node about to be run
            catalog: The catalog being used for this run
            inputs: The inputs about to be fed to the node
            is_async: Whether the node is being run in async mode
        """
        # Get the namespace of the node (which is usually the pipeline name)
        pipeline_name = node.namespace
        
        # If this node has a namespace (pipeline name), update the logging configuration
        if pipeline_name:
            # Print which pipeline this node belongs to
            print(f"Running node from pipeline: {pipeline_name}")
            
            # Update logging with the current node's pipeline name
            get_logging_config(pipeline_name)

class InjectApiKeyHook:
    @hook_impl
    def after_catalog_created(self, catalog: DataCatalog) -> DataCatalog:
        conf_path = Path.cwd() / "conf"
        conf_loader = OmegaConfigLoader(conf_source=conf_path)
        credentials = conf_loader["credentials"]
        api_key = credentials["news_api"]["api_key"]
        
        # Add the API key to the catalog credentials
        catalog["news_api_key"] = api_key
        
        return catalog
