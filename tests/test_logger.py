"""Test module for the logging configuration."""
import logging
import os
import pytest
from pathlib import Path
from datetime import datetime

from project001.config.logging_config import get_logging_config

class TestLogger:
    def test_logger_configuration(self):
        """Test that the logger is configured correctly."""
        # Get the logger
        logger = get_logging_config(pipeline_name="test_pipeline")
        
        # Check that the logger is an instance of logging.Logger
        assert isinstance(logger, logging.Logger)
        
        # Check that the logger name is correct
        assert logger.name == "project001"
        
        # Check that the logger level is set to DEBUG
        assert logger.level == logging.DEBUG
        
        # Check that the logger has the correct number of handlers
        assert len(logger.handlers) == 5  # console, debug_file, info_file, warning_file, error_file
        
        # Check that the log files exist in date-based directory
        logs_dir = Path(os.getcwd()) / "logs"
        current_date = datetime.now().strftime("%d_%m_%Y")
        date_logs_dir = logs_dir / current_date
        assert date_logs_dir.exists(), f"Date directory {date_logs_dir} does not exist"
        assert (date_logs_dir / "debug.log").exists(), "debug.log does not exist"
        assert (date_logs_dir / "info.log").exists(), "info.log does not exist"
        assert (date_logs_dir / "warning.log").exists(), "warning.log does not exist"
        assert (date_logs_dir / "error.log").exists(), "error.log does not exist"
    
    def test_logger_levels(self):
        """Test that the logger logs at the correct levels."""
        # Get the logger
        logger = get_logging_config(pipeline_name="test_pipeline")
        
        # Log messages at different levels
        test_message = "Test message for pytest"
        logger.debug(f"DEBUG: {test_message}")
        logger.info(f"INFO: {test_message}")
        logger.warning(f"WARNING: {test_message}")
        logger.error(f"ERROR: {test_message}")
        
        # Check that the messages were logged to the correct files in date-based directory
        logs_dir = Path(os.getcwd()) / "logs"
        current_date = datetime.now().strftime("%d_%m_%Y")
        date_logs_dir = logs_dir / current_date
        
        # Debug file should contain all messages
        with open(date_logs_dir / "debug.log", "r") as f:
            debug_content = f.read()
            assert f"DEBUG: {test_message}" in debug_content
            assert f"INFO: {test_message}" in debug_content
            assert f"WARNING: {test_message}" in debug_content
            assert f"ERROR: {test_message}" in debug_content
        
        # Info file should contain INFO, WARNING, and ERROR messages
        with open(date_logs_dir / "info.log", "r") as f:
            info_content = f.read()
            assert f"DEBUG: {test_message}" not in info_content
            assert f"INFO: {test_message}" in info_content
            assert f"WARNING: {test_message}" in info_content
            assert f"ERROR: {test_message}" in info_content
        
        # Warning file should contain WARNING and ERROR messages
        with open(date_logs_dir / "warning.log", "r") as f:
            warning_content = f.read()
            assert f"DEBUG: {test_message}" not in warning_content
            assert f"INFO: {test_message}" not in warning_content
            assert f"WARNING: {test_message}" in warning_content
            assert f"ERROR: {test_message}" in warning_content
        
        # Error file should contain only ERROR messages
        with open(date_logs_dir / "error.log", "r") as f:
            error_content = f.read()
            assert f"DEBUG: {test_message}" not in error_content
            assert f"INFO: {test_message}" not in error_content
            assert f"WARNING: {test_message}" not in error_content
            assert f"ERROR: {test_message}" in error_content