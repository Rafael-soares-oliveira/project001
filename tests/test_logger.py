"""Test module for the logging configuration."""
import logging
import os
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
        assert len(logger.handlers) == 2  # console, log_file
        
        # Check that the log file exists in date-based directory
        logs_dir = Path(os.getcwd()) / "logs"
        current_date = datetime.now().strftime("%d_%m_%Y")
        date_logs_dir = logs_dir / current_date
        assert date_logs_dir.exists(), f"Date directory {date_logs_dir} does not exist"
        assert (date_logs_dir / "app.log").exists(), "app.log does not exist"
    
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
        
        # Check that the messages were logged to the single log file in date-based directory
        logs_dir = Path(os.getcwd()) / "logs"
        current_date = datetime.now().strftime("%d_%m_%Y")
        date_logs_dir = logs_dir / current_date
        
        # Log file should contain all messages at all levels
        with open(date_logs_dir / "app.log", "r") as f:
            log_content = f.read()
            assert f"DEBUG: {test_message}" in log_content
            assert f"INFO: {test_message}" in log_content
            assert f"WARNING: {test_message}" in log_content
            assert f"ERROR: {test_message}" in log_content