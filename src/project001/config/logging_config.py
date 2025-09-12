import logging
import os
from pathlib import Path
import logging.config
from datetime import datetime

class PipelineNameFilter(logging.Filter):
    def __init__(self, pipeline_name: str):
        """
        Initializes the filter with the pipeline name.

        Args:
            pipeline_name (str): _description_
        """
        self.pipeline_name = pipeline_name
        super().__init__()

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Filters the log record based on the pipeline name.

        Args:
            record (logging.LogRecord): The log record to filter.

        Returns:
            bool: True if the record should be logged, False otherwise.
        """
        if (self.pipeline_name is None or self.pipeline_name == "__default__") and hasattr(record, 'name'):
            name_parts = record.name.split('.')
            if len(name_parts) >= 3 and name_parts[0] == 'kedro' and name_parts[1] == 'pipeline':
                self.pipeline_name = name_parts[2]
        
        record.pipeline_name = self.pipeline_name
        return True

def get_logging_config(pipeline_name: str) -> logging.Logger:
    """
    Configures the logging for the pipeline.

    Args:
        pipeline_name (str): The name of the pipeline.

    Returns:
        logging.Logger: The logger for the pipeline.
    """
    # Create logs directory with date subfolder if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Create date subfolder (format: DD_MM_YYYY)
    current_date = datetime.now().strftime("%d_%m_%Y")
    date_logs_dir = logs_dir / current_date
    date_logs_dir.mkdir(exist_ok=True)
    
    # Simple logging configuration
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {
            "pipeline_name": {
                "()": PipelineNameFilter,
                "pipeline_name": pipeline_name,
            }
        },
        "formatters": {
            "standard": {
                "format": "[%(asctime)s] %(levelname)s %(pipeline_name)s: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "colored": {
                "()": "colorlog.ColoredFormatter",
                "format": "%(log_color)s[%(asctime)s] %(levelname)s %(pipeline_name)s: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
                "log_colors": {
                    "DEBUG": "cyan",
                    "INFO": "green",
                    "WARNING": "yellow",
                    "ERROR": "red",
                }
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "colored",
                "filters": ["pipeline_name"],
            },
            "log_file": {
                "class": "logging.FileHandler",
                "level": "DEBUG",
                "formatter": "standard",
                "filename": str(date_logs_dir / "app.log"),
                "filters": ["pipeline_name"],
                "mode": "a",
            }
        },
        "loggers": {
            "kedro": {
                "level": "DEBUG",
                "handlers": ["console", "log_file"],
                "propagate": False
            },
            "project001": {
                "level": "DEBUG",
                "handlers": ["console", "log_file"],
                "propagate": False
            }
        },
        "root": {
            "level": "DEBUG",
            "handlers": ["console", "log_file"]
        }
    }
    
    # Apply the logging configuration
    logging.config.dictConfig(logging_config)
    
    # Return the logger for the project
    return logging.getLogger("project001")