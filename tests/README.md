# Tests
This folder contains tests for the project.

## Logger Tests
The logger tests file contains tests for the logger.

```test_logger_configuration```:
- Check that the logger is an instance of logging.Logger.
- Check that the logger name is "project001".
- Check that the logger level is logging.DEBUG.
- Check that the logger has the correct number of handlers
- Check that the logger file exist in the date-based directory

```test_logger_levels```:
- Check that the messages were logged to the single log gile in date-based directory
- Check that log file contains all messages at all levels