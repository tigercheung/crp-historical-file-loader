# logger.py
import logging
import logging.config
import yaml
import os
from datetime import datetime

# Configurable root logger name (app-level)
# APP_LOGGER_NAME = "crp_ia_exporter.app"

def setup_app_logger(app_name='app'):
    with open("logging.yaml", "r") as f:
        config = yaml.safe_load(f)

    # Setup logger
    logger = logging.getLogger(app_name)

    # Format timestamp
    # timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    timestamp = datetime.now().strftime("%Y%m%d")

    # Replace {timestamp} in all file handlers
    for handler in config.get("handlers", {}).values():
        if "filename" in handler and "{timestamp}" in handler["filename"]:
            resolved_filename = handler["filename"].replace("{app_name}", app_name).replace("{timestamp}", timestamp)
            os.makedirs(os.path.dirname(resolved_filename), exist_ok=True)
            handler["filename"] = resolved_filename

    # Apply the final config
    logging.config.dictConfig(config)

    return logger

# Set up logging once at import
# setup_logging()

# # Export app-level logger for convenience
# logger = logging.getLogger(APP_LOGGER_NAME)
