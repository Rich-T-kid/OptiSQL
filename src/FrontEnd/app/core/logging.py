import logging
import sys
from typing import Literal

def setup_logging(mode: Literal['prod', 'info', 'debug']) -> None:
    """
    Configure logging based on the mode specified in config.yml

    Args:
        mode: Logging mode - 'prod', 'info', or 'debug'
    """
    # Define logging levels
    level_map = {
        'prod': logging.WARNING,
        'info': logging.INFO,
        'debug': logging.DEBUG
    }

    log_level = level_map.get(mode, logging.INFO)

    # Configure format based on mode
    if mode == 'debug':
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    elif mode == 'info':
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
    else:  # prod
        log_format = '%(asctime)s - %(levelname)s - %(message)s'

    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Set uvicorn logger levels
    logging.getLogger("uvicorn").setLevel(log_level)
    logging.getLogger("uvicorn.access").setLevel(log_level)
    logging.getLogger("uvicorn.error").setLevel(log_level)

    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured with mode: {mode} (level: {logging.getLevelName(log_level)})")
