import logging
from logging import Formatter
from typing import Union

Logger = logging.Logger


def create_logger(
    name: str = __name__, level: Union[int, str] = logging.DEBUG
) -> Logger:
    """
    Create a logger with the specified name and log level.

    Args:
        name (str): The name of the logger. Defaults to the current module name.
        level (Union[int, str]): The log level. Can be an integer or a string.
            Defaults to logging.DEBUG.

    Returns:
        Logger: The created logger instance.
    """
    logger: Logger = logging.getLogger(name)
    level = level if isinstance(level, int) else logging.getLevelName(level)
    logger.setLevel(level)

    formatter = Formatter(
        "%(asctime)s - %(name)s - %(pathname)s:%(lineno)d - %(levelname)s - %(message)s"
    )
    handler: logging.StreamHandler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
