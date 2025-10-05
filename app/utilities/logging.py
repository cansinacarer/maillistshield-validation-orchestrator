import logging
import logging_loki

from app.config import LOKI_HOST, LOKI_PASSWORD, LOKI_USER, SERVICE_NAME, TASK_SLOT


def _set_up_logger():
    """
    Set up the logger to be used globally.
    """

    # Set up Loki handler
    loki_handler = logging_loki.LokiHandler(
        url=f"{LOKI_HOST}/loki/api/v1/push",
        tags={
            "application": "maillistshield",
            "service": f"{SERVICE_NAME}-{TASK_SLOT}",
        },
        auth=(LOKI_USER, LOKI_PASSWORD),
        version="1",
    )

    # Set up the console handler
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(console_formatter)

    # Initialize the root logger
    logger = logging.getLogger("mls")
    logger.setLevel(logging.DEBUG)

    # Add handlers to the logger
    if not logger.handlers:
        logger.addHandler(loki_handler)
        logger.addHandler(console_handler)

    return logger


logger = _set_up_logger()
