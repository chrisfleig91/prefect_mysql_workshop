import logging
from prefect import flow
from prefect.logging import get_run_logger

# Optional: Logging auf DEBUG setzen
logging.basicConfig(level=logging.DEBUG)


@flow
def logger_flow():
    logger = get_run_logger()
    logger.info("INFO log")
    logger.debug("DEBUG log")
    logger.error("ERROR log")


if __name__ == "__main__":
    logger_flow()
