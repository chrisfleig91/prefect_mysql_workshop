from prefect import flow
from prefect.logging import get_run_logger

@flow
def hello_flow():
    logger = get_run_logger()
    logger.info("INFO log")
    print("Hello from mounted flow!")

if __name__ == "__main__":
    hello_flow()