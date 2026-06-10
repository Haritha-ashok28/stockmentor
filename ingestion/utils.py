import yaml
import logging
from pathlib import Path

def setup_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logs_folder = Path("logs")
    logs_folder.mkdir(parents=True, exist_ok=True)
    stream_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(logs_folder / "ingestion.log")
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    return logger

def load_config():
    with open("config/tickers.yaml","r") as f:
        return yaml.safe_load(f)