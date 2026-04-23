import threading
import time
from datetime import datetime
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def create_heartbeat(heartbeat_file: Path, interval: int = 30):
    """
    Creates a periodic heartbeat file for docker compose healthchecks.
    """
    try:
        heartbeat_file.parent.mkdir(parents=True, exist_ok=True)
        heartbeat_file.write_text(datetime.now().isoformat())
        logger.info(f"Writing Initial heartbeat to {heartbeat_file}")
    except Exception as e:
        logger.error(f"Failed to write initial heartbeat: {e}")
        
    def heartbeat():
        while True:
            try:
                heartbeat_file.parent.mkdir(parents=True, exist_ok=True)
                heartbeat_file.write_text(datetime.now().isoformat())
            except Exception as e:
                logger.error(f"Failed to write heartbeat: {e}")
            time.sleep(interval)
    
    thread = threading.Thread(target=heartbeat, daemon=True)
    thread.start()
    logger.info(f"Heartbeat Thread started, writing to {heartbeat_file}")
