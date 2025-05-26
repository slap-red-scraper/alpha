import logging
import json
from typing import Dict, Any
from datetime import datetime
import os # Added import os

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "timestamp": self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "module": record.module,
            "method": record.funcName,
            "event": record.msg,
            "details": record.args[0] if record.args else {}
        }
        return json.dumps(log_record)

class Logger:
    """Structured logger with verbosity control."""
    VERBOSITY_LEVELS = {"LESS": 0, "MORE": 1, "MAX": 2}
    EVENT_VERBOSITY = {
        "job_start": "LESS",
        "job_complete": "LESS",
        "login_success": "MORE",
        "login_failed": "MORE",
        "api_request": "MORE", # Changed from MAX
        "api_response": "MORE", # Changed from MAX
        "bonus_fetched": "MORE",
        "downline_fetched": "MORE",
        "csv_written": "MORE",
        "exception": "LESS",
        "website_unresponsive": "LESS",
        "down_sites_summary": "LESS",
        "bonus_api_error": "MORE"
    }

    def __init__(self, log_file: str, log_level: str, console: bool, detail: str):
        self.logger = logging.getLogger("ScraperLogger")
        self.logger.setLevel(getattr(logging, log_level.upper(), logging.DEBUG)) # ensure log_level is upper
        self.verbosity = self.VERBOSITY_LEVELS.get(detail.upper(), 0) # ensure detail is upper

        # File handler
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(JsonFormatter())
        self.logger.addHandler(file_handler)

        # Console handler
        if console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter(
                "%(asctime)s [%(levelname)s] %(message)s"
            ))
            self.logger.addHandler(console_handler)

    def emit(self, event: str, details: Dict[str, Any], level: str = "INFO") -> None:
        required_level = self.VERBOSITY_LEVELS.get(self.EVENT_VERBOSITY.get(event, "MORE"), 0)
        if self.verbosity >= required_level:
            # Ensure details is a dictionary for the formatter
            actual_details = details if isinstance(details, dict) else {"data": details}
            self.logger.log(getattr(logging, level.upper()), event, actual_details)


    def load_metrics(self, log_file: str) -> Dict[str, float]:
        metrics = {
            "bonuses": 0, # Total count of individual bonus items
            "downlines": 0,
            "errors": 0, # General errors + unresponsive + API errors that lead to "ERROR" return
            "runs": 0,
            "total_runtime": 0.0,
            "total_bonus_amount": 0.0,         # Sum of amounts from all bonus_fetched events
            "successful_bonus_fetches": 0,     # Number of times bonus_fetched event occurred
            "failed_bonus_api_calls": 0        # Number of times bonus_api_error event occurred
        }
        if not os.path.exists(log_file): # Check if log_file exists
            return metrics
        with open(log_file, "r") as f:
            for line in f:
                try:
                    log = json.loads(line)
                    event = log.get("event")
                    details = log.get("details", {}) # Ensure details is always a dict

                    if event == "bonus_fetched":
                        metrics["bonuses"] += details.get("count", 0)
                        metrics["total_bonus_amount"] += details.get("total_amount", 0.0)
                        metrics["successful_bonus_fetches"] += 1
                    elif event == "downline_fetched":
                        metrics["downlines"] += details.get("count", 0)
                    elif event == "exception" or event == "website_unresponsive": # Count these as errors for historical load
                        metrics["errors"] += 1
                    elif event == "bonus_api_error":
                        metrics["failed_bonus_api_calls"] += 1
                        metrics["errors"] += 1 # Also count as a general error for overall error tracking
                    elif event == "job_complete":
                        metrics["runs"] += 1
                        metrics["total_runtime"] += details.get("duration", 0.0)
                except json.JSONDecodeError:
                    continue
        return metrics
