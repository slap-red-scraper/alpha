from dataclasses import dataclass
from typing import Optional
import configparser
import os
import sys

@dataclass
class Credentials:
    mobile: str
    password: str

@dataclass
class Settings:
    url_file: str
    downline_enabled: bool

@dataclass
class LoggingConfig:
    log_file: str
    log_level: str
    console: bool
    detail: str

@dataclass
class GoogleSheetsConfig:
    enabled: bool
    credentials_file: str
    spreadsheet_id: str
    upload_daily_bonus: bool
    upload_historical_bonus: bool
    upload_comparison_report: bool

@dataclass
class AppConfig:
    credentials: Credentials
    settings: Settings
    logging: LoggingConfig
    google_sheets: GoogleSheetsConfig

class ConfigLoader:
    """Loads and validates configuration from a .ini file."""
    def __init__(self, path: str = "config.ini"):
        if not os.path.exists(path):
            sys.exit(f"Configuration file not found: {path}")
        self.config = configparser.ConfigParser()
        self.config.read(path)

    def load(self) -> AppConfig:
        try:
            gs_config = self.config["google_sheets"]
            return AppConfig(
                credentials=Credentials(
                    mobile=self.config["credentials"]["mobile"],
                    password=self.config["credentials"]["password"]
                ),
                settings=Settings(
                    url_file=self.config["settings"]["file"],
                    downline_enabled=self.config["settings"].getboolean("downline", fallback=False)
                ),
                logging=LoggingConfig(
                    log_file=self.config["logging"]["log_file"],
                    log_level=self.config["logging"]["log_level"],
                    console=self.config["logging"].getboolean("console", fallback=True),
                    detail=self.config["logging"].get("detail", "LESS").upper()
                ),
                google_sheets=GoogleSheetsConfig(
                    enabled=gs_config.getboolean("enabled", fallback=False),
                    credentials_file=gs_config["credentials_file"],
                    spreadsheet_id=gs_config["spreadsheet_id"],
                    upload_daily_bonus=gs_config.getboolean("upload_daily_bonus", fallback=True),
                    upload_historical_bonus=gs_config.getboolean("upload_historical_bonus", fallback=True),
                    upload_comparison_report=gs_config.getboolean("upload_comparison_report", fallback=True)
                )
            )
        except KeyError as e:
            sys.exit(f"Configuration error: Missing key {e}")
