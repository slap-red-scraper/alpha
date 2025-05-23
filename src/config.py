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
class AppConfig:
    credentials: Credentials
    settings: Settings
    logging: LoggingConfig

class ConfigLoader:
    """Loads and validates configuration from a .ini file."""
    def __init__(self, path: str = "config.ini"):
        if not os.path.exists(path):
            sys.exit(f"Configuration file not found: {path}")
        self.config = configparser.ConfigParser()
        self.config.read(path)

    def load(self) -> AppConfig:
        try:
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
                )
            )
        except KeyError as e:
            sys.exit(f"Configuration error: Missing key {e}")
