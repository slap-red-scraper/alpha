import re
import requests
from typing import Optional
from .models import AuthData
from .logger import Logger

class AuthService:
    """Manages authentication and URL processing."""
    API_PATH = "/api/v1/index.php"

    def __init__(self, logger: Logger):
        self.logger = logger

    @staticmethod
    def clean_url(url: str) -> str:
        return re.sub(r"/\w+$", "", url)

    @staticmethod
    def extract_merchant_info(html: str) -> tuple[Optional[str], Optional[str]]:
        match = re.search(r'var MERCHANTID = (\d+);
var MERCHANTNAME = "(.*?)";', html)
        return match.groups() if match else (None, None)

    def login(self, url: str, mobile: str, password: str) -> Optional[AuthData]:
        try:
            response = requests.get(url)
            response.raise_for_status()
            html = response.text
        except Exception as e:
            self.logger.emit("exception", {"error": f"Failed to fetch URL {url}: {str(e)}"})
            return None

        merchant_id, merchant_name = self.extract_merchant_info(html)
        if not merchant_id:
            self.logger.emit("exception", {"error": f"No merchant ID found for {url}"})
            return None

        api_url = url + self.API_PATH
        payload = {
            "module": "/users/login",
            "mobile": mobile,
            "password": password,
            "merchantId": merchant_id,
            "domainId": "0",
            "accessId": "",
            "accessToken": "",
            "walletIsAdmin": ""
        }

        try:
            response = requests.post(api_url, data=payload)
            response.raise_for_status()
            data = response.json().get("data", {})
            if not data.get("token"):
                self.logger.emit("login_failed", {"url": url})
                return None
            self.logger.emit("login_success", {"url": url})
            return AuthData(
                merchant_id=merchant_id,
                merchant_name=merchant_name,
                access_id=data.get("id"),
                token=data.get("token"),
                api_url=api_url
            )
        except Exception as e:
            self.logger.emit("exception", {"error": f"Login failed for {url}: {str(e)}"})
            return None
