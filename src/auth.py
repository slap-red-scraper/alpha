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

        # Log the API request with non-sensitive parts of the payload
        self.logger.emit("api_request", {
            "url": api_url,
            "action": "login", # Specific to this login action
            "module": payload.get("module"), # Consistent with other api_request logs
            "mobile": payload.get("mobile") # Non-sensitive identifier
        })

        try:
            response = requests.post(api_url, data=payload)
            response.raise_for_status()
            # Assuming the response is JSON. If not, this will raise an error caught by the except block.
            res_json = response.json() 

            # Log the API response
            response_details = {"url": api_url, "action": "login", "status": res_json.get("status")}
            if res_json.get("status") != "SUCCESS":
                if res_json.get("message"):
                    response_details["error_message"] = res_json.get("message")
                if isinstance(res_json.get("data"), dict) and res_json.get("data", {}).get("description"):
                     response_details["error_description"] = res_json.get("data").get("description")
                elif isinstance(res_json.get("data"), str):
                     response_details["error_data_string"] = res_json.get("data")
            self.logger.emit("api_response", response_details)
            
            data = res_json.get("data", {})
            if not data.get("token"): # Check based on expected success criteria
                self.logger.emit("login_failed", {"url": url, "reason": res_json.get("message", "No token in response")})
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
