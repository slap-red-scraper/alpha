import csv
import os
import requests
from typing import List, Set, Tuple, Union # Union for return types
from .models import Downline, Bonus, AuthData
from .logger import Logger
from .auth import AuthService # Added import for AuthService

class Scraper:
    """Handles scraping of downlines and bonuses."""
    def __init__(self, logger: Logger, request_timeout: int):
        self.logger = logger
        self.request_timeout = request_timeout

    def fetch_downlines(self, url: str, auth: AuthData, csv_file: str = "downlines.csv") -> Union[int, str]:
        written: Set[Tuple] = set()
        if os.path.exists(csv_file):
            with open(csv_file, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                written = {tuple(row.values()) for row in reader}

        total_new_rows = 0
        page = 0
        while True:
            payload = {
                "level": "1",
                "pageIndex": str(page),
                "module": "/referrer/getDownline",
                "merchantId": auth.merchant_id,
                "domainId": "0",
                "accessId": auth.access_id,
                "accessToken": auth.token,
                "walletIsAdmin": True
            }
            self.logger.emit("api_request", {"url": auth.api_url, "module": payload.get("module")})
            try:
                response = requests.post(auth.api_url, data=payload, timeout=self.request_timeout)
                response.raise_for_status()
                res = response.json()
                
                response_details = {"url": auth.api_url, "module": payload.get("module"), "status": res.get("status")}
                if res.get("status") != "SUCCESS":
                    if res.get("message"):
                        response_details["error_message"] = res.get("message")
                    if isinstance(res.get("data"), dict) and res.get("data", {}).get("description"):
                        response_details["error_description"] = res.get("data").get("description")
                    elif isinstance(res.get("data"), str):
                        response_details["error_data_string"] = res.get("data")
                self.logger.emit("api_response", response_details)

            except requests.exceptions.Timeout as e:
                self.logger.emit("website_unresponsive", {"url": auth.api_url, "error": f"Timeout: {str(e)}"})
                return "UNRESPONSIVE"
            except requests.exceptions.ConnectionError as e:
                self.logger.emit("website_unresponsive", {"url": auth.api_url, "error": f"ConnectionError: {str(e)}"})
                return "UNRESPONSIVE"
            except Exception as e: # This includes JSONDecodeError if response is not JSON
                self.logger.emit("exception", {"error": f"Downline fetch failed for {auth.api_url}: {str(e)}"})
                return "ERROR" 

            if res.get("status") != "SUCCESS":
                # The api_response log above already captures the details.
                # The original "exception" log here is now redundant due to the enhanced api_response.
                # We still need to return "ERROR" for the main logic to handle it.
                return "ERROR" # Treat non-SUCCESS as an error

            new_rows: List[Downline] = []
            for d in res["data"].get("downlines", []):
                row = Downline(
                    url=url,
                    id=d.get("id"),
                    name=d.get("name"),
                    count=d.get("count", 0),
                    amount=float(d.get("amount", 0) or 0),
                    register_date_time=d.get("registerDateTime")
                )
                # Convert all fields to string for the key to match how DictReader reads them
                key = (
                    str(row.url),
                    str(row.id),
                    str(row.name),
                    str(row.count),
                    str(row.amount),
                    str(row.register_date_time)
                )
                if key not in written:
                    new_rows.append(row)
                    written.add(key)
            
            if not new_rows:
                break

            # Determine if header needs to be written
            file_exists_and_not_empty = os.path.exists(csv_file) and os.path.getsize(csv_file) > 0
            with open(csv_file, "a", newline="", encoding="utf-8") as f:
                # Use field names from the dataclass
                fieldnames = [field.name for field in Downline.__dataclass_fields__.values()]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if not file_exists_and_not_empty: # Write header only if file is new or empty
                    writer.writeheader()
                writer.writerows([row.__dict__ for row in new_rows])
                self.logger.emit("csv_written", {"file": csv_file, "count": len(new_rows)})

            total_new_rows += len(new_rows)
            page += 1

        self.logger.emit("downline_fetched", {"count": total_new_rows}) # This will only be reached on success
        return total_new_rows

    def fetch_bonuses(self, url: str, auth: AuthData, csv_file: str = "bonuses.csv") -> Union[Tuple[int, float], str]:
        payload = {
            "module": "/users/syncData",
            "merchantId": auth.merchant_id,
            "domainId": "0",
            "accessId": auth.access_id,
            "accessToken": auth.token,
            "walletIsAdmin": ""
        }
        self.logger.emit("api_request", {"url": auth.api_url, "module": payload.get("module")})
        try:
            response = requests.post(auth.api_url, data=payload, timeout=self.request_timeout)
            response.raise_for_status()
            res = response.json()

            response_details = {"url": auth.api_url, "module": payload.get("module"), "status": res.get("status")}
            if res.get("status") != "SUCCESS":
                if res.get("message"):
                    response_details["error_message"] = res.get("message")
                if isinstance(res.get("data"), dict) and res.get("data", {}).get("description"):
                    response_details["error_description"] = res.get("data").get("description")
                elif isinstance(res.get("data"), str):
                    response_details["error_data_string"] = res.get("data")
            self.logger.emit("api_response", response_details)

        except requests.exceptions.Timeout as e:
            self.logger.emit("website_unresponsive", {"url": auth.api_url, "error": f"Timeout: {str(e)}"})
            return "UNRESPONSIVE"
        except requests.exceptions.ConnectionError as e:
            self.logger.emit("website_unresponsive", {"url": auth.api_url, "error": f"ConnectionError: {str(e)}"})
            return "UNRESPONSIVE"
        except Exception as e: # This includes JSONDecodeError if response is not JSON
            self.logger.emit("exception", {"error": f"Bonus fetch failed for {auth.api_url}: {str(e)}"})
            return "ERROR"

        if res.get("status") != "SUCCESS":
            # The api_response log above captures details.
            # The "bonus_api_error" also logs specific details, which is good.
            # It's important that bonus_api_error is still emitted for specific metric tracking in logger.load_metrics
            self.logger.emit("bonus_api_error", {"url": auth.api_url, "status": res.get("status"), "error_message": res.get("message", "N/A"), "error_data": res.get("data", "N/A")})
            return "ERROR"

        bonuses_data_raw = res.get("data", {}).get("bonus", []) + res.get("data", {}).get("promotions", [])
        if not bonuses_data_raw:
            # Log that no bonuses were found, but with 0 amount. This is a successful API call.
            self.logger.emit("bonus_fetched", {"count": 0, "total_amount": 0.0})
            return 0, 0.0 # Return count 0 and amount 0.0 for consistency
        
        rows_to_write_obj: List[Bonus] = [] # Store Bonus objects
        # Determine if header needs to be written
        file_exists_and_not_empty = os.path.exists(csv_file) and os.path.getsize(csv_file) > 0
        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            fieldnames = [field.name for field in Bonus.__dataclass_fields__.values()]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists_and_not_empty: # Write header only if file is new or empty
                writer.writeheader()

            # Process bonuses and create Bonus objects
            for b_data in bonuses_data_raw:
                try:
                    min_w = float(b_data.get("minWithdraw", 0) or 0)
                    bonus_f = float(b_data.get("bonusFixed", 0) or 0)
                    ratio = min_w / bonus_f if bonus_f != 0 else None
                except (ValueError, TypeError):
                    self.logger.emit("exception", {"error": f"Type error processing bonus data for {url}: {b_data}"})
                    continue # Skip this bonus if there's a data issue
                
                bonus_instance = Bonus(
                    url=url,
                    merchant_name=auth.merchant_name,
                    id=b_data.get("id"),
                    name=b_data.get("name"),
                    transaction_type=b_data.get("transactionType"),
                    bonus_fixed=float(b_data.get("bonusFixed", 0) or 0),
                    amount=float(b_data.get("amount", 0) or 0), # Ensure amount is float
                    min_withdraw=float(b_data.get("minWithdraw", 0) or 0),
                    max_withdraw=float(b_data.get("maxWithdraw", 0) or 0),
                    withdraw_to_bonus_ratio=ratio,
                    rollover=float(b_data.get("rollover", 0) or 0),
                    balance=str(b_data.get("balance", "")),
                    claim_config=str(b_data.get("claimConfig", "")),
                    claim_condition=str(b_data.get("claimCondition", "")),
                    bonus=str(b_data.get("bonus", "")),
                    bonus_random=str(b_data.get("bonusRandom", "")),
                    reset=str(b_data.get("reset", "")),
                    min_topup=float(b_data.get("minTopup", 0) or 0),
                    max_topup=float(b_data.get("maxTopup", 0) or 0),
                    refer_link=str(b_data.get("referLink", ""))
                )
                rows_to_write_obj.append(bonus_instance)
            
            if rows_to_write_obj:
                writer.writerows([b.__dict__ for b in rows_to_write_obj])
                self.logger.emit("csv_written", {"file": csv_file, "count": len(rows_to_write_obj)})

        current_fetch_total_amount = sum(b.amount for b in rows_to_write_obj)
        self.logger.emit("bonus_fetched", {"count": len(rows_to_write_obj), "total_amount": current_fetch_total_amount})
        return len(rows_to_write_obj), current_fetch_total_amount

import time
from .config import ConfigLoader
# Logger, Scraper are defined above. AuthService is imported.

def load_urls(url_file: str) -> List[str]:
    # Ensure file exists before attempting to open
    if not os.path.exists(url_file):
        print(f"URL file not found: {url_file}") # Or use logger
        return []
    with open(url_file, "r") as f:
        return [url.strip() for url in f if url.strip()]

def main():
    # Load configuration
    # Assuming config.ini is in the root, adjust path if necessary
    config_loader = ConfigLoader(path="config.ini")
    config = config_loader.load()

    REQUEST_TIMEOUT = 30  # Define request timeout
    unresponsive_sites_this_run = [] # Initialize list for unresponsive sites

    logger = Logger(
        log_file=config.logging.log_file,
        log_level=config.logging.log_level,
        console=config.logging.console,
        detail=config.logging.detail
    )
    auth_service = AuthService(logger) # AuthService is now imported
    scraper = Scraper(logger, REQUEST_TIMEOUT) # Pass REQUEST_TIMEOUT

    # Load URLs and metrics
    urls = load_urls(config.settings.url_file)
    if not urls: # Exit if no URLs are loaded
        logger.emit("job_start", {"url_count": 0, "status": "No URLs to process"})
        print("No URLs to process. Exiting.")
        return
        
    total_urls = len(urls)
    history = logger.load_metrics(config.logging.log_file) # load_metrics is part of Logger
    
    # Initialize metrics correctly
    metrics = {
        "bonuses_old": history.get("bonuses", 0), # Use .get for safety
        "downlines_old": history.get("downlines", 0),
        "errors_old": history.get("errors", 0), # Total errors from past runs
        "bonuses_new": 0, # New bonus items this run
        "downlines_new": 0, # New downline items this run
        "errors_new": 0, # New errors this run (includes unresponsive, API errors, general exceptions)
        "bonus_amount_new": 0.0, # New bonus amount this run
    }
    # For total tracking, start with old values and add new ones
    metrics["bonuses_total_old"] = history.get("bonuses", 0)
    metrics["downlines_total_old"] = history.get("downlines", 0)
    metrics["errors_total_old"] = history.get("errors", 0)
    metrics["bonus_amount_total_old"] = history.get("total_bonus_amount", 0.0) # Historical total bonus amount

    # Initialize _total_new with _total_old values, then increment with _new values from this run
    metrics["bonuses_total_new"] = metrics["bonuses_total_old"]
    metrics["downlines_total_new"] = metrics["downlines_total_old"]
    metrics["errors_total_new"] = metrics["errors_total_old"]
    metrics["bonus_amount_total_new"] = metrics["bonus_amount_total_old"]


    logger.emit("job_start", {"url_count": total_urls})
    start_time = time.time() # Corrected variable name

    for idx, url in enumerate(urls, 1):
        cleaned_url = auth_service.clean_url(url)
        try:
            auth_data = auth_service.login(
                cleaned_url,
                config.credentials.mobile,
                config.credentials.password
            )
            if not auth_data:
                metrics["errors_new"] += 1
                metrics["errors_total_new"] += 1
                # Log this specific type of error if needed, e.g., auth_service.login might return None on failure
                logger.emit("exception", {"error": f"Authentication failed for {cleaned_url}"})
                continue
            
            result: Union[int, str] # Define type for result
            if config.settings.downline_enabled:
                result = scraper.fetch_downlines(cleaned_url, auth_data)
            else:
                result = scraper.fetch_bonuses(cleaned_url, auth_data)

            if isinstance(result, str): # "UNRESPONSIVE" or "ERROR"
                metrics["errors_new"] += 1
                metrics["errors_total_new"] += 1
                if result == "UNRESPONSIVE":
                    unresponsive_sites_this_run.append(cleaned_url)
                # No further action for "ERROR" as it's already logged by scraper method
            else: # Success, result is a tuple (count, amount) for bonuses, or int for downlines
                if config.settings.downline_enabled:
                    # result is int for downlines
                    metrics["downlines_new"] += result
                    metrics["downlines_total_new"] += result
                else:
                    # result is Tuple[int, float] for bonuses
                    count, current_fetch_total_amount = result
                    metrics["bonuses_new"] += count
                    metrics["bonus_amount_new"] += current_fetch_total_amount
                    # bonuses_total_new is already initialized with historical, so just add new
                    metrics["bonuses_total_new"] += count 
                    metrics["bonus_amount_total_new"] += current_fetch_total_amount
        
        except Exception as e: # Catch-all for unexpected errors in the main loop
            metrics["errors_new"] += 1
            metrics["errors_total_new"] += 1
            logger.emit("exception", {"error": f"Outer loop exception for {cleaned_url}: {str(e)}"})

        # Progress reporting
        percent = (idx / total_urls) * 100
        # Ensure history["runs"] is not zero to avoid DivisionByZeroError
        avg_runtime = (history.get("total_runtime", 0) / history.get("runs")) if history.get("runs") else 0
        
        # Calculate increments for current run
        bonuses_increment = metrics["bonuses_new"]
        downlines_increment = metrics["downlines_new"]
        errors_increment = metrics["errors_new"]
        
        # Calculate total new counts by adding current new to previous totals
        # This was slightly confusing, let's simplify:
        # *_old are from previous logs (history)
        # *_new are for this current run only
        # *_total_new are history + current run's new values
        
        current_run_bonuses = metrics["bonuses_new"]
        current_run_downlines = metrics["downlines_new"]
        current_run_errors = metrics["errors_new"]
        current_run_bonus_amount = metrics["bonus_amount_new"]

        # Calculate averages, handling division by zero
        avg_bonus_amount_hist = (history.get("total_bonus_amount", 0.0) / history.get("bonuses", 1)) if history.get("bonuses", 0) > 0 else 0.0
        avg_bonus_amount_new = (current_run_bonus_amount / current_run_bonuses) if current_run_bonuses > 0 else 0.0
        avg_bonus_amount_total = (metrics["bonus_amount_total_new"] / metrics["bonuses_total_new"]) if metrics["bonuses_total_new"] > 0 else 0.0
        
        hist_successful_bonus_fetches = history.get("successful_bonus_fetches", 0)
        hist_failed_bonus_api_calls = history.get("failed_bonus_api_calls", 0)

        print(
            f"[ {idx:03}/{total_urls} | {percent:.2f}% ] [ Avg. Run Time: {avg_runtime:.1f}s ] {cleaned_url}\n"
            f"[ Bonuses: Items Hist {metrics['bonuses_old']} / New {current_run_bonuses} | Total {metrics['bonuses_total_new']} ]\n"
            f"[ Total Bonus Amt: Hist {history.get('total_bonus_amount', 0.0):.2f} / New {current_run_bonus_amount:.2f} | Total {metrics['bonus_amount_total_new']:.2f} ]\n"
            f"[ Avg Bonus Amt: Hist {avg_bonus_amount_hist:.2f} / New {avg_bonus_amount_new:.2f} | Total {avg_bonus_amount_total:.2f} ]\n"
            f"[ Downlines: Hist {metrics['downlines_old']} / New {current_run_downlines} | Total {metrics['downlines_total_new']} ]\n"
            f"[ Errors: Hist {metrics['errors_old']} / New {current_run_errors} | Total {metrics['errors_total_new']} ]\n"
            f"[ Bonus API Stats (Hist): Fetches OK {hist_successful_bonus_fetches} / API Fails {hist_failed_bonus_api_calls} ]\n"
        )

    elapsed = time.time() - start_time
    
    # Calculate average bonus amount for the current run
    avg_bonus_amount_this_run = (metrics["bonus_amount_new"] / metrics["bonuses_new"]) if metrics["bonuses_new"] > 0 else 0.0

    job_summary_details = {
        "duration": elapsed,
        "total_urls_processed": total_urls, # total_urls is defined earlier in main()
        "bonuses_fetched_this_run": metrics["bonuses_new"],
        "bonus_amount_this_run": metrics["bonus_amount_new"],
        "avg_bonus_amount_this_run": avg_bonus_amount_this_run,
        "downlines_fetched_this_run": metrics["downlines_new"],
        "errors_this_run": metrics["errors_new"],
        "unresponsive_sites_count_this_run": len(unresponsive_sites_this_run)
    }
    logger.emit("job_complete", job_summary_details)

    if unresponsive_sites_this_run:
        logger.emit("down_sites_summary", {
            "sites": unresponsive_sites_this_run,
            "count": len(unresponsive_sites_this_run)
        })

if __name__ == "__main__":
    main()
