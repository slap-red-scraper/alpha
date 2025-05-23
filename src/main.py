import csv
import os
import requests
from typing import List, Set, Tuple
from .models import Downline, Bonus, AuthData
from .logger import Logger

class Scraper:
    """Handles scraping of downlines and bonuses."""
    def __init__(self, logger: Logger):
        self.logger = logger

    def fetch_downlines(self, url: str, auth: AuthData, csv_file: str = "downlines.csv") -> int:
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
            try:
                response = requests.post(auth.api_url, data=payload)
                response.raise_for_status()
                res = response.json()
                self.logger.emit("api_response", {"url": url, "status": res.get("status")})
            except Exception as e:
                self.logger.emit("exception", {"error": f"Downline fetch failed for {url}: {str(e)}"})
                break

            if res.get("status") != "SUCCESS":
                break

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

        self.logger.emit("downline_fetched", {"count": total_new_rows})
        return total_new_rows

    def fetch_bonuses(self, url: str, auth: AuthData, csv_file: str = "bonuses.csv") -> int:
        payload = {
            "module": "/users/syncData",
            "merchantId": auth.merchant_id,
            "domainId": "0",
            "accessId": auth.access_id,
            "accessToken": auth.token,
            "walletIsAdmin": ""
        }
        try:
            response = requests.post(auth.api_url, data=payload)
            response.raise_for_status()
            res = response.json()
            self.logger.emit("api_response", {"url": url, "status": res.get("status")})
        except Exception as e:
            self.logger.emit("exception", {"error": f"Bonus fetch failed for {url}: {str(e)}"})
            return 0

        bonuses_data = res.get("data", {}).get("bonus", []) + res.get("data", {}).get("promotions", [])
        if not bonuses_data:
            return 0
        
        # Determine if header needs to be written
        file_exists_and_not_empty = os.path.exists(csv_file) and os.path.getsize(csv_file) > 0
        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            fieldnames = [field.name for field in Bonus.__dataclass_fields__.values()]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists_and_not_empty: # Write header only if file is new or empty
                writer.writeheader()

            rows_to_write = []
            for b in bonuses_data:
                try:
                    min_w = float(b.get("minWithdraw", 0) or 0)
                    bonus_f = float(b.get("bonusFixed", 0) or 0)
                    ratio = min_w / bonus_f if bonus_f != 0 else None # Avoid division by zero
                except (ValueError, TypeError):
                    ratio = None
                
                # Create Bonus instance using its __dict__ representation for writerow
                bonus_instance = Bonus(
                    url=url,
                    merchant_name=auth.merchant_name,
                    id=b.get("id"),
                    name=b.get("name"),
                    transaction_type=b.get("transactionType"),
                    bonus_fixed=float(b.get("bonusFixed", 0) or 0),
                    amount=float(b.get("amount", 0) or 0),
                    min_withdraw=float(b.get("minWithdraw", 0) or 0),
                    max_withdraw=float(b.get("maxWithdraw", 0) or 0),
                    withdraw_to_bonus_ratio=ratio,
                    rollover=float(b.get("rollover", 0) or 0),
                    balance=str(b.get("balance", "")), # Ensure balance is string
                    claim_config=str(b.get("claimConfig", "")),
                    claim_condition=str(b.get("claimCondition", "")),
                    bonus=str(b.get("bonus", "")),
                    bonus_random=str(b.get("bonusRandom", "")),
                    reset=str(b.get("reset", "")),
                    min_topup=float(b.get("minTopup", 0) or 0),
                    max_topup=float(b.get("maxTopup", 0) or 0),
                    refer_link=str(b.get("referLink", ""))
                )
                rows_to_write.append(bonus_instance.__dict__)
            
            if rows_to_write:
                writer.writerows(rows_to_write)
                self.logger.emit("csv_written", {"file": csv_file, "count": len(rows_to_write)})

        self.logger.emit("bonus_fetched", {"count": len(bonuses_data)})
        return len(bonuses_data)

import time # Make sure time is imported for the main function part
from .config import ConfigLoader
# Logger, AuthService, Scraper are already imported or defined in this file.

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
    
    logger = Logger(
        log_file=config.logging.log_file,
        log_level=config.logging.log_level,
        console=config.logging.console,
        detail=config.logging.detail
    )
    auth_service = AuthService(logger) # This line will cause an error.
    scraper = Scraper(logger)

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
        "errors_old": history.get("errors", 0),
        "bonuses_new": 0,
        "downlines_new": 0,
        "errors_new": 0,
    }
    # For total tracking, start with old values and add new ones
    metrics["bonuses_total_old"] = history.get("bonuses",0)
    metrics["downlines_total_old"] = history.get("downlines",0)
    metrics["errors_total_old"] = history.get("errors",0)
    
    metrics["bonuses_total_new"] = metrics["bonuses_total_old"] 
    metrics["downlines_total_new"] = metrics["downlines_total_old"]
    metrics["errors_total_new"] = metrics["errors_total_old"]


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
                continue

            if config.settings.downline_enabled:
                count = scraper.fetch_downlines(cleaned_url, auth_data)
                metrics["downlines_new"] += count
                metrics["downlines_total_new"] += count
            else:
                count = scraper.fetch_bonuses(cleaned_url, auth_data)
                metrics["bonuses_new"] += count
                metrics["bonuses_total_new"] += count

        except Exception as e:
            metrics["errors_new"] += 1
            metrics["errors_total_new"] += 1
            logger.emit("exception", {"error": str(e)})

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

        print(
            f"[ {idx:03}/{total_urls} | {percent:.2f}% ] [ Avg. Run Time: {avg_runtime:.1f}s ] {cleaned_url}\n"
            f"[ Bonuses: {metrics['bonuses_old']}/{current_run_bonuses} (+{current_run_bonuses}) | "
            f"Total: {metrics['bonuses_total_old']}/{metrics['bonuses_total_old'] + current_run_bonuses} (+{current_run_bonuses}) ] "
            f"[ Downlines: {metrics['downlines_old']}/{current_run_downlines} (+{current_run_downlines}) | "
            f"Total: {metrics['downlines_total_old']}/{metrics['downlines_total_old'] + current_run_downlines} (+{current_run_downlines}) ] "
            f"[ Errors: {metrics['errors_old']}/{current_run_errors} (+current_run_errors) | "
            f"Total: {metrics['errors_total_old']}/{metrics['errors_total_old'] + current_run_errors} (+{current_run_errors}) ]\n"
        )

    elapsed = time.time() - start_time # Corrected variable name
    logger.emit("job_complete", {"duration": elapsed})

if __name__ == "__main__":
    main()
