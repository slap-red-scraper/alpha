import csv
import os
import sys # Added sys import
import time # Added time import
import requests
import pandas as pd # Added pandas import
from datetime import datetime, timedelta # Added import
from typing import List, Set, Tuple, Union # Union for return types
from .models import Downline, Bonus, AuthData
from .logger import Logger
from .auth import AuthService # Added import for AuthService
from .utils import progress, load_run_cache, save_run_cache # Added cache imports

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

    def fetch_bonuses(self, url: str, auth: AuthData, csv_file: str = "bonuses.csv") -> Union[Tuple[int, float, dict[str, bool]], str]:
        # Define keyword lists (case-insensitive search)
        C_KEYWORDS = ["commission", "affiliate"]
        D_KEYWORDS = ["downline first deposit"]
        S_KEYWORDS = ["share bonus", "referrer"]
        
        # Initialize bonus_type_flags
        bonus_type_flags = {"C": False, "D": False, "S": False, "O": False}

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
            return 0, 0.0, bonus_type_flags # Return count 0, amount 0.0, and flags
        
        rows_to_write_obj: List[Bonus] = [] # Store Bonus objects
        # Ensure the directory for the CSV file exists
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)
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

                # Keyword matching for bonus types
                name_lower = bonus_instance.name.lower() if bonus_instance.name else ""
                claim_config_lower = bonus_instance.claim_config.lower() if bonus_instance.claim_config else ""
                matched_c_d_s_for_this_bonus = False

                if any(keyword in name_lower or keyword in claim_config_lower for keyword in C_KEYWORDS):
                    bonus_type_flags["C"] = True
                    matched_c_d_s_for_this_bonus = True
                if any(keyword in name_lower or keyword in claim_config_lower for keyword in D_KEYWORDS):
                    bonus_type_flags["D"] = True
                    matched_c_d_s_for_this_bonus = True
                if any(keyword in name_lower or keyword in claim_config_lower for keyword in S_KEYWORDS):
                    bonus_type_flags["S"] = True
                    matched_c_d_s_for_this_bonus = True
                
                if not matched_c_d_s_for_this_bonus: # If a bonus instance exists and didn't match C, D, or S
                    bonus_type_flags["O"] = True

            if rows_to_write_obj:
                writer.writerows([b.__dict__ for b in rows_to_write_obj])
                self.logger.emit("csv_written", {"file": csv_file, "count": len(rows_to_write_obj)})

        current_fetch_total_amount = sum(b.amount for b in rows_to_write_obj)
        self.logger.emit("bonus_fetched", {"count": len(rows_to_write_obj), "total_amount": current_fetch_total_amount})
        return len(rows_to_write_obj), current_fetch_total_amount, bonus_type_flags

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

    # Load run cache at the beginning
    run_cache_data = load_run_cache()
    run_cache_data["total_script_runs"] += 1
    # The run_cache_data["sites"] will be updated later as per subsequent tasks.

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

    # Helper function for formatting stat parts for display
    def format_stat_display(current_val, prev_val):
        if current_val == 0 and prev_val == 0:
            return "" # Omit if 0/0
        diff = current_val - prev_val
        return f"{current_val}/{prev_val}({diff:+})"

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

    try:
        logger.emit("job_start", {"url_count": total_urls, "total_script_runs": run_cache_data.get("total_script_runs", "N/A")})
        start_time = time.time() # Corrected variable name

        for idx, url in enumerate(urls, 1):
            if idx > 1: # If not the first iteration
                sys.stdout.write('\x1b[3A') # Move cursor up 3 lines
                sys.stdout.write('\x1b[J')  # Clear from cursor to end of screen

            site_start_time = time.time() # Start timing for the site
            cleaned_url = auth_service.clean_url(url)
            site_key = cleaned_url # Use cleaned_url as the consistent key

            # a. Initialize site-specific new item counts for the current run
            cr_bonuses_site = 0
            cr_downlines_site = 0
            cr_errors_site = 0
            # current_site_bonus_flags is initialized later, just before bonus fetching

            # b. Retrieve cached "previous run" data for the cleaned_url
            site_cache_entry = run_cache_data["sites"].get(site_key, {})
            pr_bonuses = site_cache_entry.get("last_run_new_bonuses", 0)
            prt_bonuses = site_cache_entry.get("cumulative_total_bonuses", 0)
            pr_downlines = site_cache_entry.get("last_run_new_downlines", 0)
            prt_downlines = site_cache_entry.get("cumulative_total_downlines", 0)
            pr_errors = site_cache_entry.get("last_run_new_errors", 0)
            prt_errors = site_cache_entry.get("cumulative_total_errors", 0)

            try:
                auth_data = auth_service.login(
                    cleaned_url,
                config.credentials.mobile,
                config.credentials.password
            )
                if not auth_data:
                    metrics["errors_new"] += 1 # Global metric
                    metrics["errors_total_new"] += 1 # Global metric
                    cr_errors_site = 1 # Site-specific error
                    logger.emit("exception", {"error": f"Authentication failed for {cleaned_url}"})
                    # Skip to the next URL via 'continue' is implicitly handled by structure if auth_data is None
                
                current_site_bonus_flags = {"C": False, "D": False, "S": False, "O": False} # Initialize for this site
                
                if auth_data: # Only proceed if authentication was successful
                    if config.settings.downline_enabled:
                        result_dl = scraper.fetch_downlines(cleaned_url, auth_data)
                        if isinstance(result_dl, str): # "UNRESPONSIVE" or "ERROR"
                            metrics["errors_new"] += 1
                            metrics["errors_total_new"] += 1
                            cr_errors_site = 1
                            if result_dl == "UNRESPONSIVE":
                                unresponsive_sites_this_run.append(cleaned_url)
                        else: # Success, result_dl is int
                            cr_downlines_site = result_dl
                            metrics["downlines_new"] += result_dl
                            metrics["downlines_total_new"] += result_dl
                    else: # Bonus fetching mode
                        bonus_csv_path = datetime.now().strftime("data/%m-%d bonuses.csv")
                        result_bonuses = scraper.fetch_bonuses(cleaned_url, auth_data, csv_file=bonus_csv_path)
                        if isinstance(result_bonuses, str): # "UNRESPONSIVE" or "ERROR"
                            metrics["errors_new"] += 1
                            metrics["errors_total_new"] += 1
                            cr_errors_site = 1
                            if result_bonuses == "UNRESPONSIVE":
                                unresponsive_sites_this_run.append(cleaned_url)
                        else: # Success
                            count, current_fetch_total_amount, current_site_bonus_flags = result_bonuses
                            cr_bonuses_site = count
                            metrics["bonuses_new"] += count
                            metrics["bonus_amount_new"] += current_fetch_total_amount
                            metrics["bonuses_total_new"] += count 
                            metrics["bonus_amount_total_new"] += current_fetch_total_amount
            
            except Exception as e: # Catch-all for unexpected errors in the main loop for this site
                metrics["errors_new"] += 1
                metrics["errors_total_new"] += 1
                cr_errors_site = 1
                logger.emit("exception", {"error": f"Outer loop exception for {cleaned_url}: {str(e)}"})

            # d. Calculate current run's cumulative totals for the site
            crt_bonuses = prt_bonuses + cr_bonuses_site
            crt_downlines = prt_downlines + cr_downlines_site
            crt_errors = prt_errors + cr_errors_site

            # e. Store these newly calculated values into run_cache_data
            run_cache_data["sites"].setdefault(site_key, {})
            run_cache_data["sites"][site_key]["last_run_new_bonuses"] = cr_bonuses_site
            run_cache_data["sites"][site_key]["cumulative_total_bonuses"] = crt_bonuses
            run_cache_data["sites"][site_key]["last_run_new_downlines"] = cr_downlines_site
            run_cache_data["sites"][site_key]["cumulative_total_downlines"] = crt_downlines
            run_cache_data["sites"][site_key]["last_run_new_errors"] = cr_errors_site
            run_cache_data["sites"][site_key]["cumulative_total_errors"] = crt_errors
            run_cache_data["sites"][site_key]["bonus_flags"] = current_site_bonus_flags # Store bonus flags

            # f. Package stats for display (used for new print logic)
            display_stats_for_site = {
                "cr_bonuses_site": cr_bonuses_site, "pr_bonuses": pr_bonuses, "crt_bonuses": crt_bonuses, "prt_bonuses": prt_bonuses,
                "cr_downlines_site": cr_downlines_site, "pr_downlines": pr_downlines, "crt_downlines": crt_downlines, "prt_downlines": prt_downlines,
                "cr_errors_site": cr_errors_site, "pr_errors": pr_errors, "crt_errors": crt_errors, "prt_errors": prt_errors,
                "bonus_flags": current_site_bonus_flags
            }
            
            site_processing_duration = time.time() - site_start_time

            # Prepare display data for new 3-line print
            percent = (idx / total_urls) * 100
            run_count = run_cache_data["total_script_runs"]
            sfs = display_stats_for_site # Shorthand

            bonus_flags = sfs.get('bonus_flags', {})
            flags_str = f"[C] {'Y' if bonus_flags.get('C') else 'N'} " + \
                        f"[D] {'Y' if bonus_flags.get('D') else 'N'} " + \
                        f"[S] {'Y' if bonus_flags.get('S') else 'N'} " + \
                        f"[O] {'Y' if bonus_flags.get('O') else 'N'}"
            
            progress_bar_str = progress(idx, vmin=0, vmax=total_urls, length=40, title="") # Reduced length for compactness
            
            line1 = f"| {progress_bar_str} | [{percent:.2f}%] {idx}/{total_urls} |"
            line2 = f"| {site_processing_duration:.1f}s | [Run #{run_count}] | {flags_str} | [URL] {cleaned_url} |"

            r_b = format_stat_display(sfs['cr_bonuses_site'], sfs['pr_bonuses'])
            t_b = format_stat_display(sfs['crt_bonuses'], sfs['prt_bonuses'])
            stats_b_str = f"[B]|[R]:{r_b if r_b else '-'} [T]:{t_b if t_b else '-'}"

            stats_d_str = ""
            if config.settings.downline_enabled:
                r_d = format_stat_display(sfs['cr_downlines_site'], sfs['pr_downlines'])
                t_d = format_stat_display(sfs['crt_downlines'], sfs['prt_downlines'])
                stats_d_str = f"| [D]|[R]:{r_d if r_d else '-'} [T]:{t_d if t_d else '-'}"

            r_e = format_stat_display(sfs['cr_errors_site'], sfs['pr_errors'])
            t_e = format_stat_display(sfs['crt_errors'], sfs['prt_errors'])
            stats_e_str = f"| [E]|[R]:{r_e if r_e else '-'} [T]:{t_e if t_e else '-'}"
            
            line3 = f"| {stats_b_str} {stats_d_str} {stats_e_str} |"

            sys.stdout.write(f"{line1}\n")
            sys.stdout.write(f"{line2}\n")
            sys.stdout.write(f"{line3}\n")
            sys.stdout.flush()

            # The old print statement below this block will be removed by the next SEARCH/REPLACE
        
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

        # Prepare bonus flags string for display
        # current_site_bonus_flags is available here from the loop
        bonus_flags_str = ""
        if not config.settings.downline_enabled: # Only show if fetching bonuses
            flags_display = []
            if current_site_bonus_flags.get("C"): flags_display.append("C")
            if current_site_bonus_flags.get("D"): flags_display.append("D")
            if current_site_bonus_flags.get("S"): flags_display.append("S")
            if current_site_bonus_flags.get("O"): flags_display.append("O")
            if not flags_display and metrics["bonuses_new"] > 0 : # If bonuses were fetched but no C,D,S,O flags set (should be rare if O logic is correct)
                 # This case implies bonuses were fetched but none were categorized.
                 # This might happen if fetch_bonuses returns success (count > 0) but all bonus items were skipped before O flag could be set.
                 # Or if fetch_bonuses returned (0,0,flags) where flags are all False.
                 pass # No specific flags to show, or O logic will handle it.
            bonus_flags_str = f"[ Flags: {' '.join(flags_display) if flags_display else 'N/A'} ]"


        print(
            f"[ {idx:03}/{total_urls} | {percent:.2f}% ] [ Avg. Run Time: {avg_runtime:.1f}s ] {cleaned_url} {bonus_flags_str}\n" # Added bonus_flags_str
            f"[ Bonuses: Items Hist {metrics['bonuses_old']} / New {current_run_bonuses} | Total {metrics['bonuses_total_new']} ]\n"
            f"[ Total Bonus Amt: Hist {history.get('total_bonus_amount', 0.0):.2f} / New {current_run_bonus_amount:.2f} | Total {metrics['bonus_amount_total_new']:.2f} ]\n"
            f"[ Avg Bonus Amt: Hist {avg_bonus_amount_hist:.2f} / New {avg_bonus_amount_new:.2f} | Total {avg_bonus_amount_total:.2f} ]\n"
            # This old print block is removed.
        )

        elapsed = time.time() - start_time
        sys.stdout.write("\n") # Ensure prompt is on a new line after loop
        
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
        # --- Historical Bonus Data to Excel ---
        today_date_str = datetime.now().strftime('%m-%d')
        daily_bonus_csv_path = f"data/{today_date_str} bonuses.csv"
        historical_excel_path = "data/historical_bonuses.xlsx"

        if not config.settings.downline_enabled: # Only run if bonuses were being collected
            if os.path.exists(daily_bonus_csv_path) and os.path.getsize(daily_bonus_csv_path) > 0:
                try:
                    bonus_df = pd.read_csv(daily_bonus_csv_path)
                    if not bonus_df.empty:
                        # Ensure the 'data' directory exists for the Excel file
                        os.makedirs(os.path.dirname(historical_excel_path), exist_ok=True)
                        
                        mode = 'a' if os.path.exists(historical_excel_path) else 'w'
                        with pd.ExcelWriter(historical_excel_path, engine='openpyxl', mode=mode, if_sheet_exists='replace') as writer:
                            bonus_df.to_excel(writer, sheet_name=today_date_str, index=False)
                        logger.emit("historical_data_written", {"file": historical_excel_path, "sheet": today_date_str, "rows": len(bonus_df)})
                    else:
                        logger.emit("historical_data_skipped", {"reason": "Daily bonus CSV is empty", "file": daily_bonus_csv_path})
                except Exception as e:
                    logger.emit("historical_data_error", {"file": daily_bonus_csv_path, "excel_file": historical_excel_path, "error": str(e)})
            else:
                logger.emit("historical_data_skipped", {"reason": "Daily bonus CSV not found or empty", "file": daily_bonus_csv_path})
        # --- End Historical Bonus Data ---

        # --- Bonus Comparison Logic ---
        try:
            # from datetime import timedelta # Ensure timedelta is available (already imported at top)

            today_dt = datetime.now()
            yesterday_dt = today_dt - timedelta(days=1)
            today_sheet_name = today_dt.strftime('%m-%d')
            yesterday_sheet_name = yesterday_dt.strftime('%m-%d')
            
            comparison_report_path = f"data/comparison_report_{today_sheet_name}.csv"
            # historical_excel_path is defined in the block above

            today_df = None
            # Try to use bonus_df if it exists from the historical data update step
            if 'bonus_df' in locals() and isinstance(bonus_df, pd.DataFrame) and not bonus_df.empty:
                # A bit of a heuristic: if daily_bonus_csv_path (from historical step) matches today's pattern
                if 'daily_bonus_csv_path' in locals() and daily_bonus_csv_path == f"data/{today_sheet_name} bonuses.csv":
                     today_df = bonus_df
            
            if today_df is None: # If not available or not today's data, load it
                current_day_bonus_csv = f"data/{today_sheet_name} bonuses.csv"
                if os.path.exists(current_day_bonus_csv) and os.path.getsize(current_day_bonus_csv) > 0:
                    today_df = pd.read_csv(current_day_bonus_csv)
                else:
                    today_df = pd.DataFrame() 

            yesterday_df = pd.DataFrame()
            if os.path.exists(historical_excel_path): # historical_excel_path defined in previous block
                try:
                    yesterday_df = pd.read_excel(historical_excel_path, sheet_name=yesterday_sheet_name)
                except FileNotFoundError: # Should not happen if os.path.exists passed, but good for robustness
                    logger.emit("comparison_info", {"message": f"Historical Excel file not found at {historical_excel_path} for comparison. Yesterday's data assumed empty."})
                except ValueError: # Sheet not found
                    logger.emit("comparison_info", {"message": f"Sheet {yesterday_sheet_name} for yesterday not found in {historical_excel_path}. Yesterday's data assumed empty."})
                except Exception as e:
                    logger.emit("comparison_error", {"message": f"Error reading yesterday's sheet {yesterday_sheet_name} from {historical_excel_path}: {str(e)}"})
            else:
                logger.emit("comparison_info", {"message": f"Historical Excel file {historical_excel_path} does not exist. Yesterday's data assumed empty."})

            expected_columns = [
                'url', 'merchant_name', 'id', 'name', 'transaction_type', 'bonus_fixed', 'amount',
                'min_withdraw', 'max_withdraw', 'withdraw_to_bonus_ratio', 'rollover', 'balance',
                'claim_config', 'claim_condition', 'bonus', 'bonus_random', 'reset',
                'min_topup', 'max_topup', 'refer_link'
            ]

            # Initialize DataFrames with expected columns if they are empty
            if today_df.empty:
                today_df = pd.DataFrame(columns=expected_columns)
            else:
                for col in expected_columns:
                    if col not in today_df.columns: today_df[col] = pd.NA
            
            if yesterday_df.empty:
                yesterday_df = pd.DataFrame(columns=expected_columns)
            else:
                for col in expected_columns:
                    if col not in yesterday_df.columns: yesterday_df[col] = pd.NA
                
            key_cols = ['merchant_name', 'name', 'amount']
            for df_ref in [today_df, yesterday_df]:
                if not df_ref.empty:
                    for col in key_cols:
                        if col == 'amount':
                            df_ref[col] = pd.to_numeric(df_ref[col], errors='coerce').round(5)
                        else:
                            df_ref[col] = df_ref[col].astype(str).fillna('') # fillna for string keys
                    # Drop rows if any part of the composite key is NaN after coercion, as they can't be reliably compared
                    df_ref.dropna(subset=[k for k in key_cols if k in df_ref.columns], how='any', inplace=True)


            if not today_df.empty:
                today_df['_comparison_key'] = today_df['merchant_name'].astype(str) + "_" + today_df['name'].astype(str) + "_" + today_df['amount'].astype(str)
            else: # Add empty key series if df is empty, for consistent merge
                today_df['_comparison_key'] = pd.Series(dtype='object')


            if not yesterday_df.empty:
                yesterday_df['_comparison_key'] = yesterday_df['merchant_name'].astype(str) + "_" + yesterday_df['name'].astype(str) + "_" + yesterday_df['amount'].astype(str)
            else:
                yesterday_df['_comparison_key'] = pd.Series(dtype='object')

            report_data_list = []

            if today_df.empty and yesterday_df.empty:
                logger.emit("comparison_info", {"message": "Both today's and yesterday's bonus data are empty. No comparison report generated."})
            else:
                # Ensure _comparison_key exists even if DFs were originally empty
                if '_comparison_key' not in today_df.columns: today_df['_comparison_key'] = pd.Series(dtype='object')
                if '_comparison_key' not in yesterday_df.columns: yesterday_df['_comparison_key'] = pd.Series(dtype='object')

                merged_df = pd.merge(
                    today_df,
                    yesterday_df,
                    on='_comparison_key',
                    how='outer',
                    suffixes=('_today', '_yesterday'),
                    indicator=True
                )
                
                report_columns = ['status', 'change_details'] + expected_columns
                
                for idx, row in merged_df.iterrows():
                    item_details = {}
                    status = ""
                    change_details_str = ""

                    is_new = row['_merge'] == 'left_only'
                    is_used = row['_merge'] == 'right_only'
                    is_persistent = row['_merge'] == 'both'

                    current_suffix = '_today' if not is_used else '' # Should not happen for used
                    prior_suffix = '_yesterday' if not is_new else '' # Should not happen for new

                    if is_new:
                        status = "New"
                        for col in expected_columns: item_details[col] = row[col + current_suffix]
                    elif is_used:
                        status = "Used"
                        for col in expected_columns: item_details[col] = row[col + prior_suffix]
                    elif is_persistent:
                        status = "Persistent_Unchanged"
                        changes_detected_list = []
                        for col in expected_columns:
                            item_details[col] = row[col + '_today'] # Current value
                            val_today = row[col + '_today']
                            val_yesterday = row[col + '_yesterday']

                            # Robust NaN and type checking for comparison
                            if pd.isna(val_today) and pd.isna(val_yesterday): continue
                            if pd.isna(val_today) or pd.isna(val_yesterday): # One is NaN, other is not
                                changes_detected_list.append(f"{col}: '{val_yesterday}' -> '{val_today}'")
                                continue
                            
                            # Attempt type-aware comparison for floats
                            try:
                                num_today = pd.to_numeric(val_today)
                                num_yesterday = pd.to_numeric(val_yesterday)
                                if not pd.isna(num_today) and not pd.isna(num_yesterday):
                                    if round(num_today, 5) != round(num_yesterday, 5):
                                        changes_detected_list.append(f"{col}: {val_yesterday} -> {val_today}")
                                    continue # Compared as numbers
                            except (ValueError, TypeError):
                                pass # Not both numbers, compare as strings below
                                
                            if str(val_today) != str(val_yesterday):
                                changes_detected_list.append(f"{col}: '{val_yesterday}' -> '{val_today}'")
                                
                        if changes_detected_list:
                            status = "Persistent_Changed"
                            change_details_str = "; ".join(changes_detected_list)
                    
                    item_details['status'] = status
                    item_details['change_details'] = change_details_str
                    
                    # Ensure all report columns are present before appending
                    final_item_for_report = {key: item_details.get(key) for key in report_columns}
                    report_data_list.append(final_item_for_report)

                if report_data_list:
                    report_df = pd.DataFrame(report_data_list, columns=report_columns)
                    os.makedirs(os.path.dirname(comparison_report_path), exist_ok=True)
                    report_df.to_csv(comparison_report_path, index=False, encoding='utf-8')
                    logger.emit("comparison_report_generated", {
                        "path": comparison_report_path,
                        "new_count": len(report_df[report_df['status'] == 'New']),
                        "used_count": len(report_df[report_df['status'] == 'Used']),
                        "persistent_changed_count": len(report_df[report_df['status'] == 'Persistent_Changed']),
                        "persistent_unchanged_count": len(report_df[report_df['status'] == 'Persistent_Unchanged']),
                    })
                else:
                    logger.emit("comparison_info", {"message": "No bonus changes detected or data to compare. Comparison report not generated."})
                    
        except Exception as e:
            # Log the full traceback for detailed debugging if possible, or at least the error type and message
            import traceback
            logger.emit("comparison_module_error", {"error_type": type(e).__name__, "error": str(e), "traceback": traceback.format_exc()})
        # --- End Bonus Comparison Logic ---

        logger.emit("job_complete", job_summary_details)

        if unresponsive_sites_this_run:
            logger.emit("down_sites_summary", {
                "sites": unresponsive_sites_this_run,
                "count": len(unresponsive_sites_this_run)
            })
    finally:
        # Save the cache at the very end, regardless of errors during main processing
        save_run_cache(run_cache_data)
        logger.emit("cache_saved", {"path": "data/run_metrics_cache.json", "total_script_runs": run_cache_data.get("total_script_runs")})

if __name__ == "__main__":
    main()
