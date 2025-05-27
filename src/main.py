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
from .config import ConfigLoader # Added ConfigLoader import

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
                return "ERROR" 

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
                key = (
                    str(row.url), str(row.id), str(row.name),
                    str(row.count), str(row.amount), str(row.register_date_time)
                )
                if key not in written:
                    new_rows.append(row)
                    written.add(key)
            
            if not new_rows:
                break

            file_exists_and_not_empty = os.path.exists(csv_file) and os.path.getsize(csv_file) > 0
            with open(csv_file, "a", newline="", encoding="utf-8") as f:
                fieldnames = [field.name for field in Downline.__dataclass_fields__.values()]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if not file_exists_and_not_empty:
                    writer.writeheader()
                writer.writerows([row.__dict__ for row in new_rows])
                self.logger.emit("csv_written", {"file": csv_file, "count": len(new_rows)})

            total_new_rows += len(new_rows)
            page += 1

        self.logger.emit("downline_fetched", {"count": total_new_rows})
        return total_new_rows

    def fetch_bonuses(self, url: str, auth: AuthData, csv_file: str = "bonuses.csv") -> Union[Tuple[int, float, dict[str, bool]], str]:
        C_KEYWORDS = ["commission", "affiliate"]
        D_KEYWORDS = ["downline first deposit"]
        S_KEYWORDS = ["share bonus", "referrer"]
        bonus_type_flags = {"C": False, "D": False, "S": False, "O": False}

        payload = {
            "module": "/users/syncData", "merchantId": auth.merchant_id, "domainId": "0",
            "accessId": auth.access_id, "accessToken": auth.token, "walletIsAdmin": ""
        }
        self.logger.emit("api_request", {"url": auth.api_url, "module": payload.get("module")})
        try:
            response = requests.post(auth.api_url, data=payload, timeout=self.request_timeout)
            response.raise_for_status()
            res = response.json()
            response_details = {"url": auth.api_url, "module": payload.get("module"), "status": res.get("status")}
            if res.get("status") != "SUCCESS":
                if res.get("message"): response_details["error_message"] = res.get("message")
                if isinstance(res.get("data"), dict) and res.get("data", {}).get("description"):
                    response_details["error_description"] = res.get("data").get("description")
                elif isinstance(res.get("data"), str): response_details["error_data_string"] = res.get("data")
            self.logger.emit("api_response", response_details)
        except requests.exceptions.Timeout as e:
            self.logger.emit("website_unresponsive", {"url": auth.api_url, "error": f"Timeout: {str(e)}"})
            return "UNRESPONSIVE"
        except requests.exceptions.ConnectionError as e:
            self.logger.emit("website_unresponsive", {"url": auth.api_url, "error": f"ConnectionError: {str(e)}"})
            return "UNRESPONSIVE"
        except Exception as e:
            self.logger.emit("exception", {"error": f"Bonus fetch failed for {auth.api_url}: {str(e)}"})
            return "ERROR"

        if res.get("status") != "SUCCESS":
            self.logger.emit("bonus_api_error", {"url": auth.api_url, "status": res.get("status"), "error_message": res.get("message", "N/A"), "error_data": res.get("data", "N/A")})
            return "ERROR"

        bonuses_data_raw = res.get("data", {}).get("bonus", []) + res.get("data", {}).get("promotions", [])
        if not bonuses_data_raw:
            self.logger.emit("bonus_fetched", {"count": 0, "total_amount": 0.0})
            return 0, 0.0, bonus_type_flags
        
        rows_to_write_obj: List[Bonus] = []
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)
        file_exists_and_not_empty = os.path.exists(csv_file) and os.path.getsize(csv_file) > 0
        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            fieldnames = [field.name for field in Bonus.__dataclass_fields__.values()]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists_and_not_empty:
                writer.writeheader()

            for b_data in bonuses_data_raw:
                try:
                    min_w = float(b_data.get("minWithdraw", 0) or 0)
                    bonus_f = float(b_data.get("bonusFixed", 0) or 0)
                    ratio = min_w / bonus_f if bonus_f != 0 else None
                except (ValueError, TypeError):
                    self.logger.emit("exception", {"error": f"Type error processing bonus data for {url}: {b_data}"})
                    continue
                
                bonus_instance = Bonus(
                    url=url, merchant_name=auth.merchant_name, id=b_data.get("id"), name=b_data.get("name"),
                    transaction_type=b_data.get("transactionType"), bonus_fixed=float(b_data.get("bonusFixed", 0) or 0),
                    amount=float(b_data.get("amount", 0) or 0), min_withdraw=float(b_data.get("minWithdraw", 0) or 0),
                    max_withdraw=float(b_data.get("maxWithdraw", 0) or 0), withdraw_to_bonus_ratio=ratio,
                    rollover=float(b_data.get("rollover", 0) or 0), balance=str(b_data.get("balance", "")),
                    claim_config=str(b_data.get("claimConfig", "")), claim_condition=str(b_data.get("claimCondition", "")),
                    bonus=str(b_data.get("bonus", "")), bonus_random=str(b_data.get("bonusRandom", "")),
                    reset=str(b_data.get("reset", "")), min_topup=float(b_data.get("minTopup", 0) or 0),
                    max_topup=float(b_data.get("maxTopup", 0) or 0), refer_link=str(b_data.get("referLink", ""))
                )
                rows_to_write_obj.append(bonus_instance)

                name_lower = bonus_instance.name.lower() if bonus_instance.name else ""
                claim_config_lower = bonus_instance.claim_config.lower() if bonus_instance.claim_config else ""
                matched_c_d_s_for_this_bonus = False
                if any(keyword in name_lower or keyword in claim_config_lower for keyword in C_KEYWORDS):
                    bonus_type_flags["C"] = True; matched_c_d_s_for_this_bonus = True
                if any(keyword in name_lower or keyword in claim_config_lower for keyword in D_KEYWORDS):
                    bonus_type_flags["D"] = True; matched_c_d_s_for_this_bonus = True
                if any(keyword in name_lower or keyword in claim_config_lower for keyword in S_KEYWORDS):
                    bonus_type_flags["S"] = True; matched_c_d_s_for_this_bonus = True
                if not matched_c_d_s_for_this_bonus:
                    bonus_type_flags["O"] = True
            if rows_to_write_obj:
                writer.writerows([b.__dict__ for b in rows_to_write_obj])
                self.logger.emit("csv_written", {"file": csv_file, "count": len(rows_to_write_obj)})

        current_fetch_total_amount = sum(b.amount for b in rows_to_write_obj)
        self.logger.emit("bonus_fetched", {"count": len(rows_to_write_obj), "total_amount": current_fetch_total_amount})
        return len(rows_to_write_obj), current_fetch_total_amount, bonus_type_flags

def load_urls(url_file: str) -> List[str]:
    if not os.path.exists(url_file):
        print(f"URL file not found: {url_file}")
        return []
    with open(url_file, "r") as f:
        return [url.strip() for url in f if url.strip()]

def main():
    config_loader = ConfigLoader(path="config.ini")
    config = config_loader.load()
    run_cache_data = load_run_cache()
    run_cache_data["total_script_runs"] += 1
    REQUEST_TIMEOUT = 30
    unresponsive_sites_this_run = []
    logger = Logger(log_file=config.logging.log_file, log_level=config.logging.log_level, console=config.logging.console, detail=config.logging.detail)
    auth_service = AuthService(logger)
    scraper = Scraper(logger, REQUEST_TIMEOUT)
    urls = load_urls(config.settings.url_file)

    def format_stat_display(current_val, prev_val):
        if current_val == 0 and prev_val == 0: return ""
        diff = current_val - prev_val
        return f"{current_val}/{prev_val}({diff:+})"

    if not urls:
        logger.emit("job_start", {"url_count": 0, "status": "No URLs to process"})
        print("No URLs to process. Exiting.")
        return
        
    total_urls = len(urls)
    history = logger.load_metrics(config.logging.log_file)
    metrics = {
        "bonuses_old": history.get("bonuses", 0), "downlines_old": history.get("downlines", 0),
        "errors_old": history.get("errors", 0), "bonuses_new": 0, "downlines_new": 0, "errors_new": 0,
        "bonus_amount_new": 0.0
    }
    metrics["bonuses_total_old"] = history.get("bonuses", 0)
    metrics["downlines_total_old"] = history.get("downlines", 0)
    metrics["errors_total_old"] = history.get("errors", 0)
    metrics["bonus_amount_total_old"] = history.get("total_bonus_amount", 0.0)
    metrics["bonuses_total_new"] = metrics["bonuses_total_old"]
    metrics["downlines_total_new"] = metrics["downlines_total_old"]
    metrics["errors_total_new"] = metrics["errors_total_old"]
    metrics["bonus_amount_total_new"] = metrics["bonus_amount_total_old"]

    try:
        logger.emit("job_start", {"url_count": total_urls, "total_script_runs": run_cache_data.get("total_script_runs", "N/A")})
        start_time = time.time()

        for idx, url in enumerate(urls, 1):
            if idx > 1:
                sys.stdout.write('\x1b[3A')
                sys.stdout.write('\x1b[J')

            site_start_time = time.time()
            cleaned_url = auth_service.clean_url(url)
            site_key = cleaned_url
            cr_bonuses_site, cr_downlines_site, cr_errors_site = 0, 0, 0
            site_cache_entry = run_cache_data["sites"].get(site_key, {})
            pr_bonuses = site_cache_entry.get("last_run_new_bonuses", 0)
            prt_bonuses = site_cache_entry.get("cumulative_total_bonuses", 0)
            pr_downlines = site_cache_entry.get("last_run_new_downlines", 0)
            prt_downlines = site_cache_entry.get("cumulative_total_downlines", 0)
            pr_errors = site_cache_entry.get("last_run_new_errors", 0)
            prt_errors = site_cache_entry.get("cumulative_total_errors", 0)

            try:
                auth_data = auth_service.login(cleaned_url, config.credentials.mobile, config.credentials.password)
                current_site_bonus_flags = {"C": False, "D": False, "S": False, "O": False}
                if not auth_data:
                    metrics["errors_new"] += 1; metrics["errors_total_new"] += 1; cr_errors_site = 1
                    logger.emit("exception", {"error": f"Authentication failed for {cleaned_url}"})
                
                if auth_data:
                    if config.settings.downline_enabled:
                        result_dl = scraper.fetch_downlines(cleaned_url, auth_data)
                        if isinstance(result_dl, str):
                            metrics["errors_new"] += 1; metrics["errors_total_new"] += 1; cr_errors_site = 1
                            if result_dl == "UNRESPONSIVE": unresponsive_sites_this_run.append(cleaned_url)
                        else:
                            cr_downlines_site = result_dl
                            metrics["downlines_new"] += result_dl; metrics["downlines_total_new"] += result_dl
                    else:
                        bonus_csv_path = datetime.now().strftime("data/%m-%d bonuses.csv")
                        result_bonuses = scraper.fetch_bonuses(cleaned_url, auth_data, csv_file=bonus_csv_path)
                        if isinstance(result_bonuses, str):
                            metrics["errors_new"] += 1; metrics["errors_total_new"] += 1; cr_errors_site = 1
                            if result_bonuses == "UNRESPONSIVE": unresponsive_sites_this_run.append(cleaned_url)
                        else:
                            count, current_fetch_total_amount, current_site_bonus_flags = result_bonuses
                            cr_bonuses_site = count
                            metrics["bonuses_new"] += count; metrics["bonus_amount_new"] += current_fetch_total_amount
                            metrics["bonuses_total_new"] += count; metrics["bonus_amount_total_new"] += current_fetch_total_amount
            except Exception as e:
                metrics["errors_new"] += 1; metrics["errors_total_new"] += 1; cr_errors_site = 1
                logger.emit("exception", {"error": f"Outer loop exception for {cleaned_url}: {str(e)}"})

            crt_bonuses = prt_bonuses + cr_bonuses_site
            crt_downlines = prt_downlines + cr_downlines_site
            crt_errors = prt_errors + cr_errors_site
            run_cache_data["sites"].setdefault(site_key, {})
            run_cache_data["sites"][site_key].update({
                "last_run_new_bonuses": cr_bonuses_site, "cumulative_total_bonuses": crt_bonuses,
                "last_run_new_downlines": cr_downlines_site, "cumulative_total_downlines": crt_downlines,
                "last_run_new_errors": cr_errors_site, "cumulative_total_errors": crt_errors,
                "bonus_flags": current_site_bonus_flags
            })
            
            site_processing_duration = time.time() - site_start_time
            percent = (idx / total_urls) * 100
            run_count = run_cache_data["total_script_runs"]
            sfs = run_cache_data["sites"][site_key] # Use the newly updated cache entry for display stats
            
            bonus_flags = sfs.get('bonus_flags', {})
            flags_str = f"[C] {'Y' if bonus_flags.get('C') else 'N'} [D] {'Y' if bonus_flags.get('D') else 'N'} [S] {'Y' if bonus_flags.get('S') else 'N'} [O] {'Y' if bonus_flags.get('O') else 'N'}"
            progress_bar_str = progress(idx, vmin=0, vmax=total_urls, length=40, title="")
            
            line1 = f"| {progress_bar_str} | [{percent:.2f}%] {idx}/{total_urls} |"
            line2 = f"| {site_processing_duration:.1f}s | [Run #{run_count}] | {flags_str} | [URL] {cleaned_url} |"
            
            r_b = format_stat_display(sfs['last_run_new_bonuses'], pr_bonuses) # Use pr_bonuses for prev val
            t_b = format_stat_display(sfs['cumulative_total_bonuses'], prt_bonuses) # Use prt_bonuses for prev val
            stats_b_str = f"[B]|[R]:{r_b if r_b else '-'} [T]:{t_b if t_b else '-'}"
            stats_d_str = ""
            if config.settings.downline_enabled:
                r_d = format_stat_display(sfs['last_run_new_downlines'], pr_downlines)
                t_d = format_stat_display(sfs['cumulative_total_downlines'], prt_downlines)
                stats_d_str = f"| [D]|[R]:{r_d if r_d else '-'} [T]:{t_d if t_d else '-'}"
            r_e = format_stat_display(sfs['last_run_new_errors'], pr_errors)
            t_e = format_stat_display(sfs['cumulative_total_errors'], prt_errors)
            stats_e_str = f"| [E]|[R]:{r_e if r_e else '-'} [T]:{t_e if t_e else '-'}"
            line3 = f"| {stats_b_str} {stats_d_str} {stats_e_str} |"

            sys.stdout.write(f"{line1}\n{line2}\n{line3}\n"); sys.stdout.flush()
        
        # This block is now correctly indented
        elapsed = time.time() - start_time
        sys.stdout.write("\n")
        
        avg_bonus_amount_this_run = (metrics["bonus_amount_new"] / metrics["bonuses_new"]) if metrics["bonuses_new"] > 0 else 0.0
        job_summary_details = {
            "duration": elapsed, "total_urls_processed": total_urls,
            "bonuses_fetched_this_run": metrics["bonuses_new"], "bonus_amount_this_run": metrics["bonus_amount_new"],
            "avg_bonus_amount_this_run": avg_bonus_amount_this_run, "downlines_fetched_this_run": metrics["downlines_new"],
            "errors_this_run": metrics["errors_new"], "unresponsive_sites_count_this_run": len(unresponsive_sites_this_run)
        }
        
        today_date_str = datetime.now().strftime('%m-%d')
        daily_bonus_csv_path = f"data/{today_date_str} bonuses.csv"
        historical_excel_path = "data/historical_bonuses.xlsx"
        if not config.settings.downline_enabled: 
            if os.path.exists(daily_bonus_csv_path) and os.path.getsize(daily_bonus_csv_path) > 0:
                try:
                    bonus_df_for_excel = pd.read_csv(daily_bonus_csv_path) # Renamed to avoid conflict
                    if not bonus_df_for_excel.empty:
                        os.makedirs(os.path.dirname(historical_excel_path), exist_ok=True)
                        mode = 'a' if os.path.exists(historical_excel_path) else 'w'
                        with pd.ExcelWriter(historical_excel_path, engine='openpyxl', mode=mode, if_sheet_exists='replace') as writer:
                            bonus_df_for_excel.to_excel(writer, sheet_name=today_date_str, index=False)
                        logger.emit("historical_data_written", {"file": historical_excel_path, "sheet": today_date_str, "rows": len(bonus_df_for_excel)})
                    else:
                        logger.emit("historical_data_skipped", {"reason": "Daily bonus CSV is empty", "file": daily_bonus_csv_path})
                except Exception as e:
                    logger.emit("historical_data_error", {"file": daily_bonus_csv_path, "excel_file": historical_excel_path, "error": str(e)})
            else:
                logger.emit("historical_data_skipped", {"reason": "Daily bonus CSV not found or empty", "file": daily_bonus_csv_path})

        try:
            today_dt = datetime.now()
            yesterday_dt = today_dt - timedelta(days=1)
            today_sheet_name_comp = today_dt.strftime('%m-%d') # Renamed
            yesterday_sheet_name_comp = yesterday_dt.strftime('%m-%d') # Renamed
            comparison_report_path = f"data/comparison_report_{today_sheet_name_comp}.csv"
            
            today_df_comp = None # Renamed
            # Use daily_bonus_csv_path if it was for today and bonus_df_for_excel is available
            if 'bonus_df_for_excel' in locals() and isinstance(bonus_df_for_excel, pd.DataFrame) and not bonus_df_for_excel.empty and \
               daily_bonus_csv_path == f"data/{today_sheet_name_comp} bonuses.csv":
                 today_df_comp = bonus_df_for_excel
            
            if today_df_comp is None: 
                current_day_bonus_csv = f"data/{today_sheet_name_comp} bonuses.csv"
                if os.path.exists(current_day_bonus_csv) and os.path.getsize(current_day_bonus_csv) > 0:
                    today_df_comp = pd.read_csv(current_day_bonus_csv)
                else:
                    today_df_comp = pd.DataFrame() 

            yesterday_df_comp = pd.DataFrame() # Renamed
            if os.path.exists(historical_excel_path): 
                try:
                    yesterday_df_comp = pd.read_excel(historical_excel_path, sheet_name=yesterday_sheet_name_comp)
                except Exception as e: # Simplified error handling for brevity in this section
                    logger.emit("comparison_info", {"message": f"Could not read yesterday's sheet ({yesterday_sheet_name_comp}) for comparison: {str(e)}"})
            
            expected_columns = [
                'url', 'merchant_name', 'id', 'name', 'transaction_type', 'bonus_fixed', 'amount',
                'min_withdraw', 'max_withdraw', 'withdraw_to_bonus_ratio', 'rollover', 'balance',
                'claim_config', 'claim_condition', 'bonus', 'bonus_random', 'reset',
                'min_topup', 'max_topup', 'refer_link'
            ]

            if today_df_comp.empty: today_df_comp = pd.DataFrame(columns=expected_columns)
            else:
                for col in expected_columns:
                    if col not in today_df_comp.columns: today_df_comp[col] = pd.NA
            if yesterday_df_comp.empty: yesterday_df_comp = pd.DataFrame(columns=expected_columns)
            else:
                for col in expected_columns:
                    if col not in yesterday_df_comp.columns: yesterday_df_comp[col] = pd.NA
            
            key_cols = ['merchant_name', 'name', 'amount']
            for df_ref in [today_df_comp, yesterday_df_comp]:
                if not df_ref.empty:
                    for col in key_cols:
                        if col == 'amount': df_ref[col] = pd.to_numeric(df_ref[col], errors='coerce').round(5)
                        else: df_ref[col] = df_ref[col].astype(str).fillna('') 
                    df_ref.dropna(subset=[k for k in key_cols if k in df_ref.columns], how='any', inplace=True)

            if not today_df_comp.empty: today_df_comp['_comparison_key'] = today_df_comp.apply(lambda row: f"{row['merchant_name']}_{row['name']}_{row['amount']}", axis=1)
            else: today_df_comp['_comparison_key'] = pd.Series(dtype='object')
            if not yesterday_df_comp.empty: yesterday_df_comp['_comparison_key'] = yesterday_df_comp.apply(lambda row: f"{row['merchant_name']}_{row['name']}_{row['amount']}", axis=1)
            else: yesterday_df_comp['_comparison_key'] = pd.Series(dtype='object')
            
            report_data_list = []
            if not today_df_comp.empty or not yesterday_df_comp.empty: # Proceed if at least one DF has data
                merged_df = pd.merge(today_df_comp, yesterday_df_comp, on='_comparison_key', how='outer', suffixes=('_today', '_yesterday'), indicator=True)
                report_columns = ['status', 'change_details'] + expected_columns
                for _, row in merged_df.iterrows(): # Renamed idx to _ to avoid clash with outer loop
                    item_details, status, change_details_str = {}, "", ""
                    is_new, is_used, is_persistent = row['_merge'] == 'left_only', row['_merge'] == 'right_only', row['_merge'] == 'both'
                    
                    if is_new: status = "New"; suffix = '_today'
                    elif is_used: status = "Used"; suffix = '_yesterday'
                    else: status = "Persistent_Unchanged"; suffix = '_today' # Default for persistent
                    
                    for col in expected_columns: item_details[col] = row.get(col + suffix, pd.NA)

                    if is_persistent:
                        changes = []
                        for col in expected_columns:
                            val_t, val_y = row.get(col + '_today'), row.get(col + '_yesterday')
                            if pd.isna(val_t) and pd.isna(val_y): continue
                            if pd.isna(val_t) or pd.isna(val_y) or str(val_t) != str(val_y):
                                if isinstance(val_t, float) or isinstance(val_y, float): # Numerical comparison
                                    if round(pd.to_numeric(val_t, errors='coerce'),5) != round(pd.to_numeric(val_y, errors='coerce'),5):
                                        changes.append(f"{col}: '{val_y}' -> '{val_t}'")
                                else: # String comparison
                                    changes.append(f"{col}: '{val_y}' -> '{val_t}'")
                        if changes: status = "Persistent_Changed"; change_details_str = "; ".join(changes)
                    
                    item_details['status'], item_details['change_details'] = status, change_details_str
                    report_data_list.append({key: item_details.get(key) for key in report_columns})

                if report_data_list:
                    report_df = pd.DataFrame(report_data_list, columns=report_columns)
                    os.makedirs(os.path.dirname(comparison_report_path), exist_ok=True)
                    report_df.to_csv(comparison_report_path, index=False, encoding='utf-8')
                    logger.emit("comparison_report_generated", {"path": comparison_report_path, "rows": len(report_df)})
                else: logger.emit("comparison_info", {"message": "No changes for comparison report."})
            else: logger.emit("comparison_info", {"message": "Both today's and yesterday's bonus data are empty. No comparison report generated."})
        except Exception as e:
            import traceback
            logger.emit("comparison_module_error", {"error_type": type(e).__name__, "error": str(e), "traceback": traceback.format_exc()})

        logger.emit("job_complete", job_summary_details)
        if unresponsive_sites_this_run:
            logger.emit("down_sites_summary", {"sites": unresponsive_sites_this_run, "count": len(unresponsive_sites_this_run)})
    finally:
        save_run_cache(run_cache_data)
        logger.emit("cache_saved", {"path": "data/run_metrics_cache.json", "total_script_runs": run_cache_data.get("total_script_runs")})

if __name__ == "__main__":
    main()
