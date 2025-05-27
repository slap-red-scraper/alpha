import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from .config import GoogleSheetsConfig # Assuming config is in the same directory or adjust path
from .logger import Logger # Assuming logger is available

class GoogleSheetsUploader:
    def __init__(self, gs_config: GoogleSheetsConfig, logger: Logger):
        self.gs_config = gs_config
        self.logger = logger
        self.client = None
        if self.gs_config.enabled:
            try:
                scopes = [
                    "https.www.googleapis.com/auth/spreadsheets",
                    "https.www.googleapis.com/auth/drive.file" # Required to create new spreadsheets if needed, or just to see files
                ]
                creds = Credentials.from_service_account_file(
                    self.gs_config.credentials_file,
                    scopes=scopes
                )
                self.client = gspread.authorize(creds)
                self.logger.emit("google_sheets_auth_success", {"message": "Successfully authenticated with Google Sheets API."})
            except FileNotFoundError:
                self.logger.emit("google_sheets_auth_error", {"error": f"Credentials file not found: {self.gs_config.credentials_file}"})
                self.client = None # Ensure client is None if auth fails
            except Exception as e:
                self.logger.emit("google_sheets_auth_error", {"error": f"Failed to authenticate with Google Sheets API: {str(e)}"})
                self.client = None # Ensure client is None if auth fails

    def _open_spreadsheet(self):
        if not self.client:
            self.logger.emit("google_sheets_client_error", {"message": "Google Sheets client not initialized."})
            return None
        try:
            spreadsheet = self.client.open_by_id(self.gs_config.spreadsheet_id)
            self.logger.emit("google_sheets_spreadsheet_opened", {"id": self.gs_config.spreadsheet_id})
            return spreadsheet
        except gspread.exceptions.SpreadsheetNotFound:
            self.logger.emit("google_sheets_error", {"error": f"Spreadsheet not found with ID: {self.gs_config.spreadsheet_id}"})
            return None
        except Exception as e:
            self.logger.emit("google_sheets_error", {"error": f"Error opening spreadsheet {self.gs_config.spreadsheet_id}: {str(e)}"})
            return None

    def upload_dataframe(self, df: pd.DataFrame, sheet_name: str):
        if not self.gs_config.enabled or not self.client:
            if not self.gs_config.enabled:
                self.logger.emit("google_sheets_upload_skipped", {"reason": "Google Sheets uploading is disabled in config."})
            return

        spreadsheet = self._open_spreadsheet()
        if not spreadsheet:
            return

        try:
            # Check if worksheet exists
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                self.logger.emit("google_sheets_worksheet_found", {"name": sheet_name})
            except gspread.exceptions.WorksheetNotFound:
                self.logger.emit("google_sheets_worksheet_creating", {"name": sheet_name})
                worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1", cols="1") # Create with minimal size

            worksheet.clear() # Clear existing content
            
            # Convert NaN/NA to empty strings for gspread compatibility if necessary
            df_upload = df.fillna('').astype(str)
            
            header = [str(col) for col in df_upload.columns.tolist()]
            data_to_upload = [header] + df_upload.values.tolist()
            
            worksheet.update(data_to_upload, raw=False) # raw=False to parse values
            self.logger.emit("google_sheets_upload_success", {"sheet_name": sheet_name, "rows": len(df_upload)})
        except Exception as e:
            self.logger.emit("google_sheets_upload_error", {"sheet_name": sheet_name, "error": str(e)})

    def upload_csv_file(self, csv_file_path: str, sheet_name: str):
        if not self.gs_config.enabled or not self.client:
            if not self.gs_config.enabled:
                self.logger.emit("google_sheets_upload_skipped", {"reason": "Google Sheets uploading is disabled in config."})
            return
        
        try:
            df = pd.read_csv(csv_file_path)
            self.upload_dataframe(df, sheet_name)
        except FileNotFoundError:
            self.logger.emit("google_sheets_upload_error", {"sheet_name": sheet_name, "error": f"CSV file not found: {csv_file_path}"})
        except Exception as e:
            self.logger.emit("google_sheets_upload_error", {"sheet_name": sheet_name, "error": f"Error processing CSV {csv_file_path} for upload: {str(e)}"})
