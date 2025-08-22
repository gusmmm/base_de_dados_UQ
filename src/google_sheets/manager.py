"""
Google Sheets API Integration Module

This module provides a class-based interface for interacting with Google Sheets
using the Google API Python Client. It handles OAuth 2.0 authentication,
data retrieval, and CSV export.
"""
import logging
from pathlib import Path

import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("GoogleSheetsManager")

# If modifying these scopes, delete the token.json file.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = "154RKTCB7q5C6QUsVSuRo2_8AXGyrX_rKi0ZQ5xEflD8"
SHEET_NAME = "Doentes"


class GoogleSheetsManager:
    """
    Manages the connection to Google Sheets and data downloading.
    """

    def __init__(self):
        """Initializes the GoogleSheetsManager."""
        self.creds = None
        # Navigate from src/google_sheets -> src -> project root
        self.project_root = Path(__file__).parent.parent.parent
        self.credentials_path = self.project_root / ".secrets/credentials_gsheet.json"
        self.token_path = self.project_root / ".secrets/token.json"

    def authenticate(self):
        """
        Handles user authentication for Google Sheets API.
        """
        logger.info("Authenticating with Google Sheets API...")
        if self.token_path.exists():
            self.creds = Credentials.from_authorized_user_file(
                str(self.token_path), SCOPES
            )

        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                logger.info("Credentials expired. Refreshing token...")
                self.creds.refresh(Request())
            else:
                logger.info("No valid credentials found. Starting authentication flow...")
                if not self.credentials_path.exists():
                    logger.error(f"Credentials file not found at: {self.credentials_path}")
                    raise FileNotFoundError(
                        f"Please place your 'credentials_gsheet.json' file in the '{self.project_root / '.secrets'}' directory."
                    )
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(self.credentials_path), SCOPES
                )
                self.creds = flow.run_local_server(port=0)

            with open(self.token_path, "w") as token:
                token.write(self.creds.to_json())
            logger.info(f"Credentials saved to {self.token_path}")

        logger.info("Authentication successful.")

    def download_sheet_as_csv(self, output_filename: str = "doentes"):
        """
        Downloads the specified sheet and saves it as a CSV file.

        Args:
            output_filename (str): The name of the output CSV file (without extension).
        """
        if not self.creds:
            self.authenticate()

        try:
            service = build("sheets", "v4", credentials=self.creds)
            sheet = service.spreadsheets()
            result = (
                sheet.values()
                .get(spreadsheetId=SPREADSHEET_ID, range=SHEET_NAME)
                .execute()
            )
            values = result.get("values", [])

            if not values:
                logger.warning("No data found in the sheet.")
                return

            logger.info(f"Successfully retrieved {len(values) - 1} rows of data.")
            df = pd.DataFrame(values[1:], columns=values[0])

            output_dir = self.project_root / "input/google_sheet"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f"{output_filename}.csv"

            df.to_csv(output_path, index=False)
            logger.info(f"Sheet successfully downloaded to {output_path}")

        except HttpError as err:
            logger.error(f"An API error occurred: {err}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            raise
