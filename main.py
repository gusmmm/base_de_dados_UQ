import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.google_sheets.manager import GoogleSheetsManager, logger


def main():
    """
    Main function to trigger the Google Sheet download.
    """
    try:
        manager = GoogleSheetsManager()
        manager.download_sheet_as_csv()
    except Exception as e:
        logger.error(f"Failed to download Google Sheet from main script: {e}")


if __name__ == "__main__":
    main()