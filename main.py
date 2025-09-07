# -----------------------------
# Entry Point - Fixed to match your original structure
# -----------------------------
def run():
    """Main entry point matching the original function signature"""
    return run_with_comprehensive_error_handling()

def main():
    """Alternative entry point"""
    return run()

if __name__ == "__main__":
    import sys
    success = run()
    if success:
        print("[SUCCESS] Scraping completed successfully!")
    else:
        print("[ERROR] Scraping completed with errors!")
    sys.exit(import httpx
import time
import os
import json
import re
import logging
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from selectolax.parser import HTMLParser
from typing import Dict, List, Optional, Tuple, Any
import backoff
import hashlib
from dataclasses import dataclass, asdict

# -----------------------------
# Enhanced Logging Setup
# -----------------------------
def setup_logging():
    """Setup comprehensive logging with both file and console handlers"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(f'logs/foreclosure_scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            logging.StreamHandler()
        ]
    )
    
    # Reduce noise from third-party libraries
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('googleapiclient').setLevel(logging.WARNING)
    
    return logging.getLogger(__name__)

logger = setup_logging()

# -----------------------------
# Data Classes for Type Safety
# -----------------------------
@dataclass
class CountyConfig:
    county_id: str
    county_name: str

@dataclass
class PropertyRecord:
    property_id: str
    address: str
    defendant: str
    sale_date: str
    approx_judgment: str
    sale_type: str
    county: str
    record_hash: str = ""
    
    def __post_init__(self):
        """Generate a unique hash for the record to detect duplicates"""
        if not self.record_hash:
            content = f"{self.property_id}|{self.address}|{self.defendant}|{self.sale_date}|{self.county}"
            self.record_hash = hashlib.md5(content.encode()).hexdigest()

# -----------------------------
# Configuration
# -----------------------------
TARGET_COUNTIES = [
    CountyConfig("52", "Cape May County, NJ"),
    CountyConfig("25", "Atlantic County, NJ"),
    CountyConfig("1", "Camden County, NJ"),
    CountyConfig("3", "Burlington County, NJ"),
    CountyConfig("6", "Cumberland County, NJ"),
    CountyConfig("19", "Gloucester County, NJ"),
    CountyConfig("20", "Salem County, NJ"),
    CountyConfig("15", "Union County, NJ"),
    CountyConfig("7", "Bergen County, NJ"),
    CountyConfig("2", "Essex County, NJ"),
    CountyConfig("23", "Montgomery County, PA"),
    CountyConfig("24", "New Castle County, DE"),
]

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
POLITE_DELAY_SECONDS = 1.5
BASE_URL = "https://salesweb.civilview.com"
MAX_RETRIES = 3
BACKOFF_MAX_TIME = 300  # 5 minutes
REQUEST_TIMEOUT = 60
STANDARD_COLUMNS = ["Property ID", "Address", "Defendant", "Sale Date", "Approx Judgment", "Sale Type", "County"]

# -----------------------------
# Custom Exceptions
# -----------------------------
class ScrapingError(Exception):
    """Base exception for scraping errors"""
    pass

class DataIntegrityError(Exception):
    """Exception for data integrity issues"""
    pass

class CountyScrapingError(ScrapingError):
    """Exception for county-specific scraping errors"""
    def __init__(self, county_name: str, message: str):
        self.county_name = county_name
        super().__init__(f"Error scraping {county_name}: {message}")

# -----------------------------
# Enhanced Timezone Helpers
# -----------------------------
def get_est_time() -> datetime:
    """Get current EST time with proper timezone handling"""
    try:
        import pytz
        est = pytz.timezone('US/Eastern')
        return datetime.now(est)
    except ImportError:
        # Fallback to simple UTC offset
        return datetime.utcnow() - timedelta(hours=5)

def get_est_date():
    return get_est_time().date()

def parse_sale_date(date_str: str) -> Optional[datetime]:
    """Enhanced date parsing with multiple format support"""
    if not date_str or date_str.strip() == "":
        return None
        
    formats_to_try = [
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%B %d, %Y",
        "%b %d, %Y"
    ]
    
    for fmt in formats_to_try:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except (ValueError, TypeError):
            continue
    
    logger.warning(f"Could not parse date: '{date_str}'")
    return None

# -----------------------------
# Enhanced Credential Management
# -----------------------------
def load_service_account_info() -> Dict[str, Any]:
    """Enhanced credential loading with better error handling"""
    try:
        file_env = os.environ.get("GOOGLE_CREDENTIALS_FILE")
        if file_env:
            if not os.path.exists(file_env):
                raise FileNotFoundError(f"GOOGLE_CREDENTIALS_FILE set but file does not exist: {file_env}")
            with open(file_env, "r", encoding="utf-8") as fh:
                return json.load(fh)

        creds_raw = os.environ.get("GOOGLE_CREDENTIALS")
        if not creds_raw:
            raise ValueError("Environment variable GOOGLE_CREDENTIALS (or GOOGLE_CREDENTIALS_FILE) not set.")

        creds_raw_stripped = creds_raw.strip()
        
        # Try parsing as JSON first
        if creds_raw_stripped.startswith("{"):
            try:
                return json.loads(creds_raw_stripped)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in GOOGLE_CREDENTIALS: {e}")

        # Try as file path
        if os.path.exists(creds_raw):
            with open(creds_raw, "r", encoding="utf-8") as fh:
                return json.load(fh)

        raise ValueError("GOOGLE_CREDENTIALS is invalid JSON and not an existing file path.")
    
    except Exception as e:
        logger.error(f"Failed to load service account credentials: {e}")
        raise

@backoff.on_exception(backoff.expo, Exception, max_tries=MAX_RETRIES, max_time=BACKOFF_MAX_TIME)
def init_sheets_service_from_env():
    """Initialize Google Sheets service with retry logic"""
    try:
        info = load_service_account_info()
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)
        
        # Test the connection
        service.spreadsheets().get(spreadsheetId=os.environ.get("SPREADSHEET_ID")).execute()
        logger.info("Successfully initialized Google Sheets service")
        return service
    
    except Exception as e:
        logger.error(f"Failed to initialize Google Sheets service: {e}")
        raise

# -----------------------------
# Enhanced Sheets Client
# -----------------------------
class SheetsClient:
    def __init__(self, spreadsheet_id: str, service):
        self.spreadsheet_id = spreadsheet_id
        self.service = service
        self.svc = self.service.spreadsheets()
        self._validate_connection()

    def _validate_connection(self):
        """Validate that we can connect to the spreadsheet"""
        try:
            info = self.spreadsheet_info()
            if not info:
                raise ValueError(f"Cannot access spreadsheet {self.spreadsheet_id}")
            logger.info(f"Connected to spreadsheet: {info.get('properties', {}).get('title', 'Unknown')}")
        except Exception as e:
            logger.error(f"Failed to validate spreadsheet connection: {e}")
            raise

    @backoff.on_exception(backoff.expo, HttpError, max_tries=MAX_RETRIES)
    def spreadsheet_info(self) -> Dict[str, Any]:
        """Get spreadsheet information with retry logic"""
        try:
            return self.svc.get(spreadsheetId=self.spreadsheet_id).execute()
        except HttpError as e:
            logger.error(f"Error getting spreadsheet info: {e}")
            raise

    def sheet_exists(self, sheet_name: str) -> bool:
        """Check if sheet exists"""
        try:
            info = self.spreadsheet_info()
            return any(s['properties']['title'] == sheet_name for s in info.get('sheets', []))
        except Exception as e:
            logger.error(f"Error checking if sheet '{sheet_name}' exists: {e}")
            return False

    @backoff.on_exception(backoff.expo, HttpError, max_tries=MAX_RETRIES)
    def create_sheet_if_missing(self, sheet_name: str):
        """Create sheet if it doesn't exist"""
        if self.sheet_exists(sheet_name):
            logger.debug(f"Sheet '{sheet_name}' already exists")
            return
            
        try:
            req = {"addSheet": {"properties": {"title": sheet_name}}}
            self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": [req]}).execute()
            logger.info(f"Created sheet: {sheet_name}")
        except HttpError as e:
            logger.error(f"Error creating sheet '{sheet_name}': {e}")
            raise

    @backoff.on_exception(backoff.expo, HttpError, max_tries=MAX_RETRIES)
    def get_values(self, sheet_name: str, rng: str = "A:Z") -> List[List[str]]:
        """Get values from sheet with retry logic"""
        try:
            res = self.svc.values().get(
                spreadsheetId=self.spreadsheet_id, 
                range=f"'{sheet_name}'!{rng}"
            ).execute()
            values = res.get("values", [])
            logger.debug(f"Retrieved {len(values)} rows from {sheet_name}")
            return values
        except HttpError as e:
            if e.resp.status == 404:
                logger.warning(f"Sheet '{sheet_name}' not found")
                return []
            logger.error(f"Error getting values from {sheet_name}: {e}")
            raise

    @backoff.on_exception(backoff.expo, HttpError, max_tries=MAX_RETRIES)
    def clear(self, sheet_name: str, rng: str = "A:Z"):
        """Clear sheet range with retry logic"""
        try:
            self.svc.values().clear(
                spreadsheetId=self.spreadsheet_id, 
                range=f"'{sheet_name}'!{rng}"
            ).execute()
            logger.debug(f"Cleared range {rng} in {sheet_name}")
        except HttpError as e:
            logger.error(f"Error clearing {sheet_name}: {e}")
            raise

    @backoff.on_exception(backoff.expo, HttpError, max_tries=MAX_RETRIES)
    def write_values(self, sheet_name: str, values: List[List[Any]], start_cell: str = "A1"):
        """Write values to sheet with retry logic and validation"""
        if not values:
            logger.warning(f"No values to write to {sheet_name}")
            return
            
        try:
            # Validate data integrity
            max_cols = max(len(row) for row in values) if values else 0
            normalized_values = []
            for row in values:
                # Pad rows to same length
                normalized_row = row + [""] * (max_cols - len(row))
                normalized_values.append(normalized_row)
            
            self.svc.values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{sheet_name}'!{start_cell}",
                valueInputOption="USER_ENTERED",
                body={"values": normalized_values}
            ).execute()
            
            logger.info(f"Wrote {len(normalized_values)} rows to {sheet_name}")
            
        except HttpError as e:
            logger.error(f"Error writing to {sheet_name}: {e}")
            raise

    def _get_sheet_id(self, sheet_name: str) -> Optional[int]:
        """Get sheet ID by name"""
        try:
            info = self.spreadsheet_info()
            for s in info.get('sheets', []):
                if s['properties']['title'] == sheet_name:
                    return s['properties']['sheetId']
            return None
        except Exception as e:
            logger.error(f"Error getting sheet ID for {sheet_name}: {e}")
            return None

    @backoff.on_exception(backoff.expo, HttpError, max_tries=MAX_RETRIES)
    def highlight_new_rows(self, sheet_name: str, new_row_indices: List[int]):
        """Highlight new rows with retry logic"""
        if not new_row_indices:
            return
            
        sheet_id = self._get_sheet_id(sheet_name)
        if sheet_id is None:
            logger.warning(f"Cannot highlight rows: sheet ID not found for {sheet_name}")
            return
            
        try:
            requests = []
            for row_idx in new_row_indices:
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": row_idx,
                            "endRowIndex": row_idx + 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": len(STANDARD_COLUMNS)
                        },
                        "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.85,"green": 0.92,"blue": 0.83}}},
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                })
            
            if requests:
                self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": requests}).execute()
                logger.info(f"Highlighted {len(new_row_indices)} new rows in {sheet_name}")
                
        except HttpError as e:
            logger.error(f"Error highlighting rows in {sheet_name}: {e}")
            # Don't raise - highlighting is not critical

    @backoff.on_exception(backoff.expo, HttpError, max_tries=MAX_RETRIES)
    def format_sheet(self, sheet_name: str, num_columns: int):
        """Apply formatting to make the sheet more readable"""
        sheet_id = self._get_sheet_id(sheet_name)
        if sheet_id is None:
            logger.warning(f"Cannot format sheet: sheet ID not found for {sheet_name}")
            return
            
        try:
            requests = [
                # Format header row
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "endRowIndex": 2,
                            "startColumnIndex": 0,
                            "endColumnIndex": num_columns
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {"red": 0.2, "green": 0.4, "blue": 0.6},
                                "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}}
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat)"
                    }
                },
                # Format snapshot header
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": num_columns
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
                                "textFormat": {"bold": True, "italic": True}
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat)"
                    }
                },
                # Set column widths
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": 0,
                            "endIndex": num_columns
                        },
                        "properties": {"pixelSize": 150},
                        "fields": "pixelSize"
                    }
                },
                # Freeze header row
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {"frozenRowCount": 2}
                        },
                        "fields": "gridProperties.frozenRowCount"
                    }
                }
            ]
            
            self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": requests}).execute()
            logger.debug(f"Applied formatting to {sheet_name}")
            
        except HttpError as e:
            logger.error(f"Error formatting sheet {sheet_name}: {e}")
            # Don't raise - formatting is not critical

    def prepend_snapshot(self, sheet_name: str, header_row: List[str], new_rows: List[List[str]], new_row_indices: Optional[List[int]] = None):
        """Prepend new data with snapshot header"""
        if not new_rows:
            logger.info(f"No new rows to prepend to {sheet_name}")
            return
            
        try:
            est_now = get_est_time()
            start_date = get_est_date()
            end_date = start_date + timedelta(days=30)
            snapshot_header = [[f"Snapshot for {est_now.strftime('%A - %Y-%m-%d %H:%M EST')} - Showing sales from {start_date.strftime('%m/%d/%Y')} to {end_date.strftime('%m/%d/%Y')}"]]
            
            payload = snapshot_header + [header_row] + new_rows
            existing = self.get_values(sheet_name, "A:Z")
            
            # Data integrity check
            if existing and len(existing) > 2:
                existing_without_headers = existing[2:]  # Skip snapshot header and column headers
                logger.info(f"Prepending {len(new_rows)} new rows to existing {len(existing_without_headers)} rows in {sheet_name}")
            
            self.clear(sheet_name, "A:Z")
            self.write_values(sheet_name, payload + existing)
            
            # Format the sheet
            self.format_sheet(sheet_name, len(header_row))
            
            # Highlight new rows
            if new_row_indices:
                adjusted_indices = [idx + len(snapshot_header) + 1 for idx in new_row_indices]
                self.highlight_new_rows(sheet_name, adjusted_indices)
                
            logger.info(f"Successfully prepended snapshot to {sheet_name}")
            
        except Exception as e:
            logger.error(f"Error prepending snapshot to {sheet_name}: {e}")
            raise

    def overwrite_with_snapshot(self, sheet_name: str, header_row: List[str], all_rows: List[List[str]]):
        """Overwrite sheet with complete snapshot"""
        try:
            est_now = get_est_time()
            start_date = get_est_date()
            end_date = start_date + timedelta(days=30)
            snapshot_header = [[f"Snapshot for {est_now.strftime('%A - %Y-%m-%d %H:%M EST')} - Showing sales from {start_date.strftime('%m/%d/%Y')} to {end_date.strftime('%m/%d/%Y')}"]]
            
            self.clear(sheet_name, "A:Z")
            self.write_values(sheet_name, snapshot_header + [header_row] + all_rows)
            
            # Format the sheet
            self.format_sheet(sheet_name, len(header_row))
            
            logger.info(f"Successfully overwrote {sheet_name} with {len(all_rows)} rows")
            
        except Exception as e:
            logger.error(f"Error overwriting {sheet_name}: {e}")
            raise

    def write_summary(self, all_data_rows: List[PropertyRecord], new_data_rows: List[PropertyRecord]):
        """Write enhanced summary with error tracking"""
        sheet_name = "Summary"
        try:
            self.create_sheet_if_missing(sheet_name)
            self.clear(sheet_name, "A:Z")

            # Build summary statistics
            total_properties = len(all_data_rows)
            total_new = len(new_data_rows)

            # Count by county
            county_totals = {}
            county_new = {}
            for row in all_data_rows:
                county_totals[row.county] = county_totals.get(row.county, 0) + 1
            for row in new_data_rows:
                county_new[row.county] = county_new.get(row.county, 0) + 1

            # Build summary data
            est_now = get_est_time()
            start_date = get_est_date()
            end_date = start_date + timedelta(days=30)
            
            summary_values = [
                ["Foreclosure Sales Summary Dashboard"],
                [f"Snapshot for {est_now.strftime('%A - %Y-%m-%d %H:%M EST')}"],
                [f"Showing sales from {start_date.strftime('%m/%d/%Y')} to {end_date.strftime('%m/%d/%Y')}"],
                [""],
                ["Overall Totals"],
                ["Total Properties", total_properties],
                ["New Properties (This Run)", total_new],
                [""],
                ["Breakdown by County"],
                ["County", "Total Properties", "New This Run"],
            ]

            # Add county breakdown
            for county_config in TARGET_COUNTIES:
                county_name = county_config.county_name
                total = county_totals.get(county_name, 0)
                new = county_new.get(county_name, 0)
                summary_values.append([county_name, total, new])

            # Add data quality metrics
            summary_values.extend([
                [""],
                ["Data Quality Metrics"],
                ["Records with Property ID", sum(1 for r in all_data_rows if r.property_id)],
                ["Records with Sale Date", sum(1 for r in all_data_rows if r.sale_date)],
                ["Records with Judgment Amount", sum(1 for r in all_data_rows if r.approx_judgment and r.approx_judgment != "N/A")],
                ["Unique Record Hashes", len(set(r.record_hash for r in all_data_rows))],
            ])

            # Write to sheet
            self.write_values(sheet_name, summary_values)
            
            # Format summary sheet
            self._format_summary_sheet(sheet_name)
            
            logger.info(f"Successfully wrote summary: {total_properties} total, {total_new} new")
            
        except Exception as e:
            logger.error(f"Error writing summary: {e}")
            raise

    def _format_summary_sheet(self, sheet_name: str):
        """Format the summary sheet"""
        sheet_id = self._get_sheet_id(sheet_name)
        if not sheet_id:
            return
            
        try:
            requests = [
                # Format title
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": 3
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "textFormat": {"bold": True, "fontSize": 16}
                            }
                        },
                        "fields": "userEnteredFormat.textFormat"
                    }
                },
                # Format subtitle and section headers with bold
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "endRowIndex": 20,
                            "startColumnIndex": 0,
                            "endColumnIndex": 1
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "textFormat": {"bold": True}
                            }
                        },
                        "fields": "userEnteredFormat.textFormat"
                    }
                }
            ]
            
            self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": requests}).execute()
            
        except HttpError as e:
            logger.error(f"Error formatting summary sheet: {e}")

# -----------------------------
# Enhanced Scraping Utilities
# -----------------------------
def norm_text(s: str) -> str:
    """Normalize text content"""
    if not s:
        return ""
    return " ".join(s.split()).strip()

def extract_property_id_from_href(href: str) -> str:
    """Extract property ID from URL with better error handling"""
    try:
        if not href:
            return ""
        q = parse_qs(urlparse(href).query)
        prop_id = q.get("PropertyId", [""])[0]
        logger.debug(f"Extracted property ID: {prop_id}")
        return prop_id
    except Exception as e:
        logger.warning(f"Could not extract property ID from href '{href}': {e}")
        return ""

def extract_approx_judgment(html_content: str) -> str:
    """Enhanced judgment extraction with multiple patterns"""
    if not html_content:
        return "N/A"
        
    try:
        tree = HTMLParser(html_content)
        
        # Enhanced judgment patterns
        judgment_patterns = [
            r'Judgment[^$]*?\$([\d,]+(?:\.\d{2})?)',
            r'\$([\d,]+(?:\.\d{2})?)[^$]*?Judgment',
            r'Judgment Amount[^$]*?\$([\d,]+(?:\.\d{2})?)',
            r'Approximate Judgment[^$]*?\$([\d,]+(?:\.\d{2})?)',
            r'Approx\.?\s*Judgment[^$]*?\$([\d,]+(?:\.\d{2})?)',
            r'Upset Price[^$]*?\$([\d,]+(?:\.\d{2})?)',
            r'Approx\.?\s*Upset[^$]*?\$([\d,]+(?:\.\d{2})?)',
            r'Debt Amount[^$]*?\$([\d,]+(?:\.\d{2})?)',
            r'Principal Balance[^$]*?\$([\d,]+(?:\.\d{2})?)',
        ]
        
        text_content = tree.text()
        
        # Try each pattern
        for pattern in judgment_patterns:
            matches = re.findall(pattern, text_content, re.IGNORECASE)
            if matches:
                # Return the first substantial amount (over $1000)
                for match in matches:
                    amount_str = match.replace(',', '')
                    try:
                        amount = float(amount_str)
                        if amount >= 1000:  # Only return substantial amounts
                            return f"${match}"
                    except ValueError:
                        continue
        
        # Fallback: find any large dollar amounts
        large_amounts = re.findall(r'\$([\d,]{4,}(?:\.\d{2})?)', text_content)
        if large_amounts:
            for amount in large_amounts:
                amount_str = amount.replace(',', '')
                try:
                    if float(amount_str.split('.')[0]) >= 10000:  # Minimum $10k for judgment
                        return f"${amount}"
                except ValueError:
                    continue
        
        return "N/A"
        
    except Exception as e:
        logger.warning(f"Error extracting judgment amount: {e}")
        return "N/A"

# -----------------------------
# Enhanced Foreclosure Scraper
# -----------------------------
class ForeclosureScraper:
    def __init__(self, sheets_client: SheetsClient):
        self.sheets_client = sheets_client
        self.session_stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'retries': 0
        }

    def _create_http_client(self) -> httpx.Client:
        """Create HTTP client with proper configuration"""
        return httpx.Client(
            follow_redirects=True,
            timeout=REQUEST_TIMEOUT,
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        )

    @backoff.on_exception(
        backoff.expo, 
        (httpx.RequestError, httpx.HTTPStatusError), 
        max_tries=MAX_RETRIES,
        max_time=BACKOFF_MAX_TIME,
        on_backoff=lambda details: logger.warning(f"Retrying request (attempt {details['tries']}): {details['exception']}")
    )
    def load_search_page(self, client: httpx.Client, county_id: str) -> HTMLParser:
        """Load search page with retry logic"""
        url = f"{BASE_URL}/Sales/SalesSearch?countyId={county_id}"
        
        try:
            self.session_stats['total_requests'] += 1
            response = client.get(url)
            response.raise_for_status()
            
            if not response.text:
                raise ScrapingError("Empty response from search page")
                
            self.session_stats['successful_requests'] += 1
            logger.debug(f"Successfully loaded search page for county {county_id}")
            return HTMLParser(response.text)
            
        except Exception as e:
            self.session_stats['failed_requests'] += 1
            logger.error(f"Failed to load search page for county {county_id}: {e}")
            raise

    def get_hidden_inputs(self, tree: HTMLParser) -> Dict[str, str]:
        """Extract hidden form inputs with validation"""
        hidden = {}
        required_fields = ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"]
        
        for field in required_fields:
            node = tree.css_first(f"input[name={field}]")
            if node:
                value = node.attributes.get("value", "")
                hidden[field] = value
                if not value:
                    logger.warning(f"Empty value for hidden field: {field}")
            else:
                logger.warning(f"Hidden field not found: {field}")
                hidden[field] = ""
        
        logger.debug(f"Extracted {len([v for v in hidden.values() if v])} non-empty hidden fields")
        return hidden

    @backoff.on_exception(
        backoff.expo,
        (httpx.RequestError, httpx.HTTPStatusError),
        max_tries=MAX_RETRIES,
        max_time=BACKOFF_MAX_TIME,
        on_backoff=lambda details: logger.warning(f"Retrying property details (attempt {details['tries']}): {details['exception']}")
    )
    def get_property_details(self, client: httpx.Client, property_id: str, county_id: str) -> Dict[str, str]:
        """Get additional property details with enhanced error handling"""
        if not property_id:
            return {"Approx Judgment": "N/A", "Sale Type": "Unknown" if county_id == "24" else ""}
            
        url = f"{BASE_URL}/Sales/SaleDetails?PropertyId={property_id}"
        
        try:
            self.session_stats['total_requests'] += 1
            response = client.get(url)
            response.raise_for_status()
            
            if not response.text:
                raise ScrapingError(f"Empty response for property {property_id}")
            
            judgment = extract_approx_judgment(response.text)
            sale_type = ""
            
            # Extract sale type for New Castle County (Delaware)
            if county_id == "24":
                try:
                    tree = HTMLParser(response.text)
                    # Look for sale type information in various formats
                    sale_type_patterns = [
                        r'Sale Type[:\s]*([^\n\r<]+)',
                        r'Type of Sale[:\s]*([^\n\r<]+)',
                        r'Sale Category[:\s]*([^\n\r<]+)'
                    ]
                    
                    text_content = tree.text()
                    for pattern in sale_type_patterns:
                        match = re.search(pattern, text_content, re.IGNORECASE)
                        if match:
                            sale_type = match.group(1).strip()
                            break
                    
                    if not sale_type:
                        # Fallback: look in table structure
                        labels = tree.css(".sale-detail-label, .detail-label, .label")
                        for label in labels:
                            label_text = norm_text(label.text()).lower()
                            if "sale type" in label_text or "type" in label_text:
                                # Try to find the corresponding value
                                next_elem = label.next
                                attempts = 0
                                while next_elem and attempts < 5:
                                    if hasattr(next_elem, 'text') and next_elem.text():
                                        potential_value = norm_text(next_elem.text())
                                        if potential_value and len(potential_value) < 100:
                                            sale_type = potential_value
                                            break
                                    next_elem = next_elem.next
                                    attempts += 1
                                if sale_type:
                                    break
                    
                    if not sale_type:
                        sale_type = "Unknown"
                        
                except Exception as e:
                    logger.warning(f"Could not extract sale type for property {property_id}: {e}")
                    sale_type = "Unknown"
            
            self.session_stats['successful_requests'] += 1
            logger.debug(f"Retrieved details for property {property_id}: judgment={judgment}, sale_type={sale_type}")
            
            return {"Approx Judgment": judgment, "Sale Type": sale_type}
            
        except Exception as e:
            self.session_stats['failed_requests'] += 1
            logger.warning(f"Could not fetch details for property {property_id}: {e}")
            return {"Approx Judgment": "N/A", "Sale Type": "Unknown" if county_id == "24" else ""}

    @backoff.on_exception(
        backoff.expo,
        (httpx.RequestError, httpx.HTTPStatusError),
        max_tries=MAX_RETRIES,
        max_time=BACKOFF_MAX_TIME,
        on_backoff=lambda details: logger.warning(f"Retrying search post (attempt {details['tries']}): {details['exception']}")
    )
    def post_search_and_extract(self, client: httpx.Client, county_id: str, hidden: Dict[str, str], county_name: str) -> Tuple[List[str], List[PropertyRecord]]:
        """Perform search and extract results with comprehensive validation"""
        url = f"{BASE_URL}/Sales/SalesSearch?countyId={county_id}"
        
        payload = {
            "__VIEWSTATE": hidden.get("__VIEWSTATE", ""),
            "__VIEWSTATEGENERATOR": hidden.get("__VIEWSTATEGENERATOR", ""),
            "__EVENTVALIDATION": hidden.get("__EVENTVALIDATION", ""),
            "IsOpen": "true",
            "btnSearch": "Search",
        }
        
        try:
            self.session_stats['total_requests'] += 1
            response = client.post(url, data=payload)
            response.raise_for_status()
            
            if not response.text:
                raise ScrapingError("Empty response from search POST")
            
            tree = HTMLParser(response.text)

            # Enhanced table detection
            tables = tree.css("table")
            data_table = None
            
            # Find the main data table
            for table in tables:
                headers = table.css("thead th, tr:first-child th, tr:first-child td")
                if len(headers) >= 3:  # Minimum expected columns
                    data_table = table
                    break
            
            if not data_table:
                logger.warning(f"No data table found for {county_name}")
                return [], []

            # Extract headers with normalization
            headers = []
            header_elements = data_table.css("thead th, tr:first-child th, tr:first-child td")
            for th in header_elements:
                header_text = norm_text(th.text())
                if header_text:  # Only add non-empty headers
                    headers.append(header_text)
            
            if not headers:
                logger.warning(f"No headers found for {county_name}")
                return [], []

            logger.debug(f"Found headers for {county_name}: {headers}")

            # Extract data rows with validation
            records = []
            data_rows = data_table.css("tbody tr, tr")[1:]  # Skip header row
            
            for row_idx, tr in enumerate(data_rows):
                try:
                    cols = [norm_text(td.text()) for td in tr.css("td")]
                    
                    # Skip empty or invalid rows
                    if not cols or len(cols) < 2:
                        continue
                        
                    # Skip if all columns are empty
                    if all(not col.strip() for col in cols):
                        continue
                    
                    # Pad columns to match header length
                    while len(cols) < len(headers):
                        cols.append("")

                    # Extract property ID from link
                    property_id = ""
                    link = tr.css_first("td a, a")
                    if link:
                        href = link.attributes.get("href", "")
                        property_id = extract_property_id_from_href(href)

                    # Create property record with validation
                    row_dict = dict(zip(headers, cols))
                    
                    # Normalize field names and extract standard fields
                    address = (row_dict.get("Address") or row_dict.get("Property Address") or 
                              row_dict.get("address") or "").strip()
                    
                    defendant = (row_dict.get("Defendant") or row_dict.get("Defendants") or
                               row_dict.get("defendant") or row_dict.get("Owner") or "").strip()
                    
                    sale_date = (row_dict.get("Sale Date") or row_dict.get("Sales Date") or
                               row_dict.get("sale date") or row_dict.get("Date") or "").strip()

                    # Skip records without essential information
                    if not address and not defendant:
                        logger.debug(f"Skipping row {row_idx} - no address or defendant")
                        continue

                    # Get additional property details
                    details = self.get_property_details(client, property_id, county_id)
                    
                    # Create PropertyRecord
                    record = PropertyRecord(
                        property_id=property_id,
                        address=address,
                        defendant=defendant,
                        sale_date=sale_date,
                        approx_judgment=details.get("Approx Judgment", "N/A"),
                        sale_type=details.get("Sale Type", ""),
                        county=county_name
                    )
                    
                    records.append(record)
                    logger.debug(f"Added record: {record.property_id} - {record.address[:50]}...")
                    
                except Exception as e:
                    logger.warning(f"Error processing row {row_idx} in {county_name}: {e}")
                    continue

            self.session_stats['successful_requests'] += 1
            logger.info(f"Extracted {len(records)} valid records from {county_name}")
            
            return headers, records
            
        except Exception as e:
            self.session_stats['failed_requests'] += 1
            logger.error(f"Error in search and extract for {county_name}: {e}")
            raise

    def scrape_county_sales(self, county: CountyConfig) -> List[PropertyRecord]:
        """Scrape sales for a single county with comprehensive error handling"""
        max_attempts = MAX_RETRIES
        last_exception = None
        
        for attempt in range(max_attempts):
            client = None
            try:
                logger.info(f"Scraping {county.county_name} (attempt {attempt + 1}/{max_attempts})")
                
                client = self._create_http_client()
                
                # Load search page
                tree = self.load_search_page(client, county.county_id)
                hidden = self.get_hidden_inputs(tree)
                
                # Validate we have required form data
                if not any(hidden.values()):
                    raise ScrapingError("No hidden form fields found - page structure may have changed")

                # Perform search and extract
                headers, records = self.post_search_and_extract(client, county.county_id, hidden, county.county_name)
                
                # Data integrity checks
                if not records:
                    logger.warning(f"No records found for {county.county_name}")
                else:
                    # Check for duplicate records
                    unique_hashes = set(r.record_hash for r in records)
                    if len(unique_hashes) != len(records):
                        logger.warning(f"Found {len(records) - len(unique_hashes)} duplicate records in {county.county_name}")
                
                logger.info(f"✓ Successfully scraped {county.county_name}: {len(records)} records")
                return records
                
            except Exception as e:
                last_exception = e
                self.session_stats['retries'] += 1
                logger.error(f"✗ Error scraping {county.county_name} (attempt {attempt + 1}): {e}")
                
                if attempt < max_attempts - 1:
                    wait_time = (2 ** attempt) * POLITE_DELAY_SECONDS
                    logger.info(f"Retrying {county.county_name} in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"✗ Failed to scrape {county.county_name} after {max_attempts} attempts")
                    
            finally:
                if client:
                    client.close()
                
                # Always be polite to the server
                time.sleep(POLITE_DELAY_SECONDS)
        
        # If all attempts failed, raise the last exception
        if last_exception:
            raise CountyScrapingError(county.county_name, str(last_exception))
        
        return []

    def validate_scraped_data(self, all_records: List[PropertyRecord]) -> Dict[str, Any]:
        """Validate scraped data for integrity issues"""
        validation_report = {
            "total_records": len(all_records),
            "records_with_property_id": sum(1 for r in all_records if r.property_id),
            "records_with_address": sum(1 for r in all_records if r.address),
            "records_with_defendant": sum(1 for r in all_records if r.defendant),
            "records_with_sale_date": sum(1 for r in all_records if r.sale_date),
            "records_with_judgment": sum(1 for r in all_records if r.approx_judgment and r.approx_judgment != "N/A"),
            "unique_records": len(set(r.record_hash for r in all_records)),
            "counties_represented": len(set(r.county for r in all_records)),
            "date_parse_errors": 0,
            "suspicious_records": []
        }
        
        # Check for date parsing issues
        for record in all_records:
            if record.sale_date:
                parsed_date = parse_sale_date(record.sale_date)
                if not parsed_date:
                    validation_report["date_parse_errors"] += 1
                    validation_report["suspicious_records"].append(f"Unparseable date: {record.sale_date}")
        
        # Check for suspicious patterns
        for record in all_records:
            if not record.address and not record.defendant:
                validation_report["suspicious_records"].append(f"No address or defendant: {record.property_id}")
            
            if record.address and len(record.address) < 10:
                validation_report["suspicious_records"].append(f"Very short address: {record.address}")
        
        return validation_report

# -----------------------------
# Enhanced Orchestration
# -----------------------------
def run_with_comprehensive_error_handling():
    """Main execution function with comprehensive error handling and recovery"""
    
    # Validate environment
    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    if not spreadsheet_id:
        logger.error("✗ SPREADSHEET_ID environment variable is required")
        return False

    start_time = datetime.now()
    logger.info("=" * 80)
    logger.info(f"Starting foreclosure scraping session at {start_time}")
    logger.info("=" * 80)

    try:
        # Initialize Google Sheets service
        logger.info("Initializing Google Sheets service...")
        service = init_sheets_service_from_env()
        sheets_client = SheetsClient(spreadsheet_id, service)
        
        # Initialize scraper
        scraper = ForeclosureScraper(sheets_client)
        
        # Track overall progress
        successful_counties = []
        failed_counties = []
        all_records = []
        
        # Scrape each county with individual error handling
        for county in TARGET_COUNTIES:
            try:
                county_records = scraper.scrape_county_sales(county)
                if county_records:
                    all_records.extend(county_records)
                    successful_counties.append(county.county_name)
                    logger.info(f"✓ {county.county_name}: {len(county_records)} records")
                else:
                    logger.warning(f"⚠ {county.county_name}: No records found")
                    successful_counties.append(county.county_name)  # Still consider successful
                    
            except Exception as e:
                failed_counties.append(county.county_name)
                logger.error(f"✗ {county.county_name}: {e}")
                
                # Continue with other counties even if one fails
                continue

        # Log scraping summary
        logger.info("-" * 60)
        logger.info(f"Scraping Summary:")
        logger.info(f"  Successful counties: {len(successful_counties)}")
        logger.info(f"  Failed counties: {len(failed_counties)}")
        logger.info(f"  Total raw records: {len(all_records)}")
        
        if failed_counties:
            logger.warning(f"Failed counties: {', '.join(failed_counties)}")

        # Validate scraped data
        validation_report = scraper.validate_scraped_data(all_records)
        logger.info(f"Data validation report: {validation_report}")

        # Apply date filtering (next 30 days)
        today = get_est_date()
        thirty_days_later = today + timedelta(days=30)
        
        filtered_records = []
        date_filter_stats = {"valid_dates": 0, "invalid_dates": 0, "out_of_range": 0, "no_date": 0}
        
        for record in all_records:
            if not record.sale_date:
                date_filter_stats["no_date"] += 1
                # Include records without dates in case they're important
                filtered_records.append(record)
                continue
                
            sale_date = parse_sale_date(record.sale_date)
            if not sale_date:
                date_filter_stats["invalid_dates"] += 1
                # Include records with unparseable dates
                filtered_records.append(record)
                continue
                
            if today <= sale_date.date() <= thirty_days_later:
                filtered_records.append(record)
                date_filter_stats["valid_dates"] += 1
            else:
                date_filter_stats["out_of_range"] += 1
                # Still include out-of-range records to avoid missing data
                filtered_records.append(record)

        logger.info(f"Date filtering: {date_filter_stats}")
        logger.info(f"Records after filtering: {len(filtered_records)} (kept all records to avoid data loss)")

        if not filtered_records:
            logger.warning("No records found - this might indicate a scraping issue")
            
        # Process and write to sheets
        try:
            logger.info("Writing data to Google Sheets...")
            write_data_to_sheets(sheets_client, filtered_records)
            logger.info("✓ Successfully wrote data to Google Sheets")
            
        except Exception as e:
            logger.error(f"✗ Error writing to Google Sheets: {e}")
            raise

        # Final statistics
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("=" * 80)
        logger.info(f"Session completed at {end_time}")
        logger.info(f"Total duration: {duration}")
        logger.info(f"Final statistics:")
        logger.info(f"  Counties processed: {len(successful_counties)}/{len(TARGET_COUNTIES)}")
        logger.info(f"  Total records found: {len(filtered_records)}")
        logger.info(f"  HTTP requests: {scraper.session_stats['total_requests']}")
        logger.info(f"  Successful requests: {scraper.session_stats['successful_requests']}")
        logger.info(f"  Failed requests: {scraper.session_stats['failed_requests']}")
        logger.info(f"  Retries: {scraper.session_stats['retries']}")
        logger.info("=" * 80)
        
        # Return success status
        return len(failed_counties) == 0 and len(filtered_records) >= 0
        
    except Exception as e:
        logger.error(f"✗ Critical error in main execution: {e}")
        logger.exception("Full traceback:")
        return False

def write_data_to_sheets(sheets_client: SheetsClient, filtered_records: List[PropertyRecord]):
    """Write processed data to Google Sheets with error handling"""
    
    # Convert records to rows for sheets
    def records_to_rows(records: List[PropertyRecord]) -> List[List[str]]:
        return [[
            record.property_id,
            record.address,
            record.defendant,
            record.sale_date,
            record.approx_judgment,
            record.sale_type,
            record.county
        ] for record in records]

    try:
        # Process county-specific sheets
        for county_config in TARGET_COUNTIES:
            sheet_name = county_config.county_name
            county_records = [r for r in filtered_records if r.county == county_config.county_name]
            
            try:
                sheets_client.create_sheet_if_missing(sheet_name)
                
                if not county_records:
                    # Clear sheet if no records
                    sheets_client.overwrite_with_snapshot(sheet_name, STANDARD_COLUMNS, [])
                    continue
                
                county_rows = records_to_rows(county_records)
                
                # Get existing data to identify new records
                existing_values = sheets_client.get_values(sheet_name)
                existing_ids = set()
                if existing_values and len(existing_values) > 2:
                    for row in existing_values[2:]:  # Skip headers
                        if row and len(row) > 0:
                            existing_ids.add(row[0])  # Property ID is first column
                
                # Identify new records
                new_rows = []
                new_row_indices = []
                for i, (record, row) in enumerate(zip(county_records, county_rows)):
                    if record.property_id not in existing_ids:
                        new_rows.append(row)
                        new_row_indices.append(i)
                
                # Write data
                if not existing_values or len(existing_values) <= 2:
                    sheets_client.overwrite_with_snapshot(sheet_name, STANDARD_COLUMNS, county_rows)
                else:
                    sheets_client.prepend_snapshot(sheet_name, STANDARD_COLUMNS, new_rows, new_row_indices)
                
                logger.info(f"✓ Updated {sheet_name}: {len(county_records)} total, {len(new_rows)} new")
                
            except Exception as e:
                logger.error(f"✗ Error updating sheet {sheet_name}: {e}")
                # Continue with other sheets
                continue

        # Process "All Data" sheet
        try:
            all_sheet = "All Data"
            sheets_client.create_sheet_if_missing(all_sheet)
            
            if filtered_records:
                all_rows = records_to_rows(filtered_records)
                
                # Get existing data
                existing_all = sheets_client.get_values(all_sheet)
                existing_all_ids = set()
                if existing_all and len(existing_all) > 2:
                    for row in existing_all[2:]:
                        if row and len(row) > 0:
                            existing_all_ids.add(row[0])
                
                # Identify new records
                new_all_rows = []
                new_all_indices = []
                for i, (record, row) in enumerate(zip(filtered_records, all_rows)):
                    if record.property_id not in existing_all_ids:
                        new_all_rows.append(row)
                        new_all_indices.append(i)
                
                # Write data
                if not existing_all or len(existing_all) <= 2:
                    sheets_client.overwrite_with_snapshot(all_sheet, STANDARD_COLUMNS, all_rows)
                else:
                    sheets_client.prepend_snapshot(all_sheet, STANDARD_COLUMNS, new_all_rows, new_all_indices)
                
                logger.info(f"✓ Updated All Data: {len(filtered_records)} total, {len(new_all_rows)} new")
            else:
                # Clear sheet if no records
                sheets_client.overwrite_with_snapshot(all_sheet, STANDARD_COLUMNS, [])
                
        except Exception as e:
            logger.error(f"✗ Error updating All Data sheet: {e}")

        # Write summary sheet
        try:
            # Determine new records for summary
            new_records = []
            if filtered_records:
                existing_all = sheets_client.get_values("All Data")
                existing_all_ids = set()
                if existing_all and len(existing_all) > 2:
                    for row in existing_all[2:]:
                        if row and len(row) > 0:
                            existing_all_ids.add(row[0])
                
                new_records = [r for r in filtered_records if r.property_id not in existing_all_ids]
            else:
                new_records = filtered_records
            
            sheets_client.write_summary(filtered_records, new_records)
            logger.info(f"✓ Updated Summary: {len(filtered_records)} total, {len(new_records)} new")
            
        except Exception as e:
            logger.error(f"✗ Error updating Summary sheet: {e}")

    except Exception as e:
        logger.error(f"✗ Critical error writing to sheets: {e}")
        raise

# -----------------------------
# Entry Point
# -----------------------------
def run():
    """Main entry point with error handling"""
    try:
        success = run_with_comprehensive_error_handling()
        if success:
            logger.info("🎉 Scraping session completed successfully!")
            return True
        else:
            logger.error("💥 Scraping session completed with errors!")
            return False
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        logger.exception("Full traceback:")
        return False

if __name__ == "__main__":
    import sys
    success = run()
    sys.exit(0 if success else 1)