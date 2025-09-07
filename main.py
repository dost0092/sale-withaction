import httpx
import time
import os
import json
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from selectolax.parser import HTMLParser

# -----------------------------
# Config
# -----------------------------
TARGET_COUNTIES = [
    {"county_id": "52", "county_name": "Cape May County, NJ"},
    {"county_id": "25", "county_name": "Atlantic County, NJ"},
    {"county_id": "1", "county_name": "Camden County, NJ"},
    {"county_id": "3", "county_name": "Burlington County, NJ"},
    {"county_id": "6", "county_name": "Cumberland County, NJ"},
    {"county_id": "19", "county_name": "Gloucester County, NJ"},
    {"county_id": "20", "county_name": "Salem County, NJ"},
    {"county_id": "15", "county_name": "Union County, NJ"},
    {"county_id": "7", "county_name": "Bergen County, NJ"},
    {"county_id": "2", "county_name": "Essex County, NJ"},
    {"county_id": "23", "county_name": "Montgomery County, PA"},
    {"county_id": "24", "county_name": "New Castle County, DE"},
]

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
POLITE_DELAY_SECONDS = 1.5

# -----------------------------
# EST Timezone Helper
# -----------------------------
def get_est_time():
    return datetime.utcnow() - timedelta(hours=5)  # Simple UTC-5 approximation

def get_est_date():
    return get_est_time().date()

def parse_sale_date(date_str):
    try:
        if " " in date_str:
            return datetime.strptime(date_str, "%m/%d/%Y %I:%M %p")
        else:
            return datetime.strptime(date_str, "%m/%d/%Y")
    except (ValueError, TypeError):
        return None

# -----------------------------
# Credential helpers
# -----------------------------
def load_service_account_info():
    file_env = os.environ.get("GOOGLE_CREDENTIALS_FILE")
    if file_env:
        if os.path.exists(file_env):
            with open(file_env, "r", encoding="utf-8") as fh:
                return json.load(fh)
        raise ValueError(f"GOOGLE_CREDENTIALS_FILE set but file does not exist: {file_env}")

    creds_raw = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_raw:
        raise ValueError("Environment variable GOOGLE_CREDENTIALS (or GOOGLE_CREDENTIALS_FILE) not set.")

    creds_raw_stripped = creds_raw.strip()
    if creds_raw_stripped.startswith("{"):
        return json.loads(creds_raw)

    if os.path.exists(creds_raw):
        with open(creds_raw, "r", encoding="utf-8") as fh:
            return json.load(fh)

    raise ValueError("GOOGLE_CREDENTIALS is invalid JSON and not an existing file path.")

def init_sheets_service_from_env():
    info = load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build('sheets', 'v4', credentials=creds)

# -----------------------------
# Sheets client wrapper
# -----------------------------
class SheetsClient:
    def __init__(self, spreadsheet_id: str, service):
        self.spreadsheet_id = spreadsheet_id
        self.service = service
        self.svc = self.service.spreadsheets()

    def spreadsheet_info(self):
        try:
            return self.svc.get(spreadsheetId=self.spreadsheet_id).execute()
        except HttpError:
            return {}

    def sheet_exists(self, sheet_name: str) -> bool:
        info = self.spreadsheet_info()
        for s in info.get('sheets', []):
            if s['properties']['title'] == sheet_name:
                return True
        return False

    def create_sheet_if_missing(self, sheet_name: str):
        if self.sheet_exists(sheet_name):
            return
        req = {"addSheet": {"properties": {"title": sheet_name}}}
        self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": [req]}).execute()

    def get_values(self, sheet_name: str, rng: str = "A:Z"):
        try:
            res = self.svc.values().get(spreadsheetId=self.spreadsheet_id, range=f"'{sheet_name}'!{rng}").execute()
            return res.get("values", [])
        except HttpError:
            return []

    def clear(self, sheet_name: str, rng: str = "A:Z"):
        try:
            self.svc.values().clear(spreadsheetId=self.spreadsheet_id, range=f"'{sheet_name}'!{rng}").execute()
        except HttpError:
            pass

    def write_values(self, sheet_name: str, values, start_cell: str = "A1"):
        self.svc.values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"'{sheet_name}'!{start_cell}",
            valueInputOption="USER_ENTERED",
            body={"values": values}
        ).execute()

    def _get_sheet_id(self, sheet_name: str):
        info = self.spreadsheet_info()
        for s in info.get('sheets', []):
            if s['properties']['title'] == sheet_name:
                return s['properties']['sheetId']
        return None

    def highlight_new_rows(self, sheet_name: str, new_row_indices: list):
        if not new_row_indices:
            return
        sheet_id = self._get_sheet_id(sheet_name)
        if sheet_id is None:
            return
        requests = []
        for row_idx in new_row_indices:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": row_idx,
                        "endRowIndex": row_idx + 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": 10
                    },
                    "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.85,"green": 0.92,"blue": 0.83}}},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })
        if requests:
            self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": requests}).execute()

    def apply_rolling_30_day_filter(self, sheet_name: str, sales_date_column_idx: int = 3):
        sheet_id = self._get_sheet_id(sheet_name)
        if sheet_id is None:
            return
        all_values = self.get_values(sheet_name, "A:Z")
        if not all_values or len(all_values) < 3:
            return
        header_row_idx = None
        for i, row in enumerate(all_values):
            if row and row[0] and "Snapshot for" in row[0]:
                if i + 1 < len(all_values) and all_values[i + 1]:
                    header_row_idx = i + 1
                    break
        if header_row_idx is None:
            return
        sale_dates = []
        for i in range(header_row_idx + 1, len(all_values)):
            row = all_values[i]
            if not row or len(row) <= sales_date_column_idx:
                continue
            sale_date = parse_sale_date(row[sales_date_column_idx])
            if sale_date:
                sale_dates.append(sale_date)
        if not sale_dates:
            return
        min_sale_date_est = min(sale_dates)
        start_date = get_est_date()
        end_date = start_date + timedelta(days=30)
        requests = [{
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": header_row_idx,
                        "startColumnIndex": 0,
                        "endColumnIndex": 10
                    },
                    "criteria": {
                        sales_date_column_idx: {
                            "condition": {
                                "type": "DATE_BETWEEN",
                                "values": [
                                    {"userEnteredValue": start_date.strftime("%m/%d/%Y")},
                                    {"userEnteredValue": end_date.strftime("%m/%d/%Y")}
                                ]
                            }
                        }
                    }
                }
            }
        }]
        self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": requests}).execute()

    def prepend_snapshot(self, sheet_name: str, header_row, new_rows, new_row_indices=None):
        if not new_rows:
            return
        est_now = get_est_time()
        snapshot_header = [[f"Snapshot for {est_now.strftime('%A - %Y-%m-%d')}"]]
        payload = snapshot_header + [header_row] + new_rows + [[""]]
        existing = self.get_values(sheet_name, "A:Z")
        self.clear(sheet_name, "A:Z")
        self.write_values(sheet_name, payload + existing)
        if new_row_indices:
            adjusted_indices = [idx + len(snapshot_header) + 1 for idx in new_row_indices]
            self.highlight_new_rows(sheet_name, adjusted_indices)
        self.apply_rolling_30_day_filter(sheet_name, sales_date_column_idx=3)

    def overwrite_with_snapshot(self, sheet_name: str, header_row, all_rows):
        est_now = get_est_time()
        snapshot_header = [[f"Snapshot for {est_now.strftime('%A - %Y-%m-%d')}"]]
        self.clear(sheet_name, "A:Z")
        self.write_values(sheet_name, snapshot_header + [header_row] + all_rows + [[""]])
        self.apply_rolling_30_day_filter(sheet_name, sales_date_column_idx=3)
        
    def write_summary(self, all_data_rows, new_data_rows):
        sheet_name = "Summary"
        self.create_sheet_if_missing(sheet_name)
        self.clear(sheet_name, "A:Z")

        # Build summary
        total_properties = len(all_data_rows)
        total_new = len(new_data_rows)

        # Count by county
        county_totals = {}
        county_new = {}
        for row in all_data_rows:
            county_totals[row["County"]] = county_totals.get(row["County"], 0) + 1
        for row in new_data_rows:
            county_new[row["County"]] = county_new.get(row["County"], 0) + 1

        summary_values = [
            ["Summary Dashboard (Auto-Generated)"],
            [f"Snapshot for {get_est_time().strftime('%A - %Y-%m-%d %H:%M EST')}"],
            [""],
            ["Overall Totals"],
            ["Total Properties", total_properties],
            ["New Properties (This Run)", total_new],
            [""],
            ["Breakdown by County"],
            ["County", "Total Properties", "New This Run"],
        ]

        for county, total in county_totals.items():
            summary_values.append([
                county,
                total,
                county_new.get(county, 0)
            ])

        # Write to sheet
        self.write_values(sheet_name, summary_values)

# -----------------------------
# Scraper helpers
# -----------------------------
def norm_text(s: str) -> str:
    if not s:
        return ""
    return " ".join(s.split()).strip()

def extract_property_id_from_href(href: str) -> str:
    try:
        q = parse_qs(urlparse(href).query)
        return q.get("PropertyId", [""])[0]
    except Exception:
        return ""

# -----------------------------
# Foreclosure Scraper (httpx version)
# -----------------------------
class ForeclosureScraper:
    def __init__(self, sheets_client):
        self.sheets_client = sheets_client

    def load_search_page(self, client: httpx.Client, county_id: str):
        url = f"https://salesweb.civilview.com/Sales/SalesSearch?countyId={county_id}"
        r = client.get(url)
        r.raise_for_status()
        return HTMLParser(r.text)

    def get_hidden_inputs(self, tree: HTMLParser):
        hidden = {}
        for field in ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"]:
            node = tree.css_first(f"input[name={field}]")
            hidden[field] = node.attributes.get("value", "") if node else ""
        return hidden

    def post_search_and_extract(self, client: httpx.Client, county_id: str, hidden: dict, county_name: str):
        url = f"https://salesweb.civilview.com/Sales/SalesSearch?countyId={county_id}"
        payload = {
            "__VIEWSTATE": hidden["__VIEWSTATE"],
            "__VIEWSTATEGENERATOR": hidden["__VIEWSTATEGENERATOR"],
            "__EVENTVALIDATION": hidden["__EVENTVALIDATION"],
            "IsOpen": "true",   # forces the table open
            "btnSearch": "Search",
        }
        r = client.post(url, data=payload)
        r.raise_for_status()
        tree = HTMLParser(r.text)

        # Get table headers
        headers = []
        header_ths = tree.css("table thead th")
        for th in header_ths:
            headers.append(norm_text(th.text()))
        
        # Extract rows
        rows = []
        for tr in tree.css("table tbody tr"):
            cols = [norm_text(td.text()) for td in tr.css("td")]
            if cols and len(cols) == len(headers):
                # Extract property ID from link if available
                link = tr.css_first("td a")
                href = link.attributes.get("href", "") if link else ""
                property_id = extract_property_id_from_href(href)
                
                # Create row dict with all available data
                row_dict = dict(zip(headers, cols))
                row_dict["Property ID"] = property_id
                row_dict["County"] = county_name
                rows.append(row_dict)
        
        return headers, rows

    def scrape_county_sales(self, county):
        client = httpx.Client(follow_redirects=True, timeout=30)
        try:
            print(f"[INFO] Loading search page for {county['county_name']}")
            tree = self.load_search_page(client, county["county_id"])
            hidden = self.get_hidden_inputs(tree)

            print(f"[INFO] Searching {county['county_name']} (all records)")
            headers, rows = self.post_search_and_extract(client, county["county_id"], hidden, county["county_name"])
            print(f"  ✓ {len(rows)} rows found")
            
            return rows
        except Exception as e:
            print(f"  ✗ Error scraping {county['county_name']}: {e}")
            return []
        finally:
            client.close()
            time.sleep(POLITE_DELAY_SECONDS)

# -----------------------------
# Orchestration
# -----------------------------
def run():
    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    if not spreadsheet_id:
        print("✗ SPREADSHEET_ID env var is required.")
        return

    try:
        service = init_sheets_service_from_env()
    except Exception as e:
        print(f"✗ Failed to initialize Google Sheets service: {e}")
        return

    sheets_client = SheetsClient(spreadsheet_id, service)
    scraper = ForeclosureScraper(sheets_client)

    all_data_rows = []
    for county in TARGET_COUNTIES:
        county_rows = scraper.scrape_county_sales(county)
        if county_rows:
            all_data_rows.extend(county_rows)

    # Filter to only include records within the next 30 days
    today = get_est_date()
    thirty_days_later = today + timedelta(days=30)
    
    filtered_data_rows = []
    for row in all_data_rows:
        sale_date_str = row.get("Sale Date", "")
        sale_date = parse_sale_date(sale_date_str)
        if sale_date and today <= sale_date.date() <= thirty_days_later:
            filtered_data_rows.append(row)

    # Separate per-county sheets
    for county in TARGET_COUNTIES:
        sheet_name = county['county_name']
        sheets_client.create_sheet_if_missing(sheet_name)
        county_rows = [r for r in filtered_data_rows if r["County"] == county['county_name']]
        if not county_rows:
            continue
        
        # Convert to list of lists for Google Sheets
        header_row = list(county_rows[0].keys())
        county_data = [list(row.values()) for row in county_rows]
        
        # Get existing data to identify new rows
        existing_values = sheets_client.get_values(sheet_name)
        existing_ids = {r[0] for r in existing_values[1:] if r} if existing_values else set()
        
        # Identify new rows
        new_rows = []
        new_row_indices = []
        for i, row in enumerate(county_data):
            if row[0] not in existing_ids:  # Assuming Property ID is first column
                new_rows.append(row)
                new_row_indices.append(i)
        
        if not existing_values:
            sheets_client.overwrite_with_snapshot(sheet_name, header_row, county_data)
        else:
            sheets_client.prepend_snapshot(sheet_name, header_row, new_rows, new_row_indices)

    # "All Data" sheet
    all_sheet = "All Data"
    sheets_client.create_sheet_if_missing(all_sheet)
    if filtered_data_rows:
        all_header = list(filtered_data_rows[0].keys())
        all_data = [list(row.values()) for row in filtered_data_rows]
        
        existing_all = sheets_client.get_values(all_sheet)
        existing_all_ids = {r[0] for r in existing_all[1:] if r} if existing_all else set()
        
        new_all_rows = []
        new_all_indices = []
        for i, row in enumerate(all_data):
            if row[0] not in existing_all_ids:
                new_all_rows.append(row)
                new_all_indices.append(i)
        
        if not existing_all:
            sheets_client.overwrite_with_snapshot(all_sheet, all_header, all_data)
        else:
            sheets_client.prepend_snapshot(all_sheet, all_header, new_all_rows, new_all_indices)
    
    # Summary sheet
    new_data_rows = [r for r in filtered_data_rows if r["Property ID"] not in existing_all_ids] if existing_all else filtered_data_rows
    sheets_client.write_summary(filtered_data_rows, new_data_rows)

if __name__ == "__main__":
    run()