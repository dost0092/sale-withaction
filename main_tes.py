#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
main.py
Foreclosure Sales Scraper with Rolling 30-Day Window
"""

import os
import re
import sys
import json
import asyncio
import pandas as pd
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse, parse_qs

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# -----------------------------
# Config
# -----------------------------
BASE_URL = "https://salesweb.civilview.com/"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

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

POLITE_DELAY_SECONDS = 1.5
MAX_RETRIES = 5

# -----------------------------
# EST Timezone Helper
# -----------------------------
def get_est_time():
    """Get current time in EST timezone"""
    est = timezone(timedelta(hours=-5))  # EST is UTC-5
    return datetime.now(est)

def get_est_date():
    """Get current date in EST timezone"""
    return get_est_time().date()

def parse_sale_date(date_str):
    """Parse sale date string into datetime object"""
    try:
        # Handle formats like "09/08/2025 2:00 PM"
        if " " in date_str:
            return datetime.strptime(date_str, "%m/%d/%Y %I:%M %p")
        # Handle formats like "09/08/2025"
        else:
            return datetime.strptime(date_str, "%m/%d/%Y")
    except (ValueError, TypeError):
        return None

# -----------------------------
# Credential helpers
# -----------------------------
def load_service_account_info():
    """
    Loads service account JSON from:
    1) GOOGLE_CREDENTIALS_FILE (File variable path) OR
    2) GOOGLE_CREDENTIALS raw JSON string OR
    3) GOOGLE_CREDENTIALS path to local file
    Returns parsed dict or raises ValueError.
    """
    file_env = os.environ.get("GOOGLE_CREDENTIALS_FILE")
    if file_env:
        if os.path.exists(file_env):
            try:
                with open(file_env, "r", encoding="utf-8") as fh:
                    return json.load(fh)
            except Exception as e:
                raise ValueError(f"Failed to read JSON from GOOGLE_CREDENTIALS_FILE ({file_env}): {e}")
        else:
            raise ValueError(f"GOOGLE_CREDENTIALS_FILE is set but file does not exist: {file_env}")

    creds_raw = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_raw:
        raise ValueError("Environment variable GOOGLE_CREDENTIALS (or GOOGLE_CREDENTIALS_FILE) not set.")

    creds_raw_stripped = creds_raw.strip()
    # Case: raw JSON string
    if creds_raw_stripped.startswith("{"):
        try:
            return json.loads(creds_raw)
        except json.JSONDecodeError as e:
            raise ValueError(f"GOOGLE_CREDENTIALS contains invalid JSON: {e}")

    # Case: path to file
    if os.path.exists(creds_raw):
        try:
            with open(creds_raw, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception as e:
            raise ValueError(f"GOOGLE_CREDENTIALS is a path but failed to load JSON: {e}")

    raise ValueError("GOOGLE_CREDENTIALS is set but not valid JSON and not an existing file path.")

def init_sheets_service_from_env():
    info = load_service_account_info()
    try:
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)
        return service
    except Exception as e:
        raise RuntimeError(f"Failed to create Google Sheets client: {e}")

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
        except HttpError as e:
            print(f"⚠ Error fetching spreadsheet info: {e}")
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
        try:
            req = {"addSheet": {"properties": {"title": sheet_name}}}
            self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": [req]}).execute()
            print(f"✓ Created sheet: {sheet_name}")
        except HttpError as e:
            print(f"⚠ create_sheet_if_missing error on '{sheet_name}': {e}")

    def get_values(self, sheet_name: str, rng: str = "A:Z"):
        try:
            res = self.svc.values().get(spreadsheetId=self.spreadsheet_id, range=f"'{sheet_name}'!{rng}").execute()
            return res.get("values", [])
        except HttpError as e:
            return []

    def clear(self, sheet_name: str, rng: str = "A:Z"):
        try:
            self.svc.values().clear(spreadsheetId=self.spreadsheet_id, range=f"'{sheet_name}'!{rng}").execute()
        except HttpError as e:
            print(f"⚠ clear error on '{sheet_name}': {e}")

    def write_values(self, sheet_name: str, values, start_cell: str = "A1"):
        try:
            self.svc.values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{sheet_name}'!{start_cell}",
                valueInputOption="USER_ENTERED",
                body={"values": values}
            ).execute()

            # --- Beautify: bold header, freeze row, auto resize ---
            self.svc.batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={
                    "requests": [
                        {"repeatCell": {
                            "range": {
                                "sheetId": self._get_sheet_id(sheet_name),
                                "startRowIndex": 1,
                                "endRowIndex": 2
                            },
                            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                            "fields": "userEnteredFormat.textFormat.bold"
                        }},
                        {"updateSheetProperties": {
                            "properties": {"sheetId": self._get_sheet_id(sheet_name),
                                           "gridProperties": {"frozenRowCount": 2}},
                            "fields": "gridProperties.frozenRowCount"
                        }},
                        {"autoResizeDimensions": {
                            "dimensions": {
                                "sheetId": self._get_sheet_id(sheet_name),
                                "dimension": "COLUMNS",
                                "startIndex": 0,
                                "endIndex": len(values[0]) if values else 10
                            }
                        }}
                    ]
                }
            ).execute()
        except HttpError as e:
            print(f"✗ write_values error on '{sheet_name}': {e}")
            raise

    def _get_sheet_id(self, sheet_name: str):
        info = self.spreadsheet_info()
        for s in info.get('sheets', []):
            if s['properties']['title'] == sheet_name:
                return s['properties']['sheetId']
        return None

    def highlight_new_rows(self, sheet_name: str, new_row_indices: list):
        """Apply green background to new rows"""
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
                        "endColumnIndex": 10  # Adjust based on number of columns
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {
                                "red": 0.85,
                                "green": 0.92,
                                "blue": 0.83
                            }
                        }
                    },
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })
            
        try:
            self.svc.batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={"requests": requests}
            ).execute()
            print(f"✓ Highlighted {len(new_row_indices)} new rows in '{sheet_name}'")
        except HttpError as e:
            print(f"⚠ Error highlighting rows in '{sheet_name}': {e}")

    def apply_rolling_30_day_filter(self, sheet_name: str, sales_date_column_idx: int = 3):
        """
        Apply a rolling 30-day filter that advances by one day each day
        based on the minimum sale date in the data
        """
        sheet_id = self._get_sheet_id(sheet_name)
        if sheet_id is None:
            return
            
        # Get all values from the sheet
        all_values = self.get_values(sheet_name, "A:Z")
        if not all_values or len(all_values) < 3:  # Need at least header + 1 data row
            print(f"⚠ Not enough data in '{sheet_name}' to apply 30-day filter")
            return
            
        # Find the header row and data rows
        header_row_idx = None
        for i, row in enumerate(all_values):
            if row and row[0] and "Snapshot for" in row[0]:
                # Next row should be the header
                if i + 1 < len(all_values) and all_values[i + 1]:
                    header_row_idx = i + 1
                    break
                    
        if header_row_idx is None:
            print(f"⚠ Could not find header row in '{sheet_name}'")
            return
            
        # Extract sale dates from the data
        sale_dates = []
        for i in range(header_row_idx + 1, len(all_values)):
            row = all_values[i]
            if not row or len(row) <= sales_date_column_idx:
                continue
                
            date_str = row[sales_date_column_idx]
            sale_date = parse_sale_date(date_str)
            if sale_date:
                sale_dates.append(sale_date)
                
        if not sale_dates:
            print(f"⚠ No valid sale dates found in '{sheet_name}'")
            return
            
        # Find the minimum sale date in EST
        min_sale_date = min(sale_dates)
        # Convert to EST date (if not already in EST)
        min_sale_date_est = min_sale_date.astimezone(timezone(timedelta(hours=-5))).date()
        
        # Get today's date in EST
        today_est = get_est_date()
        
        # Calculate days elapsed since the minimum sale date
        days_elapsed = (today_est - min_sale_date_est).days
        
        # Calculate the start date for the filter (min date + days elapsed)
        start_date = min_sale_date_est + timedelta(days=days_elapsed)
        
        # Calculate end date (30 days from the start date)
        end_date = start_date + timedelta(days=30)
        
        # Format dates for comparison
        start_date_str = start_date.strftime("%m/%d/%Y")
        end_date_str = end_date.strftime("%m/%d/%Y")
        
        # Create filter request
        requests = [{
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": header_row_idx,  # Start from header row
                        "startColumnIndex": 0,
                        "endColumnIndex": 10  # Adjust based on number of columns
                    },
                    "criteria": {
                        sales_date_column_idx: {  # Sales Date column
                            "condition": {
                                "type": "DATE_BETWEEN",
                                "values": [
                                    {"userEnteredValue": start_date_str},
                                    {"userEnteredValue": end_date_str}
                                ]
                            }
                        }
                    }
                }
            }
        }]
        
        try:
            self.svc.batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={"requests": requests}
            ).execute()
            print(f"✓ Applied rolling 30-day filter to '{sheet_name}' (EST: {start_date_str} to {end_date_str})")
        except HttpError as e:
            print(f"⚠ Error applying filter to '{sheet_name}': {e}")

    # --- snapshot style: prepend only new rows ---
    def prepend_snapshot(self, sheet_name: str, header_row, new_rows, new_row_indices=None):
        if not new_rows:
            print(f"✓ No new rows to prepend in '{sheet_name}'")
            return
            
        # Use EST time for snapshot header
        est_now = get_est_time()
        snapshot_header = [[f"Snapshot for {est_now.strftime('%A - %Y-%m-%d')}"]]
        payload = snapshot_header + [header_row] + new_rows + [[""]]
        existing = self.get_values(sheet_name, "A:Z")
        values = payload + existing
        self.clear(sheet_name, "A:Z")
        self.write_values(sheet_name, values, "A1")
        
        # Highlight new rows if indices are provided
        if new_row_indices:
            # Adjust indices for the new rows we just added
            adjusted_indices = [idx + len(snapshot_header) + 1 for idx in new_row_indices]
            self.highlight_new_rows(sheet_name, adjusted_indices)
            
        # Apply rolling 30-day filter based on the data in the sheet
        # Sales Date is typically in column D (index 3)
        self.apply_rolling_30_day_filter(sheet_name, sales_date_column_idx=3)
        
        print(f"✓ Prepended snapshot to '{sheet_name}': {len(new_rows)} new rows")

    # first run = full overwrite
    def overwrite_with_snapshot(self, sheet_name: str, header_row, all_rows):
        # Use EST time for snapshot header
        est_now = get_est_time()
        snapshot_header = [[f"Snapshot for {est_now.strftime('%A - %Y-%m-%d')}"]]
        values = snapshot_header + [header_row] + all_rows + [[""]]
        self.clear(sheet_name, "A:Z")
        self.write_values(sheet_name, values, "A1")
        
        # Apply rolling 30-day filter based on the data in the sheet
        # Sales Date is typically in column D (index 3)
        self.apply_rolling_30_day_filter(sheet_name, sales_date_column_idx=3)
        
        print(f"✓ Wrote full snapshot to '{sheet_name}' ({len(all_rows)} rows)")

# -----------------------------
# Scrape helpers
# -----------------------------
def norm_text(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()

def extract_property_id_from_href(href: str) -> str:
    try:
        q = parse_qs(urlparse(href).query)
        return q.get("PropertyId", [""])[0]
    except Exception:
        return ""

# -----------------------------
# Scraper
# -----------------------------
class ForeclosureScraper:
    def __init__(self, sheets_client):
        self.sheets_client = sheets_client

    async def goto_with_retry(self, page, url: str, max_retries=3):
        last_exc = None
        for attempt in range(max_retries):
            try:
                resp = await page.goto(url, wait_until="networkidle", timeout=60000)
                if resp and (200 <= resp.status < 300):
                    return resp
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                last_exc = e
                await asyncio.sleep(2 ** attempt)
        if last_exc:
            raise last_exc
        return None

    async def dismiss_banners(self, page):
        selectors = [
            "button:has-text('Accept')", "button:has-text('I Agree')",
            "button:has-text('Close')", "button.cookie-accept",
            "button[aria-label='Close']", ".modal-footer button:has-text('OK')",
        ]
        for sel in selectors:
            try:
                loc = page.locator(sel)
                if await loc.count():
                    await loc.first.click(timeout=1500)
                    await page.wait_for_timeout(200)
            except Exception:
                pass

    async def get_details_data(self, page, details_url, list_url, county, current_data):
        """Extract additional data from details page."""
        extracted = {
            "approx_judgment": "",
            "sale_type": "",
            "address": current_data.get("address", ""),
            "defendant": current_data.get("defendant", ""),
            "sales_date": current_data.get("sales_date", "")
        }
        
        if not details_url:
            return extracted
            
        try:
            await self.goto_with_retry(page, details_url)
            await self.dismiss_banners(page)
            await page.wait_for_selector(".sale-details-list", timeout=15000)
            
            items = page.locator(".sale-details-list .sale-detail-item")
            for j in range(await items.count()):
                try:
                    label = (await items.nth(j).locator(".sale-detail-label").inner_text()).strip()
                    val = (await items.nth(j).locator(".sale-detail-value").inner_text()).strip()
                    label_low = label.lower()
                    
                    if "address" in label_low:
                        try:
                            val_html = await items.nth(j).locator(".sale-detail-value").inner_html()
                            val_html = re.sub(r"<br\s*/?>", " ", val_html)
                            val_clean = re.sub(r"<.*?>", "", val_html).strip()
                            if not extracted["address"] or len(val_clean) > len(extracted["address"]):
                                extracted["address"] = val_clean
                        except Exception:
                            if not extracted["address"]:
                                extracted["address"] = val
                                
                    elif ("Approx. Judgment" in label or "Approx. Upset" in label
                        or "Approximate Judgment:" in label or "Approx Judgment*" in label 
                        or "Approx. Upset*" in label or "Debt Amount" in label):
                        extracted["approx_judgment"] = val
                        
                    elif "defendant" in label_low and not extracted["defendant"]:
                        extracted["defendant"] = val
                        
                    elif "sale" in label_low and "date" in label_low and not extracted["sales_date"]:
                        extracted["sales_date"] = val
                        
                    elif county["county_id"] == "24" and "sale type" in label_low:
                        extracted["sale_type"] = val
                        
                except Exception:
                    continue
                    
        except Exception as e:
            print(f"⚠ Details page error for {county['county_name']}: {e}")
        finally:
            # Return to list page
            try:
                await self.goto_with_retry(page, list_url)
                await self.dismiss_banners(page)
                await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData", timeout=30000)
            except Exception:
                pass
                
        return extracted

    async def safe_get_cell_text(self, row, colmap, colname):
        """Safely extract text from table cell by column name."""
        try:
            idx = colmap.get(colname)
            if idx is None:
                return ""
            # Get all cells first to avoid issues with nth() selector
            cells = await row.locator("td").all()
            if idx < len(cells):
                txt = await cells[idx].inner_text()
                return re.sub(r"\s+", " ", txt).strip()
            return ""
        except Exception:
            return ""

    async def scrape_county_sales(self, page, county):
        """Main scraping function that handles different table structures dynamically."""
        url = f"{BASE_URL}Sales/SalesSearch?countyId={county['county_id']}"
        print(f"[INFO] Scraping {county['county_name']} -> {url}")

        for attempt in range(MAX_RETRIES):
            try:
                await self.goto_with_retry(page, url)
                await self.dismiss_banners(page)

                try:
                    await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData", timeout=30000)
                except PlaywrightTimeoutError:
                    print(f"[WARN] No sales found for {county['county_name']}")
                    return []

                # Build column mapping from headers
                colmap = await self.get_table_columns(page)
                if not colmap:
                    print(f"[WARN] Could not determine table structure for {county['county_name']}")
                    return []

                rows = page.locator("table.table.table-striped tbody tr")
                n = await rows.count()
                results = []

                for i in range(n):
                    row = rows.nth(i)
                    details_a = row.locator("td.hidden-print a")
                    details_href = (await details_a.get_attribute("href")) or ""
                    details_url = details_href if details_href.startswith("http") else urljoin(BASE_URL, details_href)
                    property_id = extract_property_id_from_href(details_href)

                    # Get values by column name
                    sales_date = await self.safe_get_cell_text(row, colmap, "sales_date")
                    defendant = await self.safe_get_cell_text(row, colmap, "defendant")
                    prop_address = await self.safe_get_cell_text(row, colmap, "address")

                    # Get additional data from details page
                    current_data = {
                        "address": prop_address,
                        "defendant": defendant,
                        "sales_date": sales_date
                    }
                    
                    details_data = await self.get_details_data(page, details_url, url, county, current_data)

                    # Build result row
                    row_data = {
                        "Property ID": property_id,
                        "Address": details_data["address"],
                        "Defendant": details_data["defendant"],
                        "Sales Date": details_data["sales_date"],
                        "Approx Judgment": details_data["approx_judgment"],
                        "County": county['county_name'],
                    }
                    
                    # Add Sale Type column only for New Castle County
                    if county["county_id"] == "24":
                        row_data["Sale Type"] = details_data["sale_type"]

                    results.append(row_data)

                return results

            except Exception as e:
                print(f"❌ Error scraping {county['county_name']} (Attempt {attempt+1}/{MAX_RETRIES}): {e}")
                await asyncio.sleep(2 ** attempt)

        print(f"[FAIL] Could not get complete data for {county['county_name']}")
        return []

    async def get_table_columns(self, page):
        """Get column mapping based on headers to handle different table structures."""
        try:
            header_ths = page.locator("table.table.table-striped thead tr th")
            if await header_ths.count() == 0:
                header_ths = page.locator("table.table.table-striped tr").first.locator("th")
            
            colmap = {}
            for i in range(await header_ths.count()):
                try:
                    htxt = (await header_ths.nth(i).inner_text()).strip().lower()
                    if "sale" in htxt and "date" in htxt:
                        colmap["sales_date"] = i
                    elif "defendant" in htxt:
                        colmap["defendant"] = i
                    elif "address" in htxt:
                        colmap["address"] = i
                except Exception:
                    continue
            
            return colmap
        except Exception as e:
            print(f"[ERROR] Failed to get column mapping: {e}")
            return {}

# -----------------------------
# Orchestration
# -----------------------------
async def run():
    start_ts = get_est_time()
    print(f"▶ Starting scrape at {start_ts.strftime('%Y-%m-%d %H:%M:%S %Z')} (EST)")

    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    if not spreadsheet_id:
        print("✗ SPREADSHEET_ID env var is required.")
        sys.exit(1)

    try:
        service = init_sheets_service_from_env()
        print("✓ Google Sheets API client initialized.")
    except Exception as e:
        print(f"✗ Error initializing Google Sheets client: {e}")
        raise SystemExit(1)

    sheets = SheetsClient(spreadsheet_id, service)
    ALL_DATA_SHEET = "All Data"
    first_run = not sheets.sheet_exists(ALL_DATA_SHEET)
    print(f"ℹ First run? {'YES' if first_run else 'NO'}")

    all_data_rows = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        scraper = ForeclosureScraper(sheets)

        for county in TARGET_COUNTIES:
            county_tab = county["county_name"][:30]
            try:
                county_records = await scraper.scrape_county_sales(page, county)
                if not county_records:
                    print(f"⚠ No data for {county['county_name']}")
                    await asyncio.sleep(POLITE_DELAY_SECONDS)
                    continue

                df_county = pd.DataFrame(county_records)

                # dynamic header (skip County col)
                county_columns = [col for col in df_county.columns if col != "County"]
                county_header = county_columns

                if first_run or not sheets.sheet_exists(county_tab):
                    sheets.create_sheet_if_missing(county_tab)
                    rows = df_county.drop(columns=["County"]).astype(str).values.tolist()
                    sheets.overwrite_with_snapshot(county_tab, county_header, rows)
                else:
                    existing = sheets.get_values(county_tab, "A:Z")
                    existing_ids = set()
                    existing_snapshot_data = []
                    if existing:
                        # Find the most recent snapshot
                        snapshot_start = None
                        header_idx = None
                        
                        # Look for snapshot header
                        for idx, row in enumerate(existing):
                            if row and row[0].startswith("Snapshot for"):
                                snapshot_start = idx
                                # Next row should be the header
                                if idx + 1 < len(existing) and existing[idx + 1]:
                                    header_idx = idx + 1
                                break
                                
                        if header_idx is not None and header_idx + 1 < len(existing):
                            # Extract data from the most recent snapshot
                            for r in existing[header_idx + 1:]:
                                if not r or (len(r) == 1 and r[0].strip() == ""):
                                    break
                                pid = (r[0] or "").strip()
                                if pid:
                                    existing_ids.add(pid)
                                    existing_snapshot_data.append(r)

                    # Identify new rows
                    new_df = df_county[~df_county["Property ID"].isin(existing_ids)].copy()
                    
                    # Get indices of new rows for highlighting
                    new_row_indices = []
                    for i, row in df_county.iterrows():
                        if row["Property ID"] not in existing_ids:
                            new_row_indices.append(i)
                    
                    if new_df.empty:
                        print(f"✓ No new rows for {county['county_name']}")
                    else:
                        new_rows = new_df.drop(columns=["County"]).astype(str).values.tolist()
                        sheets.prepend_snapshot(county_tab, county_header, new_rows, new_row_indices)

                all_data_rows.extend(df_county.astype(str).values.tolist())
                print(f"✓ Completed {county['county_name']}: {len(df_county)} records")
                await asyncio.sleep(POLITE_DELAY_SECONDS)
            except Exception as e:
                print(f"❌ Failed county '{county['county_name']}': {e}")
                continue

        await browser.close()

    # --- All Data sheet ---
    try:
        if not all_data_rows:
            print("⚠ No data scraped across all counties. Skipping 'All Data'.")
        else:
            # default columns
            standard_cols = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "County"]

            # New Castle adds Sale Type
            has_new_castle = any(county["county_id"] == "24" for county in TARGET_COUNTIES)
            if has_new_castle:
                # Force Sale Type to always be last
                header_all = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "County", "Sale Type"]
                padded_rows = []
                for row in all_data_rows:
                    if len(row) == 6:  # no Sale Type
                        # row = [PID, Addr, Def, Date, Judgment, County]
                        padded_row = row[:5] + [row[5]] + [""]
                        padded_rows.append(padded_row)
                    elif len(row) == 7:
                        # Ensure Sale Type is last
                        padded_row = row[:5] + [row[5]] + [row[6]]
                        padded_rows.append(padded_row)
                    else:
                        padded_rows.append(row)
                all_data_rows = padded_rows
            else:
                header_all = standard_cols

            sheets.create_sheet_if_missing(ALL_DATA_SHEET)
            if first_run:
                sheets.overwrite_with_snapshot(ALL_DATA_SHEET, header_all, all_data_rows)
            else:
                existing = sheets.get_values(ALL_DATA_SHEET, "A:Z")
                existing_pairs = set()
                existing_snapshot_data = []
                if existing:
                    # Find the most recent snapshot
                    snapshot_start = None
                    header_idx = None
                    
                    # Look for snapshot header
                    for idx, row in enumerate(existing):
                        if row and row[0].startswith("Snapshot for"):
                            snapshot_start = idx
                            # Next row should be the header
                            if idx + 1 < len(existing) and existing[idx + 1]:
                                header_idx = idx + 1
                            break
                            
                    if header_idx is not None and header_idx + 1 < len(existing):
                        # Extract data from the most recent snapshot
                        for r in existing[header_idx + 1:]:
                            if not r or (len(r) == 1 and r[0].strip() == ""):
                                break
                            pid = (r[0] if len(r) > 0 else "").strip()
                            county_col_idx = 5  # county is always before Sale Type now
                            cty = (r[county_col_idx] if len(r) > county_col_idx else "").strip()
                            if pid and cty:
                                existing_pairs.add((cty, pid))
                                existing_snapshot_data.append(r)

                # Identify new rows
                new_rows = []
                new_row_indices = []
                for idx, r in enumerate(all_data_rows):
                    pid = (r[0] if len(r) > 0 else "").strip()
                    county_col_idx = 5
                    cty = (r[county_col_idx] if len(r) > county_col_idx else "").strip()
                    if pid and cty and (cty, pid) not in existing_pairs:
                        new_rows.append(r)
                        new_row_indices.append(idx)

                if not new_rows:
                    print("✓ No new rows for 'All Data'")
                else:
                    sheets.prepend_snapshot(ALL_DATA_SHEET, header_all, new_rows, new_row_indices)
                    print(f"✓ All Data updated: {len(new_rows)} new rows")
    except Exception as e:
        print(f"✗ Error updating 'All Data': {e}")



if __name__ == "__main__":
    try:
        asyncio.run(run())
    except Exception as e:
        print("Fatal error:", e)
        sys.exit(1)