#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
main.py
Foreclosure Sales Scraper (One-Time Full Load + Incremental Updates Thereafter)
Enhancements:
- Rolling 30-day filter for visible sheets (today .. today+29 days).
- Maintain an "All Data (Archive)" sheet with full, unfiltered history.
- Snapshot-aware "new row" highlighting (compare latest snapshot vs previous).
"""

import os
import re
import sys
import json
import asyncio
import pandas as pd
from datetime import datetime, timedelta
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

# Rolling window params
WINDOW_DAYS = 30  # today .. today+29
DATE_COL_NAME = "Sales Date"
DATE_INPUT_FORMATS = [
    "%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d",
    "%m/%d/%y", "%m-%d-%y", "%Y/%m/%d",
]

# -----------------------------
# Credential helpers
# -----------------------------
def load_service_account_info():
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
    if creds_raw_stripped.startswith("{"):
        try:
            return json.loads(creds_raw)
        except json.JSONDecodeError as e:
            raise ValueError(f"GOOGLE_CREDENTIALS contains invalid JSON: {e}")

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

    def _get_sheet_id(self, sheet_name: str):
        info = self.spreadsheet_info()
        for s in info.get('sheets', []):
            if s['properties']['title'] == sheet_name:
                return s['properties']['sheetId']
        return None

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
        except HttpError:
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
            sheet_id = self._get_sheet_id(sheet_name)
            if sheet_id is None:
                return

            header_row_index = 1  # the second row is the column header (0-based: 1)
            col_count = len(values[1]) if len(values) > 1 else 10

            self.svc.batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={
                    "requests": [
                        {"repeatCell": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": header_row_index,
                                "endRowIndex": header_row_index + 1
                            },
                            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                            "fields": "userEnteredFormat.textFormat.bold"
                        }},
                        {"updateSheetProperties": {
                            "properties": {"sheetId": sheet_id,
                                           "gridProperties": {"frozenRowCount": 2}},
                            "fields": "gridProperties.frozenRowCount"
                        }},
                        {"autoResizeDimensions": {
                            "dimensions": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": 0,
                                "endIndex": col_count
                            }
                        }}
                    ]
                }
            ).execute()
        except HttpError as e:
            print(f"✗ write_values error on '{sheet_name}': {e}")
            raise

    # --- snapshot style: prepend only new rows ---
    def prepend_snapshot(self, sheet_name: str, header_row, new_rows):
        snapshot_header = [[f"Snapshot for {datetime.now().strftime('%A - %Y-%m-%d')}"]]
        payload = snapshot_header + [header_row] + new_rows + [[""]]
        existing = self.get_values(sheet_name, "A:ZZ")
        values = payload + existing
        self.clear(sheet_name, "A:ZZ")
        self.write_values(sheet_name, values, "A1")
        print(f"✓ Prepended snapshot to '{sheet_name}': {len(new_rows)} new rows")

    def overwrite_with_snapshot(self, sheet_name: str, header_row, all_rows):
        snapshot_header = [[f"Snapshot for {datetime.now().strftime('%A - %Y-%m-%d')}"]]
        values = snapshot_header + [header_row] + all_rows + [[""]]
        self.clear(sheet_name, "A:ZZ")
        self.write_values(sheet_name, values, "A1")
        print(f"✓ Wrote full snapshot to '{sheet_name}' ({len(all_rows)} rows)")

    # ---------- formatting helpers for highlighting ----------
    def clear_highlight_formatting(self, sheet_name: str):
        try:
            sheet_id = self._get_sheet_id(sheet_name)
            if sheet_id is None:
                return
            # Remove all previous conditional formats
            self.svc.batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={"requests": [{"deleteConditionalFormatRule": {"index": 0, "sheetId": sheet_id}} for _ in range(100)]}
            ).execute()
        except HttpError:
            pass  # ignore if none

    def apply_highlight_for_rows(self, sheet_name: str, row_ranges):
        """
        row_ranges: list of (start_row, end_row, start_col, end_col) in 0-based indices, end exclusive.
        Applies a light green fill to those ranges using conditional formatting rules.
        """
        if not row_ranges:
            return
        sheet_id = self._get_sheet_id(sheet_name)
        if sheet_id is None:
            return
        requests = []
        for (r0, r1, c0, c1) in row_ranges:
            requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": sheet_id,
                            "startRowIndex": r0,
                            "endRowIndex": r1,
                            "startColumnIndex": c0,
                            "endColumnIndex": c1
                        }],
                        "booleanRule": {
                            "condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": "=TRUE"}]},
                            "format": {"backgroundColor": {"red": 0.85, "green": 0.94, "blue": 0.85}}
                        }
                    },
                    "index": 0
                }
            })
        try:
            self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": requests}).execute()
        except HttpError as e:
            print(f"⚠ Failed to apply highlighting on '{sheet_name}': {e}")

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

def parse_date_safe(s: str):
    s = (s or "").strip()
    if not s:
        return None
    # Try common formats
    for fmt in DATE_INPUT_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    # Try lenient cleanup: replace double spaces, commas
    s2 = s.replace(",", " ").replace("  ", " ").strip()
    for fmt in DATE_INPUT_FORMATS:
        try:
            return datetime.strptime(s2, fmt).date()
        except ValueError:
            continue
    return None

def in_next_30_days(sales_date_str: str, today=None) -> bool:
    if today is None:
        today = datetime.now().date()
    d = parse_date_safe(sales_date_str)
    if d is None:
        return False
    return today <= d <= (today + timedelta(days=WINDOW_DAYS - 1))

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
            try:
                await self.goto_with_retry(page, list_url)
                await self.dismiss_banners(page)
                await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData", timeout=30000)
            except Exception:
                pass
        return extracted

    async def safe_get_cell_text(self, row, colmap, colname):
        try:
            idx = colmap.get(colname)
            if idx is None:
                return ""
            cells = await row.locator("td").all()
            if idx < len(cells):
                txt = await cells[idx].inner_text()
                return re.sub(r"\s+", " ", txt).strip()
            return ""
        except Exception:
            return ""

    async def scrape_county_sales(self, page, county):
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

                    sales_date = await self.safe_get_cell_text(row, colmap, "sales_date")
                    defendant = await self.safe_get_cell_text(row, colmap, "defendant")
                    prop_address = await self.safe_get_cell_text(row, colmap, "address")

                    current_data = {"address": prop_address, "defendant": defendant, "sales_date": sales_date}
                    details_data = await self.get_details_data(page, details_url, url, county, current_data)

                    row_data = {
                        "Property ID": property_id,
                        "Address": details_data["address"],
                        "Defendant": details_data["defendant"],
                        "Sales Date": details_data["sales_date"],
                        "Approx Judgment": details_data["approx_judgment"],
                        "County": county['county_name'],
                    }
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
# Snapshot parsing and highlighting helpers
# -----------------------------
def find_snapshot_blocks(values):
    """
    Identify snapshot blocks:
    Returns list of dicts with:
      {
        "snapshot_header_row": int,   # 0-based index of the "Snapshot for ..." row
        "header_row": int,            # 0-based index of the column header row
        "data_start_row": int,        # first data row
        "data_end_row": int,          # first row AFTER data block (blank line or next snapshot or end)
        "headers": [col1, col2, ...]
      }
    values: list of rows from get_values()
    """
    blocks = []
    i = 0
    n = len(values)
    while i < n:
        row = values[i] if i < n else []
        row0 = (row[0].strip() if row and len(row) > 0 else "")
        if row0.startswith("Snapshot for "):
            snapshot_header_row = i
            header_row = i + 1 if (i + 1) < n else None
            headers = values[header_row] if header_row is not None and header_row < n else []
            # find data until blank line or next snapshot
            data_start_row = (header_row + 1) if header_row is not None else (i + 1)
            j = data_start_row
            while j < n:
                r = values[j]
                r0 = (r[0].strip() if r and len(r) > 0 else "")
                if r0.startswith("Snapshot for "):
                    break
                if (not r) or (len(r) == 1 and r[0].strip() == ""):
                    # blank line ends the block
                    j += 1
                    break
                j += 1
            data_end_row = j
            blocks.append({
                "snapshot_header_row": snapshot_header_row,
                "header_row": header_row,
                "data_start_row": data_start_row,
                "data_end_row": data_end_row,
                "headers": headers
            })
            i = data_end_row
        else:
            i += 1
    return blocks

def headers_index_map(headers):
    return {h.strip(): idx for idx, h in enumerate(headers or [])}

def collect_keys_from_block(values, block, key_fn):
    """
    key_fn(row_values) -> key or None
    """
    keys = set()
    for r in range(block["data_start_row"], block["data_end_row"]):
        row = values[r] if r < len(values) else []
        if not row or (len(row) == 1 and row[0].strip() == ""):
            continue
        k = key_fn(row, block["headers"])
        if k:
            keys.add(k)
    return keys

def collect_row_ranges_for_new_rows(values, block, key_fn, old_keys):
    """
    Return list of (start_row, end_row, start_col, end_col) for highlight.
    """
    # full width: from col 0 to last non-empty in headers
    end_col = max(len(block["headers"]), max((len(values[r]) for r in range(block["data_start_row"], block["data_end_row"])), default=0))
    ranges = []
    for r in range(block["data_start_row"], block["data_end_row"]):
        row = values[r] if r < len(values) else []
        if not row or (len(row) == 1 and row[0].strip() == ""):
            continue
        k = key_fn(row, block["headers"])
        if k and (k not in old_keys):
            ranges.append((r, r+1, 0, end_col))
    return ranges

def key_fn_county(row, headers):
    # key = Property ID
    hmap = headers_index_map(headers)
    idx = hmap.get("Property ID")
    if idx is None:
        return None
    pid = (row[idx] if len(row) > idx else "").strip()
    return pid or None

def key_fn_all_data(row, headers):
    # key = (County, Property ID)
    hmap = headers_index_map(headers)
    idx_pid = hmap.get("Property ID")
    idx_cty = hmap.get("County")
    if idx_pid is None or idx_cty is None:
        return None
    pid = (row[idx_pid] if len(row) > idx_pid else "").strip()
    cty = (row[idx_cty] if len(row) > idx_cty else "").strip()
    if pid and cty:
        return (cty, pid)
    return None

def apply_snapshot_highlighting(sheets: SheetsClient, sheet_name: str, all_values, key_mode: str):
    """
    key_mode: "county" or "all"
    Compares latest snapshot against immediately previous snapshot and highlights new rows in the latest.
    """
    blocks = find_snapshot_blocks(all_values)
    if not blocks:
        return
    latest = blocks[0]
    prev = blocks[1] if len(blocks) > 1 else None

    key_fn = key_fn_county if key_mode == "county" else key_fn_all_data
    old_keys = set()
    if prev:
        old_keys = collect_keys_from_block(all_values, prev, key_fn)
    # Determine new rows in the latest block
    ranges = collect_row_ranges_for_new_rows(all_values, latest, key_fn, old_keys)

    # Clear ALL conditional formats, then apply only for the latest block new rows
    sheets.clear_highlight_formatting(sheet_name)
    sheets.apply_highlight_for_rows(sheet_name, ranges)

# -----------------------------
# Orchestration
# -----------------------------
async def run():
    start_ts = datetime.now()
    print(f"▶ Starting scrape at {start_ts}")

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
    ALL_DATA_ARCHIVE_SHEET = "All Data (Archive)"

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

                # Maintain complete rows for archive (no filter)
                all_data_rows.extend(df_county.astype(str).values.tolist())

                # Apply rolling 30-day filter for visible county tabs
                if DATE_COL_NAME in df_county.columns:
                    df_county_visible = df_county[df_county[DATE_COL_NAME].apply(in_next_30_days)]
                else:
                    df_county_visible = df_county.iloc[0:0]  # no date column -> show nothing
                # dynamic header (skip County col)
                county_columns = [col for col in df_county.columns if col != "County"]
                county_header = county_columns

                if first_run or not sheets.sheet_exists(county_tab):
                    sheets.create_sheet_if_missing(county_tab)
                    rows = df_county_visible.drop(columns=["County"]).astype(str).values.tolist()
                    sheets.overwrite_with_snapshot(county_tab, county_header, rows)
                else:
                    # Build set of existing Property IDs from the latest visible data for incremental prepend
                    # We still only prepend rows considered "new" vs sheet’s existing data.
                    existing = sheets.get_values(county_tab, "A:ZZ")
                    existing_ids = set()
                    if existing:
                        # find first snapshot header’s header row
                        blocks = find_snapshot_blocks(existing)
                        # We'll gather IDs from entire sheet (all blocks)
                        pid_idx = None
                        for blk in blocks:
                            hmap = headers_index_map(blk["headers"])
                            if pid_idx is None and "Property ID" in hmap:
                                pid_idx = hmap["Property ID"]
                            for r in range(blk["data_start_row"], blk["data_end_row"]):
                                row = existing[r] if r < len(existing) else []
                                if not row or (len(row) == 1 and row[0].strip() == ""):
                                    continue
                                if pid_idx is not None and len(row) > pid_idx:
                                    pid = (row[pid_idx] or "").strip()
                                    if pid:
                                        existing_ids.add(pid)

                    # rows to prepend = filtered visible data not in existing IDs
                    new_df = df_county_visible[~df_county_visible["Property ID"].astype(str).isin(existing_ids)].copy()
                    if new_df.empty:
                        print(f"✓ No new rows for {county['county_name']}")
                    else:
                        new_rows = new_df.drop(columns=["County"]).astype(str).values.tolist()
                        sheets.prepend_snapshot(county_tab, county_header, new_rows)

                print(f"✓ Completed {county['county_name']}: {len(df_county)} records total, {len(df_county_visible)} in 30-day window")
                await asyncio.sleep(POLITE_DELAY_SECONDS)
            except Exception as e:
                print(f"❌ Failed county '{county['county_name']}': {e}")
                continue

        await browser.close()

    # --- Build All Data (Archive) (unfiltered) and All Data (filtered 30d) ---
    try:
        if not all_data_rows:
            print("⚠ No data scraped across all counties. Skipping 'All Data' and 'All Data (Archive)'.")
            return

        # Determine header with Sale Type if any New Castle rows exist
        has_sale_type = any((len(r) >= 7) for r in all_data_rows)
        header_all = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "County"] + (["Sale Type"] if has_sale_type else [])

        # Normalize rows to header length
        target_len = len(header_all)
        norm_rows = []
        for row in all_data_rows:
            r = list(row)
            # If 6 cols and we have Sale Type, append ""
            if has_sale_type and len(r) == 6:
                r.append("")
            # If more, trim
            r = (r + [""] * target_len)[:target_len]
            norm_rows.append([str(x) for x in r])

        # ARCHIVE: keep everything, snapshot-prepend always
        sheets.create_sheet_if_missing(ALL_DATA_ARCHIVE_SHEET)
        if not sheets.sheet_exists(ALL_DATA_ARCHIVE_SHEET):
            print(f"✗ Could not create {ALL_DATA_ARCHIVE_SHEET}")
        else:
            # On first run, write everything as full snapshot, else prepend only the rows not seen before (by County+PID)
            if first_run:
                sheets.overwrite_with_snapshot(ALL_DATA_ARCHIVE_SHEET, header_all, norm_rows)
            else:
                existing = sheets.get_values(ALL_DATA_ARCHIVE_SHEET, "A:ZZ")
                existing_pairs = set()
                if existing:
                    blocks = find_snapshot_blocks(existing)
                    # collect keys from all blocks
                    h_idx = None
                    c_idx = None
                    for blk in blocks:
                        hmap = headers_index_map(blk["headers"])
                        if h_idx is None:
                            h_idx = hmap.get("Property ID")
                        if c_idx is None:
                            c_idx = hmap.get("County")
                        for r in range(blk["data_start_row"], blk["data_end_row"]):
                            row = existing[r] if r < len(existing) else []
                            pid = (row[h_idx] if h_idx is not None and len(row) > h_idx else "").strip()
                            cty = (row[c_idx] if c_idx is not None and len(row) > c_idx else "").strip()
                            if pid and cty:
                                existing_pairs.add((cty, pid))
                new_rows_archive = []
                # indices per header_all
                idx_pid = header_all.index("Property ID")
                idx_cty = header_all.index("County")
                for r in norm_rows:
                    pid = (r[idx_pid] if len(r) > idx_pid else "").strip()
                    cty = (r[idx_cty] if len(r) > idx_cty else "").strip()
                    if pid and cty and (cty, pid) not in existing_pairs:
                        new_rows_archive.append(r)
                if new_rows_archive:
                    sheets.prepend_snapshot(ALL_DATA_ARCHIVE_SHEET, header_all, new_rows_archive)
                    print(f"✓ {ALL_DATA_ARCHIVE_SHEET} updated: {len(new_rows_archive)} new rows")
                else:
                    print(f"✓ No new rows for {ALL_DATA_ARCHIVE_SHEET}")

        # VISIBLE All Data: apply 30-day filter
        df_all = pd.DataFrame(norm_rows, columns=header_all)
        if DATE_COL_NAME in df_all.columns:
            df_visible = df_all[df_all[DATE_COL_NAME].apply(in_next_30_days)].copy()
        else:
            df_visible = df_all.iloc[0:0].copy()

        # Write or prepend filtered rows to All Data
        sheets.create_sheet_if_missing(ALL_DATA_SHEET)
        if first_run:
            sheets.overwrite_with_snapshot(ALL_DATA_SHEET, header_all, df_visible.values.tolist())
        else:
            existing = sheets.get_values(ALL_DATA_SHEET, "A:ZZ")
            existing_pairs = set()
            if existing:
                blocks = find_snapshot_blocks(existing)
                # collect keys from all blocks
                pid_idx = None
                cty_idx = None
                for blk in blocks:
                    hmap = headers_index_map(blk["headers"])
                    if pid_idx is None:
                        pid_idx = hmap.get("Property ID")
                    if cty_idx is None:
                        cty_idx = hmap.get("County")
                    for r in range(blk["data_start_row"], blk["data_end_row"]):
                        row = existing[r] if r < len(existing) else []
                        pid = (row[pid_idx] if pid_idx is not None and len(row) > pid_idx else "").strip()
                        cty = (row[cty_idx] if cty_idx is not None and len(row) > cty_idx else "").strip()
                        if pid and cty:
                            existing_pairs.add((cty, pid))

            # Only prepend rows that are not already present in sheet history
            idx_pid = header_all.index("Property ID")
            idx_cty = header_all.index("County")
            to_prepend = []
            for r in df_visible.values.tolist():
                pid = (r[idx_pid] if len(r) > idx_pid else "").strip()
                cty = (r[idx_cty] if len(r) > idx_cty else "").strip()
                if pid and cty and (cty, pid) not in existing_pairs:
                    to_prepend.append(r)

            if to_prepend:
                sheets.prepend_snapshot(ALL_DATA_SHEET, header_all, to_prepend)
                print(f"✓ All Data updated: {len(to_prepend)} new rows")
            else:
                print("✓ No new rows for 'All Data'")

        # After writes, re-fetch values and apply highlighting for “new” rows vs previous snapshot
        # County tabs
        for county in TARGET_COUNTIES:
            county_tab = county["county_name"][:30]
            if sheets.sheet_exists(county_tab):
                vals = sheets.get_values(county_tab, "A:ZZ")
                apply_snapshot_highlighting(sheets, county_tab, vals, key_mode="county")

        # All Data (filtered)
        if sheets.sheet_exists(ALL_DATA_SHEET):
            vals = sheets.get_values(ALL_DATA_SHEET, "A:ZZ")
            apply_snapshot_highlighting(sheets, ALL_DATA_SHEET, vals, key_mode="all")

        # Archive sheet highlighting optional (usually not necessary); skip to reduce cost.

    except Exception as e:
        print(f"✗ Error updating sheets: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except Exception as e:
        print("Fatal error:", e)
        sys.exit(1)