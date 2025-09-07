#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs

import httpx
from selectolax.parser import HTMLParser
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# -----------------------------
# Config
# -----------------------------
BASE_URL = "https://salesweb.civilview.com"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

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
MAX_RETRIES = 3
HTTP_TIMEOUT = 30.0

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("foreclosure_scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# -----------------------------
# Time helpers (ET)
# -----------------------------
try:
    from zoneinfo import ZoneInfo
    ET_TZ = ZoneInfo("America/New_York")
except Exception:
    ET_TZ = timezone(timedelta(hours=-5))

def now_et():
    return datetime.now(ET_TZ)

def today_et():
    return now_et().date()

def parse_sale_date(date_str: str):
    if not date_str:
        return None
    for fmt in ("%m/%d/%Y %I:%M %p", "%m/%d/%Y %H:%M", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

# -----------------------------
# Credentials
# -----------------------------
def load_service_account_info():
    file_env = os.environ.get("GOOGLE_CREDENTIALS_FILE")
    if file_env:
        if os.path.exists(file_env):
            with open(file_env, "r", encoding="utf-8") as fh:
                return json.load(fh)
        raise ValueError(f"GOOGLE_CREDENTIALS_FILE set but not found: {file_env}")

    creds_raw = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_raw:
        raise ValueError("GOOGLE_CREDENTIALS or GOOGLE_CREDENTIALS_FILE is required.")

    txt = creds_raw.strip()
    if txt.startswith("{"):
        return json.loads(txt)

    if os.path.exists(creds_raw):
        with open(creds_raw, "r", encoding="utf-8") as fh:
            return json.load(fh)

    raise ValueError("GOOGLE_CREDENTIALS is neither valid JSON nor an existing file path.")

def init_sheets_service_from_env():
    info = load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    sa_email = info.get("client_email", "<unknown-service-account>")
    logger.info(f"Google Sheets initialized. Service account: {sa_email}")
    logger.info("Ensure this email has Editor access to your spreadsheet.")
    return svc, sa_email

# -----------------------------
# Google Sheets wrapper
# -----------------------------
class SheetsClient:
    def __init__(self, spreadsheet_id: str, service):
        self.spreadsheet_id = spreadsheet_id
        self.svc = service.spreadsheets()

    def spreadsheet_info(self):
        try:
            info = self.svc.get(spreadsheetId=self.spreadsheet_id).execute()
            title = info.get("properties", {}).get("title", "")
            logger.info(f"Connected to spreadsheet: {title}")
            return info
        except HttpError as e:
            logger.error(f"Failed to open spreadsheet: {e}")
            raise

    def _get_sheet_id(self, sheet_name: str):
        info = self.spreadsheet_info()
        for s in info.get("sheets", []):
            if s["properties"]["title"] == sheet_name:
                return s["properties"]["sheetId"]
        return None

    def sheet_exists(self, sheet_name: str):
        return self._get_sheet_id(sheet_name) is not None

    def create_sheet_if_missing(self, sheet_name: str):
        if self.sheet_exists(sheet_name):
            return
        try:
            self.svc.batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]},
            ).execute()
            logger.info(f"Created sheet: {sheet_name}")
        except HttpError as e:
            logger.error(f"Error creating sheet {sheet_name}: {e}")
            raise

    def get_values(self, sheet_name: str, rng: str = "A:Z"):
        try:
            res = self.svc.values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{sheet_name}'!{rng}"
            ).execute()
            return res.get("values", [])
        except HttpError as e:
            logger.error(f"Error reading range {sheet_name}!{rng}: {e}")
            return []

    def clear(self, sheet_name: str, rng: str = "A:Z"):
        try:
            self.svc.values().clear(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{sheet_name}'!{rng}"
            ).execute()
        except HttpError as e:
            logger.error(f"Error clearing range {sheet_name}!{rng}: {e}")
            raise

    def write_values(self, sheet_name: str, values, start_cell: str = "A1"):
        if not values:
            return
        try:
            self.svc.values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{sheet_name}'!{start_cell}",
                valueInputOption="USER_ENTERED",
                body={"values": values},
            ).execute()
        except HttpError as e:
            logger.error(f"Error writing to {sheet_name}!{start_cell}: {e}")
            raise

    def format_sheet(self, sheet_name: str, num_columns: int):
        sheet_id = self._get_sheet_id(sheet_name)
        if sheet_id is None:
            return
        requests = [
            # Snapshot row (row 1)
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": num_columns},
                    "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.92, "green": 0.92, "blue": 0.92}, "textFormat": {"bold": True, "italic": True}}},
                    "fields": "userEnteredFormat(backgroundColor,textFormat)"
                }
            },
            # Header row (row 2)
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": num_columns},
                    "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.2, "green": 0.4, "blue": 0.6}, "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}}}},
                    "fields": "userEnteredFormat(backgroundColor,textFormat)"
                }
            },
            # Freeze top 2 rows
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 2}},
                    "fields": "gridProperties.frozenRowCount"
                }
            },
            # Auto-resize columns
            {
                "autoResizeDimensions": {
                    "dimensions": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": num_columns}
                }
            },
        ]
        try:
            self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": requests}).execute()
        except HttpError as e:
            logger.warning(f"Could not format sheet {sheet_name}: {e}")

    def detect_header_row_index(self, values):
        for idx, row in enumerate(values[:10]):
            if not row:
                continue
            first = (row[0] or "").strip().lower().replace(" ", "")
            if first in {"propertyid", "propertyid*"}:
                return idx
        if values and values[0] and str(values[0][0]).lower().startswith("snapshot for"):
            return 1
        return 0

    def prepend_snapshot(self, sheet_name: str, header_row, new_rows):
        existing = self.get_values(sheet_name, "A:Z")
        prefix = [[f"Snapshot for {now_et().strftime('%A - %Y-%m-%d %H:%M %Z')}"]]
        # Always write a snapshot row + header. If no new rows, we still add a dated snapshot.
        payload = prefix + [header_row] + (new_rows if new_rows else [])
        if existing:
            payload += existing
        self.clear(sheet_name, "A:Z")
        self.write_values(sheet_name, payload, "A1")
        self.format_sheet(sheet_name, len(header_row))
        logger.info(f"Prepended snapshot to '{sheet_name}' with {len(new_rows)} new rows")

    def overwrite_with_snapshot(self, sheet_name: str, header_row, all_rows):
        snap = [[f"Snapshot for {now_et().strftime('%A - %Y-%m-%d %H:%M %Z')}"]]
        payload = snap + [header_row] + all_rows
        self.clear(sheet_name, "A:Z")
        self.write_values(sheet_name, payload, "A1")
        self.format_sheet(sheet_name, len(header_row))
        logger.info(f"Wrote full snapshot to '{sheet_name}' with {len(all_rows)} rows")

# -----------------------------
# Scrape helpers
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

def extract_approx_judgment(html_content: str, county_id: str) -> str:
    tree = HTMLParser(html_content)
    text_content = tree.text()
    patterns = [
        r'Approx(?:imate|\.)?\s*Judgment[^$]*\$(\d[\d,]*)',
        r'Judgment Amount[^$]*\$(\d[\d,]*)',
        r'Approx(?:imate|\.)?\s*Upset[^$]*\$(\d[\d,]*)',
        r'Upset Price[^$]*\$(\d[\d,]*)',
        r'Debt Amount[^$]*\$(\d[\d,]*)',
    ]
    if county_id == "24":
        patterns = [r'Upset[^$]*\$(\d[\d,]*)', r'Amount Due[^$]*\$(\d[\d,]*)'] + patterns
    for p in patterns:
        m = re.search(p, text_content, re.IGNORECASE)
        if m:
            return f"${m.group(1)}"
    any_money = re.findall(r"\$(\d[\d,]{3,})", text_content)
    if any_money:
        return f"${any_money[0]}"
    return ""

def extract_sale_type(html_content: str, county_id: str) -> str:
    if county_id != "24":
        return ""
    tree = HTMLParser(html_content)
    text = tree.text()
    for p in [r"Sale Type\s*:\s*([^\n\r]+)", r"Type of Sale\s*:\s*([^\n\r]+)"]:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return norm_text(m.group(1))
    return "Unknown"

# -----------------------------
# HTTP scraper
# -----------------------------
class ForeclosureScraper:
    def __init__(self):
        pass

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=8),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
    )
    def load_search_page(self, client: httpx.Client, county_id: str):
        url = f"{BASE_URL}/Sales/SalesSearch?countyId={county_id}"
        r = client.get(url, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return HTMLParser(r.text)

    def get_hidden_inputs(self, tree: HTMLParser):
        hidden = {}
        for field in ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"]:
            node = tree.css_first(f"input[name={field}]")
            hidden[field] = node.attributes.get("value", "") if node else ""
        return hidden

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=8),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
    )
    def post_search(self, client: httpx.Client, county_id: str, hidden: dict):
        url = f"{BASE_URL}/Sales/SalesSearch?countyId={county_id}"
        payload = {
            "__VIEWSTATE": hidden.get("__VIEWSTATE", ""),
            "__VIEWSTATEGENERATOR": hidden.get("__VIEWSTATEGENERATOR", ""),
            "__EVENTVALIDATION": hidden.get("__EVENTVALIDATION", ""),
            "IsOpen": "true",
            "btnSearch": "Search",
        }
        r = client.post(url, data=payload, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return HTMLParser(r.text)

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=8),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
    )
    def fetch_details(self, client: httpx.Client, property_id: str):
        if not property_id:
            return ""
        url = f"{BASE_URL}/Sales/SaleDetails?PropertyId={property_id}"
        r = client.get(url, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return r.text

    def extract_rows(self, tree: HTMLParser, county):
        # read headers
        headers = [norm_text(th.text()) for th in tree.css("table thead th")]
        if not headers:
            thead_tr = tree.css_first("table thead tr")
            if thead_tr:
                headers = [norm_text(n.text()) for n in thead_tr.css("th")]
        header_lower = [h.lower() for h in headers]

        rows_out = []
        for tr in tree.css("table tbody tr"):
            tds = tr.css("td")
            if not tds:
                continue
            cols = [norm_text(td.text()) for td in tds]
            link = tr.css_first("td a")
            href = link.attributes.get("href", "") if link else ""
            pid = extract_property_id_from_href(href)

            address = ""
            defendant = ""
            sale_date = ""
            for i, h in enumerate(header_lower):
                if "address" in h and i < len(cols) and not address:
                    address = cols[i]
                if "defendant" in h and i < len(cols) and not defendant:
                    defendant = cols[i]
                if "sale" in h and "date" in h and i < len(cols) and not sale_date:
                    sale_date = cols[i]

            # Note: standardize to "Sales Date" (Playwright style)
            rows_out.append({
                "Property ID": pid or "",
                "Address": address,
                "Defendant": defendant,
                "Sales Date": sale_date,
                "County": county["county_name"],
            })

        return rows_out

    def scrape_county(self, county):
        client = httpx.Client(follow_redirects=True, timeout=HTTP_TIMEOUT)
        try:
            logger.info(f"[INFO] Loading search page for {county['county_name']}")
            tree = self.load_search_page(client, county["county_id"])
            hidden = self.get_hidden_inputs(tree)

            logger.info(f"[INFO] Searching {county['county_name']} (all records)")
            results_tree = self.post_search(client, county["county_id"], hidden)

            rows = self.extract_rows(results_tree, county)

            # Enrich with details
            enriched = []
            for r in rows:
                details_html = ""
                if r["Property ID"]:
                    try:
                        details_html = self.fetch_details(client, r["Property ID"])
                    except Exception as e:
                        logger.warning(f"Details fetch failed for {county['county_name']} PID={r['Property ID']}: {e}")
                approx = extract_approx_judgment(details_html, county["county_id"]) if details_html else ""
                sale_type = extract_sale_type(details_html, county["county_id"]) if details_html else ("Unknown" if county["county_id"] == "24" else "")
                r["Approx Judgment"] = approx
                if county["county_id"] == "24":
                    r["Sale Type"] = sale_type
                enriched.append(r)

            logger.info(f"  ✓ {len(enriched)} rows found")
            return enriched
        finally:
            try:
                client.close()
            except:
                pass
            time.sleep(POLITE_DELAY_SECONDS)

# -----------------------------
# Orchestration
# -----------------------------
def run():
    logger.info("Starting foreclosure scraper (httpx lightweight)")

    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    if not spreadsheet_id:
        logger.error("SPREADSHEET_ID is required.")
        return

    try:
        service, sa_email = init_sheets_service_from_env()
    except Exception as e:
        logger.error(f"Failed to initialize Sheets service: {e}")
        return

    sheets = SheetsClient(spreadsheet_id, service)
    try:
        sheets.spreadsheet_info()
    except Exception:
        logger.error("Cannot access spreadsheet. Verify ID and permissions (share with service account).")
        return

    scraper = ForeclosureScraper()

    all_rows_raw = []
    success_cty = 0
    for county in TARGET_COUNTIES:
        try:
            county_rows = scraper.scrape_county(county)
            if county_rows:
                all_rows_raw.extend(county_rows)
                success_cty += 1
                logger.info(f"Successfully processed {county['county_name']} with {len(county_rows)} records")
            else:
                logger.info(f"No rows for {county['county_name']}")
        except Exception as e:
            logger.error(f"Error scraping {county['county_name']}: {e}")

    # Filter to next 30 days
    start = today_et()
    end = start + timedelta(days=30)

    def within_30(row):
        dt = parse_sale_date(row.get("Sales Date", ""))
        return bool(dt and start <= dt.date() <= end)

    filtered = [r for r in all_rows_raw if within_30(r)]
    logger.info(f"Filtered {len(filtered)} records within the 30-day window from {len(all_rows_raw)} total")

    # Standard columns for All Data
    all_cols = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "County", "Sale Type"]

    def normalize(row: dict):
        return {
            "Property ID": row.get("Property ID", ""),
            "Address": row.get("Address", ""),
            "Defendant": row.get("Defendant", ""),
            "Sales Date": row.get("Sales Date", ""),
            "Approx Judgment": row.get("Approx Judgment", ""),
            "Sale Type": row.get("Sale Type", "") if row.get("County") == "New Castle County, DE" else "",
            "County": row.get("County", ""),
        }

    standardized = [normalize(r) for r in filtered]

    # Per-county sheets
    for county in TARGET_COUNTIES:
        tab = county["county_name"][:30]
        sheets.create_sheet_if_missing(tab)
        county_rows = [r for r in standardized if r["County"] == county["county_name"]]
        if not county_rows:
            logger.info(f"No records for {county['county_name']} within window.")
            continue

        # Per-county header: exclude County; include Sale Type only for New Castle (24)
        if county["county_id"] == "24":
            cols = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "Sale Type"]
        else:
            cols = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment"]

        data_rows = [[row.get(c, "") for c in cols] for row in county_rows]
        header = cols

        existing = sheets.get_values(tab, "A:Z")
        if not existing or len(existing) <= 1:
            # first-time write for the tab
            sheets.overwrite_with_snapshot(tab, header, data_rows)
            logger.info(f"Created new sheet for {county['county_name']} with {len(data_rows)} rows")
        else:
            # Build existing Property ID set from old content (first data column)
            header_idx = sheets.detect_header_row_index(existing)

            existing_ids = set()
            for r in existing[header_idx + 1:]:
                if not r:
                    continue
                pid = (r[0] if len(r) > 0 else "").strip()
                if pid:
                    existing_ids.add(pid)

            new_rows = [row for row in data_rows if (row[0] or "").strip() not in existing_ids]
            sheets.prepend_snapshot(tab, header, new_rows)
            if new_rows:
                logger.info(f"Updated sheet for {county['county_name']} with {len(new_rows)} new rows")
            else:
                logger.info(f"No new rows for {county['county_name']}. Snapshot header added.")

    # All Data sheet
    all_sheet = "All Data"
    sheets.create_sheet_if_missing(all_sheet)

    all_data_rows = [[row.get(c, "") for c in all_cols] for row in standardized]
    existing = sheets.get_values(all_sheet, "A:Z")

    if not existing or len(existing) <= 1:
        sheets.overwrite_with_snapshot(all_sheet, all_cols, all_data_rows)
        logger.info(f"Created 'All Data' with {len(all_data_rows)} rows")
    else:
        # Compare (County, Property ID) to detect new rows
        header_idx = sheets.detect_header_row_index(existing)

        # Determine County column index from existing header row if possible
        county_col_idx = 5  # default position in all_cols
        try:
            county_col_idx = existing[header_idx].index("County")
        except Exception:
            pass

        existing_pairs = set()
        for r in existing[header_idx + 1:]:
            if not r:
                continue
            pid = (r[0] if len(r) > 0 else "").strip()
            cty = (r[county_col_idx] if len(r) > county_col_idx else "").strip()
            if pid and cty:
                existing_pairs.add((cty, pid))

        new_rows = []
        for r in all_data_rows:
            pid = (r[0] if len(r) > 0 else "").strip()
            cty = (r[all_cols.index("County")] if len(r) > all_cols.index("County") else "").strip()
            if pid and cty and (cty, pid) not in existing_pairs:
                new_rows.append(r)

        sheets.prepend_snapshot(all_sheet, all_cols, new_rows)
        if new_rows:
            logger.info(f"Updated 'All Data' with {len(new_rows)} new rows")
        else:
            logger.info("No new rows for 'All Data'. Snapshot header added.")

    logger.info(f"[SUCCESS] Completed. Processed {success_cty}/{len(TARGET_COUNTIES)} counties with {len(standardized)} rows in window.")

if __name__ == "__main__":
    run()