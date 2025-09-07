#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
main.py
Foreclosure Sales Scraper with Rolling 30-Day Window and Highlighted New Rows
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
    est = timezone(timedelta(hours=-5))
    return datetime.now(est)

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
        min_sale_date_est = min(sale_dates).astimezone(timezone(timedelta(hours=-5))).date()
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

        # --- Build summary ---
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

        # --- Write to sheet ---
        self.write_values(sheet_name, summary_values)

# -----------------------------
# Scraper helpers
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
# Foreclosure Scraper
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
                continue

    async def safe_get_cell_text(self, row, colmap, colname):
        try:
            idx = colmap.get(colname)
            if idx is None:
                return ""
            cells = await row.locator("td").all()
            if idx < len(cells):
                return re.sub(r"\s+", " ", (await cells[idx].inner_text()).strip())
            return ""
        except Exception:
            return ""

    async def scrape_county_sales(self, page, county):
        url = f"{BASE_URL}Sales/SalesSearch?countyId={county['county_id']}"
        print(f"[INFO] Scraping {county['county_name']}")
        for attempt in range(MAX_RETRIES):
            try:
                await self.goto_with_retry(page, url)
                await self.dismiss_banners(page)
                try:
                    await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData", timeout=30000)
                except PlaywrightTimeoutError:
                    return []

                colmap = await self.get_table_columns(page)
                if not colmap:
                    return []

                rows = page.locator("table.table.table-striped tbody tr")
                n = await rows.count()
                results = []
                for i in range(n):
                    row = rows.nth(i)
                    sales_date = await self.safe_get_cell_text(row, colmap, "sales_date")
                    defendant = await self.safe_get_cell_text(row, colmap, "defendant")
                    address = await self.safe_get_cell_text(row, colmap, "address")
                    prop_id = extract_property_id_from_href(await row.locator("td.hidden-print a").get_attribute("href") or "")

                    results.append({
                        "Property ID": prop_id,
                        "Address": address,
                        "Defendant": defendant,
                        "Sales Date": sales_date,
                        "Approx Judgment": "",
                        "County": county['county_name'],
                        "Sale Type": "" if county["county_id"] != "24" else "Unknown"
                    })
                return results
            except Exception as e:
                await asyncio.sleep(2 ** attempt)
        return []

    async def get_table_columns(self, page):
        try:
            header_ths = page.locator("table.table.table-striped thead tr th")
            if await header_ths.count() == 0:
                header_ths = page.locator("table.table.table-striped tr").first.locator("th")
            colmap = {}
            for i in range(await header_ths.count()):
                htxt = (await header_ths.nth(i).inner_text()).strip().lower()
                if "sale" in htxt and "date" in htxt:
                    colmap["sales_date"] = i
                elif "defendant" in htxt:
                    colmap["defendant"] = i
                elif "address" in htxt:
                    colmap["address"] = i
            return colmap
        except Exception:
            return {}

# -----------------------------
# Orchestration
# -----------------------------
async def run():
    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    if not spreadsheet_id:
        sys.exit("✗ SPREADSHEET_ID env var is required.")
    service = init_sheets_service_from_env()
    sheets_client = SheetsClient(spreadsheet_id, service)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        scraper = ForeclosureScraper(sheets_client)

        all_data_rows = []
        for county in TARGET_COUNTIES:
            county_rows = await scraper.scrape_county_sales(page, county)
            if county_rows:
                all_data_rows.extend(county_rows)
            await asyncio.sleep(POLITE_DELAY_SECONDS)

        # Separate per-county sheets
        for county in TARGET_COUNTIES:
            sheet_name = county['county_name']
            sheets_client.create_sheet_if_missing(sheet_name)
            county_rows = [r for r in all_data_rows if r["County"] == county['county_name']]
            if not county_rows:
                continue
            header_row = list(county_rows[0].keys())
            existing_values = sheets_client.get_values(sheet_name)
            existing_ids = {r[0] for r in existing_values[1:] if r}
            new_rows = [list(r.values()) for r in county_rows if r["Property ID"] not in existing_ids]
            if not existing_values:
                sheets_client.overwrite_with_snapshot(sheet_name, header_row, new_rows)
            else:
                sheets_client.prepend_snapshot(sheet_name, header_row, new_rows, list(range(len(new_rows))))

        # "All Data" sheet
        all_sheet = "All Data"
        sheets_client.create_sheet_if_missing(all_sheet)
        all_header = list(all_data_rows[0].keys()) if all_data_rows else []
        existing_all = sheets_client.get_values(all_sheet)
        existing_all_set = {(r[5], r[0]) for r in existing_all[1:]} if existing_all else set()
        all_new_rows = [list(r.values()) for r in all_data_rows if (r["County"], r["Property ID"]) not in existing_all_set]
        if not existing_all:
            sheets_client.overwrite_with_snapshot(all_sheet, all_header, all_new_rows)
        else:
            sheets_client.prepend_snapshot(all_sheet, all_header, all_new_rows, list(range(len(all_new_rows))))
        # Summary sheet
        sheets_client.write_summary(all_data_rows, [
            r for r in all_data_rows
            if (r["County"], r["Property ID"]) not in existing_all_set
        ])
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())
