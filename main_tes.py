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
ALL_DATA_SHEET = "All Data"

# -----------------------------
# EST Timezone Helper
# -----------------------------
EST = timezone(timedelta(hours=-5))

def get_est_time():
    return datetime.now(EST)

def get_est_date():
    return get_est_time().date()

def parse_sale_date(date_str):
    if not date_str:
        return None
    try:
        if " " in date_str:
            return datetime.strptime(date_str.strip(), "%m/%d/%Y %I:%M %p").replace(tzinfo=EST)
        else:
            return datetime.strptime(date_str.strip(), "%m/%d/%Y").replace(tzinfo=EST)
    except Exception:
        return None

# -----------------------------
# Google Sheets Helpers
# -----------------------------
def load_service_account_info():
    file_env = os.environ.get("GOOGLE_CREDENTIALS_FILE")
    if file_env and os.path.exists(file_env):
        with open(file_env, "r", encoding="utf-8") as fh:
            return json.load(fh)

    creds_raw = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_raw:
        raise ValueError("GOOGLE_CREDENTIALS or GOOGLE_CREDENTIALS_FILE required")

    creds_raw = creds_raw.strip()
    if creds_raw.startswith("{"):
        return json.loads(creds_raw)
    if os.path.exists(creds_raw):
        with open(creds_raw, "r", encoding="utf-8") as fh:
            return json.load(fh)
    raise ValueError("GOOGLE_CREDENTIALS is invalid")

def init_sheets_service():
    info = load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

# -----------------------------
# Sheets Client
# -----------------------------
class SheetsClient:
    def __init__(self, spreadsheet_id, service):
        self.spreadsheet_id = spreadsheet_id
        self.svc = service.spreadsheets()

    def sheet_exists(self, sheet_name):
        info = self.svc.get(spreadsheetId=self.spreadsheet_id).execute()
        for s in info.get("sheets", []):
            if s["properties"]["title"] == sheet_name:
                return True
        return False

    def create_sheet_if_missing(self, sheet_name):
        if self.sheet_exists(sheet_name):
            return
        req = {"addSheet": {"properties": {"title": sheet_name}}}
        self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": [req]}).execute()
        print(f"✓ Created sheet: {sheet_name}")

    def get_values(self, sheet_name, rng="A:Z"):
        try:
            res = self.svc.values().get(spreadsheetId=self.spreadsheet_id, range=f"'{sheet_name}'!{rng}").execute()
            return res.get("values", [])
        except HttpError:
            return []

    def clear(self, sheet_name, rng="A:Z"):
        try:
            self.svc.values().clear(spreadsheetId=self.spreadsheet_id, range=f"'{sheet_name}'!{rng}").execute()
        except HttpError:
            pass

    def write_values(self, sheet_name, values, start_cell="A1"):
        self.svc.values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"'{sheet_name}'!{start_cell}",
            valueInputOption="USER_ENTERED",
            body={"values": values}
        ).execute()

    def prepend_snapshot(self, sheet_name, header_row, new_rows):
        if not new_rows:
            return
        est_now = get_est_time()
        snapshot_header = [[f"Snapshot for {est_now.strftime('%A - %Y-%m-%d')}"]]
        values = snapshot_header + [header_row] + new_rows + [[""]]
        existing = self.get_values(sheet_name, "A:Z")
        self.clear(sheet_name, "A:Z")
        self.write_values(sheet_name, values + existing)

# -----------------------------
# Helpers
# -----------------------------
def norm_text(s):
    return re.sub(r"\s+", " ", s.strip()) if s else ""

def extract_property_id_from_href(href):
    try:
        return parse_qs(urlparse(href).query).get("PropertyId", [""])[0]
    except Exception:
        return ""

# -----------------------------
# Scraper
# -----------------------------
class ForeclosureScraper:
    def __init__(self, sheets_client):
        self.sheets_client = sheets_client

    async def goto_with_retry(self, page, url, retries=3):
        last_exc = None
        for attempt in range(retries):
            try:
                resp = await page.goto(url, wait_until="networkidle", timeout=60000)
                if resp and 200 <= resp.status < 300:
                    return resp
            except Exception as e:
                last_exc = e
                await asyncio.sleep(2 ** attempt)
        if last_exc:
            raise last_exc

    async def dismiss_banners(self, page):
        selectors = ["button:has-text('Accept')", "button:has-text('I Agree')",
                     "button:has-text('Close')", "button.cookie-accept",
                     "button[aria-label='Close']", ".modal-footer button:has-text('OK')"]
        for sel in selectors:
            try:
                loc = page.locator(sel)
                if await loc.count():
                    await loc.first.click(timeout=1500)
                    await page.wait_for_timeout(200)
            except Exception:
                pass

    async def get_table_columns(self, page):
        colmap = {}
        try:
            ths = page.locator("table.table.table-striped thead tr th")
            for i in range(await ths.count()):
                txt = (await ths.nth(i).inner_text()).strip().lower()
                if "sale" in txt and "date" in txt:
                    colmap["sales_date"] = i
                elif "defendant" in txt:
                    colmap["defendant"] = i
                elif "address" in txt:
                    colmap["address"] = i
        except Exception:
            pass
        return colmap

    async def safe_get_cell_text(self, row, colmap, colname):
        try:
            idx = colmap.get(colname)
            if idx is None:
                return ""
            cells = await row.locator("td").all()
            return re.sub(r"\s+", " ", cells[idx].inner_text().strip()) if idx < len(cells) else ""
        except Exception:
            return ""

    async def scrape_county_sales(self, page, county):
        url = f"{BASE_URL}Sales/SalesSearch?countyId={county['county_id']}"
        print(f"[INFO] Scraping {county['county_name']} -> {url}")
        try:
            await self.goto_with_retry(page, url)
            await self.dismiss_banners(page)
            await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData", timeout=30000)
            colmap = await self.get_table_columns(page)
            rows = page.locator("table.table.table-striped tbody tr")
            results = []
            for i in range(await rows.count()):
                row = rows.nth(i)
                sales_date = await self.safe_get_cell_text(row, colmap, "sales_date")
                defendant = await self.safe_get_cell_text(row, colmap, "defendant")
                address = await self.safe_get_cell_text(row, colmap, "address")
                href = await row.locator("td.hidden-print a").get_attribute("href") or ""
                prop_id = extract_property_id_from_href(href)
                results.append({
                    "Property ID": prop_id,
                    "Address": address,
                    "Defendant": defendant,
                    "Sales Date": sales_date,
                    "Approx Judgment": "",
                    "County": county["county_name"],
                    "Sale Type": "" if county["county_id"] == "24" else None
                })
            return results
        except Exception as e:
            print(f"❌ Error scraping {county['county_name']}: {e}")
            return []

# -----------------------------
# Main
# -----------------------------
async def run():
    start_ts = get_est_time()
    print(f"▶ Starting scrape at {start_ts.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    if not spreadsheet_id:
        print("✗ SPREADSHEET_ID required")
        sys.exit(1)

    service = init_sheets_service()
    sheets = SheetsClient(spreadsheet_id, service)
    first_run = not sheets.sheet_exists(ALL_DATA_SHEET)
    all_data_rows = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        scraper = ForeclosureScraper(sheets)

        for county in TARGET_COUNTIES:
            try:
                data = await scraper.scrape_county_sales(page, county)
                if not data:
                    continue
                df = pd.DataFrame(data)
                county_tab = county["county_name"][:30]
                sheets.create_sheet_if_missing(county_tab)
                header = [col for col in df.columns if col != "County"]
                rows = df.drop(columns=["County"]).astype(str).values.tolist()
                if first_run:
                    sheets.prepend_snapshot(county_tab, header, rows)
                else:
                    sheets.prepend_snapshot(county_tab, header, rows)
                all_data_rows.extend(rows)
                await asyncio.sleep(POLITE_DELAY_SECONDS)
            except Exception as e:
                print(f"❌ Failed {county['county_name']}: {e}")

        # All Data sheet
        sheets.create_sheet_if_missing(ALL_DATA_SHEET)
        header_all = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "County", "Sale Type"]
        sheets.prepend_snapshot(ALL_DATA_SHEET, header_all, all_data_rows)

        await browser.close()
    print("✅ Scrape completed.")

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except Exception as e:
        print("Fatal error:", e)
        sys.exit(1)
