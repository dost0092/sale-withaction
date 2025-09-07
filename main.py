#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
main.py
Foreclosure Sales Scraper with Google Sheets
- Rolling 30-day window (auto-shifts daily)
- Highlights new rows in green
- Adds per-county, All Data, and Summary sheets
"""

import os
import re
import json
import httpx
import asyncio
from datetime import datetime, timedelta, timezone
from selectolax.parser import HTMLParser

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ---------------------------------------------
# Config
# ---------------------------------------------
BASE_URL = "https://salesweb.civilview.com/"
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

# ---------------------------------------------
# Time helpers (EST)
# ---------------------------------------------
def get_est_now():
    est = timezone(timedelta(hours=-5))
    return datetime.now(est)

def get_est_date():
    return get_est_now().date()

def parse_sale_date(s: str):
    try:
        if not s:
            return None
        if " " in s:
            return datetime.strptime(s, "%m/%d/%Y %I:%M %p")
        return datetime.strptime(s, "%m/%d/%Y")
    except Exception:
        return None

# ---------------------------------------------
# Google Sheets Helpers
# ---------------------------------------------
def load_service_account():
    raw = os.environ.get("GOOGLE_CREDENTIALS")
    if not raw:
        raise SystemExit("✗ GOOGLE_CREDENTIALS not set.")
    info = json.loads(raw)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

class SheetsClient:
    def __init__(self, spreadsheet_id, service):
        self.spreadsheet_id = spreadsheet_id
        self.svc = service.spreadsheets()

    def create_sheet_if_missing(self, sheet_name):
        info = self.svc.get(spreadsheetId=self.spreadsheet_id).execute()
        for s in info.get("sheets", []):
            if s["properties"]["title"] == sheet_name:
                return
        req = {"addSheet": {"properties": {"title": sheet_name}}}
        self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": [req]}).execute()

    def clear(self, sheet_name):
        try:
            self.svc.values().clear(spreadsheetId=self.spreadsheet_id, range=f"'{sheet_name}'!A:Z").execute()
        except HttpError:
            pass

    def write_values(self, sheet_name, values):
        self.svc.values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"'{sheet_name}'!A1",
            valueInputOption="USER_ENTERED",
            body={"values": values}
        ).execute()

    def get_values(self, sheet_name):
        try:
            res = self.svc.values().get(spreadsheetId=self.spreadsheet_id, range=f"'{sheet_name}'!A:Z").execute()
            return res.get("values", [])
        except HttpError:
            return []

    def _get_sheet_id(self, sheet_name):
        info = self.svc.get(spreadsheetId=self.spreadsheet_id).execute()
        for s in info.get("sheets", []):
            if s["properties"]["title"] == sheet_name:
                return s["properties"]["sheetId"]
        return None

    def highlight_rows(self, sheet_name, row_indices):
        sid = self._get_sheet_id(sheet_name)
        if sid is None or not row_indices:
            return
        requests = []
        for i in row_indices:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sid,
                        "startRowIndex": i,
                        "endRowIndex": i + 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": 10
                    },
                    "cell": {"userEnteredFormat": {"backgroundColor": {"red": 1, "green": 1, "blue": 0.6}}},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })
        self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": requests}).execute()

# ---------------------------------------------
# Scraper
# ---------------------------------------------
async def load_search_page(client, county_id):
    url = f"{BASE_URL}Sales/SalesSearch?countyId={county_id}"
    r = await client.get(url)
    r.raise_for_status()
    return HTMLParser(r.text)

def get_hidden_inputs(tree):
    hidden = {}
    for field in ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"]:
        node = tree.css_first(f"input[name={field}]")
        hidden[field] = node.attributes.get("value", "") if node else ""
    return hidden

async def post_search(client, county_id, hidden):
    url = f"{BASE_URL}Sales/SalesSearch?countyId={county_id}"
    payload = {
        "__VIEWSTATE": hidden["__VIEWSTATE"],
        "__VIEWSTATEGENERATOR": hidden["__VIEWSTATEGENERATOR"],
        "__EVENTVALIDATION": hidden["__EVENTVALIDATION"],
        "IsOpen": "true",
        "btnSearch": "Search",
    }
    r = await client.post(url, data=payload)
    r.raise_for_status()
    tree = HTMLParser(r.text)

    headers = [th.text(strip=True) for th in tree.css("table thead th")]
    rows = []
    for tr in tree.css("table tbody tr"):
        cols = [td.text(strip=True) for td in tr.css("td")]
        if cols and len(cols) == len(headers):
            rows.append(dict(zip(headers, cols)))
    return headers, rows
# ---------------------------------------------
# Orchestration
# ---------------------------------------------
async def run():
    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    if not spreadsheet_id:
        raise SystemExit("✗ SPREADSHEET_ID env var required.")

    service = load_service_account()
    sheets = SheetsClient(spreadsheet_id, service)

    all_data = []
    async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
        for county in TARGET_COUNTIES:
            tree = await load_search_page(client, county["county_id"])
            hidden = get_hidden_inputs(tree)
            headers, rows = await post_search(client, county["county_id"], hidden)

            for r in rows:
                r["County"] = county["county_name"]
            all_data.extend(rows)
            await asyncio.sleep(POLITE_DELAY_SECONDS)

    # --- Rolling 30 days filter ---
    today = get_est_date()
    cutoff = today + timedelta(days=30)
    all_data = [r for r in all_data if parse_sale_date(r.get("Sales Date")) and today <= parse_sale_date(r["Sales Date"]).date() <= cutoff]

    if not all_data:
        print("No rows found in 30-day window.")
        return

    # --- Per-county sheets ---
    for county in TARGET_COUNTIES:
        cname = county["county_name"]
        sheets.create_sheet_if_missing(cname)
        county_rows = [r for r in all_data if r["County"] == cname]
        if not county_rows:
            continue
        header = list(county_rows[0].keys())
        existing = sheets.get_values(cname)
        existing_ids = {row[0] for row in existing[1:] if row} if existing else set()

        values = [header] + [list(r.values()) for r in county_rows]
        sheets.clear(cname)
        sheets.write_values(cname, values)

        # highlight new rows
        new_indices = []
        for idx, r in enumerate(county_rows, start=1):
            if r.get("Sheriff #", "") not in existing_ids:
                new_indices.append(idx)
        sheets.highlight_rows(cname, new_indices)

    # --- All Data sheet ---
    all_name = "All Data"
    sheets.create_sheet_if_missing(all_name)
    header = list(all_data[0].keys())
    sheets.clear(all_name)
    sheets.write_values(all_name, [header] + [list(r.values()) for r in all_data])

    # --- Summary ---
    summary = "Summary"
    sheets.create_sheet_if_missing(summary)
    sheets.clear(summary)
    total = len(all_data)
    county_counts = {}
    for r in all_data:
        county_counts[r["County"]] = county_counts.get(r["County"], 0) + 1
    summary_values = [
        ["Summary Dashboard"],
        [f"Snapshot for {get_est_now().strftime('%Y-%m-%d %H:%M EST')}"],
        [""],
        ["Total Properties", total],
        [""],
        ["County", "Count"],
    ] + [[c, n] for c, n in county_counts.items()]
    sheets.write_values(summary, summary_values)

if __name__ == "__main__":
    asyncio.run(run())
