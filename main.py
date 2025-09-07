#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main Orchestrator
- Runs scraper.py
- Uses highlighting.py for Sheets handling
"""

import os
import sys
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright

from scraper import ForeclosureScraper
from highlighting import SheetsClient, init_sheets_service_from_env

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

async def run():
    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    if not spreadsheet_id:
        sys.exit("Missing SPREADSHEET_ID env variable.")

    service = init_sheets_service_from_env()
    sheets = SheetsClient(spreadsheet_id, service)
    dashboard_data = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        scraper = ForeclosureScraper(sheets)

        for c in TARGET_COUNTIES:
            print(f"Processing {c['county_name']}...")
            tab = c["county_name"][:30]
            sheets.create_sheet_if_missing(tab)

            prev_keys = sheets.get_previous_keys(tab)
            recs = await scraper.scrape_county_sales(page, c)

            if not recs:
                print(f"No records for {c['county_name']}")
                continue

            headers = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment"]
            total, new = sheets.write_snapshot(tab, headers, recs, prev_keys, filter_dates=True)
            print(f"--> {c['county_name']}: {total} active ({new} new)")
            dashboard_data.append([c["county_name"], total, new])

            await asyncio.sleep(POLITE_DELAY_SECONDS)
        await browser.close()

    if dashboard_data:
        sheets.create_sheet_if_missing("Dashboard")
        today = datetime.now().strftime("%Y-%m-%d")
        values = [["Dashboard - " + today], ["County", "Active (30d)", "New Today"]] + dashboard_data
        sheets.clear("Dashboard")
        sheets.write_values("Dashboard", values)

    print("🎉 Scraping + Sheets update complete.")

if __name__ == "__main__":
    asyncio.run(run())
