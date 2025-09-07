#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Foreclosure Scraper Logic (Playwright)
"""

import asyncio
from urllib.parse import urlparse, parse_qs
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

BASE_URL = "https://salesweb.civilview.com/"
MAX_RETRIES = 5

def extract_property_id_from_href(href):
    try:
        q = parse_qs(urlparse(href).query)
        return q.get("PropertyId", [""])[0]
    except:
        return ""

class ForeclosureScraper:
    def __init__(self, sheets_client):
        self.sheets = sheets_client

    async def goto_with_retry(self, page, url):
        for attempt in range(MAX_RETRIES):
            try:
                resp = await page.goto(url, wait_until="networkidle", timeout=60000)
                if resp and resp.status == 200:
                    return resp
            except PlaywrightTimeoutError:
                print(f"Attempt {attempt+1}/{MAX_RETRIES}: Timeout navigating to {url}")
            except Exception as e:
                print(f"Attempt {attempt+1}/{MAX_RETRIES}: Error: {e}")
            await asyncio.sleep(2**attempt)
        print(f"Failed to navigate after {MAX_RETRIES} attempts: {url}")
        return None

    async def scrape_county_sales(self, page, county):
        url = f"{BASE_URL}Sales/SalesSearch?countyId={county['county_id']}"
        await self.goto_with_retry(page, url)
        try:
            await page.wait_for_selector("table.table tbody tr, .no-sales, #noData", timeout=30000)
        except PlaywrightTimeoutError:
            print(f"Timeout waiting for {county['county_name']}")
            return []
        if await page.locator(".no-sales, #noData").count() > 0:
            return []

        colmap = await self.get_columns(page)
        rows = page.locator("table.table tbody tr")
        n = await rows.count()
        results = []
        for i in range(n):
            r = rows.nth(i)
            a = r.locator("td.hidden-print a")
            href = (await a.get_attribute("href")) or ""
            pid = extract_property_id_from_href(href)

            cells = await r.locator("td").all()
            data = {
                "Property ID": pid,
                "Address": "",
                "Defendant": "",
                "Sales Date": "",
                "Approx Judgment": "",
            }
            if "sales_date" in colmap:
                data["Sales Date"] = await cells[colmap["sales_date"]].inner_text()
            if "defendant" in colmap:
                data["Defendant"] = await cells[colmap["defendant"]].inner_text()
            if "address" in colmap:
                data["Address"] = await cells[colmap["address"]].inner_text()

            results.append([
                data["Property ID"],
                data["Address"],
                data["Defendant"],
                data["Sales Date"],
                data["Approx Judgment"],
            ])
        return results

    async def get_columns(self, page):
        cols = {}
        ths = page.locator("table.table thead th")
        n = await ths.count()
        for i in range(n):
            t = (await ths.nth(i).inner_text()).lower()
            if "sale" in t and "date" in t:
                cols["sales_date"] = i
            elif "defendant" in t:
                cols["defendant"] = i
            elif "address" in t:
                cols["address"] = i
            elif "judgment" in t:
                cols["approx_judgment"] = i
        return cols
