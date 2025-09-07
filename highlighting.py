#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Sheets helpers:
- 30-day rolling window
- Highlight new rows (compared to previous snapshot)
- Formatting (bold headers, freeze header row, auto-resize, green highlights)
"""

import os
import json
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# -----------------------------
# Date Utilities
# -----------------------------
def today_str():
    return datetime.now().strftime("%Y-%m-%d")

def parse_date(date_str):
    if not date_str:
        return None
    fmts = [
        "%m/%d/%Y %I:%M %p", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"
    ]
    for f in fmts:
        try:
            return datetime.strptime(date_str.strip(), f)
        except Exception:
            continue
    return None

def is_within_30_days(date_str, today=None):
    today = today or datetime.now()
    dt = parse_date(date_str)
    if not dt:
        return False
    # Next 30 days inclusive
    return today.date() <= dt.date() <= (today + timedelta(days=29)).date()

# -----------------------------
# Credentials
# -----------------------------
def load_service_account_info():
    file_env = os.environ.get("GOOGLE_CREDENTIALS_FILE")
    if file_env and os.path.exists(file_env):
        with open(file_env, "r", encoding="utf-8") as fh:
            return json.load(fh)
    creds_raw = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_raw:
        raise ValueError("Missing GOOGLE_CREDENTIALS or GOOGLE_CREDENTIALS_FILE")
    creds_raw = creds_raw.strip()
    if creds_raw.startswith("{"):
        return json.loads(creds_raw)
    if os.path.exists(creds_raw):
        with open(creds_raw, "r", encoding="utf-8") as fh:
            return json.load(fh)
    raise ValueError("Invalid GOOGLE_CREDENTIALS")

def init_sheets_service_from_env():
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

    def sheet_exists(self, name):
        info = self.svc.get(spreadsheetId=self.spreadsheet_id).execute()
        return any(s["properties"]["title"] == name for s in info.get("sheets", []))

    def create_sheet_if_missing(self, name):
        if not self.sheet_exists(name):
            self.svc.batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={"requests": [{"addSheet": {"properties": {"title": name}}}]}
            ).execute()

    def get_values(self, sheet, rng="A:Z"):
        try:
            res = self.svc.values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{sheet}'!{rng}"
            ).execute()
            return res.get("values", [])
        except Exception:
            return []

    def clear(self, sheet, rng="A:Z"):
        self.svc.values().clear(
            spreadsheetId=self.spreadsheet_id, range=f"'{sheet}'!{rng}"
        ).execute()

    def write_values(self, sheet, values):
        self.svc.values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"'{sheet}'!A1",
            valueInputOption="USER_ENTERED",
            body={"values": values}
        ).execute()

    def get_previous_keys(self, sheet_name):
        vals = self.get_values(sheet_name)
        if not vals or len(vals) < 2:
            return set()
        keys = set()
        header_idx = -1
        for i, row in enumerate(vals):
            if row and row[0].lower().strip() == "property id":
                header_idx = i
                break
        if header_idx == -1:
            return set()
        for row in vals[header_idx + 1:]:
            try:
                key = row[0].strip() or (row[1].strip() + "|" + row[2].strip())
                keys.add(key)
            except IndexError:
                continue
        return keys

    def write_snapshot(self, sheet_name, headers, rows, prev_keys, filter_dates):
        today = datetime.now()
        filtered_rows, new_keys = [], set()

        for r in rows:
            is_valid = True
            if filter_dates:
                date_idx = next((i for i, c in enumerate(headers) if "sale" in c.lower()), -1)
                if date_idx != -1 and len(r) > date_idx:
                    is_valid = is_within_30_days(r[date_idx], today)
                else:
                    is_valid = False
            if is_valid:
                filtered_rows.append(r)
                key = r[0].strip() or (r[1].strip() + "|" + r[2].strip())
                if key not in prev_keys:
                    new_keys.add(key)

        values = [[f"Snapshot - {today_str()}"]] + [headers] + filtered_rows
        self.clear(sheet_name)
        self.write_values(sheet_name, values)
        self.apply_formatting(sheet_name, headers, filtered_rows, new_keys)
        return len(filtered_rows), len(new_keys)

    def apply_formatting(self, sheet_name, headers, rows, new_keys):
        info = self.svc.get(spreadsheetId=self.spreadsheet_id).execute()
        sheet_id = None
        for s in info["sheets"]:
            if s["properties"]["title"] == sheet_name:
                sheet_id = s["properties"]["sheetId"]
        if not sheet_id:
            return

        num_cols = len(headers)
        requests = [
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold"
            }},
            {"updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 2}},
                "fields": "gridProperties.frozenRowCount"
            }},
            {"autoResizeDimensions": {
                "dimensions": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": num_cols}
            }}
        ]
        for idx, row in enumerate(rows):
            key = row[0].strip() or (row[1].strip() + "|" + row[2].strip())
            if key in new_keys:
                sheet_row = idx + 2
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": sheet_row, "endRowIndex": sheet_row + 1,
                            "startColumnIndex": 0, "endColumnIndex": num_cols
                        },
                        "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.85, "green": 0.95, "blue": 0.85}}},
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                })
        if requests:
            self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": requests}).execute()
