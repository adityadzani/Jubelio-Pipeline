import os
import json
import pandas as pd
import gspread
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pytz

# --- 1. CONFIGURATION & AUTH ---
# Get JSON credentials from GitHub Secrets environment variable
info_str = os.getenv('GCP_SA_JSON')
if not info_str:
    raise ValueError("GCP_SA_JSON environment variable is not set")

SERVICE_ACCOUNT_INFO = json.loads(info_str)

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/bigquery'
]

creds = service_account.Credentials.from_service_account_info(
    SERVICE_ACCOUNT_INFO, 
    scopes=SCOPES
)

# Initialize Clients
bq_client = bigquery.Client(credentials=creds, project=SERVICE_ACCOUNT_INFO['project_id'])
gc = gspread.authorize(creds)
SHEET_URL = "https://docs.google.com/spreadsheets/d/1l4E-xnQVts1xvDFdD0WmzMcQaRiqC6SpYVwYUuL0ZJ0/edit"

def get_audit_inventory():
    """Fetches inventory from BQ for Today at 14:00 Jakarta Time."""
    query = """
    SELECT
      item_name,
      item_code,
      stock_available,
      variant,
      DATETIME(updated_at, 'Asia/Jakarta') as stock_updated_at,
      sell_price
    FROM `the-daily-481107.the_daily.inventory`
    WHERE
      DATE(updated_at, 'Asia/Jakarta') = CURRENT_DATE('Asia/Jakarta')
      AND EXTRACT(HOUR FROM DATETIME(updated_at, 'Asia/Jakarta')) = 14
    ORDER BY sell_price DESC
    """
    return bq_client.query(query).to_dataframe(create_bqstorage_client=False)

def run_audit_flow():
    jakarta_tz = pytz.timezone('Asia/Jakarta')
    now_jakarta = datetime.now(jakarta_tz)
    print(f"🚀 Starting Audit Flow at {now_jakarta.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. Get BigQuery Data
    df_inventory = get_audit_inventory()
    if df_inventory.empty:
        print("❌ No inventory data found for 14:00 today.")
        return

    # 2. Get Archive Data
    sh = gc.open_by_url(SHEET_URL)
    archive_sheet = sh.worksheet("Archive")
    records = archive_sheet.get_all_records()
    
    done_codes = []
    if records:
        df_archive = pd.DataFrame(records)
        current_month = now_jakarta.strftime('%Y-%m')
        
        # Ensure column exists and filter by month
        if 'audit_timestamp' in df_archive.columns:
            df_archive['audit_date_dt'] = pd.to_datetime(df_archive['audit_timestamp'])
            mask = df_archive['audit_date_dt'].dt.strftime('%Y-%m') == current_month
            done_codes = df_archive[mask]['item_code'].astype(str).unique()

    # 3. Filter & Prioritize
    to_audit = df_inventory[~df_inventory['item_code'].astype(str).isin(done_codes)].copy()
    to_audit = to_audit.sort_values(by='sell_price', ascending=False).head(30)
    
    if to_audit.empty:
        print("ℹ️ All items have been audited this month.")
        return

    # 4. Write to "Audit" Tab
    final_list = to_audit[['item_name', 'item_code', 'variant','stock_available', 'stock_updated_at']]
    final_list['stock_updated_at'] = final_list['stock_updated_at'].astype(str)

    audit_sheet = sh.worksheet("Audit")
    audit_sheet.batch_clear(["A2:E31"])
    audit_sheet.update(
        range_name='A2', 
        values=final_list.values.tolist(), 
        value_input_option='USER_ENTERED'
    )
    
    # Optional: Log sync time in cell F1
    audit_sheet.update(range_name='F1', values=[[f"Last Sync: {now_jakarta.strftime("%Y-%m-%d %H:%M:%S")}"]])
    print(f"✅ Successfully updated Audit tab with {len(final_list)} items.")

if __name__ == "__main__":
    run_audit_flow()
