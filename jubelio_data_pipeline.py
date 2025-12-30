# -*- coding: utf-8 -*-
"""
Jubelio Hourly Data Pipeline
Option 2: Single-file, single execution flow
LOGIC 100% PRESERVED
"""

# ===========================================================
# IMPORTS
# ===========================================================
import os
import json
import time
import asyncio
import aiohttp
import requests
import nest_asyncio
import pandas as pd

from datetime import datetime, timedelta, UTC
from typing import Optional, Any, Dict, List, Tuple
from pytz import timezone

from google.oauth2 import service_account
from google.cloud import bigquery

print("🔥 RUNNING FILE:", __file__)

# ===========================================================
# ENV
# ===========================================================
email = os.environ["JUBELIO_EMAIL"]
password = os.environ["JUBELIO_PASSWORD"]
GCP_SA_JSON = os.environ["GCP_SA_JSON"]

# ===========================================================
# CONFIG
# ===========================================================
LOGIN_URL = "https://api2.jubelio.com/login"
ORDERS_BASE_URL = "https://api2.jubelio.com/sales/orders/"
INVENTORY_URL = "https://api2.jubelio.com/inventory"
ITEM_DETAIL_URL = "https://api2.jubelio.com/inventory/items"

DAYS_BACK = 1
PAGE_SIZE = 200
MAX_CONCURRENCY = 3

BQ_ORDERS = "the-daily-481107.the_daily.orders"
BQ_ORDER_DETAIL = "the-daily-481107.the_daily.order_detail"
BQ_INVENTORY = "the-daily-481107.the_daily.inventory"

# ===========================================================
# BIGQUERY CLIENT
# ===========================================================
credentials = service_account.Credentials.from_service_account_info(
    json.loads(GCP_SA_JSON)
)
bq_client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id
)

# ===========================================================
# GLOBAL STATE (UNCHANGED LOGIC)
# ===========================================================
token: Optional[str] = None
headers: Dict[str, str] = {}

df_orders = pd.DataFrame()  # Ensuring it's initialized
df_items = pd.DataFrame()  # Ensuring it's initialized
df_inventory = pd.DataFrame()

sem = asyncio.Semaphore(MAX_CONCURRENCY)
GLOBAL_RATE_LIMIT_HITS = 0

# ===========================================================
# LOGIN (UNCHANGED)
# ===========================================================
def setup_environment() -> bool:
    global token, headers, date_from_str, date_to_str # Add these globals

    print("🔐 Attempting Login...")
    try:
        res = requests.post(
            LOGIN_URL,
            json={"email": email, "password": password},
            timeout=30
        )
        res.raise_for_status()
        token = res.json().get("token")
    except Exception as e:
        print(f"❌ Login failed: {e}")
        return False

    if not token:
        print("❌ Login failed: No token returned.")
        return False

    headers = {"Authorization": token}
    
    # --- TIMEZONE FIX START ---
    jakarta_tz = timezone('Asia/Jakarta')
    now_jakarta = datetime.now(jakarta_tz)
    
    # Start from 2 days ago at 00:00:00
    date_from = (now_jakarta - timedelta(days=DAYS_BACK)).replace(hour=0, minute=0, second=0)
    # End tomorrow at 23:59:59 for safety buffer
    date_to = (now_jakarta + timedelta(days=1)).replace(hour=23, minute=59, second=59)

    date_from_str = date_from.strftime("%Y-%m-%dT%H:%M:%SZ")
    date_to_str = date_to.strftime("%Y-%m-%dT%H:%M:%SZ")
    # --- TIMEZONE FIX END ---

    print(f"✅ Login successful! Window: {date_from_str} -> {date_to_str}")
    return True

# ===========================================================
# HELPERS (UNCHANGED)
# ===========================================================
def safe_float(v: Any) -> float:
    try:
        return float(v)
    except:
        return 0.0

async def fetch_json(session: aiohttp.ClientSession, url: str) -> Optional[Dict[str, Any]]:
    global GLOBAL_RATE_LIMIT_HITS, sem
    async with sem:
        backoff = 2
        for _ in range(8):
            try:
                async with session.get(url, timeout=30) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    if resp.status == 429:
                        GLOBAL_RATE_LIMIT_HITS += 1
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 60)
                        continue
                    if resp.status >= 500:
                        await asyncio.sleep(3)
                        continue
                    return None
            except:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
        return None

# ===========================================================
# COERCE NUMERIC COLUMNS TO FLOAT (FIX FOR BIGQUERY)
# ===========================================================
def coerce_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Specify the columns that should be numeric (coercing all possible numeric columns)
    numeric_columns = [
        'sub_total', 'total_disc', 'total_tax', 'grand_total', 'buyer_shipping_cost',
        'shipping_cost', 'insurance_cost', 'service_fee', 'discount_marketplace', 
        'voucher_amount', 'commission_fee', 'campaign_fee', 'settlement_amount',
        'price', 'amount', 'weight_in_gram', 'sell_price'
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df

# ===========================================================
# ORDERS LOGIC (UNCHANGED)
# ===========================================================
def transform_order(d: Dict[str, Any]) -> Dict[str, Any]:
    esc = d.get("escrow_list") or {}

    # --- NEW SETTLEMENT LOGIC (DO NOT CHANGE COLUMN NAME) ---
    escrow_list_settlement = safe_float(esc.get("settlement_amount"))
    escrow_amount_fallback = safe_float(d.get("escrow_amount"))

    # Priority:
    # 1. escrow_list.settlement_amount (Tokopedia / TikTok)
    # 2. escrow_amount (Shopee)
    settlement_amount = (
        escrow_list_settlement
        if escrow_list_settlement > 0
        else escrow_amount_fallback
    )
    # -------------------------------------------------------

    return {
        "id": d.get("salesorder_id"),
        "salesorder_no": d.get("salesorder_no"),
        "channel_status": d.get("channel_status"),
        "transaction_date": d.get("transaction_date"),
        "created_date": d.get("created_date"),
        "store_name": d.get("store_name"),
        "customer_name": d.get("customer_name"),
        "sub_total": safe_float(d.get("sub_total")),
        "total_disc": safe_float(d.get("total_disc")),
        "total_tax": safe_float(d.get("total_tax")),
        "grand_total": safe_float(d.get("grand_total")),
        "payment_method": d.get("payment_method"),
        "buyer_shipping_cost": safe_float(d.get("buyer_shipping_cost")),
        "shipping_cost": safe_float(d.get("shipping_cost")),
        "insurance_cost": safe_float(d.get("insurance_cost")),
        "service_fee": safe_float(d.get("service_fee")),
        "discount_marketplace": safe_float(d.get("discount_marketplace")),
        "voucher_amount": safe_float(d.get("voucher_amount")),
        "commission_fee": safe_float(esc.get("commission_fee")),
        "campaign_fee": safe_float(esc.get("campaign_fee")),

        # ✅ SAME COLUMN NAME, NEW LOGIC
        "settlement_amount": settlement_amount,
    }

async def fetch_orders(start_date: str, end_date: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    async with aiohttp.ClientSession(headers=headers) as session:
        orders = []
        page = 1
        while True:
            url = (
                f"{ORDERS_BASE_URL}?page={page}&pageSize=1000"
                f"&sortDirection=DESC&sortBy=salesorder_id" 
                f"&transactionDateFrom={start_date}"
                f"&transactionDateTo={end_date}"
            )
            data = await fetch_json(session, url)
            rows = (data or {}).get("data") or []
            
            if not rows:
                break
                
            orders.extend(rows)
            
            # Check if we've reached the last page
            total_pages = (data or {}).get("totalPages", 1)
            if page >= total_pages:
                break
                
            page += 1

        if not orders:
            return pd.DataFrame(), pd.DataFrame()

        order_ids = [o["salesorder_id"] for o in orders]
        details = await asyncio.gather(
            *[fetch_json(session, f"{ORDERS_BASE_URL}{oid}") for oid in order_ids]
        )

    order_rows, item_rows = [], []
    for oid, d in zip(order_ids, details):
        if not d:
            continue
        order_rows.append(transform_order(d))
        for it in d.get("items", []):
            it["parent_salesorder_id"] = oid
            item_rows.append(it)

    df_o = pd.DataFrame(order_rows)
    df_i = pd.DataFrame(item_rows)

    if "thumbnail" in df_i.columns:
        df_i.drop(columns=["thumbnail"], inplace=True)

    return df_o, df_i

def run_orders_pipeline(start_date: str, end_date: str):
    global df_orders, df_items
    if not setup_environment():
        raise RuntimeError("Login failed")

    nest_asyncio.apply()
    print("🚀 Fetching orders...")
    df_orders, df_items = asyncio.run(fetch_orders(start_date, end_date))
    print("📊 Orders:", df_orders.shape)
    print("📦 Items:", df_items.shape)

    # Return the dataframes to ensure they are available for the next steps
    return df_orders, df_items


# ===========================================================
# INVENTORY LOGIC (UNCHANGED)
# ===========================================================
async def fetch_inventory_pages():
    all_items = []
    async with aiohttp.ClientSession(headers=headers) as session:
        first = await fetch_json(session, f"{INVENTORY_URL}/")
        total_pages = first.get("totalPages", 1)
        all_items.extend(first.get("data", []))

        tasks = [
            fetch_json(session, f"{INVENTORY_URL}/?page={p}&pageSize={PAGE_SIZE}")
            for p in range(2, total_pages + 1)
        ]
        results = await asyncio.gather(*tasks)
        for r in results:
            if r and "data" in r:
                all_items.extend(r["data"])
    return all_items

async def fetch_item_detail(session, item_id: int):
    return await fetch_json(session, f"{ITEM_DETAIL_URL}/{item_id}")

async def fetch_all_item_details(items):
    details = {}
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [fetch_item_detail(session, i["item_id"]) for i in items]
        results = await asyncio.gather(*tasks)
        for itm, res in zip(items, results):
            if res:
                details[itm["item_id"]] = res
    return details

STORE_MAP = {
    101222: "lazada_sell_price",
    101220: "shopee_sell_price",
    101507: "tokopedia_daily_sell_price",
    104194: "tokopedia_brewhub_sell_price",
    101218: "tiktok_sell_price",
}

def flatten_inventory(items, details_map):
    rows = []
    for itm in items:
        detail = details_map.get(itm["item_id"], {})
        sku = next(
            (s for s in detail.get("product_skus", []) if s.get("item_id") == itm["item_id"]),
            None
        )
        if not sku:
            continue

        # 1. We change the variable name to 'variation'
        # 2. We use str(v.get("value", "")) to prevent the "int found" error
        variation = ", ".join(
            str(v.get("value", "")) for v in (itm.get("variation_values") or [])
        )

        row = {
            "item_id": itm["item_id"],
            "item_group_id": itm.get("item_group_id"),
            "item_name": itm.get("item_name"),
            "item_code": itm.get("item_code"),
            "variant": variation,  # The key must stay "variant" if your BigQuery column is named "variant"
            "stock_on_hand": itm.get("total_stocks", {}).get("on_hand"),
            "stock_on_order": itm.get("total_stocks", {}).get("on_order"),
            "stock_available": itm.get("total_stocks", {}).get("available"),
            "sell_price": sku.get("sell_price"),
        }
        # ... rest of your code ...

        for sid, col in STORE_MAP.items():
            row[col] = None

        for p in sku.get("prices") or []:
            if p.get("store_id") in STORE_MAP:
                row[STORE_MAP[p["store_id"]]] = p.get("sell_price")

        rows.append(row)

    return pd.DataFrame(rows)

def run_inventory_pipeline():
    global df_inventory
    if not setup_environment():
        raise RuntimeError("Login failed")

    nest_asyncio.apply()
    print("🚀 Fetching inventory...")
    items = asyncio.run(fetch_inventory_pages())
    details = asyncio.run(fetch_all_item_details(items))
    df_inventory = flatten_inventory(items, details)
    df_inventory["updated_at"] = pd.Timestamp.now(tz=timezone("Asia/Jakarta"))
    print("📊 Inventory:", df_inventory.shape)

# ===========================================================
# BIGQUERY INGESTION (UNCHANGED LOGIC, FIXED ORDER)
# ===========================================================
def update_orders(df_orders):
    if df_orders.empty:
        return

    # Standardize data
    df_orders["transaction_date"] = pd.to_datetime(df_orders["transaction_date"], errors="coerce")
    df_orders["created_date"] = pd.to_datetime(df_orders["created_date"], errors="coerce")
    df_orders = coerce_numeric_columns(df_orders)

    # SECURE FIX: Instead of deleting everything >= min, 
    # delete only the specific IDs we are re-uploading to avoid gaps.
    ids_to_replace = ",".join(map(str, df_orders["id"].unique()))
    bq_client.query(f"DELETE FROM `{BQ_ORDERS}` WHERE id IN ({ids_to_replace})").result()
    
    bq_client.load_table_from_dataframe(df_orders, BQ_ORDERS).result()
    print(f"✅ Orders updated (replaced {len(df_orders)} specific IDs)")

def update_order_detail(df_items):
    if df_items.empty:
        print("⚠️ No items to update.")
        return

    # 1. Handle Datetimes (Missing in your repo)
    date_cols = ['shipped_date', 'awb_created_date', 'pack_scanned_date', 'pick_scanned_date']
    for col in date_cols:
        if col in df_items.columns:
            df_items[col] = pd.to_datetime(df_items[col], errors='coerce')

    # 2. Robust Numeric Coercion (Fixes the '0.0000' string error)
    numeric_cols = [
        'disc_marketplace', 'price', 'qty', 'qty_in_base', 'disc', 'disc_amount',
        'tax_amount', 'amount', 'sell_price', 'original_price', 'rate', 'weight_in_gram',
        'qty_picked', 'sub_total', 'total_disc', 'total_tax', 'grand_total'
    ]
    for col in numeric_cols:
        if col in df_items.columns:
            # This ensures strings like '0.0000' become actual floats
            df_items[col] = pd.to_numeric(df_items[col], errors='coerce').fillna(0.0)

    # 3. Handle Booleans and Objects (Missing in your repo)
    # Serial numbers often cause issues if they are mixed types
    if 'serials' in df_items.columns:
        df_items['serials'] = df_items['serials'].astype(str)

    boolean_cols = ['is_bundle_deal', 'is_fbm', 'is_free_gift', 'is_bundle', 'use_serial_number', 'use_batch_number']
    for col in boolean_cols:
        if col in df_items.columns:
            df_items[col] = df_items[col].astype(str).str.upper()

    # 4. BigQuery Ingestion
    min_id = int(df_items["parent_salesorder_id"].min())
    bq_client.query(
        f"DELETE FROM `{BQ_ORDER_DETAIL}` WHERE parent_salesorder_id >= {min_id}"
    ).result()
    
    bq_client.load_table_from_dataframe(df_items, BQ_ORDER_DETAIL).result()
    print("✅ Order detail updated")


def upload_inventory(df_inventory):
    bq_client.load_table_from_dataframe(df_inventory, BQ_INVENTORY).result()
    print("✅ Inventory appended")

# ===========================================================
# MAIN — SINGLE SOURCE OF TRUTH
# ===========================================================
if __name__ == "__main__":
    print("=== Jubelio Hourly Pipeline Start ===")

    # 1. Setup env handles the login AND the date strings
    if not setup_environment():
        exit(1)

    # 2. Use the global strings generated in setup_environment
    # These now contain the full T23:59:59Z timestamps
    df_orders, df_items = run_orders_pipeline(date_from_str, date_to_str)
    
    update_orders(df_orders)
    update_order_detail(df_items)

    # Inventory
    run_inventory_pipeline()
    upload_inventory(df_inventory)

    print("=== Jubelio Hourly Pipeline Finished ===")
