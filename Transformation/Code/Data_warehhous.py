import pandas as pd
import os
import sys
sys.stdout.reconfigure(encoding='utf-8')

PATH = r"D:\Ahmed\DataCarft_Material\Python-End-to-End-ETL-Pipeline-Project\Transformation\staging_2" + "\\"
OUT  = r"D:\Ahmed\DataCarft_Material\Python-End-to-End-ETL-Pipeline-Project\Data_model" + "\\"
os.makedirs(OUT, exist_ok=True)

# ── Load source CSVs ──────────────────────────────────────────────────────────
customers   = pd.read_csv(PATH + "customers.csv")
orders      = pd.read_csv(PATH + "orders.csv")
order_items = pd.read_csv(PATH + "order_items.csv")
products    = pd.read_csv(PATH + "products.csv")
staffs      = pd.read_csv(PATH + "staffs.csv")
stores      = pd.read_csv(PATH + "store_lookup.csv")
status_lkp  = pd.read_csv(PATH + "order_status_lookup.csv")

# ══════════════════════════════════════════════════════════════════════════════
# DIM_CUSTOMER
# ══════════════════════════════════════════════════════════════════════════════
dim_customer = customers[[
    "customer_id", "first_name", "last_name",
    "email", "phone", "city", "state", "zip_code"
]].copy()
dim_customer.insert(0, "SK_customer", range(1, len(dim_customer) + 1))
dim_customer.rename(columns={"customer_id": "BK_customer_id"}, inplace=True)
dim_customer["full_name"] = dim_customer["first_name"] + " " + dim_customer["last_name"]
print(f"DIM_CUSTOMER : {dim_customer.shape}")

# ══════════════════════════════════════════════════════════════════════════════
# DIM_PRODUCT
# ══════════════════════════════════════════════════════════════════════════════
dim_product = products[[
    "product_id", "product_name", "brand_name",
    "category_name", "model_year", "list_price"
]].copy()
dim_product.insert(0, "SK_product", range(1, len(dim_product) + 1))
dim_product.rename(columns={"product_id": "BK_product_id"}, inplace=True)
print(f"DIM_PRODUCT  : {dim_product.shape}")

# ══════════════════════════════════════════════════════════════════════════════
# DIM_STAFF
# ══════════════════════════════════════════════════════════════════════════════
dim_staff = staffs[[
    "staff_id", "first_name", "last_name",
    "email", "phone", "active", "store_name", "manager_id"
]].copy()
dim_staff.insert(0, "SK_staff", range(1, len(dim_staff) + 1))
dim_staff.rename(columns={"staff_id": "BK_staff_id"}, inplace=True)
dim_staff["full_name"] = dim_staff["first_name"].fillna("") + " " + dim_staff["last_name"].fillna("")
print(f"DIM_STAFF    : {dim_staff.shape}")

# ══════════════════════════════════════════════════════════════════════════════
# DIM_STORE
# ══════════════════════════════════════════════════════════════════════════════
dim_store = stores[[
    "store_id", "store_name", "store_city", "store_state"
]].copy()
dim_store.insert(0, "SK_store", range(1, len(dim_store) + 1))
dim_store.rename(columns={"store_id": "BK_store_id"}, inplace=True)
print(f"DIM_STORE    : {dim_store.shape}")

# ══════════════════════════════════════════════════════════════════════════════
# DIM_ORDER_STATUS
# ══════════════════════════════════════════════════════════════════════════════
dim_order_status = status_lkp.copy()
dim_order_status.insert(0, "SK_order_status", range(1, len(dim_order_status) + 1))
dim_order_status.rename(columns={"order_status": "BK_order_status"}, inplace=True)
print(f"DIM_ORDER_STATUS : {dim_order_status.shape}")

# ══════════════════════════════════════════════════════════════════════════════
# DIM_DATE  – generated from all dates in the orders table
# ══════════════════════════════════════════════════════════════════════════════
all_dates = pd.to_datetime(
    pd.concat([
        orders["order_date"],
        orders["required_date"],
        orders["shipped_date"]
    ]).dropna().unique()
).sort_values()

dim_date = pd.DataFrame({"full_date": all_dates})
dim_date.insert(0, "SK_date", range(1, len(dim_date) + 1))
dim_date["year"]        = dim_date["full_date"].dt.year
dim_date["quarter"]     = dim_date["full_date"].dt.quarter
dim_date["month"]       = dim_date["full_date"].dt.month
dim_date["month_name"]  = dim_date["full_date"].dt.strftime("%B")
dim_date["week"]        = dim_date["full_date"].dt.isocalendar().week.astype(int)
dim_date["day"]         = dim_date["full_date"].dt.day
dim_date["day_name"]    = dim_date["full_date"].dt.strftime("%A")
dim_date["is_weekend"]  = dim_date["full_date"].dt.dayofweek >= 5
print(f"DIM_DATE     : {dim_date.shape}")

# ══════════════════════════════════════════════════════════════════════════════
# FACT_SALES – join everything to resolve surrogate keys
# ══════════════════════════════════════════════════════════════════════════════

# Helper: date → SK_date lookup
date_lookup = dim_date.set_index("full_date")["SK_date"]

# Start with order_items (grain = 1 row per order line)
fact = order_items[["order_id","item_id","product_id","quantity","list_price","discount",
                     "net_revenue_usd","net_revenue_EGP","net_revenue_EUR",
                     "net_revenue_GBP","net_revenue_AED","net_revenue_SAR"]].copy()

# Bring in order-level columns
fact = fact.merge(
    orders[["order_id","customer_id","order_status","order_date",
            "required_date","shipped_date","store_id","staff_id",
            "is_late_delivery","delivery_status","locality"]],
    on="order_id", how="left"
)

# Resolve surrogate keys
cust_sk  = dim_customer[["SK_customer","BK_customer_id"]].rename(columns={"BK_customer_id":"customer_id"})
prod_sk  = dim_product[["SK_product","BK_product_id"]].rename(columns={"BK_product_id":"product_id"})
staff_sk = dim_staff[["SK_staff","BK_staff_id"]].rename(columns={"BK_staff_id":"staff_id"})
store_sk = dim_store[["SK_store","BK_store_id"]].rename(columns={"BK_store_id":"store_id"})
stat_sk  = dim_order_status[["SK_order_status","BK_order_status"]].rename(columns={"BK_order_status":"order_status"})

fact = (fact
        .merge(cust_sk,  on="customer_id",  how="left")
        .merge(prod_sk,  on="product_id",   how="left")
        .merge(staff_sk, on="staff_id",      how="left")
        .merge(store_sk, on="store_id",      how="left")
        .merge(stat_sk,  on="order_status",  how="left"))

# Resolve date SKs
fact["order_date"]    = pd.to_datetime(fact["order_date"])
fact["required_date"] = pd.to_datetime(fact["required_date"])
fact["shipped_date"]  = pd.to_datetime(fact["shipped_date"])

fact["SK_order_date"]    = fact["order_date"].map(date_lookup)
fact["SK_required_date"] = fact["required_date"].map(date_lookup)
fact["SK_shipped_date"]  = fact["shipped_date"].map(date_lookup)

# Compute measures
fact["revenue_usd"]    = fact["net_revenue_usd"]
fact["discount_amount"]= fact["list_price"] * fact["quantity"] * fact["discount"]

# Final FACT_SALES column selection
fact_sales = fact[[
    "order_id","item_id",
    "SK_customer","SK_product","SK_staff","SK_store","SK_order_status",
    "SK_order_date","SK_required_date","SK_shipped_date",
    "quantity","list_price","discount","discount_amount",
    "revenue_usd","net_revenue_EGP","net_revenue_EUR",
    "net_revenue_GBP","net_revenue_AED","net_revenue_SAR",
    "is_late_delivery","delivery_status","locality"
]].copy()
fact_sales.insert(0, "SK_sale", range(1, len(fact_sales) + 1))
print(f"FACT_SALES   : {fact_sales.shape}")

# ══════════════════════════════════════════════════════════════════════════════
# Save to CSV
# ══════════════════════════════════════════════════════════════════════════════
tables = {
    "DIM_CUSTOMER"    : dim_customer,
    "DIM_PRODUCT"     : dim_product,
    "DIM_STAFF"       : dim_staff,
    "DIM_STORE"       : dim_store,
    "DIM_ORDER_STATUS": dim_order_status,
    "DIM_DATE"        : dim_date,
    "FACT_SALES"      : fact_sales,
}

for name, df in tables.items():
    path = OUT + f"{name}.csv"
    df.to_csv(path, index=False)
    print(f"  [OK] Saved {path}  ({len(df):,} rows x {df.shape[1]} cols)")

print("\n[DONE] DWH build complete.")
# ── Quick sanity check ───────────────────────────────────────────────────────
print("\n── FACT_SALES sample ─────────────────────────────────────────────────")
print(fact_sales[["SK_sale","SK_customer","SK_product","SK_staff","SK_store",
                   "SK_order_date","quantity","revenue_usd"]].head(5).to_string(index=False))
print(f"\nTotal revenue (USD): ${fact_sales['revenue_usd'].sum():,.2f}")
