import pandas as pd
from sqlalchemy import create_engine

# ============================
# DATABASE CONNECTION
# ============================
engine = create_engine(
    "mysql+pymysql://root:Lakshmi@7!localhost/fleximart"
)

# ============================
# EXTRACT
# ============================
customers = pd.read_csv("customers_raw.csv")
products = pd.read_csv("products_raw.csv")
sales = pd.read_csv("sales_raw.csv")

# ============================
# TRANSFORM: CUSTOMERS
# ============================
report = {}

report["customers_total"] = len(customers)
customers = customers.drop_duplicates()
report["customers_duplicates_removed"] = report["customers_total"] - len(customers)

customers['email'] = customers['email'].fillna("not_provided@email.com")
customers['phone'] = customers['phone'].astype(str).str.replace(r"\D", "", regex=True)
customers['phone'] = "+91" + customers['phone'].str[-10:]
customers['registration_date'] = pd.to_datetime(customers['registration_date'], errors='coerce')

# ============================
# TRANSFORM: PRODUCTS
# ============================
report["products_total"] = len(products)
products = products.drop_duplicates()
report["products_duplicates_removed"] = report["products_total"] - len(products)

products['price'] = products['price'].fillna(products['price'].mean())
products['stock_quantity'] = products['stock_quantity'].fillna(0)
products['category'] = products['category'].str.title()

# ============================
# TRANSFORM: SALES
# ============================
report["sales_total"] = len(sales)
sales = sales.drop_duplicates()
report["sales_duplicates_removed"] = report["sales_total"] - len(sales)

sales.rename(columns={'transaction_date': 'order_date'}, inplace=True)
sales['order_date'] = pd.to_datetime(sales['order_date'], errors='coerce')
sales = sales.dropna(subset=['customer_id', 'product_id'])

# ============================
# CREATE ORDERS TABLE DATA
# ============================

orders = sales[['customer_id', 'order_date']].copy()
orders['status'] = 'Completed'

order_items = sales[['transaction_id', 'product_id', 'quantity', 'unit_price']].copy()
order_items.rename(columns={'transaction_id': 'order_id'}, inplace=True)

# ============================
# LOAD TO DATABASE
# ============================
customers.to_sql('customers', engine, if_exists='append', index=False)
products.to_sql('products', engine, if_exists='append', index=False)
orders.to_sql('orders', engine, if_exists='append', index=False)
order_items.to_sql('order_items', engine, if_exists='append', index=False)

# ============================
# DATA QUALITY REPORT
# ============================
report["orders_total"] = len(orders)
report["order_items_total"] = len(order_items)

with open("data_quality_report.txt", "w") as f:
    f.write("DATA QUALITY REPORT\n")
    f.write("====================\n")
    for k, v in report.items():
        f.write(f"{k}: {v}\n")

print("✅ ETL Pipeline Executed Successfully")
print("📄 Data Quality Report Generated")




