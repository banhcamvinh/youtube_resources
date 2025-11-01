import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

# =============================
# 🔧 CONFIGURATION
# =============================
DB_HOST = "xxxx"
DB_USER = "xxxx"
DB_PASS = "xxxx"
DB_SOURCE = "xxxx"
DB_TARGET = "xxxx"

# =============================
# 🧱 EXTRACT
# =============================
print("📤 Extracting data from MySQL...")
engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_SOURCE}")
df_customers = pd.read_sql("SELECT * FROM customers", con=engine)
print("Extracted:")
engine.dispose()
print("🔒 Connection closed.")
print(df_customers)


# =============================
# 📥 LOAD
# =============================
print("\n📥 Loading data back to MySQL...")
engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_TARGET}")
# Ghi dữ liệu vào bảng mới customers_loaded
df_customers.to_sql(
    name="customers_loaded",
    con=engine,
    if_exists="replace",  # hoặc "append" nếu muốn thêm dữ liệu
    index=False
)
print("Data loaded successfully into `customers_loaded`")
engine.dispose()
print("🔒 Connection closed.")


