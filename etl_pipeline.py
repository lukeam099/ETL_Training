import os
import json
import pandas as pd
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv

print("🚀 Starting ETL pipeline...")

# --- Load environment variables ---
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "etl_db")

# Create database connection string
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# --- Phase 1: Extract ---
url = "https://api.coindesk.com/v1/bpi/currentprice.json"

def extract_data():
    try:
        print("📡 Trying to fetch live data from CoinDesk...")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        print("✅ Live data successfully fetched!")
        return data
    except Exception as e:
        print(f"⚠️ Could not reach CoinDesk API: {e}")
        print("📁 Falling back to local sample_data.json instead...")
        with open("sample_data.json") as f:
            return json.load(f)

data = extract_data()

# --- Phase 2: Transform ---
print("🔄 Transforming data...")
records = []

for currency, info in data["bpi"].items():
    records.append({
        "currency": currency,
        "rate": info["rate_float"]
    })

df = pd.DataFrame(records)
print(df)

# --- Phase 3: Load ---
print("💾 Loading data into PostgreSQL...")
try:
    df.to_sql("crypto_rates", engine, if_exists="replace", index=False)
    print("✅ Data successfully loaded into 'crypto_rates' table.")
except Exception as e:
    print(f"❌ Database load failed: {e}")

print("🏁 ETL pipeline completed.")
