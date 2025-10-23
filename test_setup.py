# test_setup.py
import os
from dotenv import load_dotenv
import pandas as pd
import sqlalchemy
import requests

print("✅ Starting environment test...")

# Load .env file
load_dotenv()

# Test 1: Check if environment variables are loaded
required_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASS", "DB_PORT"]
for var in required_vars:
    value = os.getenv(var)
    if value:
        print(f"🔹 {var} loaded successfully.")
    else:
        print(f"⚠️  {var} not found in .env file.")

# Test 2: Check basic pandas + SQLAlchemy operation
try:
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Luke", "Moore", "ETL"]
    })
    print("\n🧮 Sample DataFrame created:")
    print(df)
except Exception as e:
    print("❌ Pandas test failed:", e)

# Test 3: Check SQLAlchemy version
try:
    print("\n🧱 SQLAlchemy version:", sqlalchemy.__version__)
except Exception as e:
    print("❌ SQLAlchemy not found:", e)

# Test 4: Check internet request (optional)
try:
    response = requests.get("https://api.github.com", timeout=5)
    print(f"\n🌐 Internet access test successful (status: {response.status_code})")
except Exception as e:
    print("⚠️  Internet test failed:", e)

print("\n✅ Environment setup test complete.")
