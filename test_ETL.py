import os
import json
import pandas as pd
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv
import csv
#Load invironment variables
load_dotenv()


DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "etl_db")

# Create database connection string
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

#Extract From JSON file
df = pd.read_csv("sales_data.csv")
df['date'] = pd.to_datetime(df['date'])
df.to_sql("Sales_Data", engine, if_exists="replace", index=False)

#Preview the first 5 rows
print(df.head())






