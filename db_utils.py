import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

def get_engine():
    db_url = os.getenv("DB_URl")
    if not db_url:
        raise ValueError("DB_URL not found")
    return create_engine(db_url, echo=False)

