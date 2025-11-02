import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()  # Load environment variables from a .env file if present

def get_engine() -> create_engine:
    """Create and return a SQLAlchemy engine using environment variables."""
    db_user = os.getenv("DB_USER", "user")
    db_password = os.getenv("DB_PASS", "password")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "database")

    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(connection_string)
    return engine