"""Database connection for Azure SQL Database."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
import bingefriend.shows.infra_azure.config as config

# 1. Get the connection string
SQLALCHEMY_DATABASE_URL = config.SQLALCHEMY_CONNECTION_STRING
if not SQLALCHEMY_DATABASE_URL:
    raise ValueError("AZURE_SQL_CONNECTION_STRING is not set in the configuration.")

# 2. Create the SQLAlchemy engine
engine = create_engine(SQLALCHEMY_DATABASE_URL)

# 3. Create a SessionLocal class (session factory)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 4. Create a Base class (even if models are elsewhere, it's standard)
Base = declarative_base()
