from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession
)
from dotenv import load_dotenv
import os

from sqlalchemy.orm import sessionmaker, DeclarativeBase   # ← new import

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL environment variable is not set."
    )

engine = create_async_engine(
    DATABASE_URL,
    echo=False          # ← change to False for production, use env var to toggle
)

AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)


class Base(DeclarativeBase):   # ← replaces declarative_base()
    pass