"""
conftest.py — shared pytest configuration for the test suite.

Three things happen here before any test file is imported:

1. JSONB patch: Event.payload uses PostgreSQL's JSONB type. SQLite doesn't
   understand it. We patch sqlalchemy.dialects.postgresql.JSONB → JSON so
   SQLite can create all tables without errors.

2. Database stub: app/core/database.py creates an async engine at module level
   and raises RuntimeError if DATABASE_URL is missing. We replace the whole
   module in sys.modules with a fake that provides a real synchronous
   SQLAlchemy Base backed by in-memory SQLite. Every model that does
   `from app.core.database import Base` gets our test Base automatically.

3. External service stubs: OpenTelemetry (Jaeger) and Celery (RabbitMQ/Redis)
   are replaced with MagicMocks so no running services are needed.
"""

import sys
import types
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# 1. Patch JSONB → JSON before any model is imported.
#    app/models/event.py does: from sqlalchemy.dialects.postgresql import JSONB
#    SQLite can't create a JSONB column, but JSON works fine.
# ---------------------------------------------------------------------------
from sqlalchemy import JSON
import sqlalchemy.dialects.postgresql as _pg_dialect
_pg_dialect.JSONB = JSON  # type: ignore[attr-defined]

# Also patch the module-level name that SQLAlchemy itself uses internally:
try:
    import sqlalchemy.dialects.postgresql.json as _pg_json
    _pg_json.JSONB = JSON  # type: ignore[attr-defined]
except Exception:
    pass

# ---------------------------------------------------------------------------
# 2. Build a real synchronous SQLite Base for tests.
#    Models import `Base` from app.core.database — we intercept that import.
# ---------------------------------------------------------------------------
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

class Base(DeclarativeBase):
    pass

_test_engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
_TestSessionLocal = sessionmaker(bind=_test_engine)

_fake_db_module = types.ModuleType("app.core.database")
_fake_db_module.Base = Base                          # type: ignore[attr-defined]
_fake_db_module.engine = _test_engine               # type: ignore[attr-defined]
_fake_db_module.AsyncSessionLocal = MagicMock()     # not used in sync tests

sys.modules["app.core.database"] = _fake_db_module

# ---------------------------------------------------------------------------
# 3. Stub OpenTelemetry (no Jaeger needed).
# ---------------------------------------------------------------------------
sys.modules.setdefault("app.core.tracing", MagicMock())

# ---------------------------------------------------------------------------
# 4. Stub Celery / worker (no RabbitMQ or Redis needed).
# ---------------------------------------------------------------------------
sys.modules.setdefault("worker.celery_app", MagicMock())
sys.modules.setdefault("worker.tasks", MagicMock())
