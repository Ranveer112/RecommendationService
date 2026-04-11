import asyncio
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base

# Use an in-memory SQLite database for tests
TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def test_engine():
    engine = create_async_engine(TEST_DB_URL, echo=False)

    @event.listens_for(engine.sync_engine, "connect")
    def _set_sqlite_pragma(dbapi_conn: object, connection_record: object) -> None:
        cursor = dbapi_conn.cursor()  # type: ignore[attr-defined]
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture(autouse=True)
async def test_session(test_engine):
    """Override the app's AsyncSessionLocal with a test-scoped session."""
    import app.database as db_module

    test_session_local = async_sessionmaker(
        test_engine, class_=AsyncSession, expire_on_commit=False
    )
    # Patch the session factory used by repositories
    original = db_module.AsyncSessionLocal
    db_module.AsyncSessionLocal = test_session_local
    yield test_session_local
    db_module.AsyncSessionLocal = original

    # Clean up all tables between tests
    async with test_engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(table.delete())


@pytest_asyncio.fixture
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Direct access to the test database session for verification."""
    import app.database as db_module

    async with db_module.AsyncSessionLocal() as session:
        yield session


@pytest_asyncio.fixture
async def client():
    """Async test client that talks to the FastAPI app."""
    from app.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture
async def registered_instance(client: AsyncClient):
    """Register an instance and return (instanceId, secretKey)."""
    resp = await client.post("/instances/register")
    assert resp.status_code == 201
    data = resp.json()
    return data["instanceId"], data["secretKey"]


@pytest_asyncio.fixture
async def instance_with_catalog(client: AsyncClient, registered_instance):
    """Register an instance with a catalog and return (instanceId, secretKey, catalogId)."""
    instance_id, secret_key = registered_instance
    resp = await client.post(
        f"/instances/{instance_id}/catalog",
        json={
            "catalog": [
                {
                    "productId": "seed-1",
                    "name": "Seed Product",
                    "categories": ["cat-a"],
                },
            ],
            "ratings": [],
        },
        headers={"X-Instance-Key": secret_key},
    )
    assert resp.status_code == 200
    # Get the catalog_id that was assigned
    import app.database as db_module

    async with db_module.AsyncSessionLocal() as session:
        instance = await session.get(db_module.Instance, instance_id)
        catalog_id = instance.catalog_id
    return instance_id, secret_key, catalog_id
