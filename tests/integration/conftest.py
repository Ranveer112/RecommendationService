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
async def registered_catalog(client: AsyncClient):
    """Register a catalog and return (catalogId, secretKey)."""
    resp = await client.post("/catalogs/register")
    assert resp.status_code == 201
    data = resp.json()
    return data["catalogId"], data["secretKey"]


@pytest_asyncio.fixture
async def catalog_with_product(client: AsyncClient, registered_catalog):
    """Register a catalog with a seed product and return (catalogId, secretKey)."""
    catalog_id, secret_key = registered_catalog
    resp = await client.post(
        f"/catalogs/{catalog_id}/products",
        json={
            "productId": "seed-1",
            "name": "Seed Product",
            "categories": ["cat-a"],
        },
        headers={"X-Catalog-Key": secret_key},
    )
    assert resp.status_code == 201
    return catalog_id, secret_key
