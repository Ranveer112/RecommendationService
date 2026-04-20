import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import Catalog, CatalogTrainingProgress


pytestmark = pytest.mark.asyncio


class TestRegisterCatalog:
    async def test_register_returns_201_with_ids(
        self, client: AsyncClient, db_session: AsyncSession
    ):
        resp = await client.post("/catalogs/register")
        assert resp.status_code == 201
        data = resp.json()
        assert "catalogId" in data
        assert "secretKey" in data
        assert len(data["catalogId"]) > 0
        assert len(data["secretKey"]) > 0

        # Verify DB state
        stmt = select(Catalog).where(Catalog.catalog_id == data["catalogId"])
        result = await db_session.execute(stmt)
        db_catalog = result.scalars().first()
        assert db_catalog is not None
        assert db_catalog.secret_key == data["secretKey"]

        # Verify training progress initialized to 0
        progress = await db_session.get(CatalogTrainingProgress, data["catalogId"])
        assert progress is not None
        assert progress.untrained_ratings == 0
        assert progress.trained_ratings == 0

    async def test_register_returns_unique_catalogs(
        self, client: AsyncClient, db_session: AsyncSession
    ):
        resp1 = await client.post("/catalogs/register")
        resp2 = await client.post("/catalogs/register")
        assert resp1.status_code == 201
        assert resp2.status_code == 201

        id1 = resp1.json()["catalogId"]
        id2 = resp2.json()["catalogId"]
        assert id1 != id2

        # Verify both exist in DB
        stmt = select(Catalog).where(Catalog.catalog_id.in_([id1, id2]))
        result = await db_session.execute(stmt)
        catalogs = result.scalars().all()
        assert len(catalogs) == 2

    async def test_register_secret_keys_are_unique(
        self, client: AsyncClient, db_session: AsyncSession
    ):
        resp1 = await client.post("/catalogs/register")
        resp2 = await client.post("/catalogs/register")
        key1 = resp1.json()["secretKey"]
        key2 = resp2.json()["secretKey"]
        assert key1 != key2
