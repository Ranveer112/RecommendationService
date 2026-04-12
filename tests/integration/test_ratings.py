import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import Rating as DBRating


pytestmark = pytest.mark.asyncio


class TestUpsertRating:
    async def test_upsert_rating_success(
        self, client: AsyncClient, catalog_with_product, db_session: AsyncSession
    ):
        catalog_id, secret_key = catalog_with_product
        headers = {"X-Catalog-Key": secret_key}

        resp = await client.put(
            f"/catalogs/{catalog_id}/ratings",
            json={"userId": "u1", "productId": "seed-1", "score": 4.5},
            headers=headers,
        )
        assert resp.status_code == 200

        # Verify DB state
        stmt = select(DBRating).where(
            DBRating.catalog_id == catalog_id,
            DBRating.user_id == "u1",
            DBRating.product_id == "seed-1",
        )
        result = await db_session.execute(stmt)
        db_rating = result.scalars().first()
        assert db_rating is not None
        assert float(db_rating.score) == 4.5

    async def test_upsert_rating_updates_existing(
        self, client: AsyncClient, catalog_with_product, db_session: AsyncSession
    ):
        catalog_id, secret_key = catalog_with_product
        headers = {"X-Catalog-Key": secret_key}
        payload = {"userId": "u1", "productId": "seed-1", "score": 3.0}

        # First upsert
        resp1 = await client.put(
            f"/catalogs/{catalog_id}/ratings", json=payload, headers=headers
        )
        assert resp1.status_code == 200

        # Second upsert with new score
        payload["score"] = 5.0
        resp2 = await client.put(
            f"/catalogs/{catalog_id}/ratings", json=payload, headers=headers
        )
        assert resp2.status_code == 200

        # Verify only one rating exists with the updated score
        stmt = select(DBRating).where(
            DBRating.catalog_id == catalog_id,
            DBRating.user_id == "u1",
            DBRating.product_id == "seed-1",
        )
        result = await db_session.execute(stmt)
        ratings = result.scalars().all()
        assert len(ratings) == 1
        assert float(ratings[0].score) == 5.0

    async def test_upsert_rating_unauthorized(
        self, client: AsyncClient, catalog_with_product
    ):
        catalog_id, _ = catalog_with_product
        resp = await client.put(
            f"/catalogs/{catalog_id}/ratings",
            json={"userId": "u1", "productId": "seed-1", "score": 4.0},
            headers={"X-Catalog-Key": "wrong-key"},
        )
        assert resp.status_code == 401

    async def test_upsert_rating_invalid_payload(
        self, client: AsyncClient, catalog_with_product
    ):
        catalog_id, secret_key = catalog_with_product
        resp = await client.put(
            f"/catalogs/{catalog_id}/ratings",
            json={"userId": "u1"},
            headers={"X-Catalog-Key": secret_key},
        )
        assert resp.status_code == 422


class TestDeleteRating:
    async def test_delete_rating_success(
        self, client: AsyncClient, catalog_with_product, db_session: AsyncSession
    ):
        catalog_id, secret_key = catalog_with_product
        headers = {"X-Catalog-Key": secret_key}

        # Create a rating via bulk (which is implemented)
        resp_create = await client.put(
            f"/catalogs/{catalog_id}/ratings/bulk",
            json=[{"userId": "u1", "productId": "seed-1", "score": 4.0}],
            headers=headers,
        )
        assert resp_create.status_code == 200
        assert len(resp_create.json()["saved"]) == 1

        # Verify DB state
        stmt = select(DBRating).where(
            DBRating.catalog_id == catalog_id,
            DBRating.user_id == "u1",
            DBRating.product_id == "seed-1",
        )
        result = await db_session.execute(stmt)
        db_rating = result.scalars().first()
        assert db_rating is not None
        assert float(db_rating.score) == 4.0

        # Delete it
        resp = await client.delete(
            f"/catalogs/{catalog_id}/ratings/u1/seed-1",
            headers=headers,
        )
        assert resp.status_code == 204

        # Verify it's gone from DB
        stmt = select(DBRating).where(
            DBRating.catalog_id == catalog_id,
            DBRating.user_id == "u1",
            DBRating.product_id == "seed-1",
        )
        result = await db_session.execute(stmt)
        db_rating = result.scalars().first()
        assert db_rating is None

    async def test_delete_rating_unauthorized(
        self, client: AsyncClient, catalog_with_product
    ):
        catalog_id, _ = catalog_with_product
        resp = await client.delete(
            f"/catalogs/{catalog_id}/ratings/u1/seed-1",
            headers={"X-Catalog-Key": "wrong-key"},
        )
        assert resp.status_code == 401


class TestBulkUpsertRatings:
    async def test_bulk_upsert_success(
        self, client: AsyncClient, catalog_with_product, db_session: AsyncSession
    ):
        catalog_id, secret_key = catalog_with_product
        ratings = [
            {"userId": "u1", "productId": "seed-1", "score": 4.0},
            {"userId": "u2", "productId": "seed-1", "score": 3.5},
        ]
        resp = await client.put(
            f"/catalogs/{catalog_id}/ratings/bulk",
            json=ratings,
            headers={"X-Catalog-Key": secret_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["saved"]) == 2
        assert len(data["skipped"]) == 0

        # Verify DB state
        stmt = select(DBRating).where(DBRating.catalog_id == catalog_id)
        result = await db_session.execute(stmt)
        db_ratings = result.scalars().all()
        assert len(db_ratings) == 2
        scores = {(r.user_id, r.product_id): float(r.score) for r in db_ratings}
        assert scores[("u1", "seed-1")] == 4.0
        assert scores[("u2", "seed-1")] == 3.5

    async def test_bulk_upsert_skips_invalid_products(
        self, client: AsyncClient, catalog_with_product, db_session: AsyncSession
    ):
        catalog_id, secret_key = catalog_with_product
        ratings = [
            {"userId": "u1", "productId": "seed-1", "score": 4.0},
            {"userId": "u2", "productId": "nonexistent", "score": 3.0},
        ]
        resp = await client.put(
            f"/catalogs/{catalog_id}/ratings/bulk",
            json=ratings,
            headers={"X-Catalog-Key": secret_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["saved"]) == 1
        assert data["saved"][0]["productId"] == "seed-1"
        assert len(data["skipped"]) == 1
        assert data["skipped"][0]["productId"] == "nonexistent"
        assert data["skipped"][0]["reason"] == "Product not found"

        # Verify only the valid rating is in DB
        stmt = select(DBRating).where(DBRating.catalog_id == catalog_id)
        result = await db_session.execute(stmt)
        db_ratings = result.scalars().all()
        assert len(db_ratings) == 1
        assert db_ratings[0].product_id == "seed-1"

    async def test_bulk_upsert_updates_existing_scores(
        self, client: AsyncClient, catalog_with_product, db_session: AsyncSession
    ):
        catalog_id, secret_key = catalog_with_product
        headers = {"X-Catalog-Key": secret_key}

        # First bulk insert
        resp1 = await client.put(
            f"/catalogs/{catalog_id}/ratings/bulk",
            json=[{"userId": "u1", "productId": "seed-1", "score": 2.0}],
            headers=headers,
        )
        assert resp1.status_code == 200

        # Second bulk with updated score
        resp2 = await client.put(
            f"/catalogs/{catalog_id}/ratings/bulk",
            json=[{"userId": "u1", "productId": "seed-1", "score": 5.0}],
            headers=headers,
        )
        assert resp2.status_code == 200
        assert len(resp2.json()["saved"]) == 1

        # Verify only one rating exists with updated score
        stmt = select(DBRating).where(
            DBRating.catalog_id == catalog_id,
            DBRating.user_id == "u1",
            DBRating.product_id == "seed-1",
        )
        result = await db_session.execute(stmt)
        db_ratings = result.scalars().all()
        assert len(db_ratings) == 1
        assert float(db_ratings[0].score) == 5.0

    async def test_bulk_upsert_empty_array_rejected(
        self, client: AsyncClient, catalog_with_product
    ):
        catalog_id, secret_key = catalog_with_product
        resp = await client.put(
            f"/catalogs/{catalog_id}/ratings/bulk",
            json=[],
            headers={"X-Catalog-Key": secret_key},
        )
        assert resp.status_code == 400

    async def test_bulk_upsert_exceeds_limit(
        self, client: AsyncClient, catalog_with_product
    ):
        catalog_id, secret_key = catalog_with_product
        ratings = [
            {"userId": f"u{i}", "productId": "seed-1", "score": 1.0}
            for i in range(2001)
        ]
        resp = await client.put(
            f"/catalogs/{catalog_id}/ratings/bulk",
            json=ratings,
            headers={"X-Catalog-Key": secret_key},
        )
        assert resp.status_code == 400

    async def test_bulk_upsert_unauthorized(
        self, client: AsyncClient, catalog_with_product
    ):
        catalog_id, _ = catalog_with_product
        resp = await client.put(
            f"/catalogs/{catalog_id}/ratings/bulk",
            json=[{"userId": "u1", "productId": "seed-1", "score": 4.0}],
            headers={"X-Catalog-Key": "wrong-key"},
        )
        assert resp.status_code == 401

    async def test_bulk_upsert_catalog_not_found(self, client: AsyncClient):
        resp = await client.put(
            "/catalogs/nonexistent-id/ratings/bulk",
            json=[{"userId": "u1", "productId": "p1", "score": 4.0}],
            headers={"X-Catalog-Key": "some-key"},
        )
        assert resp.status_code == 404
