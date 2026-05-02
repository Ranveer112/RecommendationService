import pytest
from unittest.mock import AsyncMock, patch
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import CatalogTrainingProgress


pytestmark = pytest.mark.asyncio


class TestRetrainTrigger:
    async def test_enqueue_after_min_untrained_reached(
        self, client: AsyncClient, catalog_with_product, db_session: AsyncSession
    ):
        """Insert exactly RETRAIN_TRIGGER_MIN_UNTRAINED ratings and verify
        a training job is enqueued (trained_ratings==0 path)."""
        from app.routes import RETRAIN_TRIGGER_MIN_UNTRAINED

        catalog_id, secret_key = catalog_with_product
        headers = {"X-Catalog-Key": secret_key}

        n = RETRAIN_TRIGGER_MIN_UNTRAINED
        ratings = [
            {"userId": f"u{i}", "productId": "seed-1", "score": 3.0} for i in range(n)
        ]

        with patch(
            "app.routes.enqueue_retrain_catalog", new_callable=AsyncMock
        ) as mock_enqueue:
            resp = await client.put(
                f"/catalogs/{catalog_id}/ratings/bulk",
                json=ratings,
                headers=headers,
            )
            assert resp.status_code == 200
            assert len(resp.json()["saved"]) == n

            mock_enqueue.assert_called_once_with(catalog_id)

        # Verify DB training progress
        progress = await db_session.get(CatalogTrainingProgress, catalog_id)
        assert progress is not None
        assert progress.untrained_ratings == n
        assert progress.trained_ratings == 0

    async def test_enqueue_after_ratio_threshold_on_trained_catalog(
        self, client: AsyncClient, catalog_with_product, db_session: AsyncSession
    ):
        """After simulating a completed training cycle, insert 2x the original
        amount and verify the retrain job is enqueued again."""
        from app.routes import RETRAIN_TRIGGER_MIN_UNTRAINED

        catalog_id, secret_key = catalog_with_product
        headers = {"X-Catalog-Key": secret_key}

        n = RETRAIN_TRIGGER_MIN_UNTRAINED

        with patch(
            "app.routes.enqueue_retrain_catalog", new_callable=AsyncMock
        ) as mock_enqueue:
            # Step 1: Insert n ratings — triggers initial enqueue
            first_batch = [
                {"userId": f"u{i}", "productId": "seed-1", "score": 3.0}
                for i in range(n)
            ]
            resp = await client.put(
                f"/catalogs/{catalog_id}/ratings/bulk",
                json=first_batch,
                headers=headers,
            )
            assert resp.status_code == 200
            assert len(resp.json()["saved"]) == n
            mock_enqueue.assert_called_once_with(catalog_id)

            # Step 2: Simulate training completion — mark all ratings as trained
            mock_enqueue.reset_mock()

            progress = await db_session.get(CatalogTrainingProgress, catalog_id)
            progress.trained_ratings = n
            progress.untrained_ratings = 0
            await db_session.commit()

            # Step 3: Insert 2*n new ratings (different users to avoid upsert overlap)
            second_batch = [
                {"userId": f"u{i}", "productId": "seed-1", "score": 4.0}
                for i in range(n, n + 2 * n)
            ]
            resp = await client.put(
                f"/catalogs/{catalog_id}/ratings/bulk",
                json=second_batch,
                headers=headers,
            )
            assert resp.status_code == 200
            assert len(resp.json()["saved"]) == 2 * n

            mock_enqueue.assert_called_once_with(catalog_id)

        # Verify final DB state
        progress = await db_session.get(CatalogTrainingProgress, catalog_id)
        await db_session.refresh(progress)
        assert progress.untrained_ratings == 2 * n
        assert progress.trained_ratings == n
