import random

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import ProductEmbedding
from app.tasks import process_retrain_catalog


pytestmark = pytest.mark.asyncio


NUM_PRODUCTS = 10
NUM_USERS = 60


class TestTrainingSmoke:
    async def test_training_runs_and_loss_decreases(
        self, client: AsyncClient, registered_catalog, db_session: AsyncSession
    ):
        """Bulk-load products + ratings, run training, verify:
        - No runtime errors
        - Loss decreases over epochs
        - Embeddings are persisted for every product that received ratings
        """
        catalog_id, secret_key = registered_catalog
        headers = {"X-Catalog-Key": secret_key}

        # --- Step 1: Create products ---
        products = [
            {"productId": f"p{i}", "name": f"Product {i}", "categories": ["cat-a"]}
            for i in range(NUM_PRODUCTS)
        ]
        resp = await client.post(
            f"/catalogs/{catalog_id}/products/bulk",
            json=products,
            headers=headers,
        )
        assert resp.status_code == 201
        assert len(resp.json()["created"]) == NUM_PRODUCTS

        # --- Step 2: Generate ratings (each user rates a random subset of products) ---
        random.seed(42)
        ratings = []
        for u in range(NUM_USERS):
            # Each user rates 3-7 random products
            rated_products = random.sample(range(NUM_PRODUCTS), k=random.randint(3, 7))
            for p in rated_products:
                ratings.append(
                    {
                        "userId": f"user-{u}",
                        "productId": f"p{p}",
                        "score": round(random.uniform(1.0, 5.0), 1),
                    }
                )

        resp = await client.put(
            f"/catalogs/{catalog_id}/ratings/bulk",
            json=ratings,
            headers=headers,
        )
        assert resp.status_code == 200
        assert len(resp.json()["saved"]) == len(ratings)

        # --- Step 3: Run training directly ---
        epoch_losses = await process_retrain_catalog(catalog_id)

        # --- Step 4: Assert loss decreased ---
        assert len(epoch_losses) == 30  # all epochs ran
        assert (
            epoch_losses[-1] < epoch_losses[0]
        ), f"Loss did not decrease: first={epoch_losses[0]:.4f}, last={epoch_losses[-1]:.4f}"

        # --- Step 5: Assert embeddings were saved ---
        stmt = select(ProductEmbedding).where(ProductEmbedding.catalog_id == catalog_id)
        result = await db_session.execute(stmt)
        embeddings = result.scalars().all()

        assert len(embeddings) == NUM_PRODUCTS
        for emb in embeddings:
            assert len(emb.embedding) == 15  # LATENT_FACTORS
