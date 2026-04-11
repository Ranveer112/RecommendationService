import pytest
from httpx import AsyncClient


pytestmark = pytest.mark.asyncio


class TestCreateProduct:
    async def test_create_product_success(self, client: AsyncClient, instance_with_catalog):
        instance_id, secret_key = instance_with_catalog
        resp = await client.post(
            f"/instances/{instance_id}/products",
            json={"productId": "p1", "name": "Widget", "categories": ["electronics"]},
            headers={"X-Instance-Key": secret_key},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["productId"] == "p1"
        assert data["name"] == "Widget"
        assert data["categories"] == ["electronics"]

    async def test_create_product_duplicate_rejected(self, client: AsyncClient, instance_with_catalog):
        instance_id, secret_key = instance_with_catalog
        product = {"productId": "dup1", "name": "Dup", "categories": ["cat"]}
        headers = {"X-Instance-Key": secret_key}

        resp1 = await client.post(f"/instances/{instance_id}/products", json=product, headers=headers)
        assert resp1.status_code == 201

        resp2 = await client.post(f"/instances/{instance_id}/products", json=product, headers=headers)
        assert resp2.status_code == 409

    async def test_create_product_missing_categories(self, client: AsyncClient, instance_with_catalog):
        instance_id, secret_key = instance_with_catalog
        resp = await client.post(
            f"/instances/{instance_id}/products",
            json={"productId": "p-nocat", "name": "No Cat"},
            headers={"X-Instance-Key": secret_key},
        )
        assert resp.status_code == 422

    async def test_create_product_empty_categories(self, client: AsyncClient, instance_with_catalog):
        instance_id, secret_key = instance_with_catalog
        resp = await client.post(
            f"/instances/{instance_id}/products",
            json={"productId": "p-empty", "name": "Empty Cat", "categories": []},
            headers={"X-Instance-Key": secret_key},
        )
        assert resp.status_code == 422

    async def test_create_product_unauthorized(self, client: AsyncClient, instance_with_catalog):
        instance_id, _ = instance_with_catalog
        resp = await client.post(
            f"/instances/{instance_id}/products",
            json={"productId": "p-unauth", "name": "X", "categories": ["c"]},
            headers={"X-Instance-Key": "wrong-key"},
        )
        assert resp.status_code == 401


class TestUpdateProduct:
    async def test_update_name(self, client: AsyncClient, instance_with_catalog):
        instance_id, secret_key = instance_with_catalog
        headers = {"X-Instance-Key": secret_key}

        # Create first
        await client.post(
            f"/instances/{instance_id}/products",
            json={"productId": "upd1", "name": "Old Name", "categories": ["a"]},
            headers=headers,
        )

        resp = await client.put(
            f"/instances/{instance_id}/products/upd1",
            json={"name": "New Name"},
            headers=headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "New Name"
        assert data["categories"] == ["a"]  # unchanged

    async def test_update_categories(self, client: AsyncClient, instance_with_catalog):
        instance_id, secret_key = instance_with_catalog
        headers = {"X-Instance-Key": secret_key}

        await client.post(
            f"/instances/{instance_id}/products",
            json={"productId": "upd2", "name": "P", "categories": ["old"]},
            headers=headers,
        )

        resp = await client.put(
            f"/instances/{instance_id}/products/upd2",
            json={"categories": ["new-a", "new-b"]},
            headers=headers,
        )
        assert resp.status_code == 200
        assert resp.json()["categories"] == ["new-a", "new-b"]

    async def test_update_nonexistent_product(self, client: AsyncClient, instance_with_catalog):
        instance_id, secret_key = instance_with_catalog
        resp = await client.put(
            f"/instances/{instance_id}/products/does-not-exist",
            json={"name": "X"},
            headers={"X-Instance-Key": secret_key},
        )
        assert resp.status_code == 404


class TestDeleteProduct:
    async def test_delete_product_success(self, client: AsyncClient, instance_with_catalog):
        instance_id, secret_key = instance_with_catalog
        headers = {"X-Instance-Key": secret_key}

        await client.post(
            f"/instances/{instance_id}/products",
            json={"productId": "del1", "name": "ToDelete", "categories": ["x"]},
            headers=headers,
        )

        resp = await client.delete(
            f"/instances/{instance_id}/products/del1",
            headers=headers,
        )
        assert resp.status_code == 204

        # Verify it's gone — update should 404
        resp2 = await client.put(
            f"/instances/{instance_id}/products/del1",
            json={"name": "Ghost"},
            headers=headers,
        )
        assert resp2.status_code == 404

    async def test_delete_nonexistent_product(self, client: AsyncClient, instance_with_catalog):
        instance_id, secret_key = instance_with_catalog
        resp = await client.delete(
            f"/instances/{instance_id}/products/nope",
            headers={"X-Instance-Key": secret_key},
        )
        assert resp.status_code == 404


class TestBulkCreateProducts:
    async def test_bulk_create_success(self, client: AsyncClient, instance_with_catalog):
        instance_id, secret_key = instance_with_catalog
        products = [
            {"productId": f"bulk-{i}", "name": f"Bulk {i}", "categories": ["cat"]}
            for i in range(5)
        ]
        resp = await client.post(
            f"/instances/{instance_id}/products/bulk",
            json=products,
            headers={"X-Instance-Key": secret_key},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert len(data["created"]) == 5
        assert len(data["skipped"]) == 0

    async def test_bulk_create_skips_duplicates(self, client: AsyncClient, instance_with_catalog):
        instance_id, secret_key = instance_with_catalog
        headers = {"X-Instance-Key": secret_key}

        # Create one product first
        await client.post(
            f"/instances/{instance_id}/products",
            json={"productId": "existing", "name": "Exists", "categories": ["a"]},
            headers=headers,
        )

        products = [
            {"productId": "existing", "name": "Dup", "categories": ["a"]},
            {"productId": "new-one", "name": "New", "categories": ["b"]},
        ]
        resp = await client.post(
            f"/instances/{instance_id}/products/bulk",
            json=products,
            headers=headers,
        )
        assert resp.status_code == 201
        data = resp.json()
        assert len(data["created"]) == 1
        assert data["created"][0]["productId"] == "new-one"
        assert len(data["skipped"]) == 1
        assert data["skipped"][0]["productId"] == "existing"

    async def test_bulk_create_empty_array_rejected(self, client: AsyncClient, instance_with_catalog):
        instance_id, secret_key = instance_with_catalog
        resp = await client.post(
            f"/instances/{instance_id}/products/bulk",
            json=[],
            headers={"X-Instance-Key": secret_key},
        )
        assert resp.status_code == 400

    async def test_bulk_create_exceeds_limit(self, client: AsyncClient, instance_with_catalog):
        instance_id, secret_key = instance_with_catalog
        products = [
            {"productId": f"over-{i}", "name": f"O {i}", "categories": ["c"]}
            for i in range(2001)
        ]
        resp = await client.post(
            f"/instances/{instance_id}/products/bulk",
            json=products,
            headers={"X-Instance-Key": secret_key},
        )
        assert resp.status_code == 400
