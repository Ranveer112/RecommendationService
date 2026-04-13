#!/usr/bin/env python3
"""
Script to load MovieLens dataset and test similarity endpoint.

Requirements:
1. Start the FastAPI server first: uvicorn app.main:app --reload
2. Install dry-run dependencies: pip install -r requirements-dryrun.txt
3. Run: python scripts/load_movielens.py
"""

import asyncio
import os
import zipfile
from pathlib import Path
import httpx
import pandas as pd

BASE_URL = "http://localhost:8000"


async def fetch_movielens():
    """Download MovieLens Small dataset."""
    print("Fetching MovieLens Small dataset...")

    url = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    zip_path = data_dir / "ml-latest-small.zip"

    if zip_path.exists():
        print("Dataset already exists, skipping download")
        return

    async with httpx.AsyncClient() as client:
        response = client.stream("GET", url)
        async with response as r:
            r.raise_for_status()
            with open(zip_path, "wb") as f:
                async for chunk in r.aiter_bytes():
                    f.write(chunk)

    # Extract
    print("Extracting dataset...")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(data_dir)

    print("Dataset ready in data/ml-latest-small/")


async def load_data():
    """Load MovieLens data via API calls."""
    async with httpx.AsyncClient() as client:
        # Create catalog
        print("Creating catalog...")
        resp = await client.post(f"{BASE_URL}/catalogs/register")
        resp.raise_for_status()
        catalog_data = resp.json()
        catalog_id = catalog_data["catalogId"]
        secret_key = catalog_data["secretKey"]

        headers = {"X-Catalog-Key": secret_key}

        # Load movies
        movies_path = Path("data/ml-latest-small/movies.csv")
        movies_df = pd.read_csv(movies_path)

        print(f"Loading {len(movies_df)} movies...")

        # Convert to Product objects and bulk create
        products = []
        for _, row in movies_df.iterrows():
            # Parse genres from "Genre1|Genre2|Genre3" format
            genres = row["genres"].split("|")
            if genres == ["(no genres listed)"]:
                genres = []

            product = {
                "productId": str(row["movieId"]),
                "name": row["title"],
                "categories": genres,
            }
            products.append(product)

        # Bulk create products in batches of 2000
        batch_size = 2000
        total_created = 0
        total_skipped = 0

        for i in range(0, len(products), batch_size):
            batch = products[i : i + batch_size]
            resp = await client.post(
                f"{BASE_URL}/catalogs/{catalog_id}/products/bulk",
                json=batch,
                headers=headers,
            )
            resp.raise_for_status()
            result = resp.json()
            total_created += len(result["created"])
            total_skipped += len(result["skipped"])

            if i % 5000 == 0:
                print(f"Processed {i+len(batch)} products...")

        print(f"Created {total_created} products, skipped {total_skipped}")

        # Load ratings
        ratings_path = Path("data/ml-latest-small/ratings.csv")
        ratings_df = pd.read_csv(ratings_path)

        print(f"Loading {len(ratings_df)} ratings...")

        # Convert to Rating objects and bulk create in batches
        batch_size = 2000
        total_saved = 0
        total_skipped = 0

        for i in range(0, len(ratings_df), batch_size):
            batch_df = ratings_df.iloc[i : i + batch_size]
            ratings = []

            for _, row in batch_df.iterrows():
                rating = {
                    "userId": str(row["userId"]),
                    "productId": str(row["movieId"]),
                    "score": float(row["rating"]),
                }
                ratings.append(rating)

            resp = await client.put(
                f"{BASE_URL}/catalogs/{catalog_id}/ratings/bulk",
                json=ratings,
                headers=headers,
            )
            resp.raise_for_status()
            result = resp.json()
            total_saved += len(result["saved"])
            total_skipped += len(result["skipped"])

            if i % 10000 == 0:
                print(f"Processed {i+len(batch)} ratings...")

        print(f"Saved {total_saved} ratings, skipped {total_skipped}")
        return catalog_id


async def test_similarity(catalog_id: str):
    """Test the similarity endpoint via API calls."""
    print("\nTesting similarity endpoint...")

    async with httpx.AsyncClient() as client:
        # Get a sample product by testing the similarity endpoint with a known movie ID
        # Let's use movie ID 1 (usually a popular movie in MovieLens)
        sample_product_id = "1"

        try:
            # Test the similarity endpoint
            resp = await client.get(
                f"{BASE_URL}/catalogs/{catalog_id}/products/{sample_product_id}/similar?limit=5"
            )
            resp.raise_for_status()
            recommendations = resp.json()

            print(f"Testing similarity for product ID: {sample_product_id}")
            print(f"Top 5 similar products:")

            # Get product details for display
            for i, rec in enumerate(recommendations, 1):
                product_resp = await client.get(
                    f"{BASE_URL}/catalogs/{catalog_id}/products/{rec['productId']}"
                )
                if product_resp.status_code == 200:
                    product_data = product_resp.json()
                    name = product_data.get("name", rec["productId"])
                else:
                    name = rec["productId"]

                print(f"{i}. {name} (score: {rec['score']:.3f})")

        except Exception as e:
            print(f"Error testing similarity: {e}")
            # Try to get any product to test with
            try:
                # This endpoint doesn't exist yet, so we'll just show the error
                print(
                    "Note: You may need to implement a get all products endpoint to browse available products"
                )
            except:
                pass


async def main():
    """Main function."""
    try:
        # Step 1: Fetch dataset
        await fetch_movielens()

        # Step 2: Load data
        catalog_id = await load_data()

        # Step 3: Test similarity
        await test_similarity(catalog_id)

        print(f"\nDone! Catalog ID: {catalog_id}")
        print("You can now test the API endpoints directly.")

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
