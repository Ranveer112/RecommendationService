#!/usr/bin/env python3
"""
Script to load MovieLens dataset and test similarity endpoint.
Run with: python load_movielens.py
"""

import asyncio
import os
import zipfile
from pathlib import Path
import httpx
import pandas as pd

from app.database import init_db, AsyncSessionLocal
from app.repositories import CatalogRepository
from app.schemas import Product, Rating


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
    """Load MovieLens data into database."""
    print("Initializing database...")
    await init_db()
    
    # Create catalog
    catalog_id = "movielens-small"
    secret_key = "test-key-for-movielens"
    
    print(f"Creating catalog: {catalog_id}")
    await CatalogRepository.create(catalog_id, secret_key)
    
    # Load movies
    movies_path = Path("data/ml-latest-small/movies.csv")
    movies_df = pd.read_csv(movies_path)
    
    print(f"Loading {len(movies_df)} movies...")
    
    # Convert to Product objects
    products = []
    for _, row in movies_df.iterrows():
        # Parse genres from "Genre1|Genre2|Genre3" format
        genres = row["genres"].split("|")
        if genres == ["(no genres listed)"]:
            genres = []
        
        product = Product(
            productId=str(row["movieId"]),
            name=row["title"],
            categories=genres
        )
        products.append(product)
    
    # Bulk create products
    created, skipped = await CatalogRepository.bulk_create_products(catalog_id, products)
    print(f"Created {len(created)} products, skipped {len(skipped)}")
    
    # Load ratings
    ratings_path = Path("data/ml-latest-small/ratings.csv")
    ratings_df = pd.read_csv(ratings_path)
    
    print(f"Loading {len(ratings_df)} ratings...")
    
    # Convert to Rating objects in batches
    batch_size = 2000
    total_saved = 0
    total_skipped = 0
    
    for i in range(0, len(ratings_df), batch_size):
        batch_df = ratings_df.iloc[i:i+batch_size]
        ratings = []
        
        for _, row in batch_df.iterrows():
            rating = Rating(
                userId=str(row["userId"]),
                productId=str(row["movieId"]),
                score=float(row["rating"])
            )
            ratings.append(rating)
        
        saved, skipped = await CatalogRepository.bulk_upsert_ratings(catalog_id, ratings)
        total_saved += len(saved)
        total_skipped += len(skipped)
        
        if i % 10000 == 0:
            print(f"Processed {i+len(batch_df)} ratings...")
    
    print(f"Saved {total_saved} ratings, skipped {total_skipped}")
    return catalog_id


async def test_similarity(catalog_id: str):
    """Test the similarity endpoint."""
    print("\nTesting similarity endpoint...")
    
    # Get a sample movie
    products = await CatalogRepository.get_all_products(catalog_id)
    if not products:
        print("No products found!")
        return
    
    sample_product = products[0]
    print(f"Testing similarity for: {sample_product.product_name}")
    print(f"Categories: {sample_product.categories}")
    
    # Import here to avoid circular imports
    from app.routes import get_similar_products
    
    # Test the endpoint
    try:
        recommendations = await get_similar_products(
            catalogId=catalog_id,
            productId=sample_product.product_id,
            limit=5
        )
        
        print(f"\nTop 5 similar products:")
        for i, rec in enumerate(recommendations, 1):
            # Get product name for display
            product = await CatalogRepository.get_product(catalog_id, rec.productId)
            name = product.product_name if product else rec.productId
            print(f"{i}. {name} (score: {rec.score:.3f})")
            
    except Exception as e:
        print(f"Error testing similarity: {e}")


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
