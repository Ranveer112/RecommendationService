#!/usr/bin/env python3
"""
Script to load MovieLens dataset and test similarity endpoint.

Requirements:
1. Start the FastAPI server first: uvicorn app.main:app --reload
2. Install dry-run dependencies: pip install -r requirements-dryrun.txt
3. Run: python scripts/load_movielens.py
"""

import argparse
import asyncio
import zipfile
from pathlib import Path
from typing import Dict, List, TypedDict
import httpx
import pandas as pd

BASE_URL = "http://localhost:8000"


class Product(TypedDict):
    productId: str
    name: str
    categories: List[str]


async def fetch_movielens() -> None:
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
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            with open(zip_path, "wb") as f:
                async for chunk in response.aiter_bytes():
                    f.write(chunk)

    # Extract
    print("Extracting dataset...")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(data_dir)

    print("Dataset ready in data/ml-latest-small/")


async def load_data() -> tuple[str, str]:
    """Load MovieLens data via API calls."""
    async with httpx.AsyncClient() as client:
        # Create catalog
        print("Creating catalog...")
        resp = await client.post(f"{BASE_URL}/catalogs/register")
        resp.raise_for_status()
        catalog_data: Dict[str, str] = resp.json()
        catalog_id: str = catalog_data["catalogId"]
        secret_key: str = catalog_data["secretKey"]

        headers = {"X-Catalog-Key": secret_key}

        # Load movies
        movies_path = Path("data/ml-latest-small/movies.csv")
        movies_df = pd.read_csv(movies_path)

        print(f"movies.csv movieId dtype: {movies_df['movieId'].dtype}")
        print(
            f"Sample movie productIds: {[str(mid) for mid in movies_df['movieId'].head(3)]}"
        )
        print(f"Loading {len(movies_df)} movies...")

        # Convert to Product objects and bulk create
        products: List[Product] = []
        for _, row in movies_df.iterrows():
            # Parse genres from "Genre1|Genre2|Genre3" format
            genres = row["genres"].split("|")
            if genres == ["(no genres listed)"]:
                genres = ["Uncategorized"]

            product: Product = {
                "productId": str(int(row["movieId"])),
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

            # Log first few skipped items to see why they're being skipped
            if result["skipped"] and i == 0:
                print(f"First few skipped products: {result['skipped'][:3]}")

            if i % 5000 == 0:
                print(f"Processed {i+len(batch)} products...")

        print(f"Created {total_created} products, skipped {total_skipped}")

        # Load ratings
        ratings_path = Path("data/ml-latest-small/ratings.csv")
        ratings_df = pd.read_csv(ratings_path)

        print(f"ratings.csv movieId dtype: {ratings_df['movieId'].dtype}")
        print(
            f"Sample rating productIds: {[str(mid) for mid in ratings_df['movieId'].head(3)]}"
        )
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
                    "userId": str(int(row["userId"])),
                    "productId": str(int(row["movieId"])),
                    "score": float(row["rating"]),
                }
                ratings.append(rating)

            # Print first batch's first few ratings to inspect actual values
            if i == 0:
                print(f"First few rating payloads: {ratings[:3]}")

            resp = await client.put(
                f"{BASE_URL}/catalogs/{catalog_id}/ratings/bulk",
                json=ratings,
                headers=headers,
            )
            resp.raise_for_status()
            result = resp.json()
            total_saved += len(result["saved"])
            total_skipped += len(result["skipped"])

            # Log first few skipped items to see why they're being skipped
            if result["skipped"] and i == 0:
                print(f"First few skipped ratings: {result['skipped'][:3]}")

            if i % 10000 == 0:
                print(f"Processed {i+len(batch)} ratings...")

        print(f"Saved {total_saved} ratings, skipped {total_skipped}")
        return catalog_id, secret_key


async def test_similarity(
    catalog_id: str, secret_key: str, product_id: str = "1", limit: int = 5
) -> None:
    """Test the similarity endpoint via API calls."""
    print("\nTesting similarity endpoint...")

    async with httpx.AsyncClient() as client:
        sample_product_id = product_id

        try:
            # Test the similarity endpoint
            headers = {"X-Catalog-Key": secret_key}
            resp = await client.get(
                f"{BASE_URL}/catalogs/{catalog_id}/products/{sample_product_id}/similar?limit={limit}",
                headers=headers,
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


async def interactive_mode(catalog_id: str, secret_key: str) -> None:
    """Interactive mode: search movies by name and get similar products."""
    movies_path = Path("data/ml-latest-small/movies.csv")
    if not movies_path.exists():
        print("Movies data not found. Run 'load' first.")
        return

    movies_df = pd.read_csv(movies_path)
    headers = {"X-Catalog-Key": secret_key}

    print("\n=== Interactive Similarity Search ===")
    print("Type a movie name to search (or 'quit' to exit)\n")

    while True:
        query = input("Search: ").strip()
        if query.lower() in ("quit", "exit", "q"):
            print("Bye!")
            break
        if not query:
            continue

        # Fuzzy search: case-insensitive substring match
        matches = movies_df[
            movies_df["title"].str.contains(query, case=False, na=False)
        ].head(10)

        if matches.empty:
            print(f"No movies found matching '{query}'\n")
            continue

        print(f"\nFound {len(matches)} match(es):")
        for idx, (_, row) in enumerate(matches.iterrows(), 1):
            print(f"  {idx}. [{row['movieId']}] {row['title']} ({row['genres']})")

        choice = input("\nSelect a number (or press Enter to search again): ").strip()
        if not choice:
            print()
            continue

        try:
            choice_idx = int(choice) - 1
            if choice_idx < 0 or choice_idx >= len(matches):
                print("Invalid selection\n")
                continue
            selected = matches.iloc[choice_idx]
        except ValueError:
            print("Invalid selection\n")
            continue

        product_id = str(int(selected["movieId"]))
        print(f"\nFinding movies similar to: {selected['title']}")

        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{BASE_URL}/catalogs/{catalog_id}/products/{product_id}/similar?limit=5",
                headers=headers,
            )
            if resp.status_code != 200:
                print(f"Error: {resp.status_code} - {resp.text}\n")
                continue

            recommendations = resp.json()
            if not recommendations:
                print("No similar movies found.\n")
                continue

            print(f"Top {len(recommendations)} similar movies:")
            for i, rec in enumerate(recommendations, 1):
                # Look up movie name from local CSV
                movie_row = movies_df[movies_df["movieId"] == int(rec["productId"])]
                if not movie_row.empty:
                    name = movie_row.iloc[0]["title"]
                else:
                    name = rec["productId"]
                print(f"  {i}. {name} (score: {rec['score']:.3f})")
        print()


async def main() -> None:
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Load MovieLens data or query similarity"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Load command (default)
    subparsers.add_parser("load", help="Load MovieLens dataset and test similarity")

    # Query command
    query_parser = subparsers.add_parser("query", help="Query similarity for a product")
    query_parser.add_argument("--catalog-id", required=True, help="Catalog ID")
    query_parser.add_argument("--secret-key", required=True, help="Catalog secret key")
    query_parser.add_argument(
        "--product-id", required=True, help="Product ID to find similar products for"
    )
    query_parser.add_argument(
        "--limit", type=int, default=5, help="Number of similar products (default: 5)"
    )

    # Interactive command
    interactive_parser = subparsers.add_parser(
        "interactive", help="Interactive mode: search movies and get similar products"
    )
    interactive_parser.add_argument("--catalog-id", required=True, help="Catalog ID")
    interactive_parser.add_argument(
        "--secret-key", required=True, help="Catalog secret key"
    )

    args = parser.parse_args()

    # Default to "load" if no command given
    if args.command is None:
        args.command = "load"

    try:
        if args.command == "load":
            # Step 1: Fetch dataset
            await fetch_movielens()

            # Step 2: Load data
            catalog_id, secret_key = await load_data()

            # Step 3: Test similarity
            await test_similarity(catalog_id, secret_key)

            print(f"\nDone! Catalog ID: {catalog_id}")
            print(f"Secret Key: {secret_key}")
            print("You can now test the API endpoints directly.")

        elif args.command == "query":
            await test_similarity(
                args.catalog_id, args.secret_key, args.product_id, args.limit
            )

        elif args.command == "interactive":
            await interactive_mode(args.catalog_id, args.secret_key)

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
