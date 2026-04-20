#!/usr/bin/env python3
"""
Script to run MovieLens evaluation pipeline for similar-products quality.

Requirements:
1. Start the FastAPI server first: uvicorn app.main:app --reload
2. Install dry-run dependencies: pip install -r requirements-dryrun.txt
3. Run: python scripts/evaluate_movielens.py
"""

import argparse
import asyncio
import random
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
    from load_movielens import fetch_movielens as _fetch_movielens

    await _fetch_movielens()


def split_users_train_validation(
    ratings_df: pd.DataFrame,
    validation_fraction: float = 0.2,
    seed: int = 42,
) -> tuple[set[int], set[int]]:
    """Split users into train and validation user sets."""
    if validation_fraction <= 0 or validation_fraction >= 1:
        raise ValueError("validation_fraction must be between 0 and 1")

    unique_users = sorted(ratings_df["userId"].unique().tolist())
    rng = random.Random(seed)
    rng.shuffle(unique_users)

    validation_count = max(1, int(len(unique_users) * validation_fraction))
    validation_users = set(unique_users[:validation_count])
    train_users = set(unique_users[validation_count:])
    return train_users, validation_users


async def load_data_user_split(
    validation_fraction: float = 0.2,
    seed: int = 42,
) -> tuple[str, str, pd.DataFrame]:
    """Load products + train-user ratings only, and return validation ratings."""
    movies_path = Path("data/ml-latest-small/movies.csv")
    ratings_path = Path("data/ml-latest-small/ratings.csv")
    movies_df = pd.read_csv(movies_path)
    ratings_df = pd.read_csv(ratings_path)

    train_users, validation_users = split_users_train_validation(
        ratings_df, validation_fraction=validation_fraction, seed=seed
    )
    train_ratings_df = ratings_df[ratings_df["userId"].isin(train_users)]
    validation_ratings_df = ratings_df[ratings_df["userId"].isin(validation_users)]

    print(
        f"User split: train={len(train_users)} users, "
        f"validation={len(validation_users)} users"
    )
    print(
        f"Ratings split: train={len(train_ratings_df)} rows, "
        f"validation={len(validation_ratings_df)} rows"
    )

    async with httpx.AsyncClient() as client:
        print("Creating catalog for user-split evaluation...")
        resp = await client.post(f"{BASE_URL}/catalogs/register")
        resp.raise_for_status()
        catalog_data: Dict[str, str] = resp.json()
        catalog_id = catalog_data["catalogId"]
        secret_key = catalog_data["secretKey"]
        headers = {"X-Catalog-Key": secret_key}

        print(f"Loading {len(movies_df)} products...")
        products: List[Product] = []
        for _, row in movies_df.iterrows():
            genres = row["genres"].split("|")
            if genres == ["(no genres listed)"]:
                genres = ["Uncategorized"]
            products.append(
                {
                    "productId": str(int(row["movieId"])),
                    "name": row["title"],
                    "categories": genres,
                }
            )

        product_batch_size = 2000
        for i in range(0, len(products), product_batch_size):
            batch = products[i : i + product_batch_size]
            resp = await client.post(
                f"{BASE_URL}/catalogs/{catalog_id}/products/bulk",
                json=batch,
                headers=headers,
            )
            resp.raise_for_status()

        print(f"Loading {len(train_ratings_df)} train ratings...")
        rating_batch_size = 2000
        for i in range(0, len(train_ratings_df), rating_batch_size):
            batch_df = train_ratings_df.iloc[i : i + rating_batch_size]
            ratings_payload = [
                {
                    "userId": str(int(row["userId"])),
                    "productId": str(int(row["movieId"])),
                    "score": float(row["rating"]),
                }
                for _, row in batch_df.iterrows()
            ]
            resp = await client.put(
                f"{BASE_URL}/catalogs/{catalog_id}/ratings/bulk",
                json=ratings_payload,
                headers=headers,
            )
            resp.raise_for_status()

    return catalog_id, secret_key, validation_ratings_df


async def evaluate_overlap_on_validation_users(
    catalog_id: str,
    secret_key: str,
    validation_ratings_df: pd.DataFrame,
    top_k: int = 5,
    max_validation_users: int = 5,
    max_anchors_per_user: int = 10,
    strategy: str = "auto",
) -> None:
    """Evaluate overlap@K on validation users.

    Metric:
    - For each validation user and each anchor product they rated,
      query top-K similar products.
    - Overlap with that same user's rated products is counted.
    - Report average overlap count and average overlap rate.

    Planned user-level protocol (next step):
    - Remove top-K ratings from each validation user as held-out targets.
    - Train on the remainder ratings.
    - Predict K recommendations for each validation user and average overlap
      with the held-out K items.
    """
    headers = {"X-Catalog-Key": secret_key}

    user_groups = validation_ratings_df.groupby("userId")
    validation_user_ids = sorted(validation_ratings_df["userId"].unique().tolist())[
        :max_validation_users
    ]

    per_user_rates: list[float] = []
    per_event_rates: list[float] = []
    per_event_overlaps: list[int] = []
    total_queries = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        for user_id in validation_user_ids:
            user_df = user_groups.get_group(user_id)
            user_rated_products = [
                str(int(movie_id)) for movie_id in user_df["movieId"].unique().tolist()
            ][:max_anchors_per_user]
            anchor_products = user_rated_products
            user_rated_set = set(user_rated_products)

            user_rates: list[float] = []
            for anchor_product_id in anchor_products:
                resp = await client.get(
                    f"{BASE_URL}/catalogs/{catalog_id}/products/{anchor_product_id}/similar?limit={top_k}&strategy={strategy}",
                    headers=headers,
                )
                if resp.status_code != 200:
                    continue

                recommendations = resp.json()
                predicted_ids = {str(rec["productId"]) for rec in recommendations}

                target_set = user_rated_set - {anchor_product_id}
                overlap_count = len(predicted_ids & target_set)
                overlap_rate = overlap_count / top_k

                total_queries += 1
                user_rates.append(overlap_rate)
                per_event_rates.append(overlap_rate)
                per_event_overlaps.append(overlap_count)

            if user_rates:
                per_user_rates.append(sum(user_rates) / len(user_rates))

    if total_queries == 0:
        print("No successful similarity queries; cannot evaluate.")
        return

    avg_overlap_count = sum(per_event_overlaps) / len(per_event_overlaps)
    avg_overlap_rate_event = sum(per_event_rates) / len(per_event_rates)
    avg_overlap_rate_user = (
        sum(per_user_rates) / len(per_user_rates) if per_user_rates else 0.0
    )

    print("\n=== Validation Evaluation (User Split) ===")
    print(f"Evaluated users: {len(per_user_rates)} / {len(validation_user_ids)}")
    print(f"Evaluated anchor queries: {total_queries}")
    print(f"Top-K: {top_k}")
    print(f"Max validation users: {max_validation_users}")
    print(f"Max anchors per user: {max_anchors_per_user}")
    print(f"Average overlap count per query: {avg_overlap_count:.3f}")
    print(f"Average overlap rate per query: {avg_overlap_rate_event:.4f}")
    print(f"Strategy: {strategy}")
    print(f"Average overlap rate per user: {avg_overlap_rate_user:.4f}")


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run MovieLens validation pipeline for similar-products endpoint"
    )
    parser.add_argument(
        "--validation-fraction",
        type=float,
        default=0.2,
        help="Fraction of users reserved for validation (default: 0.2)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for user split (default: 42)",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Top-K recommendations for overlap evaluation (default: 5)",
    )
    parser.add_argument(
        "--max-validation-users",
        type=int,
        default=10,
        help="Max validation users to evaluate (default: 50)",
    )
    parser.add_argument(
        "--max-anchors-per-user",
        type=int,
        default=10,
        help="Max anchor products per validation user (default: 50)",
    )
    parser.add_argument(
        "--strategy",
        type=str,
        default="auto",
        choices=[
            "auto",
            "jaccard",
            "euclidean",
            "pearson",
            "cosine",
            "matrix_factorization",
        ],
        help="Similarity strategy to use (default: auto)",
    )
    args = parser.parse_args()

    await fetch_movielens()
    catalog_id, secret_key, validation_ratings_df = await load_data_user_split(
        validation_fraction=args.validation_fraction,
        seed=args.seed,
    )
    await evaluate_overlap_on_validation_users(
        catalog_id=catalog_id,
        secret_key=secret_key,
        validation_ratings_df=validation_ratings_df,
        top_k=args.top_k,
        max_validation_users=args.max_validation_users,
        max_anchors_per_user=args.max_anchors_per_user,
        strategy=args.strategy,
    )

    print(f"\nCatalog ID: {catalog_id}")
    print(f"Secret Key: {secret_key}")
    print("Evaluation complete.")


if __name__ == "__main__":
    asyncio.run(main())
