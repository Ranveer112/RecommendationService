from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError
from app.database import (
    AsyncSessionLocal,
    Catalog,
    CatalogTrainingProgress as DBCatalogTrainingProgress,
    Product as DBProduct,
    ProductEmbedding as DBProductEmbedding,
    Rating as DBRating,
)
from app.schemas import Product, ProductUpdate, Rating


class CatalogRepository:

    @staticmethod
    async def create(catalog_id: str, secret_key: str) -> None:
        async with AsyncSessionLocal() as session:
            catalog = Catalog(catalog_id=catalog_id, secret_key=secret_key)
            session.add(catalog)
            await session.commit()

    @staticmethod
    async def create_training_progress(catalog_id: str) -> None:
        """Create an initial training progress row for a catalog."""
        async with AsyncSessionLocal() as session:
            session.add(
                DBCatalogTrainingProgress(
                    catalog_id=catalog_id,
                    untrained_ratings=0,
                    trained_ratings=0,
                )
            )
            await session.commit()

    @staticmethod
    async def get_training_progress(
        catalog_id: str,
    ) -> DBCatalogTrainingProgress | None:
        """Read training progress row for a catalog."""
        async with AsyncSessionLocal() as session:
            return await session.get(DBCatalogTrainingProgress, catalog_id)

    @staticmethod
    async def increment_untrained_ratings(
        catalog_id: str, count: int
    ) -> DBCatalogTrainingProgress:
        """Increment untrained_ratings by count. Returns the updated row."""
        async with AsyncSessionLocal() as session:
            progress = await session.get(DBCatalogTrainingProgress, catalog_id)
            if progress is None:
                progress = DBCatalogTrainingProgress(
                    catalog_id=catalog_id,
                    untrained_ratings=0,
                    trained_ratings=0,
                )
                session.add(progress)
            progress.untrained_ratings += count
            await session.commit()
            await session.refresh(progress)
            return progress

    @staticmethod
    async def get_by_id(catalog_id: str) -> Catalog | None:
        async with AsyncSessionLocal() as session:
            result = await session.get(Catalog, catalog_id)
            return result

    @staticmethod
    async def get_total_users(catalog_id: str) -> int:
        async with AsyncSessionLocal() as session:
            stmt = (
                select(func.count())
                .select_from(DBRating)
                .where(DBRating.catalog_id == catalog_id)
            )
            result = await session.execute(stmt)
            count = result.scalar()
            return 0 if count is None else count

    @staticmethod
    async def get_total_products(catalog_id: str) -> int:
        async with AsyncSessionLocal() as session:
            stmt = (
                select(func.count())
                .select_from(DBProduct)
                .where(DBProduct.catalog_id == catalog_id)
            )
            result = await session.execute(stmt)
            count = result.scalar()
            return 0 if count is None else count

    @staticmethod
    async def get_product(catalog_id: str, product_id: str) -> DBProduct | None:
        async with AsyncSessionLocal() as session:
            stmt = select(DBProduct).where(
                DBProduct.catalog_id == catalog_id,
                DBProduct.product_id == product_id,
            )
            result = await session.execute(stmt)
            return result.scalars().first()

    @staticmethod
    async def create_product(catalog_id: str, product: Product) -> DBProduct | None:
        async with AsyncSessionLocal() as session:
            db_product = DBProduct(
                product_id=product.productId,
                product_name=product.name,
                catalog_id=catalog_id,
                categories=product.categories,
            )
            session.add(db_product)
            try:
                await session.commit()
            except IntegrityError:
                return None
            await session.refresh(db_product)
            return db_product

    @staticmethod
    async def update_product(
        catalog_id: str, product_id: str, update: ProductUpdate
    ) -> DBProduct | None:
        async with AsyncSessionLocal() as session:
            stmt = select(DBProduct).where(
                DBProduct.catalog_id == catalog_id,
                DBProduct.product_id == product_id,
            )
            result = await session.execute(stmt)
            db_product = result.scalars().first()
            if db_product is None:
                return None
            if update.name is not None:
                db_product.product_name = update.name
            if update.categories is not None:
                db_product.categories = update.categories
            await session.commit()
            await session.refresh(db_product)
            return db_product

    @staticmethod
    async def delete_product(catalog_id: str, product_id: str) -> bool:
        """Delete a product."""
        async with AsyncSessionLocal() as session:
            stmt = select(DBProduct).where(
                DBProduct.catalog_id == catalog_id,
                DBProduct.product_id == product_id,
            )
            result = await session.execute(stmt)
            db_product = result.scalars().first()
            if db_product is None:
                return False
            await session.delete(db_product)
            await session.commit()
            return True

    @staticmethod
    async def count_product_ratings(catalog_id: str, product_id: str) -> int:
        """Count ratings for a specific product."""
        async with AsyncSessionLocal() as session:
            stmt = (
                select(func.count())
                .select_from(DBRating)
                .where(
                    DBRating.catalog_id == catalog_id,
                    DBRating.product_id == product_id,
                )
            )
            result = await session.execute(stmt)
            return result.scalar() or 0

    @staticmethod
    async def bulk_create_products(
        catalog_id: str, products: list[Product]
    ) -> tuple[list[Product], list[tuple[str, str]]]:
        """Create products in bulk, skipping duplicates (optimized: 1 SELECT + 1 INSERT).
        Returns (created_products_as_schemas, skipped_as_tuples_of_id_and_reason)."""
        from sqlalchemy import insert

        async with AsyncSessionLocal() as session:
            # 1 SELECT: fetch all existing product IDs for this catalog
            stmt = select(DBProduct.product_id).where(
                DBProduct.catalog_id == catalog_id
            )
            result = await session.execute(stmt)
            existing_ids: set[str] = {row[0] for row in result.all()}

            to_insert: list[dict[str, object]] = []
            created: list[Product] = []
            skipped: list[tuple[str, str]] = []
            seen: set[str] = set()

            for product in products:
                if product.productId in existing_ids or product.productId in seen:
                    skipped.append((product.productId, "Duplicate productId"))
                    continue
                seen.add(product.productId)
                to_insert.append(
                    {
                        "product_id": product.productId,
                        "product_name": product.name,
                        "catalog_id": catalog_id,
                        "categories": product.categories,
                    }
                )
                created.append(product)

            # 1 INSERT: bulk insert all new rows at once
            if to_insert:
                await session.execute(insert(DBProduct), to_insert)
            await session.commit()
        return created, skipped

    @staticmethod
    async def get_all_products(
        catalog_id: str, exclude_product_id: str | None = None
    ) -> list[DBProduct]:
        async with AsyncSessionLocal() as session:
            stmt = select(DBProduct).where(DBProduct.catalog_id == catalog_id)
            if exclude_product_id is not None:
                stmt = stmt.where(DBProduct.product_id != exclude_product_id)
            result = await session.execute(stmt)
            return list(result.scalars().all())

    @staticmethod
    async def upsert_rating(catalog_id: str, rating: Rating) -> bool:
        """Upsert a single rating. Returns False if the product does not exist."""
        async with AsyncSessionLocal() as session:
            # Check product exists
            stmt = select(DBProduct.product_id).where(
                DBProduct.catalog_id == catalog_id,
                DBProduct.product_id == rating.productId,
            )
            result = await session.execute(stmt)
            if result.scalars().first() is None:
                return False

            # Check for existing rating
            stmt = select(DBRating).where(
                DBRating.catalog_id == catalog_id,
                DBRating.user_id == rating.userId,
                DBRating.product_id == rating.productId,
            )
            result = await session.execute(stmt)
            db_rating = result.scalars().first()

            if db_rating is not None:
                db_rating.score = str(rating.score)
            else:
                session.add(
                    DBRating(
                        product_id=rating.productId,
                        user_id=rating.userId,
                        score=str(rating.score),
                        catalog_id=catalog_id,
                    )
                )
            await session.commit()

        return True

    @staticmethod
    async def delete_rating(catalog_id: str, user_id: str, product_id: str) -> bool:
        """Delete a single rating. Returns False if the rating does not exist."""
        async with AsyncSessionLocal() as session:
            stmt = select(DBRating).where(
                DBRating.catalog_id == catalog_id,
                DBRating.user_id == user_id,
                DBRating.product_id == product_id,
            )
            result = await session.execute(stmt)
            db_rating = result.scalars().first()
            if db_rating is None:
                return False
            await session.delete(db_rating)
            await session.commit()
            return True

    @staticmethod
    async def bulk_upsert_ratings(
        catalog_id: str, ratings: list[Rating]
    ) -> tuple[list[Rating], list[tuple[str, str, str]]]:
        """Upsert ratings in bulk, skipping those that reference non-existent products.
        Returns (saved_ratings, skipped_tuples).
        """
        from sqlalchemy import bindparam, insert, update

        async with AsyncSessionLocal() as session:
            # Fetch existing product IDs for this catalog
            stmt = select(DBProduct.product_id).where(
                DBProduct.catalog_id == catalog_id
            )
            result = await session.execute(stmt)
            valid_product_ids: set[str] = {row[0] for row in result.all()}

            # Fetch existing ratings keyed by (user_id, product_id)
            stmt = select(DBRating).where(DBRating.catalog_id == catalog_id)
            result = await session.execute(stmt)
            existing_ratings: dict[tuple[str, str], DBRating] = {
                (r.user_id, r.product_id): r for r in result.scalars().all()
            }

            saved: list[Rating] = []
            skipped: list[tuple[str, str, str]] = []
            to_insert: list[dict[str, object]] = []
            to_update: list[dict[str, object]] = []
            seen_keys: set[tuple[str, str]] = set()

            for rating in ratings:
                if rating.productId not in valid_product_ids:
                    skipped.append(
                        (rating.userId, rating.productId, "Product not found")
                    )
                    continue

                key = (rating.userId, rating.productId)
                if key in seen_keys:
                    continue  # skip duplicate within this batch
                seen_keys.add(key)

                if key in existing_ratings:
                    db_rating = existing_ratings[key]
                    to_update.append({"_id": db_rating.id, "_score": str(rating.score)})
                else:
                    to_insert.append(
                        {
                            "product_id": rating.productId,
                            "user_id": rating.userId,
                            "score": str(rating.score),
                            "catalog_id": catalog_id,
                        }
                    )

                saved.append(rating)

            # 1 bulk UPDATE for existing ratings (via raw connection to bypass ORM)
            if to_update:
                conn = await session.connection()
                await conn.execute(
                    update(DBRating)
                    .where(DBRating.id == bindparam("_id"))
                    .values(score=bindparam("_score")),
                    to_update,
                )
            # 1 bulk INSERT for new ratings
            if to_insert:
                await session.execute(insert(DBRating), to_insert)
            await session.commit()

        return saved, skipped

    @staticmethod
    async def get_product_ratings(catalog_id: str, product_id: str) -> dict[str, float]:
        """Get all ratings for a single product as user_id -> score mapping."""
        async with AsyncSessionLocal() as session:
            stmt = (
                select(DBRating.user_id, DBRating.score)
                .where(DBRating.catalog_id == catalog_id)
                .where(DBRating.product_id == product_id)
            )
            result = await session.execute(stmt)
            return {row[0]: float(row[1]) for row in result.all()}

    @staticmethod
    async def get_all_ratings(
        catalog_id: str,
    ) -> list[dict[str, str | float]]:
        """Get all ratings for a catalog as dicts with product_id, user_id, score."""
        async with AsyncSessionLocal() as session:
            stmt = select(DBRating.product_id, DBRating.user_id, DBRating.score).where(
                DBRating.catalog_id == catalog_id
            )
            result = await session.execute(stmt)
            return [
                {"product_id": product_id, "user_id": user_id, "score": float(score)}
                for product_id, user_id, score in result.all()
            ]

    @staticmethod
    async def get_ratings_by_product_ids(
        catalog_id: str, product_ids: list[str]
    ) -> dict[str, dict[str, float]]:
        """Get ratings for many products as product_id -> (user_id -> score)."""
        if len(product_ids) == 0:
            return {}

        ratings_by_product: dict[str, dict[str, float]] = {
            product_id: {} for product_id in product_ids
        }

        # Keep below SQLite parameter limits for IN clauses.
        chunk_size = 900

        async with AsyncSessionLocal() as session:
            for start in range(0, len(product_ids), chunk_size):
                chunk_ids = product_ids[start : start + chunk_size]
                stmt = (
                    select(DBRating.product_id, DBRating.user_id, DBRating.score)
                    .where(DBRating.catalog_id == catalog_id)
                    .where(DBRating.product_id.in_(chunk_ids))
                )
                result = await session.execute(stmt)

                for product_id, user_id, score in result.all():
                    ratings_by_product[product_id][user_id] = float(score)

        return ratings_by_product

    @staticmethod
    async def bulk_save_embeddings(
        catalog_id: str,
        embeddings: dict[str, list[float]],
    ) -> None:
        """Replace all product embeddings for a catalog.

        Args:
            catalog_id: The catalog these embeddings belong to.
            embeddings: Mapping of product_id -> embedding vector (list of floats).
        """
        from sqlalchemy import delete, insert

        async with AsyncSessionLocal() as session:
            await session.execute(
                delete(DBProductEmbedding).where(
                    DBProductEmbedding.catalog_id == catalog_id
                )
            )
            if embeddings:
                rows = [
                    {
                        "product_id": product_id,
                        "catalog_id": catalog_id,
                        "embedding": embedding,
                    }
                    for product_id, embedding in embeddings.items()
                ]
                await session.execute(insert(DBProductEmbedding), rows)
            await session.commit()

    @staticmethod
    async def get_all_embeddings(
        catalog_id: str,
    ) -> dict[str, list[float]]:
        """Get all product embeddings for a catalog as product_id -> embedding."""
        async with AsyncSessionLocal() as session:
            stmt = select(DBProductEmbedding).where(
                DBProductEmbedding.catalog_id == catalog_id
            )
            result = await session.execute(stmt)
            return {row.product_id: row.embedding for row in result.scalars().all()}

    @staticmethod
    async def mark_training_complete(catalog_id: str, trained_count: int) -> None:
        """After training, set trained_ratings to the count we actually trained on."""
        async with AsyncSessionLocal() as session:
            progress = await session.get(DBCatalogTrainingProgress, catalog_id)
            if progress is None:
                return
            progress.trained_ratings = trained_count
            progress.untrained_ratings = 0
            await session.commit()

    @staticmethod
    async def get_common_user_ratings(
        catalog_id: str, product_a_id: str, product_b_id: str
    ) -> tuple[list[float], list[float]]:
        """Get ratings from users who rated both products.

        Returns two vectors:
        - First vector: ratings for product_a by common users
        - Second vector: ratings for product_b by common users (same order)
        """
        # Get ratings for each product separately
        ratings_a = await CatalogRepository.get_product_ratings(
            catalog_id, product_a_id
        )
        ratings_b = await CatalogRepository.get_product_ratings(
            catalog_id, product_b_id
        )

        # Find common users and build vectors in consistent order
        common_users = sorted(set(ratings_a.keys()) & set(ratings_b.keys()))
        vec_a = [ratings_a[user] for user in common_users]
        vec_b = [ratings_b[user] for user in common_users]

        return vec_a, vec_b
