from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError
from app.database import (
    AsyncSessionLocal,
    Catalog,
    Product as DBProduct,
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
    async def create_ratings(catalog_id: str, ratings: list[Rating]) -> None:
        async with AsyncSessionLocal() as session:
            for rating in ratings:
                db_rating = DBRating(
                    product_id=rating.productId,
                    user_id=rating.userId,
                    score=str(rating.score),
                    catalog_id=catalog_id,
                )
                session.add(db_rating)
            await session.commit()

    @staticmethod
    async def bulk_upsert_ratings(
        catalog_id: str, ratings: list[Rating]
    ) -> tuple[list[Rating], list[tuple[str, str, str]]]:
        """Upsert ratings in bulk, skipping those that reference non-existent products.
        Returns (saved_ratings_as_schemas, skipped_as_tuples_of_userId_productId_reason).
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

            for rating in ratings:
                if rating.productId not in valid_product_ids:
                    skipped.append(
                        (rating.userId, rating.productId, "Product not found")
                    )
                    continue

                key = (rating.userId, rating.productId)
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
                    # Track in existing so later duplicates in the same batch update
                    existing_ratings[key] = None  # type: ignore[assignment]

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
