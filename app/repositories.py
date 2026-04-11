from sqlalchemy import select, func
from app.database import (
    AsyncSessionLocal,
    Instance,
    Product as DBProduct,
    Rating as DBRating,
)
from app.schemas import Product, ProductUpdate, Rating


class InstanceRepository:
    @staticmethod
    async def create(instance_id: str, secret_key: str) -> None:
        async with AsyncSessionLocal() as session:
            instance = Instance(instance_id=instance_id, secret_key=secret_key)
            session.add(instance)
            await session.commit()

    @staticmethod
    async def get_by_id(instance_id: str) -> Instance | None:
        async with AsyncSessionLocal() as session:
            result = await session.get(Instance, instance_id)
            return result

    @staticmethod
    async def get_rating_by_order(catalog_id: str, insertion_order: int):
        async with AsyncSessionLocal() as session:
            stmt = select(DBRating).where(
                DBRating.catalog_id == catalog_id,
                DBRating.insertion_order == insertion_order,
            )
            result = await session.execute(stmt)
            return result.scalars().first()

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
    async def has_catalog(instance_id: str) -> bool:
        async with AsyncSessionLocal() as session:
            result = await session.get(Instance, instance_id)
            if result is None:
                return False
            return result.catalog_id is not None

    @staticmethod
    async def assign_catalog(instance_id: str, catalog_id: str) -> None:
        async with AsyncSessionLocal() as session:
            instance = await session.get(Instance, instance_id)
            if instance is None:
                raise ValueError(f"Instance {instance_id} not found")
            instance.catalog_id = catalog_id
            await session.commit()

    @staticmethod
    async def get_catalog_id(instance_id: str) -> str | None:
        async with AsyncSessionLocal() as session:
            instance = await session.get(Instance, instance_id)
            if instance is None:
                return None
            return instance.catalog_id

    @staticmethod
    async def create_products(catalog_id: str, products: list[Product]) -> None:
        async with AsyncSessionLocal() as session:
            for product in products:
                db_product = DBProduct(
                    product_id=product.productId,
                    product_name=product.name,
                    catalog_id=catalog_id,
                    categories=product.categories,
                )
                session.add(db_product)
            await session.commit()

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
    async def create_product(catalog_id: str, product: Product) -> DBProduct:
        async with AsyncSessionLocal() as session:
            db_product = DBProduct(
                product_id=product.productId,
                product_name=product.name,
                catalog_id=catalog_id,
                categories=product.categories,
            )
            session.add(db_product)
            await session.commit()
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
    async def create_ratings(catalog_id: str, ratings: list[Rating]) -> None:
        async with AsyncSessionLocal() as session:
            for idx, rating in enumerate(ratings):
                db_rating = DBRating(
                    product_id=rating.productId,
                    user_id=rating.userId,
                    score=str(rating.score),
                    catalog_id=catalog_id,
                    insertion_order=idx,
                )
                session.add(db_rating)
            await session.commit()
