from sqlalchemy import select, func
from app.database import (
    AsyncSessionLocal,
    Instance,
    Product as DBProduct,
    Rating as DBRating,
)
from app.schemas import Product, Rating


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
    async def get_total_ratings(catalog_id: str) -> int:
        async with AsyncSessionLocal() as session:
            stmt = (
                select(func.count())
                .select_from(DBRating)
                .where(DBRating.c.catalog_id == catalog_id)
            )
            result = await session.execute(stmt)
            return result.scalar()

    @staticmethod
    async def has_catalog(instance_id: str) -> bool:
        async with AsyncSessionLocal() as session:
            result = await session.get(Instance, instance_id)
            return result is not None and result.catalog_id is not None

    @staticmethod
    async def assign_catalog(instance_id: str, catalog_id: str) -> None:
        async with AsyncSessionLocal() as session:
            instance = await session.get(Instance, instance_id)
            instance.catalog_id = catalog_id
            await session.commit()

    @staticmethod
    async def create_products(catalog_id: str, products: list[Product]) -> None:
        async with AsyncSessionLocal() as session:
            for product in products:
                db_product = DBProduct(
                    product_id=product.productId,
                    product_name=product.name,
                    catalog_id=catalog_id,
                )
                session.add(db_product)
            await session.commit()

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
