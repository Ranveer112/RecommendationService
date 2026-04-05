from sqlalchemy import Column, String, create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Instance(Base):
    __tablename__ = "instances"

    instance_id = Column(String, primary_key=True)
    secret_key = Column(String, nullable=False)
    catalog_id = Column(String, nullable=True)


class Product(Base):
    __tablename__ = "products"

    product_id = Column(String, primary_key=True)
    product_name = Column(String, nullable=False)
    catalog_id = Column(String, nullable=False)


class Rating(Base):
    __tablename__ = "ratings"

    product_id = Column(String, nullable=False)
    user_id = Column(String, nullable=False)
    score = Column(
        String, nullable=False
    )  # Using String to avoid float precision issues in SQLite; convert in app layer
    catalog_id = Column(String, nullable=False)


# SQLite async engine
engine = create_async_engine("sqlite+aiosqlite:///./recommendations.db", echo=False)
AsyncSessionLocal = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
