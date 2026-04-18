from typing import Any

from sqlalchemy import JSON, String, ForeignKey, ForeignKeyConstraint, event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Catalog(Base):
    __tablename__ = "catalogs"

    catalog_id: Mapped[str] = mapped_column(String, primary_key=True)
    secret_key: Mapped[str] = mapped_column(String)


class Product(Base):
    __tablename__ = "products"

    product_id: Mapped[str] = mapped_column(String, primary_key=True)
    catalog_id: Mapped[str] = mapped_column(
        ForeignKey("catalogs.catalog_id", ondelete="CASCADE"), primary_key=True
    )
    product_name: Mapped[str] = mapped_column(String)
    categories: Mapped[list[str]] = mapped_column(JSON, default=list)


class Rating(Base):
    __tablename__ = "ratings"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    product_id: Mapped[str] = mapped_column(String)
    user_id: Mapped[str] = mapped_column(String)
    score: Mapped[str] = mapped_column(
        String
    )  # Using String to avoid float precision issues in SQLite; convert in app layer
    catalog_id: Mapped[str] = mapped_column(String)

    __table_args__ = (
        ForeignKeyConstraint(
            ["product_id", "catalog_id"],
            ["products.product_id", "products.catalog_id"],
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["catalog_id"],
            ["catalogs.catalog_id"],
            ondelete="CASCADE",
        ),
    )
    # TODO: add ordering / timestamp column if needed for training data sequencing


# SQLite async engine
engine = create_async_engine("sqlite+aiosqlite:///./recommendations.db", echo=False)


@event.listens_for(engine.sync_engine, "connect")
def _set_sqlite_pragma(dbapi_conn: Any, connection_record: Any) -> None:
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


AsyncSessionLocal = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
