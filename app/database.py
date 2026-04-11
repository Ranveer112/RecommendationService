from typing import Any, Optional

from sqlalchemy import Index, JSON, String, ForeignKey, event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Instance(Base):
    __tablename__ = "instances"

    instance_id: Mapped[str] = mapped_column(String, primary_key=True)
    secret_key: Mapped[str] = mapped_column(String)
    catalog_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)


class Product(Base):
    __tablename__ = "products"

    product_id: Mapped[str] = mapped_column(String, primary_key=True)
    product_name: Mapped[str] = mapped_column(String)
    catalog_id: Mapped[str] = mapped_column(String)
    categories: Mapped[list[str]] = mapped_column(JSON, default=list)


class Rating(Base):
    __tablename__ = "ratings"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    product_id: Mapped[str] = mapped_column(
        ForeignKey("products.product_id", ondelete="CASCADE")
    )
    user_id: Mapped[str] = mapped_column(String)
    score: Mapped[str] = mapped_column(
        String
    )  # Using String to avoid float precision issues in SQLite; convert in app layer
    catalog_id: Mapped[str] = mapped_column(String)
    insertion_order: Mapped[int] = mapped_column()

    __table_args__ = (
        Index("idx_catalog_insertion_order", "catalog_id", "insertion_order"),
    )


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
