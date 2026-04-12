from fastapi import APIRouter, Depends, HTTPException

from app.dependencies import verify_catalog_key
from app.repositories import CatalogRepository
from app.schemas import (
    BulkProductResult,
    BulkRatingResult,
    BulkSkippedProduct,
    BulkSkippedRating,
    CatalogWithKey,
    Product,
    ProductUpdate,
    Rating,
    Recommendation,
)
from app.database import Product as DBProduct

router = APIRouter()


def _db_product_to_schema(db_product: DBProduct) -> Product:
    return Product(
        productId=db_product.product_id,
        name=db_product.product_name,
        categories=db_product.categories,
    )


@router.post("/catalogs/register", response_model=CatalogWithKey, status_code=201)
async def register_catalog() -> CatalogWithKey:
    """Register a new catalog and return its id + secret key."""
    import uuid
    import secrets

    catalog_id = str(uuid.uuid4())
    secret_key = secrets.token_urlsafe(64)

    await CatalogRepository.create(catalog_id, secret_key)

    return CatalogWithKey(catalogId=catalog_id, secretKey=secret_key)


@router.put(
    "/catalogs/{catalogId}/ratings",
    dependencies=[Depends(verify_catalog_key)],
)
async def upsert_rating(
    catalogId: str,
    body: Rating,
) -> None:
    """Add or modify a user rating for a product."""
    saved = await CatalogRepository.upsert_rating(catalogId, body)
    if not saved:
        raise HTTPException(status_code=404, detail="Product not found")


@router.delete(
    "/catalogs/{catalogId}/ratings/{userId}/{productId}",
    status_code=204,
    dependencies=[Depends(verify_catalog_key)],
)
async def delete_rating(
    catalogId: str,
    userId: str,
    productId: str,
) -> None:
    """Delete a user rating for a product."""
    deleted = await CatalogRepository.delete_rating(catalogId, userId, productId)
    if not deleted:
        raise HTTPException(status_code=404, detail="Rating not found")


@router.put(
    "/catalogs/{catalogId}/ratings/bulk",
    response_model=BulkRatingResult,
    dependencies=[Depends(verify_catalog_key)],
)
async def bulk_upsert_ratings(
    catalogId: str,
    body: list[Rating],
) -> BulkRatingResult:
    """Bulk add or modify ratings (max 2000, skip invalid)."""
    if len(body) == 0 or len(body) > 2000:
        raise HTTPException(
            status_code=400,
            detail="Request must contain between 1 and 2000 ratings",
        )
    saved_schemas, skipped_tuples = await CatalogRepository.bulk_upsert_ratings(
        catalogId, body
    )
    return BulkRatingResult(
        saved=saved_schemas,
        skipped=[
            BulkSkippedRating(userId=uid, productId=pid, reason=reason)
            for uid, pid, reason in skipped_tuples
        ],
    )


@router.post(
    "/catalogs/{catalogId}/products",
    response_model=Product,
    status_code=201,
    dependencies=[Depends(verify_catalog_key)],
)
async def create_product(
    catalogId: str,
    body: Product,
) -> Product:
    """Create a new product."""
    db_product = await CatalogRepository.create_product(catalogId, body)
    if db_product is None:
        raise HTTPException(
            status_code=409, detail="A product with this ID already exists"
        )
    return _db_product_to_schema(db_product)


@router.post(
    "/catalogs/{catalogId}/products/bulk",
    response_model=BulkProductResult,
    status_code=201,
    dependencies=[Depends(verify_catalog_key)],
)
async def bulk_create_products(
    catalogId: str,
    body: list[Product],
) -> BulkProductResult:
    """Bulk-create products (max 2000, skip duplicates)."""
    if len(body) == 0 or len(body) > 2000:
        raise HTTPException(
            status_code=400,
            detail="Request must contain between 1 and 2000 products",
        )
    created_schemas, skipped_tuples = await CatalogRepository.bulk_create_products(
        catalogId, body
    )
    return BulkProductResult(
        created=created_schemas,
        skipped=[
            BulkSkippedProduct(productId=pid, reason=reason)
            for pid, reason in skipped_tuples
        ],
    )


@router.put(
    "/catalogs/{catalogId}/products/{productId}",
    response_model=Product,
    dependencies=[Depends(verify_catalog_key)],
)
async def update_product(
    catalogId: str,
    productId: str,
    body: ProductUpdate,
) -> Product:
    """Update a product's name or categories."""
    db_product = await CatalogRepository.update_product(catalogId, productId, body)
    if db_product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return _db_product_to_schema(db_product)


@router.delete(
    "/catalogs/{catalogId}/products/{productId}",
    status_code=204,
    dependencies=[Depends(verify_catalog_key)],
)
async def delete_product(
    catalogId: str,
    productId: str,
) -> None:
    """Delete a product and gracefully remove its associated ratings."""
    deleted = await CatalogRepository.delete_product(catalogId, productId)
    if not deleted:
        raise HTTPException(status_code=404, detail="Product not found")


@router.get(
    "/catalogs/{catalogId}/users/{userId}/recommendations",
    response_model=list[Recommendation],
    dependencies=[Depends(verify_catalog_key)],
)
async def get_recommendations(
    catalogId: str,
    userId: str,
) -> list[Recommendation]:
    """Get recommendations for a user."""
    # TODO: run inference, return recommendations
    return []
