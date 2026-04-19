from dataclasses import dataclass
from fastapi import APIRouter, Depends, HTTPException

from app.dependencies import verify_catalog_key
from app.repositories import CatalogRepository
from app.utils import get_jaccard_score, euclidean_distance
from app.collaborative import skip_collaborative_filtering
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


@dataclass
class SimilarityResult:
    product_id: str
    score: float


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
    "/catalogs/{catalogId}/products/{productId}/similar",
    response_model=list[Recommendation],
    dependencies=[Depends(verify_catalog_key)],
)
async def get_similar_products(
    catalogId: str,
    productId: str,
    limit: int = 10,
) -> list[Recommendation]:
    """Get similar products for a given product."""
    if limit < 1:
        raise HTTPException(
            status_code=400,
            detail="limit must be at least 1",
        )

    # Validate product exists
    product = await CatalogRepository.get_product(catalogId, productId)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")

    all_other_products = await CatalogRepository.get_all_products(
        catalog_id=catalogId, exclude_product_id=productId
    )

    if skip_collaborative_filtering(productId, catalogId):

        similarity_results: list[SimilarityResult] = []
        for other_product in all_other_products:
            score = get_jaccard_score(product.categories, other_product.categories)
            similarity_results.append(
                SimilarityResult(product_id=other_product.product_id, score=score)
            )

    else:
        similarity_results: list[SimilarityResult] = []
        all_product_ids = [productId] + [
            other_product.product_id for other_product in all_other_products
        ]
        ratings_by_product = await CatalogRepository.get_ratings_by_product_ids(
            catalogId, all_product_ids
        )
        product_ratings = ratings_by_product.get(productId, {})

        for other_product in all_other_products:
            other_product_ratings = ratings_by_product.get(other_product.product_id, {})
            common_users = sorted(
                set(product_ratings.keys()) & set(other_product_ratings.keys())
            )
            product_vec = [product_ratings[user_id] for user_id in common_users]
            other_product_vec = [
                other_product_ratings[user_id] for user_id in common_users
            ]
            score = (
                0
                if len(other_product_vec) < 7
                else 1.0 / (1.0 + euclidean_distance(other_product_vec, product_vec))
            )

            similarity_results.append(
                SimilarityResult(product_id=other_product.product_id, score=score)
            )

    # sort by score descending
    similarity_results.sort(key=lambda result: result.score, reverse=True)

    # take top limit results and convert to Recommendation objects
    top_results = similarity_results[:limit]
    return [
        Recommendation(productId=result.product_id, score=result.score)
        for result in top_results
    ]


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
