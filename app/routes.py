from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException

from app.dependencies import verify_instance_key
from app.repositories import InstanceRepository
from app.schemas import (
    BulkProductResult,
    BulkSkippedProduct,
    InstanceRegistration,
    InstanceWithKey,
    Product,
    ProductUpdate,
    Rating,
    RatingKey,
    Recommendation,
)
from app.tasks import process_catalog_registration
from app.database import Product as DBProduct

router = APIRouter()


def _db_product_to_schema(db_product: DBProduct) -> Product:
    return Product(
        productId=db_product.product_id,
        name=db_product.product_name,
        categories=db_product.categories,
    )


async def _get_catalog_id(instance_id: str) -> str:
    catalog_id = await InstanceRepository.get_catalog_id(instance_id)
    if catalog_id is None:
        raise HTTPException(status_code=404, detail="Instance has no catalog")
    return catalog_id


@router.post("/instances/register", response_model=InstanceWithKey, status_code=201)
async def register_instance() -> InstanceWithKey:
    """Register a new instance and return its id + secret key."""
    import uuid
    import secrets

    instance_id = str(uuid.uuid4())
    secret_key = secrets.token_urlsafe(64)

    await InstanceRepository.create(instance_id, secret_key)

    return InstanceWithKey(instanceId=instance_id, secretKey=secret_key)


@router.post(
    "/instances/{instanceId}/catalog",
    dependencies=[Depends(verify_instance_key)],
    status_code=200,
)
async def register_catalog(
    instanceId: str,
    body: InstanceRegistration,
    background_tasks: BackgroundTasks,
) -> None:
    """Register catalog and existing user ratings for an instance."""
    if await InstanceRepository.has_catalog(instanceId):
        raise HTTPException(
            status_code=409,
            detail="Catalog associated with instance already found, to use a new catalog, use a new instance",
        )

    import uuid

    catalog_id = str(uuid.uuid4())
    await InstanceRepository.assign_catalog(instanceId, catalog_id)
    await InstanceRepository.create_products(catalog_id, body.catalog)
    await InstanceRepository.create_ratings(catalog_id, body.ratings)

    # Enqueue for processing
    background_tasks.add_task(
        process_catalog_registration,
        instance_id=instanceId,
        catalog_id=catalog_id,
    )


@router.put(
    "/instances/{instanceId}/ratings",
    dependencies=[Depends(verify_instance_key)],
)
async def upsert_rating(
    instanceId: str,
    body: Rating,
) -> None:
    """Add or modify a user rating for a product."""
    # TODO: upsert rating
    ...


@router.delete(
    "/instances/{instanceId}/ratings",
    status_code=204,
    dependencies=[Depends(verify_instance_key)],
)
async def delete_rating(
    instanceId: str,
    body: RatingKey,
) -> None:
    """Delete a user rating for a product."""
    # TODO: delete rating
    ...


@router.post(
    "/instances/{instanceId}/products",
    response_model=Product,
    status_code=201,
    dependencies=[Depends(verify_instance_key)],
)
async def create_product(
    instanceId: str,
    body: Product,
) -> Product:
    """Create a new product."""
    catalog_id = await _get_catalog_id(instanceId)
    existing = await InstanceRepository.get_product(catalog_id, body.productId)
    if existing is not None:
        raise HTTPException(
            status_code=409, detail="A product with this ID already exists"
        )
    db_product = await InstanceRepository.create_product(catalog_id, body)
    return _db_product_to_schema(db_product)


@router.post(
    "/instances/{instanceId}/products/bulk",
    response_model=BulkProductResult,
    status_code=201,
    dependencies=[Depends(verify_instance_key)],
)
async def bulk_create_products(
    instanceId: str,
    body: list[Product],
) -> BulkProductResult:
    """Bulk-create products (max 2000, skip duplicates)."""
    if len(body) == 0 or len(body) > 2000:
        raise HTTPException(
            status_code=400,
            detail="Request must contain between 1 and 2000 products",
        )
    catalog_id = await _get_catalog_id(instanceId)
    created_schemas, skipped_tuples = await InstanceRepository.bulk_create_products(
        catalog_id, body
    )
    return BulkProductResult(
        created=created_schemas,
        skipped=[
            BulkSkippedProduct(productId=pid, reason=reason)
            for pid, reason in skipped_tuples
        ],
    )


@router.put(
    "/instances/{instanceId}/products/{productId}",
    response_model=Product,
    dependencies=[Depends(verify_instance_key)],
)
async def update_product(
    instanceId: str,
    productId: str,
    body: ProductUpdate,
) -> Product:
    """Update a product's name or categories."""
    catalog_id = await _get_catalog_id(instanceId)
    db_product = await InstanceRepository.update_product(catalog_id, productId, body)
    if db_product is None:
        raise HTTPException(status_code=404, detail="Instance or product not found")
    return _db_product_to_schema(db_product)


@router.delete(
    "/instances/{instanceId}/products/{productId}",
    status_code=204,
    dependencies=[Depends(verify_instance_key)],
)
async def delete_product(
    instanceId: str,
    productId: str,
) -> None:
    """Delete a product and gracefully remove its associated ratings."""
    catalog_id = await _get_catalog_id(instanceId)
    deleted = await InstanceRepository.delete_product(catalog_id, productId)
    if not deleted:
        raise HTTPException(status_code=404, detail="Instance or product not found")


@router.get(
    "/instances/{instanceId}/users/{userId}/recommendations",
    response_model=list[Recommendation],
    dependencies=[Depends(verify_instance_key)],
)
async def get_recommendations(
    instanceId: str,
    userId: str,
) -> list[Recommendation]:
    """Get recommendations for a user."""
    # TODO: run inference, return recommendations
    return []
