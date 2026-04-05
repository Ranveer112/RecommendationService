from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Path

from app.dependencies import verify_instance_key
from app.repositories import InstanceRepository
from app.schemas import (
    InstanceRegistration,
    InstanceWithKey,
    Rating,
    RatingKey,
    Recommendation,
)
from app.tasks import process_catalog_registration

router = APIRouter()


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
