from fastapi import Header, HTTPException, Path

from app.repositories import InstanceRepository


async def verify_instance_key(
    instanceId: str = Path(),
    x_instance_key: str = Header(),
) -> None:
    """Validate that the instance exists and the provided key matches.

    Raises:
        HTTPException 404 – instance not found
        HTTPException 401 – key mismatch
    """
    instance = await InstanceRepository.get_by_id(instanceId)
    if instance is None:
        raise HTTPException(status_code=404, detail="Instance not found")
    if instance.secret_key != x_instance_key:
        raise HTTPException(status_code=401, detail="Secret key missing or mismatched")
