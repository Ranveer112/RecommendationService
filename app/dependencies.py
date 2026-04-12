import hmac

from fastapi import Header, HTTPException, Path

from app.repositories import CatalogRepository


async def verify_catalog_key(
    catalogId: str = Path(),
    x_catalog_key: str = Header(),
) -> None:
    """Validate that the catalog exists and the provided key matches.

    Raises:
        HTTPException 404 – catalog not found
        HTTPException 401 – key mismatch
    """
    catalog = await CatalogRepository.get_by_id(catalogId)
    if catalog is None:
        raise HTTPException(status_code=404, detail="Catalog not found")
    if not hmac.compare_digest(catalog.secret_key, x_catalog_key):
        raise HTTPException(status_code=401, detail="Secret key missing or mismatched")
