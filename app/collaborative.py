from app.database import Product as DBProduct
from app.repositories import CatalogRepository


def skip_collaborative_filtering(product_id: str, catalog_id: str) -> bool:
    """Stub function - always return True to skip collaborative filtering for now."""
    return False


async def get_vector(
    product_a: DBProduct, product_b: DBProduct
) -> tuple[list[float], list[float]]:
    """Get rating vectors for two products using only common users.

    Returns two vectors where:
    - First vector: ratings by common users for product_a
    - Second vector: ratings by common users for product_b
    """
    return await CatalogRepository.get_common_user_ratings(
        product_a.catalog_id, product_a.product_id, product_b.product_id
    )
