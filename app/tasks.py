from app.repositories import InstanceRepository
import torch


def get_latent_factor_per_entry(entries: int) -> int:
    return max(5, entries / 10)


def process_catalog_registration(
    instance_id: str,
    catalog_id: str,
) -> None:
    """Background task to process catalog registration (ML training, etc.)."""

    # fetch the ratings for this catalog.
    ratings = InstanceRepository.get_ratings(catalog_id)

    # List of product_id, user_id, score

    # Step 1 - get distinct product_ids and user_ids

    total_users = len(set(rating[1] for rating in ratings))
    total_products = len(set(rating[0] for rating in ratings))

    user_latent_factors = torch.randn(
        get_latent_factor_per_entry(total_users), total_users
    )
    product_latent_factors = torch.randn(
        total_products, get_latent_factor_per_entry(total_products)
    )

    # loop over the catalog in batch sizes
    # for each batch use the latent factors for these (user, product) pairs
    # compute the modelled ratings
    # diff with expected ratings
    # tweak the weights
