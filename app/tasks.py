import asyncio

from app.repositories import InstanceRepository
import torch
from torch.utils.data import Dataset, DataLoader


class CatalogData(Dataset[tuple[list[str], float]]):
    def __init__(self, catalog_id: str, total_ratings: int):
        self.catalog_id = catalog_id
        self.total_ratings = total_ratings

    def __len__(self) -> int:
        return self.total_ratings

    def __getitem__(self, idx: int) -> tuple[list[str], float]:
        rating = asyncio.get_event_loop().run_until_complete(
            InstanceRepository.get_rating_by_order(self.catalog_id, idx)
        )
        if rating is None:
            raise IndexError(
                f"Rating not found for catalog {self.catalog_id} at index {idx}"
            )
        return [rating.product_id, rating.user_id], float(rating.score)


async def process_catalog_registration(
    instance_id: str,
    catalog_id: str,
) -> None:
    """Background task to process catalog registration (ML training, etc.)."""

    # List of product_id, user_id, score

    # Step 1 - get distinct product_ids and user_ids

    total_users = await InstanceRepository.get_total_users(catalog_id)
    total_products = await InstanceRepository.get_total_products(catalog_id)

    user_latent_factors = torch.nn.Embedding(total_users, 5)

    product_latent_factors = torch.nn.Embedding(total_products, 5)
    loss = torch.nn.MSELoss()

    training_data = CatalogData(catalog_id, total_users * total_products)
    training_dataloader = DataLoader(training_data, batch_size=64, shuffle=True)
    learning_rate = 0.1
    n_epoch = 10
    for epoch in range(n_epoch):
        for idx, (batch_products_and_users, batch_ratings) in enumerate(
            training_dataloader
        ):
            users = user_latent_factors(batch_products_and_users[:, 1])
            products = product_latent_factors(batch_products_and_users[:, 0])
            predictions = (users * products).sum(1)
            mse = loss(predictions, batch_ratings)
            mse.backward()
            assert user_latent_factors.weight.grad is not None
            assert product_latent_factors.weight.grad is not None
            with torch.no_grad():
                user_latent_factors.weight -= (
                    learning_rate * user_latent_factors.weight.grad
                )
                product_latent_factors.weight -= (
                    learning_rate * product_latent_factors.weight.grad
                )
            assert user_latent_factors.weight.grad is not None
            assert product_latent_factors.weight.grad is not None
            user_latent_factors.weight.grad.zero_()
            product_latent_factors.weight.grad.zero_()
