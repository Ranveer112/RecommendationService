from app.repositories import InstanceRepository
import torch
from torch.utils.data import Dataset, DataLoader


class CatalogData(Dataset):
    def __init__(self, catalog_id: int, total_ratings: int):
        self.catalog_id = catalog_id
        self.total_ratings = total_ratings

    def __len__(self) -> int:
        return self.total_ratings

    async def __getitem__(self, idx: int) -> tuple[list[str], float]:
        rating = await InstanceRepository.get_rating_by_order(self.catalog_id, idx)
        return [rating.product_id, rating.user_id], float(rating.score)


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

    user_latent_factors = Embedding(total_users, 5)

    product_latent_factors = Embedding(total_products, 5)
    loss = torch.nn.MSELoss()

    training_data = CatalogData(catalog_id)
    training_dataloader = DataLoader(training_data, batch_size=64, shuffle=True)
    learning_rate = 0.1
    for epoch in range(n_epoch):
        for idx, (batch_products_and_users, batch_ratings) in enumerate(
            training_dataloader
        ):
            users = user_latent_factors(batch_products_and_users[:, 1])
            products = product_latent_factors(batch_products_and_users[:, 0])
            predictions = (users * products).sum(1)
            mse = loss(predictions, batch_ratings)
            mse.backward()
            with torch.no_grad():
                user_latent_factors.weight -= (
                    learning_rate * user_latent_factors.weight.grad
                )
                product_latent_factors.weight -= (
                    learning_rate * product_latent_factors.weight.grad
                )
            user_latent_factors.weight.grad.zero_()
            product_latent_factors.weight.grad.zero_()
