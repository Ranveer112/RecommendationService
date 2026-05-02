import asyncio
from app.repositories import CatalogRepository
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader, TensorDataset
import torch

_retrain_queue: asyncio.Queue[str] = asyncio.Queue()
_queued_catalog_ids: set[str] = set()


async def enqueue_retrain_catalog(catalog_id: str) -> None:
    if catalog_id in _queued_catalog_ids:
        return

    _queued_catalog_ids.add(catalog_id)
    await _retrain_queue.put(catalog_id)


async def process_retrain_catalog(catalog_id: str) -> list[float]:
    # TODO: implement model retraining for a catalog.

    # Fecth all ratings from the db associated with the catalog
    ratings = await CatalogRepository.get_all_ratings(catalog_id)

    LATENT_FACTORS = 15

    user_id_to_index = {}
    product_id_to_index = {}
    rating_ids = []
    user_indices = []
    product_indices = []
    scores = []
    for rating in ratings:
        if rating["user_id"] not in user_id_to_index:
            user_id_to_index[rating["user_id"]] = len(user_id_to_index)
        if rating["product_id"] not in product_id_to_index:
            product_id_to_index[rating["product_id"]] = len(product_id_to_index)
        user_indices.append(user_id_to_index[rating["user_id"]])
        product_indices.append(product_id_to_index[rating["product_id"]])
        scores.append(rating["score"])

    get_user_embedding = nn.Embedding(len(user_id_to_index), LATENT_FACTORS)

    get_product_embedding = nn.Embedding(len(product_id_to_index), LATENT_FACTORS)

    # Note we use nn.Embedding for convinence but this is not an embedding
    get_user_bias = nn.Embedding(len(user_id_to_index), 1)
    get_product_bias = nn.Embedding(len(product_id_to_index), 1)

    dataset = TensorDataset(
        torch.tensor(user_indices, dtype=torch.long),
        torch.tensor(product_indices, dtype=torch.long),
        torch.tensor(scores, dtype=torch.float32),
    )
    BATCH_SIZE = 256

    dataloader = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=True)

    EPOCHS = 30
    optimizer = torch.optim.SGD(
        list(get_user_embedding.parameters())
        + list(get_product_embedding.parameters())
        + list(get_user_bias.parameters())
        + list(get_product_bias.parameters()),
        lr=0.01,
    )

    epoch_losses: list[float] = []

    for epoch in range(EPOCHS):
        epoch_loss = 0.0
        num_batches = 0
        for user_batch, product_batch, score_batch in dataloader:
            # Get user embeddings
            batch_len = user_batch.shape[0]
            assert user_batch.shape == torch.Size([batch_len])
            assert product_batch.shape == torch.Size([batch_len])
            assert score_batch.shape == torch.Size([batch_len])

            user_embeddings = get_user_embedding(user_batch)
            product_embeddings = get_product_embedding(product_batch)
            assert (
                user_embeddings.shape == product_embeddings.shape
                and user_embeddings.shape == torch.Size([batch_len, LATENT_FACTORS])
            )
            user_biases = get_user_bias(user_batch)
            product_biases = get_product_bias(product_batch)
            assert (
                product_biases.shape == user_biases.shape
                and user_biases.shape == torch.Size([batch_len, 1])
            )
            # Compute rating
            # What we want is a tensor of torch.Size([batch_len])
            predicted_ratings = (
                user_biases.squeeze(dim=1)
                + product_biases.squeeze(dim=1)
                + (user_embeddings * product_embeddings).sum(dim=1)
            )

            # Compute loss
            loss = nn.MSELoss()(predicted_ratings, score_batch)
            loss.backward()
            # Training loop
            optimizer.step()
            optimizer.zero_grad()

            epoch_loss += loss.item()
            num_batches += 1

        epoch_losses.append(epoch_loss / num_batches)

    # Extract learned product embeddings and save to DB
    index_to_product_id = {idx: pid for pid, idx in product_id_to_index.items()}
    learned_embeddings = get_product_embedding.weight.detach()
    embeddings_to_save = {
        index_to_product_id[idx]: learned_embeddings[idx].tolist()
        for idx in range(len(product_id_to_index))
    }
    await CatalogRepository.bulk_save_embeddings(catalog_id, embeddings_to_save)
    await CatalogRepository.mark_training_complete(catalog_id, len(ratings))

    return epoch_losses


async def retrain_worker() -> None:
    while True:
        catalog_id = await _retrain_queue.get()
        try:
            await process_retrain_catalog(catalog_id)
        finally:
            _queued_catalog_ids.discard(catalog_id)
            _retrain_queue.task_done()


def start_retrain_worker() -> asyncio.Task[None]:
    return asyncio.create_task(retrain_worker())
