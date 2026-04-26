import asyncio
from app.repositories import CatalogRepository
import torch.nn as nn

_retrain_queue: asyncio.Queue[str] = asyncio.Queue()
_queued_catalog_ids: set[str] = set()


async def enqueue_retrain_catalog(catalog_id: str) -> None:
    if catalog_id in _queued_catalog_ids:
        return

    _queued_catalog_ids.add(catalog_id)
    await _retrain_queue.put(catalog_id)


async def process_retrain_catalog(catalog_id: str) -> None:
    # TODO: implement model retraining for a catalog.

    # Fecth all ratings from the db associated with the catalog
    ratings = await CatalogRepository.get_all_ratings(catalog_id)

    LATENT_FACTORS = 15

    user_id_to_index = {}
    product_id_to_index = {}
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

    user_embedding = nn.Embedding(len(user_id_to_index), LATENT_FACTORS)

    product_embedding = nn.Embedding(len(product_id_to_index), LATENT_FACTORS)

    await asyncio.sleep(0)


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
