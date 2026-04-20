import asyncio

_retrain_queue: asyncio.Queue[str] = asyncio.Queue()
_queued_catalog_ids: set[str] = set()


async def enqueue_retrain_catalog(catalog_id: str) -> None:
    if catalog_id in _queued_catalog_ids:
        return

    _queued_catalog_ids.add(catalog_id)
    await _retrain_queue.put(catalog_id)


async def process_retrain_catalog(catalog_id: str) -> None:
    # TODO: implement model retraining for a catalog.
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
