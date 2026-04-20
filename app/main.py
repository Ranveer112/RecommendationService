from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database import init_db
from app.routes import router
from app.tasks import start_retrain_worker


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    retrain_worker_task = start_retrain_worker()
    try:
        yield
    finally:
        retrain_worker_task.cancel()


app = FastAPI(title="Recommendation Service", version="0.1.0", lifespan=lifespan)
app.include_router(router)
