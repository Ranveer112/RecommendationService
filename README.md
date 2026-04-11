# RecommendationService

A free recommendation service for your marketplace, built for devs by a dev.

**Problem statement:** You own/manage a marketplace and want recommendations in it. However, you do not want to hire an ML team to build it, and you want a solution that gets you 80% there with 10% of the effort.

---

## What It Does (v1)

All interaction is via a headless REST API. A single persona — the **API Consumer** — drives every operation.

### Story 1: Manage Products
Add, update, and remove products (each with a name and one or more categories) so the recommendation engine operates on an up-to-date catalog.
- Supports bulk operations with per-item error reporting on partial failure.

### Story 2: Submit Ratings
Submit user-product ratings so the system can learn user preferences.
- Upserts on duplicate user-product pairs.
- Supports bulk operations with per-item error reporting on partial failure.

### Story 3: Retrain on New Data
The model retrains automatically when new data is submitted.
- Newly trained model is promoted immediately on a best-effort basis (no quality gate).
- Retraining does not block the API.

### Story 4: Recommend Similar Products
Get a ranked list of similar products given a product ID.
- Returns an empty list (not an error) on cold start.

### Story 5: Recommend Products for a User
Get a ranked list of recommended products for a user.
- Falls back to popular products for new users with no ratings.

### Story 6: Evaluate Recommendation Quality
Retrieve quality metrics (e.g., RMSE, precision@k) for the current model.
- Metrics are computed against the held-out validation set each retrain cycle.

---

## Dependencies

- FastAPI
- Uvicorn
- Pydantic
- SQLAlchemy
- aiosqlite
- torch
