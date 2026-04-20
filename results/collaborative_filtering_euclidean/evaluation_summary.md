# Collaborative Filtering Evaluation (Euclidean)

## Approach
- Collaborative filtering with closeness computed via Euclidean distance between common user-rating vectors for two products.

### Baseline run
- Command: `python scripts/evaluate_movielens.py --seed 42 --max-validation-users 50 --max-anchors-per-user 50`
- Max validation users: `50`
- Max anchors per user: `50`
- Average overlap count per query: `0.181`
- Average overlap rate per query: `0.0362`
- Average overlap rate per user: `0.0330`

## Notes
- This result is from the current evaluation pipeline run for product-similarity recommendations.

## Genre based filtering

### Run 1
- Command: `python scripts/evaluate_movielens.py --seed 10`
- User split: `train=488`, `validation=122`
- Ratings split: `train=79822`, `validation=21014`
- Evaluated users: `10 / 10`
- Evaluated anchor queries: `100`
- Top-K: `5`
- Max validation users: `10`
- Max anchors per user: `10`
- Average overlap count per query: `0.190`
- Average overlap rate per query: `0.0380`
- Average overlap rate per user: `0.0380`

### Run 2
- Command: `python scripts/evaluate_movielens.py --seed 10 --max-validation-users 20`
- User split: `train=488`, `validation=122`
- Ratings split: `train=79822`, `validation=21014`
- Evaluated users: `20 / 20`
- Evaluated anchor queries: `200`
- Top-K: `5`
- Max validation users: `20`
- Max anchors per user: `10`
- Average overlap count per query: `0.155`
- Average overlap rate per query: `0.0310`
- Average overlap rate per user: `0.0310`

## Pearson Correlation

### Run 1
- Command: `python scripts/evaluate_movielens.py --seed 10 --max-validation-users 20`
- User split: `train=488`, `validation=122`
- Ratings split: `train=79822`, `validation=21014`
- Evaluated users: `20 / 20`
- Evaluated anchor queries: `200`
- Top-K: `5`
- Max validation users: `20`
- Max anchors per user: `10`
- Average overlap count per query: `0.070`
- Average overlap rate per query: `0.0140`
- Average overlap rate per user: `0.0140`
