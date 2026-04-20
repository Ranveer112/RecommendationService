import torch
import math


def get_jaccard_score(categories_a: list[str], categories_b: list[str]) -> float:
    """Calculate Jaccard similarity between two category lists."""
    set_a = set(categories_a)
    set_b = set(categories_b)

    if not set_a and not set_b:
        return 1.0  # Both empty sets are identical

    if not set_a or not set_b:
        return 0.0  # One empty, one non-empty have no overlap

    intersection = set_a & set_b
    union = set_a | set_b

    return len(intersection) / len(union)


def cosine_similarity(vec_a: list[float], vec_b: list[float]) -> float:
    """Calculate cosine similarity between two vectors."""
    if len(vec_a) != len(vec_b):
        raise ValueError("Vectors must have the same length")

    if len(vec_a) == 0:
        # No common users -> no evidence of similarity
        return 0.0

    # Convert to torch tensors
    tensor_a = torch.tensor(vec_a, dtype=torch.float32)
    tensor_b = torch.tensor(vec_b, dtype=torch.float32)

    # Calculate cosine similarity
    cos_sim = torch.nn.functional.cosine_similarity(
        tensor_a.unsqueeze(0), tensor_b.unsqueeze(0), dim=1
    )

    return float(cos_sim.item())


def euclidean_distance(vec_a: list[float], vec_b: list[float]) -> float:
    """Calculate Euclidean distance between two vectors."""
    if len(vec_a) != len(vec_b):
        raise ValueError("Vectors must have the same length")

    if len(vec_a) == 0:
        return 0.0

    return math.sqrt(sum((a - b) ** 2 for a, b in zip(vec_a, vec_b)))


def pearson_correlation(vec_a: list[float], vec_b: list[float]) -> float:
    """Calculate Pearson correlation coefficient between two vectors.

    Returns a value in [-1, 1]. Returns 0.0 if either vector has zero variance
    (all identical values) or if vectors are empty.
    """
    if len(vec_a) != len(vec_b):
        raise ValueError("Vectors must have the same length")

    n = len(vec_a)
    if n == 0:
        return 0.0

    mean_a = sum(vec_a) / n
    mean_b = sum(vec_b) / n

    numerator = sum((a - mean_a) * (b - mean_b) for a, b in zip(vec_a, vec_b))
    denom_a = math.sqrt(sum((a - mean_a) ** 2 for a in vec_a))
    denom_b = math.sqrt(sum((b - mean_b) ** 2 for b in vec_b))

    if denom_a == 0 or denom_b == 0:
        return 0.0

    return numerator / (denom_a * denom_b)
