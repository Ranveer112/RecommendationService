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
