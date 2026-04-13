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
