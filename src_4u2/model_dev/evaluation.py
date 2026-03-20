import numpy as np


def get_user_representation(embedding, event2embedding, event_hist):
    """
    Represent a user as the centroid of their attended events' embeddings.

    Args:
        embedding: 2D array of event embeddings, shape (n_events, embedding_dim)
        event2embedding: dict mapping event ID -> row index in embedding
        event_hist: list of event IDs the user has attended

    Returns:
        centroid (np.ndarray) of the user's events, or [] if none could be looked up
    """
    # naive method so far - compute centroid
    fail_count = 0
    points = []
    for eid in event_hist:
        try:
            point = embedding[[event2embedding[eid]]]
            points.append(point)
        except:
            fail_count += 1
    if fail_count == len(event_hist):
        return []
    else:
        centroid = np.mean(points, axis=0)
        return centroid


def generate_reclist(user_rep, embedding, all_event_ids):
    """
    Generate recommendation list by ranking all events by cosine similarity to a user representation.

    Args:
        user_rep: 1D array representing the user (e.g. centroid of attended events)
        embedding: 2D array of event embeddings, shape (n_events, embedding_dim)
        all_event_ids: list of event IDs whereby order corresponds to each row in embedding

    Returns:
        tuple of event IDs sorted from most to least similar to user_rep
    """
    cossim = [np.dot(user_rep, point) / (np.linalg.norm(user_rep) * np.linalg.norm(point)) for point in embedding]
    sorted_pairs = sorted(zip(cossim, all_event_ids), reverse=True)
    cossim_sorted, event_ids_sorted = zip(*sorted_pairs)
    return event_ids_sorted


def ndcg(rec_list, query_eid):
    """
    Compute NDCG for a single relevant item in a ranked recommendation list.

    Args:
        rec_list: ordered list of event IDs recommended (most to least relevant)
        query_eid: the event ID of the single ground truth (i.e. what the user engaged with) item being evaluated

    Returns:
        NDCG (float): normalised discounted cumulative gain (IDCG assumed to be 1.0)
        position (int): 0-based index of query_eid in rec_list
    """
    position = rec_list.index(query_eid)
    DCG  = 1 / np.log2(position+1)
    # IDCG = 1 / log2(1+1) = 1 / log2(2) = 1.0
    NDCG = DCG / 1.0
    return NDCG, position


def evaluate(user_rep, embedding, all_event_ids, query_eid):
    """
    Generate a recommendation list and score it with NDCG.

    Args:
        user_rep: 1D array representing the user
        embedding: 2D array of event embeddings, shape (n_events, embedding_dim)
        all_event_ids: list of event IDs corresponding to each row in embedding
        query_eid: the held-out event ID to evaluate against

    Returns:
        ndcg (float): NDCG score for query_eid in the ranked list
        position (int): 0-based position of query_eid in the ranked list
    """
    rec_list = generate_reclist(user_rep, embedding, all_event_ids)
    ndcg, position = ndcg(rec_list, query_eid)
    return ndcg, position
