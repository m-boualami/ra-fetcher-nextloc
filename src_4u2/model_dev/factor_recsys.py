import pandas as pd
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.decomposition import TruncatedSVD


def one_hot_encode_df(event_df, mlb=None):
    """
    One-hot encode the 'genres' column of an event DataFrame using MultiLabelBinarizer.

    If no encoder is provided, fits a new one on the data. If one is provided (e.g. fitted
    on training data), transforms using its known classes — unseen genres are ignored and
    missing ones are filled with 0s.

    Args:
        event_df: DataFrame with a 'genres' column containing lists of genre strings
        mlb: fitted MultiLabelBinarizer to use for transform; if None, fits a new one

    Returns:
        genre_matrix: DataFrame of binary genre columns, indexed like event_df
        mlb: the fitted MultiLabelBinarizer (either passed in or newly fitted)
    """
    if mlb is None:
        mlb = MultiLabelBinarizer()
        genre_matrix = pd.DataFrame(
            mlb.fit_transform(event_df['genres']),
            columns=mlb.classes_,
            index=event_df.index
        )
    else:
        # uses training classes, fills 0s for unseen genres, ignores new ones
        genre_matrix = pd.DataFrame(
            mlb.transform(event_df['genres']),
            columns=mlb.classes_,
            index=event_df.index
        )
    return genre_matrix, mlb


def svd_genres(event_df, n_dim=10):
    """
    Fit a genre embedding on an event DataFrame using one-hot encoding and TruncatedSVD.

    Encodes the 'genres' column into a binary matrix, then reduces it to a lower-dimensional
    dense embedding via SVD.

    Args:
        event_df: DataFrame with a 'genres' column containing lists of genre strings
        n_dim: number of SVD components (embedding dimensions) to produce

    Returns:
        event_embedding_df: DataFrame of shape (n_events, n_dim), indexed like event_df
        svd: fitted TruncatedSVD model (can be used to embed new events)
        onehot_encoder: fitted MultiLabelBinarizer (needed to consistently encode new events)
    """
    genre_encoding, onehot_encoder = one_hot_encode_df(event_df)
    svd = TruncatedSVD(n_components=n_dim)
    event_embedding = svd.fit_transform(genre_encoding)
    event_embedding_df = pd.DataFrame(event_embedding, index=event_df.index)
    return event_embedding_df, svd, onehot_encoder


def embed_new_events(event_df, model, binary_encoder):
    """
    Project new events into an existing SVD embedding space.

    Uses a pre-fitted encoder and SVD model (from svd_genres) to embed unseen events
    consistently with the training embedding.

    Args:
        event_df: DataFrame with a 'genres' column containing lists of genre strings
        model: fitted TruncatedSVD model from svd_genres
        binary_encoder: fitted MultiLabelBinarizer from svd_genres

    Returns:
        event_embedding_df: DataFrame of shape (n_events, n_dim), indexed like event_df
    """
    genre_encoding, _ = one_hot_encode_df(event_df, mlb=binary_encoder)
    new_embedding = model.transform(genre_encoding)
    event_embedding_df = pd.DataFrame(new_embedding, index=event_df.index)
    return event_embedding_df
