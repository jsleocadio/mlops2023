import pytest
import pandas as pd
from movie_recommendation import MovieRecommender

def test_clean_title():
    title = "Toy Story (1995)"
    clean_title = MovieRecommender.clean_title(title)
    assert clean_title == "Toy Story 1995"

def test_search():
    movie_dataset_path = "data/movies.csv"
    ratings_dataset_path = "data/ratings.csv"
    movie_recommender = MovieRecommender(movie_dataset_path, ratings_dataset_path)
    results = movie_recommender.search("Toy Story (1995)")
    assert isinstance(results, pd.DataFrame)
    assert len(results) == 5

def test_find_similar_movies():
    movie_dataset_path = "data/movies.csv"
    ratings_dataset_path = "data/ratings.csv"
    movie_recommender = MovieRecommender(movie_dataset_path, ratings_dataset_path)
    results = movie_recommender.find_similar_movies(1)
    assert isinstance(results, pd.DataFrame)
    assert len(results) == 5