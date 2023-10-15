"""
movie_recommendation.py - recommend movies based on movie titles and genres.
Created by Vik Paruchuri
Adapted by Jefferson Leocadio
"""

import re
import logging
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import pandas as pd

logging.basicConfig(filename='movie_recommendation.log',
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')


class MovieRecommender:
    """Movie recommender based on movie titles and genres."""
    def __init__(self, movie_dataset_path: str, ratings_dataset_path: str) -> None:
        """
        Initialize the recommender.
        
        Args: movie_dataset_path (str): Path to the movie dataset, 
        ratings_dataset_path (str): Path to the ratings dataset.

        Returns: None
        """
        logging.info("Initializing the recommender.")

        self.movies = pd.read_csv(movie_dataset_path)

        logging.info("Loaded the movie dataset.")

        self.ratings = pd.read_csv(ratings_dataset_path)

        logging.info("Loaded the ratings dataset.")

        self.movies["clean_title"] = self.movies["title"].apply(self.clean_title)

        logging.info("Cleaned the movie titles.")

        self.vectorizer = TfidfVectorizer(ngram_range=(1,2))
        self.tfidf = self.vectorizer.fit_transform(self.movies["clean_title"])

    def clean_title(self, title: str) -> str:
        """
        Remove special characters from the title.
        
        Args: title (str): The title of the movie.

        Returns: str: The title of the movie without special characters.
        """
        title = re.sub("[^a-zA-Z0-9 ]", "", title)
        return title

    def search(self, title: str) -> pd.DataFrame():
        """
        Search for movies based on the title.

        Args: title (str): The title of the movie.

        Returns: pd.DataFrame(): The top 5 movies based on the title.
        """
        logging.info("Searching for movies based on the title: %s", title)

        title = self.clean_title(title)
        query_vec = self.vectorizer.transform([title])
        similarity = cosine_similarity(query_vec, self.tfidf).flatten()
        indices = np.argpartition(similarity, -5)[-5:]

        logging.info("Found %s movies.", len(indices))

        results = self.movies.iloc[indices].iloc[::-1]

        return results

    def find_similar_movies(self, movie_id: int) -> pd.DataFrame():
        """
        Find similar movies based on the movie id.
        
        Args: movie_id (int): The id of the movie.

        Returns: pd.DataFrame(): The top 5 similar movies.
        """
        logging.info("Finding similar movies based on the movie id: %s", movie_id)
        similar_users = self.ratings[(self.ratings["movieId"] == movie_id) &
                                     (self.ratings["rating"] > 4)]["userId"].unique()
        similar_user_recs = self.ratings[(self.ratings["userId"].isin(similar_users)) &
                                         (self.ratings["rating"] > 4)]["movieId"]
        similar_user_recs = similar_user_recs.value_counts() / len(similar_users)

        similar_user_recs = similar_user_recs[similar_user_recs > .10]
        all_users = self.ratings[(self.ratings["movieId"].isin(similar_user_recs.index)) &
                                 (self.ratings["rating"] > 4)]
        all_user_recs = all_users["movieId"].value_counts() / len(all_users["userId"].unique())
        rec_percentages = pd.concat([similar_user_recs, all_user_recs], axis=1)
        rec_percentages.columns = ["similar", "all"]

        logging.info("Found %s similar movies.", len(rec_percentages))

        rec_percentages["score"] = rec_percentages["similar"] / rec_percentages["all"]
        rec_percentages = rec_percentages.sort_values("score", ascending=False)

        logging.info("Sorted the movies based on the score.")

        return rec_percentages.head(5).merge(self.movies, left_index=True, right_on="movieId")[
            ["score", "title", "genres"]]

def main():
    """Main function."""
    logging.info("Starting the main function.")

    movie_recommender = MovieRecommender("./data/movies.csv", "./data/ratings.csv")

    logging.info("Initialized the movie recommender.")

    movie_input = input("Type the movie title: ")

    logging.info("Searching for movies.")

    movie_result = movie_recommender.search(movie_input)

    logging.info("Found the movies.")
    logging.info("Finding similar movies.")

    movie_id = movie_result.iloc[0]["movieId"]
    similar_movies = movie_recommender.find_similar_movies(movie_id)

    logging.info("Found similar movies.")

    print(similar_movies)

if __name__ == "__main__":
    main()
