"""
podcast.py - Generate a Data Pipeline using Airflow to download Podcasts
Created by Vik Paruchuri
Adapted by Jefferson Leocadio
"""

import json
import logging
import os
import requests
import xmltodict

import pendulum
from airflow.decorators import dag, task
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from vosk import Model, KaldiRecognizer
from pydub import AudioSegment

logging.basicConfig(
    filename='podcast.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

PODCAST_URL = "https://www.marketplace.org/feed/podcast/marketplace/"
EPISODE_FOLDER = "episodes"
FRAME_RATE = 16000

@dag(
    dag_id='podcast_summary',
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023, 10, 15),
    catchup=False,
)
def podcast_dag():
    """
    Create Table on DB
    """
    create_database = SqliteOperator(
        task_id='create_table_sqlite',
        sql=r"""
        CREATE TABLE IF NOT EXISTS episodes (
            link TEXT PRIMARY KEY,
            title TEXT,
            filename TEXT,
            published TEXT,
            description TEXT,
            transcript TEXT
        );
        """,
        sqlite_conn_id="podcasts"
    )

    @task()
    def fetch_episodes_from_feed() -> list:
        """
        Task to obtain data from the podcast feed.
        Makes an HTTP request to the podcast feed URL,
        parses the XML data, and returns the list of episodes.

        Returns: list: List of episodes obtained from the podcast feed.
        """
        try:
            data = requests.get(PODCAST_URL, timeout=600)
            feed = xmltodict.parse(data.text)
            episodes = feed["rss"]["channel"]["item"]
            logging.info("Found %s episodes.", len(episodes))
            return episodes
        except requests.Timeout:
            logging.error("Timeout error while fetching episodes from the feed.")
            raise
        except Exception as e:
            logging.error("Error in fetch_episodes_from_feed: %s", str(e))
            raise

    @task()
    def filter_and_load_episodes(episodes: list) -> list:
        """
        Task to filter and load new episodes into the database.
        Compares the obtained episodes with those already stored in
        the SQLite database and adds new episodes.

        Args: episodes (list): List of episodes obtained from the podcast feed.

        Returns: list: List of new episodes.
        """
        try:
            hook = SqliteHook(sqlite_conn_id="podcasts")
            stored_episodes = hook.get_pandas_df("SELECT * from episodes;")
            new_episodes = []

            for episode in episodes:
                if episode["link"] not in stored_episodes["link"].values:
                    filename = f"{episode['link'].split('/')[-1]}.mp3"
                    new_episodes.append([episode["link"], episode["title"],
                                         episode["pubDate"], episode["description"], filename])

            hook.insert_rows(table='episodes', rows=new_episodes, target_fields=[
                             "link", "title", "published", "description", "filename"])
            return new_episodes
        except Exception as e:
            logging.error("Error in filter_and_load_episodes: %s", str(e))
            raise

    @task()
    def download_audio_episode(episode: list) -> dict:
        """
        Task to download an audio episode.
        Downloads the audio file of the episode,
        saves it locally and returns information about the episode.

        Args: episode (list): List containing information about the episode.

        Returns: dict: Dictionary containing information about the episode.
        """
        try:
            link = episode["link"]
            name_end = link.split('/')[-1]
            filename = f"{name_end}.mp3"
            audio_path = os.path.join(EPISODE_FOLDER, filename)

            if not os.path.exists(audio_path):
                logging.info("Downloading %s", filename)
                audio = requests.get(episode["enclosure"]["@url"], timeout=600)
                with open(audio_path, "wb+") as f:
                    f.write(audio.content)

            return {"link": link, "filename": filename}
        except requests.Timeout:
            logging.error("Timeout error while downloading the audio episode.")
            raise
        except Exception as e:
            logging.error("Error in download_audio_episode: %s", str(e))
            raise

    @task()
    def transcribe_audio_episode(episode: list) -> dict:
        """
        Task to transcribe an audio episode.
        Uses a speech recognition model to transcribe
        the audio of the episode into text.

        Args: episode (list): List containing information about the episode.

        Returns: dict: Dictionary containing information about the episode.
        """
        try:
            link = episode["link"]
            filename = episode["filename"]
            filepath = os.path.join(EPISODE_FOLDER, filename)

            model = Model(model_name="vosk-model-en-us-0.22-lgraph")
            rec = KaldiRecognizer(model, FRAME_RATE)
            rec.SetWords(True)

            mp3 = AudioSegment.from_mp3(filepath)
            mp3 = mp3.set_channels(1)
            mp3 = mp3.set_frame_rate(FRAME_RATE)

            step = 20000
            transcript = ""

            for i in range(0, len(mp3), step):
                logging.info("Progress: %s", i/len(mp3))
                segment = mp3[i:i+step]
                rec.AcceptWaveform(segment.raw_data)
                result = rec.Result()
                text = json.loads(result)["text"]
                transcript += text

            return {"link": link, "transcript": transcript}
        except Exception as e:
            logging.error("Error in transcribe_audio_episode: %s", str(e))
            raise

    episodes = fetch_episodes_from_feed()
    new_episodes = filter_and_load_episodes(episodes)
    downloaded_episodes = [download_audio_episode(
        episode) for episode in new_episodes]
    transcribed_episodes = [transcribe_audio_episode(
        episode) for episode in downloaded_episodes]

    create_database >> episodes >> new_episodes >> downloaded_episodes >> transcribed_episodes

DAG_INSTANCE = podcast_dag()
