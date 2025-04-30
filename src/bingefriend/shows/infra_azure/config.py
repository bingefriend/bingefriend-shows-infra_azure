"""Configuration for infra_azure."""

from dotenv import load_dotenv

load_dotenv()

import os

SQLALCHEMY_CONNECTION_STRING = os.getenv("SQLALCHEMY_CONNECTION_STRING")

SHOWS_TABLE = os.getenv("SHOWS_TABLE", "dbo.shows")
SHOW_GENRES_TABLE = os.getenv("SHOW_GENRES_TABLE", "dbo.show_genre")
SEASONS_TABLE = os.getenv("SEASONS_TABLE", "dbo.easons")
EPISODES_TABLE = os.getenv("EPISODES_TABLE", "dbo.episodes")
