from __future__ import annotations

import os

from pymongo.uri_parser import parse_uri

DEFAULT_MONGODB_URL = "mongodb://localhost:27017/dr_queues"
DEFAULT_COLLECTION = "pipeline_events"


def mongodb_url() -> str:
    return os.environ.get("MONGODB_URL", DEFAULT_MONGODB_URL)


def _database_name(url: str) -> str:
    parsed = parse_uri(url)
    database = parsed.get("database")
    if database:
        return database
    return "dr_queues"
