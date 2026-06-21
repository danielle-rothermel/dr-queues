from __future__ import annotations

import os
from threading import Lock

from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.uri_parser import parse_uri

from dr_queues.events.schema import PipelineEvent

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


class MongoEventSink:
    def __init__(
        self,
        *,
        url: str | None = None,
        collection_name: str = DEFAULT_COLLECTION,
        client: MongoClient | None = None,
    ) -> None:
        resolved_url = url or mongodb_url()
        self._owns_client = client is None
        self._client = client or MongoClient(resolved_url)
        database = self._client.get_database(_database_name(resolved_url))
        self._collection: Collection = database[collection_name]
        self._lock = Lock()
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        self._collection.create_index(
            [("run_id", ASCENDING), ("timestamp", ASCENDING)],
        )
        self._collection.create_index([("event_id", ASCENDING)], unique=True)

    def append(self, event: PipelineEvent) -> None:
        with self._lock:
            self._collection.insert_one(event.model_dump())

    def read_by_run_id(self, run_id: str) -> list[PipelineEvent]:
        cursor = self._collection.find({"run_id": run_id}).sort(
            "timestamp",
            ASCENDING,
        )
        return [PipelineEvent.model_validate(doc) for doc in cursor]

    def close(self) -> None:
        if self._owns_client:
            self._client.close()
